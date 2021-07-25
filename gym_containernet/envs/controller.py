from ryu.base import app_manager
from ryu.controller import ofp_event
from ryu.controller.handler import CONFIG_DISPATCHER, MAIN_DISPATCHER
from ryu.controller.handler import set_ev_cls
from ryu.ofproto import ofproto_v1_3
from ryu.topology import event
from ryu.topology.api import get_switch, get_link
from ryu.controller.controller import Datapath
from ryu.lib import hub
from ryu.lib.packet import packet, ethernet, ether_types

from collections import defaultdict
from operator import attrgetter
import time
from typing import Any, Dict, List, Tuple


def host_discovery_from_topology_file(file: str) -> Dict[str, Tuple[int, int]]:
    switch_ports: Dict[int, int] = {}
    host_switch_port: Dict[str, Tuple[int, int]] = {}
    link_bw: Dict[Tuple[int, int], float] = {}

    with open(file, 'r') as topology:
        for line in topology.readlines()[2:]:
            atts: List[str] = line.split()
            if atts[0][0] != 'S':  # host connects to
                hexadecimal: str = '{0:012x}'.format(len(host_switch_port) + 1)
                host_mac: str = ':'.join(hexadecimal[i:i+2] for i in range(0, 12, 2))
                if atts[1][0] == 'S':  # a switch
                    idx: int = int(atts[1][1:])
                    switch_ports[idx] = switch_ports[idx] + 1 if switch_ports.get(idx) else 1
                    host_switch_port[host_mac] = (idx, switch_ports[idx])

            else:  # switch connects to
                s1_idx: int = int(atts[0][1:])
                if atts[1][0] == 'S':  # another switch
                    s2_idx: int = int(atts[1][1:])
                    # necessary to match mininet ports
                    switch_ports[s1_idx] = switch_ports[s1_idx] + 1 if switch_ports.get(s1_idx) else 1
                    switch_ports[s2_idx] = switch_ports[s2_idx] + 1 if switch_ports.get(s2_idx) else 1
                    link_bw[(s1_idx, s2_idx)] = float(atts[2])
                    link_bw[(s2_idx, s1_idx)] = float(atts[2])
                else:  # a host
                    hexadecimal: str = '{0:012x}'.format(len(host_switch_port) + 1)
                    host_mac: str = ':'.join(hexadecimal[i:i + 2] for i in range(0, 12, 2))
                    switch_ports[s1_idx] = switch_ports[s1_idx] + 1 if switch_ports.get(s1_idx) else 1
                    host_switch_port[host_mac] = (s1_idx, switch_ports[s1_idx])

    return host_switch_port, link_bw


def max_available_bw(available_bw, switches):
    maximum = float('-Inf')
    node = 0
    for s in switches:
        if available_bw[s] > maximum:
            maximum = available_bw[s]
            node = s
    return node


def get_path(src, dst, first_port, final_port, link_available_bw, switches, adjacency):
    available_bw = {}
    previous = {}
    for s in switches:
        available_bw[s] = float('-Inf')
        previous[s] = None
    available_bw[src] = float('Inf')

    Q = set(switches)
    while len(Q) > 0:
        u = max_available_bw(available_bw, Q)
        Q.remove(u)

        for p in switches:
            if adjacency[u, p]:
                link_abw = link_available_bw[u, p]
                if available_bw[u] < link_abw:
                    tmp = available_bw[u]
                else:
                    tmp = link_abw

                if available_bw[p] > tmp:
                    alt = available_bw[p]
                else:
                    alt = tmp

                if alt > available_bw[p]:
                    available_bw[p] = alt
                    previous[p] = u

    r = []
    p = dst
    r.append(p)
    q = previous[p]
    while q:
        if q == src:
            r.append(q)
            break
        p = q
        r.append(p)
        q = previous[p]
    r.reverse()
    if src == dst:
        path = [src]
    else:
        path = r

    r = []
    in_port = first_port
    for s1, s2 in zip(path[:-1], path[1:]):
        out_port = adjacency[s1, s2]
        r.append((s1, in_port, out_port))
        in_port = adjacency[s2, s1]
    r.append((dst, in_port, final_port))

    return r


def add_flow(datapath, priority, match, actions, switch_flows: List):
    ofproto = datapath.ofproto
    parser = datapath.ofproto_parser

    if not switch_flows or match not in switch_flows:
        switch_flows.append(match)
        inst = [parser.OFPInstructionActions(ofproto.OFPIT_APPLY_ACTIONS, actions)]
        mod = parser.OFPFlowMod(datapath=datapath, priority=priority,
                                match=parser.OFPMatch(**match), instructions=inst,
                                idle_timeout=0, hard_timeout=0)
        datapath.send_msg(mod)

    return switch_flows


def request_stats(datapath):
    ofproto = datapath.ofproto
    parser = datapath.ofproto_parser
    req = parser.OFPPortStatsRequest(datapath, 0, ofproto.OFPP_ANY)
    datapath.send_msg(req)


class Controller(app_manager.RyuApp):
    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]

    def __init__(self, *args: Tuple, **kwargs: Dict[str, Any]) -> None:
        super(Controller, self).__init__(*args, **kwargs)

        self.host_switch_port: Dict[str, Tuple[int, int]]
        self.link_bw: Dict[Tuple[int, int], float]
        self.host_switch_port, self.link_bw = host_discovery_from_topology_file("topology.txt")

        self.switch_datapath: Dict[int, Datapath] = {}
        self.adjacency: Dict[Tuple[int, int], int] = defaultdict(lambda: None)
        self.switch_flows: Dict[int, List] = defaultdict(lambda: None)
        self.paths: Dict[Tuple[str, str], List[Tuple[int, int, int]]] = {}

        self.link_available_bw: Dict[Tuple[int, int], float] = defaultdict(lambda: 0.0)
        self.link_used_bw: Dict[Tuple[int, int], float] = defaultdict(lambda: 0.0)
        self.link_last_tx_bytes: Dict[Tuple[int, int], int] = defaultdict(lambda: 0.0)
        self.link_last_clock: Dict[Tuple[int, int], float] = defaultdict(lambda: 0.0)

        self.topology_api_app = self
        self.monitor_thread = hub.spawn(self._monitor)

    def _monitor(self) -> None:
        while True:
            for datapath in self.switch_datapath.values():
                request_stats(datapath)
            hub.sleep(5)

    def install_path(self, path: List[Tuple[int, int, int]], ev, src: str, dst: str) -> None:
        parser = ev.msg.datapath.ofproto_parser
        self.paths[src, dst] = path
        for switch, in_port, out_port in path:
            datapath = self.switch_datapath[switch]
            match = dict(in_port=in_port, eth_src=src, eth_dst=dst)
            actions = [parser.OFPActionOutput(out_port)]
            self.switch_flows[switch] = add_flow(
                datapath, 1, match, actions, self.switch_flows.get(switch, []))

    def install_all_switch_paths(self, dpid, ev):
        for src_mac, (src_switch, src_port) in self.host_switch_port.items():
            if src_switch == dpid:  # only compute paths for current switch
                for dst_mac, (dst_switch, dst_port) in self.host_switch_port.items():
                    if src_mac != dst_mac:
                        path = get_path(src_switch, dst_switch, src_port, dst_port, self.link_available_bw,
                                        list(self.switch_datapath.keys()), self.adjacency)
                        self.install_path(path, ev, src_mac, dst_mac)

    @set_ev_cls(ofp_event.EventOFPSwitchFeatures, CONFIG_DISPATCHER)
    def switch_features_handler(self, ev):
        datapath = ev.msg.datapath
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        match = parser.OFPMatch()
        actions = [parser.OFPActionOutput(ofproto.OFPP_CONTROLLER, ofproto.OFPCML_NO_BUFFER)]
        inst = [parser.OFPInstructionActions(ofproto.OFPIT_APPLY_ACTIONS, actions)]
        mod = datapath.ofproto_parser.OFPFlowMod(
            datapath=datapath, match=match, cookie=0,
            command=ofproto.OFPFC_ADD, idle_timeout=0, hard_timeout=0,
            priority=0, instructions=inst)
        datapath.send_msg(mod)

    @set_ev_cls([event.EventSwitchEnter, event.EventSwitchLeave, event.EventPortAdd, event.EventPortDelete,
                 event.EventPortModify, event.EventLinkAdd, event.EventLinkDelete])
    def _get_topology_data(self, ev):
        for switch in get_switch(self.topology_api_app, None):
            self.switch_datapath[switch.dp.id] = switch.dp
        links = [(link.src.dpid, link.dst.dpid, link.src.port_no, link.dst.port_no)
                 for link in get_link(self.topology_api_app, None)
                 if link.src.dpid != link.dst.dpid]
        for s1, s2, port1, port2 in links:
            self.adjacency[s1, s2] = port1
            self.adjacency[s2, s1] = port2

    @set_ev_cls(ofp_event.EventOFPPortStatsReply, MAIN_DISPATCHER)
    def _port_stats_reply_handler(self, ev):
        body = ev.msg.body
        dpid = ev.msg.datapath.id
        for stat in sorted(body, key=attrgetter('port_no')):
            for switch in self.switch_datapath.keys():
                if self.adjacency[dpid, switch] == stat.port_no:
                    if self.link_last_tx_bytes[dpid, switch] > 0:
                        self.link_used_bw[dpid, switch] = (stat.tx_bytes - self.link_last_tx_bytes[
                            dpid, switch]) * 8.0 / (time.time() - self.link_last_clock[dpid, switch]) / 1000
                        self.link_available_bw[dpid, switch] = int(self.link_bw[dpid, switch]) * 1024.0 - \
                                                               self.link_used_bw[dpid, switch]
                        # print("Available bandwidth %s -> %s = %.3f kbps" % (dpid, switch, self.link_available_bw[dpid, switch]))
                    self.link_last_tx_bytes[dpid, switch] = stat.tx_bytes
                    self.link_last_clock[dpid, switch] = time.time()
        self.install_all_switch_paths(dpid, ev)

    @set_ev_cls(ofp_event.EventOFPPacketIn, MAIN_DISPATCHER)
    def _packet_in_handler(self, ev):
        msg = ev.msg
        datapath = msg.datapath
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        in_port = msg.match['in_port']

        pkt = packet.Packet(msg.data)
        eth = pkt.get_protocol(ethernet.ethernet)
        if eth.ethertype == ether_types.ETH_TYPE_LLDP:
            return

        src = eth.src
        dst = eth.dst
        dpid = datapath.id

        if dst in self.host_switch_port.keys():
            path = self.paths.get((src, dst), None)
            if path:
                for s, p1, p2 in path:
                    if s == dpid:
                        out_port = p2
        else:
            out_port = ofproto.OFPP_FLOOD

        data = None
        if msg.buffer_id == ofproto.OFP_NO_BUFFER:
            data = msg.data

        if out_port == ofproto.OFPP_FLOOD:
            actions = []
            for i in range(1, 23):
                actions.append(parser.OFPActionOutput(i))
        else:
            actions = [parser.OFPActionOutput(out_port)]

        out = parser.OFPPacketOut(
            datapath=datapath, buffer_id=msg.buffer_id, in_port=in_port, actions=actions, data=data)
        datapath.send_msg(out)
