from ryu.base import app_manager
from ryu.controller import ofp_event
from ryu.controller.handler import CONFIG_DISPATCHER, MAIN_DISPATCHER, DEAD_DISPATCHER
from ryu.controller.handler import set_ev_cls
from ryu.ofproto import ofproto_v1_3
from ryu.lib.packet import packet
from ryu.lib.packet import ethernet
from ryu.topology import event
from ryu.topology.api import get_switch, get_link
from ryu.controller.controller import Datapath

from collections import defaultdict
from ryu.lib import hub
from operator import attrgetter
import time
from typing import Any, Dict, List, Tuple


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


class Controller(app_manager.RyuApp):
    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]

    def __init__(self, *args: Tuple, **kwargs: Dict[str, Any]) -> None:
        super(Controller, self).__init__(*args, **kwargs)
        self.datapaths: Dict[int, Datapath] = {}
        self.switches: List[int] = []
        self.mac_to_switch_port: Dict[str, Tuple[int, int]] = {}  # mac->(switch, port)
        self.adjacency: Dict[Tuple[int, int], int] = defaultdict(lambda: None)  # (s1, s2)->port from s1 to s2
        self.topology_api_app = self
        self.monitor_thread = hub.spawn(self._monitor)

        self.link_bw: Dict[Tuple[int, int], float] = defaultdict(lambda: 0.0)
        self.link_available_bw: Dict[Tuple[int, int], float] = defaultdict(lambda: 0.0)
        self.link_used_bw: Dict[Tuple[int, int], float] = defaultdict(lambda: 0.0)
        self.link_last_tx_bytes: Dict[Tuple[int, int], int] = defaultdict(lambda: 0.0)
        self.link_last_clock: Dict[Tuple[int, int], float] = defaultdict(lambda: 0.0)

        with open('bw.txt', 'r') as bw_file:
            for line in bw_file.readlines()[2:]:
                split: List[str] = line.split()
                self.link_bw[int(split[0]), int(split[1])] = float(split[2])
                self.link_bw[int(split[1]), int(split[0])] = float(split[2])

    def _monitor(self):
        while True:
            for datapath in self.datapaths.values():
                self._request_stats(datapath)
            hub.sleep(5)

    def _request_stats(self, datapath):
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser

        req = parser.OFPPortStatsRequest(datapath, 0, ofproto.OFPP_ANY)
        datapath.send_msg(req)

    @set_ev_cls(ofp_event.EventOFPSwitchFeatures, CONFIG_DISPATCHER)
    def _switch_features_handler(self, ev):  # adds table-miss flow entries
        datapath = ev.msg.datapath
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser

        match = parser.OFPMatch()  # match all packets
        actions = [parser.OFPActionOutput(ofproto.OFPP_CONTROLLER, ofproto.OFPCML_NO_BUFFER)]  # transfer to controller
        inst = [parser.OFPInstructionActions(ofproto.OFPIT_APPLY_ACTIONS, actions)]
        mod = datapath.ofproto_parser.OFPFlowMod(datapath=datapath, match=match, command=ofproto.OFPFC_ADD,
                                                 priority=0, instructions=inst)
        datapath.send_msg(mod)

    @set_ev_cls([event.EventSwitchEnter, event.EventSwitchLeave, event.EventPortAdd, event.EventPortDelete,
                 event.EventPortModify, event.EventLinkAdd, event.EventLinkDelete])
    def _get_topology_data(self, ev):
        switch_list = get_switch(self.topology_api_app, None)
        for switch in switch_list:
            self.datapaths[switch.dp.id] = switch.dp
        self.switches = [switch.dp.id for switch in switch_list]
        links_list = get_link(self.topology_api_app, None)
        links = [(link.src.dpid, link.dst.dpid, link.src.port_no, link.dst.port_no) for link in links_list]
        for s1, s2, port1, port2 in links:
            self.adjacency[s1, s2] = port1
            self.adjacency[s2, s1] = port2

    @set_ev_cls(ofp_event.EventOFPStateChange, [MAIN_DISPATCHER, DEAD_DISPATCHER])
    def _state_change_handler(self, ev):
        datapath = ev.datapath
        if ev.state == MAIN_DISPATCHER:
            if not datapath.id in self.datapaths:
                self.datapaths[datapath.id] = datapath
        elif ev.state == DEAD_DISPATCHER:
            if datapath.id in self.datapaths:
                del self.datapaths[datapath.id]

    @set_ev_cls(ofp_event.EventOFPPortStatsReply, MAIN_DISPATCHER)
    def _port_stats_reply_handler(self, ev):
        body = ev.msg.body 
        dpid = ev.msg.datapath.id
        for stat in sorted(body, key=attrgetter('port_no')):
            for switch in self.switches:
                if self.adjacency[dpid, switch] == stat.port_no:
                    if self.link_last_tx_bytes[dpid, switch] > 0:
                        self.link_used_bw[dpid, switch] = (stat.tx_bytes - self.link_last_tx_bytes[dpid, switch]) * 8.0 / (time.time() - self.link_last_clock[dpid, switch]) / 1000
                        self.link_available_bw[dpid, switch] = int(self.link_bw[dpid, switch]) * 1024.0 - self.link_used_bw[dpid, switch]
                        # print("Available bandwidth %s -> %s = %s kbps" % (dpid, s2, self.link_available_bw[dpid, s2]))
                    self.link_last_tx_bytes[dpid, switch] = stat.tx_bytes
                    self.link_last_clock[dpid, switch] = time.time()

    def install_path(self, p, ev, src_mac, dst_mac):
        msg = ev.msg
        datapath = msg.datapath
        dpid = datapath.id
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        for sw, in_port, out_port in p:
            print("%s: %s -> %s via %s in_port = %s, out_port = %s" % (dpid, src_mac, dst_mac, sw, in_port, out_port))
            match = parser.OFPMatch(in_port=in_port, eth_src=src_mac, eth_dst=dst_mac)
            actions = [parser.OFPActionOutput(out_port)]
            datapath = self.datapaths[sw]
            inst = [parser.OFPInstructionActions(ofproto.OFPIT_APPLY_ACTIONS, actions)]
            mod = datapath.ofproto_parser.OFPFlowMod(
                datapath=datapath, match=match, idle_timeout=0, hard_timeout=0,
                priority=1, instructions=inst)
            datapath.send_msg(mod)

    @set_ev_cls(ofp_event.EventOFPPacketIn, MAIN_DISPATCHER)
    def _packet_in_handler(self, ev):
        msg = ev.msg
        datapath = msg.datapath
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser

        dpid = datapath.id
        pkt = packet.Packet(msg.data)
        eth = pkt.get_protocol(ethernet.ethernet)
        src = eth.src
        dst = eth.dst
        in_port = msg.match['in_port']

        if eth.ethertype == 35020:  # LLDP
            return

        if src not in self.mac_to_switch_port.keys():
            self.mac_to_switch_port[src] = (dpid, in_port)
            print("%s: %s" % (dpid, self.mac_to_switch_port))

        if dst in self.mac_to_switch_port.keys():
            start_time: time = time.time()
            p = get_path(self.mac_to_switch_port[src][0], self.mac_to_switch_port[dst][0],
                          self.mac_to_switch_port[src][1], self.mac_to_switch_port[dst][1],
                          self.link_available_bw, self.switches, self.adjacency)
            self.install_path(p, ev, src, dst)
            out_port = p[0][2]
        else:
            out_port = ofproto.OFPP_FLOOD

        actions = [parser.OFPActionOutput(out_port)]
        if out_port != ofproto.OFPP_FLOOD:  # install a flow to avoid packet_in next time
            match = parser.OFPMatch(in_port=in_port, eth_src=src, eth_dst=dst)

        data = None
        if msg.buffer_id == ofproto.OFP_NO_BUFFER:
            data = msg.data

        if out_port == ofproto.OFPP_FLOOD:
            while len(actions) > 0:
                actions.pop()
            for i in range(1, 23):
                actions.append(parser.OFPActionOutput(i))
            out = parser.OFPPacketOut(datapath=datapath, buffer_id=msg.buffer_id,
                                      in_port=in_port, actions=actions, data=data)
            datapath.send_msg(out)
        else:
            out = parser.OFPPacketOut(
                datapath=datapath, buffer_id=msg.buffer_id, in_port=in_port,
                actions=actions, data=data)
            datapath.send_msg(out)
