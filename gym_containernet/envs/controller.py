import network_observator
import path_calculator

from ryu.base import app_manager
from ryu.controller import ofp_event
from ryu.controller.handler import CONFIG_DISPATCHER, MAIN_DISPATCHER
from ryu.controller.handler import set_ev_cls
from ryu.ofproto import ofproto_v1_3
from ryu.topology import event
from ryu.topology.api import get_switch
from ryu.controller.controller import Datapath
from ryu.lib import hub
from ryu.lib.packet import packet, ethernet, ether_types

from collections import defaultdict
from datetime import datetime
import os
from typing import Any, Dict, List, Tuple, Union


UPDATE_PERIOD = 10  # seconds
BW_VARIANCE = 5000  # kbits

# Custom types
EndpointPair = Tuple[int, int]
SwitchPortPair = Tuple[int, int]
MacPair = Tuple[str, str]
Match = Dict[Any, Union[int, str]]
Path = List[Tuple[int, int, int]]


def host_discovery_from_topology_file(file: str) -> (Dict[str, SwitchPortPair], Dict[EndpointPair, int], Dict[EndpointPair, float]):
    switch_ports: Dict[int, int] = {}
    host_switch_port: Dict[str, SwitchPortPair] = {}
    adjacency: Dict[EndpointPair, int] = defaultdict(lambda: None)
    link_bw: Dict[EndpointPair, float] = {}

    with open(file, 'r') as topology:
        for line in topology.readlines()[2:]:
            atts: List[str] = line.split()
            if atts[0][0] != 'S':  # host connects to
                hexadecimal: str = '{0:012x}'.format(len(host_switch_port) + 1)
                host_mac: str = ':'.join(hexadecimal[i:i + 2] for i in range(0, 12, 2))
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
                    adjacency[s1_idx, s2_idx] = switch_ports[s1_idx]
                    adjacency[s2_idx, s1_idx] = switch_ports[s2_idx]
                    link_bw[s1_idx, s2_idx] = float(atts[2])
                    link_bw[s2_idx, s1_idx] = float(atts[2])
                else:  # a host
                    hexadecimal: str = '{0:012x}'.format(len(host_switch_port) + 1)
                    host_mac: str = ':'.join(hexadecimal[i:i + 2] for i in range(0, 12, 2))
                    switch_ports[s1_idx] = switch_ports[s1_idx] + 1 if switch_ports.get(s1_idx) else 1
                    host_switch_port[host_mac] = (s1_idx, switch_ports[s1_idx])
    return host_switch_port, adjacency, link_bw


def request_stats(datapath: Datapath) -> None:
    ofproto = datapath.ofproto
    parser = datapath.ofproto_parser
    req = parser.OFPPortStatsRequest(datapath, 0, ofproto.OFPP_ANY)
    datapath.send_msg(req)


def install_path(src: str, dst: str, path: Path, switch_datapath: Dict[int, Datapath],
                 switch_flows: Dict[int, List[Match]]) -> Dict[int, List[Match]]:
    datapath: Datapath = switch_datapath[1]  # any datapath will do as long as all the switches use the same protocol and version
    ofproto = datapath.ofproto
    parser = datapath.ofproto_parser
    for switch, in_port, out_port in path:
        datapath: Datapath = switch_datapath[switch]
        match: Match = dict(in_port=in_port, eth_src=src, eth_dst=dst)
        actions: List = [parser.OFPActionOutput(out_port)]
        if match not in switch_flows[switch]:
            switch_flows[switch].append(match)
            inst = [parser.OFPInstructionActions(ofproto.OFPIT_APPLY_ACTIONS, actions)]
            mod = parser.OFPFlowMod(datapath=datapath, priority=1, match=parser.OFPMatch(**match), instructions=inst,
                                    idle_timeout=0, hard_timeout=0)
            datapath.send_msg(mod)
    return switch_flows


class Controller(app_manager.RyuApp):
    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]

    def __init__(self, *args: Tuple, **kwargs: Dict[str, Any]) -> None:
        os.system("clear")
        super(Controller, self).__init__(*args, **kwargs)

        self.host_switch_port: Dict[str, SwitchPortPair]
        self.adjacency: Dict[EndpointPair, int]
        self.bw: Dict[EndpointPair, float]
        self.host_switch_port, self.adjacency, self.bw = host_discovery_from_topology_file("topology.txt")

        self.switch_datapath: Dict[int, Datapath] = {}

        self.prev_available_bw: Dict[EndpointPair, float] = defaultdict(lambda: 0.0)
        self.available_bw: Dict[EndpointPair, float] = defaultdict(lambda: 0.0)
        self.used_bw: Dict[EndpointPair, float] = defaultdict(lambda: 0.0)
        self.tx_bytes: Dict[EndpointPair, int] = defaultdict(lambda: 0)
        self.clock: Dict[EndpointPair, float] = defaultdict(lambda: 0.0)

        self.updated: List[int] = []  # which switches already calculated the available bandwidth in its links

        self.paths: Dict[MacPair, List[Path]] = {}
        self.best_path: Dict[MacPair, Path] = {}
        self.switch_flows: Dict[int, List[Match]] = defaultdict(lambda: [])

        self.topology_api_app = self
        self.monitor_thread = hub.spawn(self.monitor)

    def monitor(self) -> None:
        while True:
            print(datetime.now().strftime("\n\n%H:%M:%S\n"))
            for datapath in self.switch_datapath.values():
                request_stats(datapath)
            hub.sleep(UPDATE_PERIOD)

    @set_ev_cls([event.EventSwitchEnter, event.EventSwitchLeave, event.EventPortAdd, event.EventPortDelete, event.EventPortModify,
                 event.EventLinkAdd, event.EventLinkDelete])
    def get_topology_data(self, ev) -> None:
        for switch in get_switch(self.topology_api_app, None):  # get all switches and corresponding datapaths
            self.switch_datapath[switch.dp.id] = switch.dp

    @set_ev_cls(ofp_event.EventOFPSwitchFeatures, CONFIG_DISPATCHER)
    def switch_features_handler(self, ev) -> None:  # create table-miss entries
        datapath = ev.msg.datapath
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        match = parser.OFPMatch()
        actions = [parser.OFPActionOutput(ofproto.OFPP_CONTROLLER, ofproto.OFPCML_NO_BUFFER)]
        inst = [parser.OFPInstructionActions(ofproto.OFPIT_APPLY_ACTIONS, actions)]
        mod = datapath.ofproto_parser.OFPFlowMod(datapath=datapath, match=match, cookie=0, command=ofproto.OFPFC_ADD,
                                                 idle_timeout=0, hard_timeout=0, priority=0, instructions=inst)
        datapath.send_msg(mod)

    @set_ev_cls(ofp_event.EventOFPPortStatsReply, MAIN_DISPATCHER)
    def port_stats_reply_handler(self, ev) -> None:
        msg = ev.msg
        datapath = ev.msg.datapath
        dpid = datapath.id
        self.used_bw, self.available_bw, self.tx_bytes, self.clock = \
            network_observator.update_bw(msg, list(self.switch_datapath.keys()), self.adjacency, self.bw, self.available_bw,
                                         self.used_bw, self.tx_bytes, self.clock)
        for dst in self.switch_datapath.keys():
            if self.adjacency[dpid, dst]:
                print("%s -> %s: %.3f Kbps" % (dpid, dst, self.available_bw[dpid, dst]))
        print()

        self.updated.append(dpid)
        if len(set(self.updated)) == len(self.switch_datapath.keys()):  # if all switches have updated their bw
            self.updated = []  # reset it for next path calculation
            for (s1, s2), bw in self.available_bw.items():
                if abs(bw - self.prev_available_bw.get((s1, s2), 0.0)) > 5000:  # only recalculate paths when BW changes > 5000
                    self.paths, self.best_path = path_calculator.possible_and_best_paths(self.host_switch_port, self.adjacency, self.available_bw)
                    for src in self.host_switch_port.keys():
                        for dst in self.host_switch_port.keys():
                            if src != dst:
                                self.switch_flows = install_path(src, dst, self.best_path[src, dst], self.switch_datapath, self.switch_flows)
                                for path in self.paths[src, dst]:
                                    print("%s -> %s: %s    %s" % (src, dst, path, "IN USE" if path == self.best_path[src, dst] else ""))
                            print()
                    self.prev_available_bw = self.available_bw
                    break
                else:
                    self.prev_available_bw[s1, s2] = self.available_bw[s1, s2]

    @set_ev_cls(ofp_event.EventOFPPacketIn, MAIN_DISPATCHER)
    def packet_in_handler(self, ev) -> None:
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

        out_port: int = ofproto.OFPP_FLOOD
        if dst in self.host_switch_port.keys():
            path = self.best_path.get((src, dst), None)
            if path:
                for s, p1, p2 in path:
                    if s == dpid:
                        out_port = p2

        data = None
        if msg.buffer_id == ofproto.OFP_NO_BUFFER:
            data = msg.data
        if out_port == ofproto.OFPP_FLOOD:
            actions = []
            for i in range(1, 23):
                actions.append(parser.OFPActionOutput(i))
        else:
            actions = [parser.OFPActionOutput(out_port)]
        out = parser.OFPPacketOut(datapath=datapath, buffer_id=msg.buffer_id, in_port=in_port, actions=actions, data=data)
        datapath.send_msg(out)
