import path_calculator

from ryu.base import app_manager
from ryu.controller import ofp_event
from ryu.controller.handler import CONFIG_DISPATCHER, MAIN_DISPATCHER
from ryu.controller.handler import set_ev_cls
from ryu.ofproto import ofproto_v1_3
from ryu.controller.controller import Datapath
from ryu.lib import hub
from ryu.lib.packet import packet, arp

from collections import defaultdict
from datetime import datetime
from operator import attrgetter
import os
import socket
import time
from typing import Any, DefaultDict, Dict, List, Tuple, Union


TOPOLOGY_FILE = "topology.txt"
UPDATE_PERIOD = 5   # seconds
BW_VARIANCE = 2000  # kbits

# Custom types
EndpointPair = Tuple[int, int]
SwitchPortPair = Tuple[int, int]
MacPair = Tuple[str, str]
Match = Dict[Any, Union[int, str]]
Path = List[Tuple[int, int, int]]


def load_topology(file: str) -> (Dict[str, str], Dict[str, SwitchPortPair], Dict[EndpointPair, int], Dict[EndpointPair, float]):
    ip_mac: Dict[str, str] = {}
    switch_ports: Dict[int, int] = {}
    host_switch_port: Dict[str, SwitchPortPair] = {}
    adjacency: DefaultDict[EndpointPair, int] = defaultdict(lambda: 0)
    link_bw: Dict[EndpointPair, float] = {}

    with open(file, 'r') as topology:
        for line in topology.readlines()[2:]:
            atts: List[str] = line.split()
            if atts[0][0] != 'S':  # host connects to
                host_ip: str = '10.0.0.%s' % (len(host_switch_port) + 1)
                hexadecimal: str = '{0:012x}'.format(len(host_switch_port) + 1)
                host_mac: str = ':'.join(hexadecimal[i:i + 2] for i in range(0, 12, 2))
                ip_mac[host_ip] = host_mac
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
    return ip_mac, host_switch_port, adjacency, link_bw


def request_stats(datapath: Datapath) -> None:
    ofproto = datapath.ofproto
    parser = datapath.ofproto_parser
    req = parser.OFPPortStatsRequest(datapath, 0, ofproto.OFPP_ANY)
    datapath.send_msg(req)


def install_path(src: str, dst: str, path: Path, switch_datapath: Dict[int, Datapath]) -> None:
    for switch, in_port, out_port in path:
        datapath = switch_datapath[switch]
        proto = datapath.ofproto
        parser = datapath.ofproto_parser
        match = parser.OFPMatch(in_port=in_port, eth_src=src, eth_dst=dst)
        actions = [parser.OFPActionOutput(out_port)]
        inst = [parser.OFPInstructionActions(proto.OFPIT_APPLY_ACTIONS, actions)]
        mod = parser.OFPFlowMod(datapath=datapath, priority=1, match=match, instructions=inst, idle_timeout=0, hard_timeout=0)
        datapath.send_msg(mod)
    print(f"Added path {src} -> {dst}: {path}")


def uninstall_path(src: str, dst: str, path: Path, switch_datapath: Dict[int, Datapath]) -> None:
    for switch, in_port, out_port in path:
        datapath = switch_datapath[switch]
        proto = datapath.ofproto
        parser = datapath.ofproto_parser
        match = parser.OFPMatch(in_port=in_port, eth_src=src, eth_dst=dst)
        mod = parser.OFPFlowMod(datapath=datapath, priority=1, match=match, command=proto.OFPFC_DELETE,
                                out_group=proto.OFPG_ANY, out_port=proto.OFPP_ANY)
        datapath.send_msg(mod)
    print(f"Removed path {src} -> {dst}: {path}")


class Controller(app_manager.RyuApp):
    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]

    def __init__(self, *args: Tuple, **kwargs: Dict[str, Any]) -> None:
        os.system("clear")
        super(Controller, self).__init__(*args, **kwargs)

        self.ip_mac: Dict[str, str]
        self.host_switch_port: Dict[str, SwitchPortPair]
        self.adjacency: Dict[EndpointPair, int]
        self.bw: Dict[EndpointPair, float]
        self.ip_mac, self.host_switch_port, self.adjacency, self.bw = load_topology(TOPOLOGY_FILE)

        self.switch_datapath: Dict[int, Datapath] = {}

        self.prev_available_bw: Dict[EndpointPair, float] = defaultdict(lambda: 0.0)
        self.available_bw: Dict[EndpointPair, float] = defaultdict(lambda: 0.0)
        self.used_bw: Dict[EndpointPair, float] = defaultdict(lambda: 0.0)
        self.tx_bytes: Dict[EndpointPair, int] = defaultdict(lambda: 0)
        self.clock: Dict[EndpointPair, float] = defaultdict(lambda: 0.0)

        self.prev_paths: Dict[MacPair, Path] = {}
        self.paths: Dict[MacPair, Path] = {}

        self.topology_api_app = self
        self.monitor_thread = hub.spawn(self.monitor)

    def monitor(self) -> None:
        # server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # server_socket.bind(('127.0.0.1', 6654))
        # server_socket.listen()
        # connection, _ = server_socket.accept()

        while True:
            print(datetime.now().strftime("\n\n%H:%M:%S\n"))
            for datapath in self.switch_datapath.values():
                request_stats(datapath)
            hub.sleep(UPDATE_PERIOD)

    def update_paths(self):
        for (s1, s2), bw in self.available_bw.items():
            print(f"{s1} -> {s2}: {self.prev_available_bw[s1, s2]:.3f} -> {bw:.3f} Kbps")
            if abs(bw - self.prev_available_bw[s1, s2]) > BW_VARIANCE:  # only recalculate paths when BW changes > BW_VARIANCE
                self.prev_available_bw = self.available_bw.copy()
                self.paths = path_calculator.best_paths(self.host_switch_port, self.adjacency, self.available_bw)
                for src in self.host_switch_port.keys():
                    for dst in self.host_switch_port.keys():
                        if src != dst:
                            if self.prev_paths.get((src, dst)):
                                if self.prev_paths[src, dst] != self.paths[src, dst]:
                                    uninstall_path(src, dst, self.prev_paths[src, dst], self.switch_datapath)
                                    install_path(src, dst, self.paths[src, dst], self.switch_datapath)
                            else:
                                install_path(src, dst, self.paths[src, dst], self.switch_datapath)
                            self.prev_paths[src, dst] = self.paths[src, dst]
                break
            else:
                self.prev_available_bw[s1, s2] = self.available_bw[s1, s2]

    @set_ev_cls(ofp_event.EventOFPStateChange, MAIN_DISPATCHER)
    def state_change_handler(self, ev):
        datapath = ev.datapath
        self.switch_datapath[datapath.id] = datapath
        if len(self.switch_datapath) == len(set(s1 for (s1, s2) in self.adjacency.keys())):
            self.paths = path_calculator.best_paths(self.host_switch_port, self.adjacency, self.available_bw)
            for src in self.host_switch_port.keys():
                for dst in self.host_switch_port.keys():
                    if src != dst:
                        install_path(src, dst, self.paths[src, dst], self.switch_datapath)

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
        dpid = msg.datapath.id
        for stat in sorted(msg.body, key=attrgetter('port_no')):
            for switch in self.switch_datapath.keys():
                if self.adjacency[dpid, switch] == stat.port_no:
                    if self.tx_bytes[dpid, switch] > 0:
                        self.used_bw[dpid, switch] = (stat.tx_bytes - self.tx_bytes[dpid, switch]) * 8.0 \
                                                     / (time.time() - self.clock[dpid, switch]) / 1000
                        self.available_bw[dpid, switch] = int(self.bw[dpid, switch]) * 1024.0 - self.used_bw[dpid, switch]
                    self.tx_bytes[dpid, switch] = stat.tx_bytes
                    self.clock[dpid, switch] = time.time()

    @set_ev_cls(ofp_event.EventOFPPacketIn, MAIN_DISPATCHER)
    def packet_in_handler(self, ev) -> None:
        msg = ev.msg
        datapath = msg.datapath
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        in_port = msg.match['in_port']

        pkt = packet.Packet(msg.data)
        pkt = pkt.get_protocol(arp.arp)
        if not pkt:
            return

        src = pkt.src_mac
        dst = self.ip_mac[pkt.dst_ip]
        dpid = datapath.id

        out_port = None
        path = self.paths.get((src, dst), None)
        if path:
            for sw, p1, p2 in path:
                if sw == dpid:
                    out_port = p2

        data = None
        if msg.buffer_id == ofproto.OFP_NO_BUFFER:
            data = msg.data

        if out_port:
            actions = [parser.OFPActionOutput(out_port)]
            out = parser.OFPPacketOut(datapath=datapath, buffer_id=msg.buffer_id, in_port=in_port, actions=actions, data=data)
            datapath.send_msg(out)
