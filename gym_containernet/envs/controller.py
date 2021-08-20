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
from os import system
import socket
import time
from typing import Any, DefaultDict, Dict, List, Tuple


TOPOLOGY_FILE = "topology.txt"
UPDATE_PERIOD = 5   # seconds
BW_VARIANCE = 2000  # kbits

# Custom types
EndpointPair = Tuple[int, int]  # (1, 2)
SwitchPort = Tuple[int, int]  # (1, 2)
MacPair = Tuple[str, str]  # ("00:00:00:00:00:01", "00:00:00:00:00:02")
Path = List[Tuple[int, int, int]]  # [(1, 1, 4), (6, 1, 2), (2, 4, 1)]


def load_topology(file: str) -> (Dict[str, str], Dict[str, str], Dict[str, SwitchPort], Dict[EndpointPair, int], Dict[EndpointPair, float]):
    name_mac: Dict[str, str] = {}
    ip_mac: Dict[str, str] = {}
    switch_ports: Dict[int, int] = {}
    host_switch_port: Dict[str, SwitchPort] = {}
    adjacency: DefaultDict[EndpointPair, int] = defaultdict(lambda: 0)
    link_bw: Dict[EndpointPair, float] = {}

    with open(file, 'r') as topology:
        for line in topology.readlines()[2:]:
            cols: List[str] = line.split()
            if cols[0][0] != 'S':  # host connects to
                host_ip: str = '10.0.0.%s' % (len(host_switch_port) + 1)
                hexadecimal: str = '{0:012x}'.format(len(host_switch_port) + 1)
                host_mac: str = ':'.join(hexadecimal[i:i + 2] for i in range(0, 12, 2))
                name_mac[cols[0]] = host_mac
                ip_mac[host_ip] = host_mac
                if cols[1][0] == 'S':  # a switch
                    idx: int = int(cols[1][1:])
                    switch_ports[idx] = switch_ports[idx] + 1 if switch_ports.get(idx) else 1
                    host_switch_port[host_mac] = (idx, switch_ports[idx])
            else:  # switch connects to
                s1_idx: int = int(cols[0][1:])
                if cols[1][0] == 'S':  # another switch
                    s2_idx: int = int(cols[1][1:])
                    # necessary to match mininet ports
                    switch_ports[s1_idx] = switch_ports[s1_idx] + 1 if switch_ports.get(s1_idx) else 1
                    switch_ports[s2_idx] = switch_ports[s2_idx] + 1 if switch_ports.get(s2_idx) else 1
                    adjacency[s1_idx, s2_idx] = switch_ports[s1_idx]
                    adjacency[s2_idx, s1_idx] = switch_ports[s2_idx]
                    link_bw[s1_idx, s2_idx] = float(cols[2])
                    link_bw[s2_idx, s1_idx] = float(cols[2])
                else:  # a host
                    hexadecimal: str = '{0:012x}'.format(len(host_switch_port) + 1)
                    host_mac: str = ':'.join(hexadecimal[i:i + 2] for i in range(0, 12, 2))
                    switch_ports[s1_idx] = switch_ports[s1_idx] + 1 if switch_ports.get(s1_idx) else 1
                    host_switch_port[host_mac] = (s1_idx, switch_ports[s1_idx])
    return name_mac, ip_mac, host_switch_port, adjacency, link_bw


def request_stats(datapath: Datapath) -> None:
    proto = datapath.ofproto
    parser = datapath.ofproto_parser
    req = parser.OFPPortStatsRequest(datapath, 0, proto.OFPP_ANY)
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
        system("clear")
        super(Controller, self).__init__(*args, **kwargs)

        self.name_mac: Dict[str, str]
        self.ip_mac: Dict[str, str]
        self.host_switch_port: Dict[str, SwitchPort]
        self.adjacency: Dict[EndpointPair, int]
        self.bw: Dict[EndpointPair, float]
        self.name_mac, self.ip_mac, self.host_switch_port, self.adjacency, self.bw = load_topology(TOPOLOGY_FILE)

        self.switch_datapath: Dict[int, Datapath] = {}

        self.prev_available_bw: Dict[EndpointPair, float] = defaultdict(lambda: 0.0)
        self.available_bw: Dict[EndpointPair, float] = defaultdict(lambda: 0.0)
        self.used_bw: Dict[EndpointPair, float] = defaultdict(lambda: 0.0)
        self.tx_bytes: Dict[EndpointPair, int] = defaultdict(lambda: 0)
        self.clock: Dict[EndpointPair, float] = defaultdict(lambda: 0.0)

        self.done_switches: List[int] = []
        self.paths: Dict[MacPair, Path] = {}
        self.active_pairs: List[MacPair] = []

        self.topology_api_app = self
        self.monitor_bw_thread = hub.spawn(self.monitor_bw)
        self.monitor_active_pairs_thread = hub.spawn(self.monitor_active_pairs)

    def monitor_bw(self) -> None:
        while True:
            print(datetime.now().strftime("\n\n%H:%M:%S\n"))
            for switch, datapath in self.switch_datapath.items():
                request_stats(datapath)
            hub.sleep(UPDATE_PERIOD)

    def monitor_active_pairs(self) -> None:
        active_pairs_client_socket: socket.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        active_pairs_client_socket.connect(('127.0.0.1', 6654))
        while True:
            data: str = active_pairs_client_socket.recv(1024).decode('utf-8')
            if data:
                if data == "reset":
                    self.active_pairs = []
                else:
                    active_pairs: List[MacPair] = [(self.name_mac[path.split('_')[0]], self.name_mac[path.split('_')[1]])
                                                   for path in data.split(',')]
                    if active_pairs != self.active_pairs:
                        self.active_pairs = active_pairs.copy()

    def update_paths(self) -> None:
        print(f"Active paths: {self.active_pairs}")
        new_paths: Dict[MacPair, Path] = path_calculator.best_paths(self.host_switch_port, self.adjacency, self.available_bw, self.active_pairs)
        for (src, dst) in new_paths.keys():
            if self.paths.get(src, dst):  # on startup this dict will be empty
                if new_paths[src, dst] != self.paths[src, dst]:
                    uninstall_path(src, dst, self.paths[src, dst], self.switch_datapath)
                    install_path(src, dst, new_paths[src, dst], self.switch_datapath)
                    self.paths[src, dst] = new_paths[src, dst]

    @set_ev_cls(ofp_event.EventOFPStateChange, MAIN_DISPATCHER)
    def state_change_handler(self, ev):
        datapath = ev.datapath
        self.switch_datapath[datapath.id] = datapath
        if len(self.switch_datapath) == len(set(s1 for (s1, s2) in self.adjacency.keys())):  # after all switches register
            self.paths = path_calculator.best_paths(self.host_switch_port, self.adjacency, self.available_bw, [])
            for src in self.host_switch_port.keys():
                for dst in self.host_switch_port.keys():
                    if src != dst:
                        install_path(src, dst, self.paths[src, dst], self.switch_datapath)

    @set_ev_cls(ofp_event.EventOFPSwitchFeatures, CONFIG_DISPATCHER)
    def switch_features_handler(self, ev) -> None:  # create table-miss entries
        datapath = ev.msg.datapath
        proto = datapath.ofproto
        parser = datapath.ofproto_parser
        match = parser.OFPMatch()
        actions = [parser.OFPActionOutput(proto.OFPP_CONTROLLER, proto.OFPCML_NO_BUFFER)]
        inst = [parser.OFPInstructionActions(proto.OFPIT_APPLY_ACTIONS, actions)]
        mod = datapath.ofproto_parser.OFPFlowMod(datapath=datapath, match=match, cookie=0, command=proto.OFPFC_ADD,
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

        self.done_switches += [dpid]
        if len(set(self.done_switches)) == len([sw for sw in self.switch_datapath.keys()]):  # all switches recalculated links' bw
            self.update_paths()
            self.done_switches = []

    @set_ev_cls(ofp_event.EventOFPPacketIn, MAIN_DISPATCHER)
    def packet_in_handler(self, ev) -> None:
        msg = ev.msg
        datapath = msg.datapath
        proto = datapath.ofproto
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
        if msg.buffer_id == proto.OFP_NO_BUFFER:
            data = msg.data

        if out_port:
            actions = [parser.OFPActionOutput(out_port)]
            out = parser.OFPPacketOut(datapath=datapath, buffer_id=msg.buffer_id, in_port=in_port, actions=actions, data=data)
            datapath.send_msg(out)
