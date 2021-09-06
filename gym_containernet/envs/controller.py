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
import networkx as nx
from operator import attrgetter
from os import system
import socket
from sys import byteorder
import time
from typing import Any, DefaultDict, Dict, List, Tuple, Union


TOPOLOGY_FILE = "topology.txt"
NUMBER_OF_PATHS = 4
UPDATE_PERIOD = 5   # seconds

# Custom types
EndpointPair = Tuple[int, int]  # (1, 2)
SwitchPort = Tuple[int, int]  # (1, 2)
MacPair = Tuple[str, str]  # ("00:00:00:00:00:01", "00:00:00:00:00:02")
Path = List[Tuple[int, int, int]]  # [(1, 1, 4), (6, 1, 2), (2, 4, 1)]


def load_topology(file: str) -> (Dict[str, str], Dict[str, str], Dict[str, SwitchPort], Dict[EndpointPair, int], Dict[EndpointPair, float], nx.Graph):
    mac_name: Dict[str, str] = {}
    ip_mac: Dict[str, str] = {}
    switch_ports: Dict[int, int] = {}
    host_switch_port: Dict[str, SwitchPort] = {}
    adjacency: DefaultDict[EndpointPair, int] = defaultdict(lambda: 0)
    link_bw: Dict[EndpointPair, float] = {}
    graph: nx.Graph = nx.Graph()

    with open(file, 'r') as topology:
        for line in topology.readlines()[2:]:
            cols: List[str] = line.split()
            if cols[0][0] != 'S':  # host connects to
                hexadecimal: str = f'{len(host_switch_port) + 1:012X}'
                host_mac: str = ':'.join(hexadecimal[i:i + 2] for i in range(0, 12, 2))
                mac_name[host_mac] = cols[0]
                if host_mac not in graph:
                    graph.add_node(host_mac)
                ip_mac[f'10.0.0.{len(host_switch_port) + 1}'] = host_mac
                if cols[1][0] == 'S':  # a switch
                    idx: int = int(cols[1][1:])
                    switch_ports[idx] = switch_ports[idx] + 1 if switch_ports.get(idx) else 1
                    host_switch_port[host_mac] = (idx, switch_ports[idx])
                    if idx not in graph:
                        graph.add_node(idx)
                    graph.add_edge(host_mac, idx)
            else:  # switch connects to
                s1_idx: int = int(cols[0][1:])
                if s1_idx not in graph:
                    graph.add_node(s1_idx)
                if cols[1][0] == 'S':  # another switch
                    s2_idx: int = int(cols[1][1:])
                    # necessary to match mininet ports
                    switch_ports[s1_idx] = switch_ports[s1_idx] + 1 if switch_ports.get(s1_idx) else 1
                    switch_ports[s2_idx] = switch_ports[s2_idx] + 1 if switch_ports.get(s2_idx) else 1
                    adjacency[s1_idx, s2_idx] = switch_ports[s1_idx]
                    adjacency[s2_idx, s1_idx] = switch_ports[s2_idx]
                    link_bw[s1_idx, s2_idx] = float(cols[2])
                    link_bw[s2_idx, s1_idx] = float(cols[2])
                    if s2_idx not in graph:
                        graph.add_node(s2_idx)
                    graph.add_edge(s1_idx, s2_idx, weight=cols[2])
                else:  # a host
                    hexadecimal: str = f'{len(host_switch_port) + 1:012X}'
                    host_mac: str = ':'.join(hexadecimal[i:i + 2] for i in range(0, 12, 2))
                    mac_name[host_mac] = cols[1]
                    if host_mac not in graph:
                        graph.add_node(host_mac)
                    ip_mac[f'10.0.0.{len(host_switch_port) + 1}'] = host_mac
                    switch_ports[s1_idx] = switch_ports[s1_idx] + 1 if switch_ports.get(s1_idx) else 1
                    host_switch_port[host_mac] = (s1_idx, switch_ports[s1_idx])
                    graph.add_edge(host_mac, s1_idx, weight=cols[2])
    return mac_name, ip_mac, host_switch_port, adjacency, link_bw, graph


def create_paths(graph: nx.Graph, mac_name: Dict[str, str], host_switch_port: Dict[str, SwitchPort], adjacency: Dict[EndpointPair, int]
                 ) -> Dict[MacPair, List[Path]]:
    base_stations: List = [node for node in list(graph.nodes) if mac_name.get(node) and mac_name[node][0] == 'B']
    computing_stations: List = [node for node in list(graph.nodes) if mac_name.get(node) and mac_name[node][0] == 'C']

    all_paths = {}
    for bs in base_stations:
        for cs in computing_stations:
            all_paths[bs, cs] = sorted(list(nx.all_simple_paths(graph, bs, cs)), key=lambda x: len(x))[:NUMBER_OF_PATHS]
            all_paths[cs, bs] = sorted(list(nx.all_simple_paths(graph, cs, bs)), key=lambda x: len(x))[:NUMBER_OF_PATHS]

    for paths in all_paths.values():
        for path_idx, path in enumerate(paths):
            installable_path: Path = []
            (switch, in_port) = host_switch_port[path[0]]
            if len(path) == 3:  # only goes through one switch
                out_port: int = host_switch_port[path[2]][1]
                installable_path.append((switch, in_port, out_port))
            else:
                out_port: int = adjacency[path[1], path[2]]
                installable_path.append((switch, in_port, out_port))
                for i in range(1, len(path) - 3):
                    section: List[Union[str, int]] = path[i:i + 3]
                    switch: int = section[1]
                    in_port: int = adjacency[switch, section[0]]
                    out_port: int = adjacency[switch, section[2]]
                    installable_path.append((switch, in_port, out_port))
                (switch, out_port) = host_switch_port[path[-1]]
                in_port: int = adjacency[path[1], path[2]]
                installable_path.append((switch, in_port, out_port))
            paths[path_idx] = installable_path
    return all_paths


def get_paths_bottlenecks(graph: nx.Graph, paths: Dict[MacPair, List[Path]]) -> Dict[MacPair, List[float]]:
    bottlenecks: Dict[MacPair, Union[List[None], List[float]]] = defaultdict(lambda: NUMBER_OF_PATHS * [None])
    for (src, dst), path_list in paths.items():
        for path_idx, path in enumerate(path_list):
            switches = [switch for (switch, in_port, out_port) in path]
            pairs = [(switches[i], switches[i + 1]) for i in range(len(switches) - 1)]
            bottlenecks[src, dst][path_idx] = min(graph.get_edge_data(*pair).get('weight', 0.0) for pair in pairs)
    return bottlenecks


def select_best_paths(paths: Dict[MacPair, List[Path]], bottlenecks: Dict[MacPair, List[float]]) -> Dict[MacPair, Path]:
    best_bottleneck: float = float("Inf")
    best_paths: Dict[MacPair, Path] = {}
    for (src, dst), path_list in paths.items():
        for path_idx, path in enumerate(path_list):
            if bottlenecks[src, dst][path_idx] < best_bottleneck:
                best_bottleneck = bottlenecks[src, dst][path_idx]
                best_paths[src, dst] = path
    return best_paths


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

        self.mac_name: Dict[str, str]
        self.ip_mac: Dict[str, str]
        self.host_switch_port: Dict[str, SwitchPort]
        self.adjacency: Dict[EndpointPair, int]
        self.bw: Dict[EndpointPair, float]
        self.graph: nx.Graph
        self.mac_name, self.ip_mac, self.host_switch_port, self.adjacency, \
            self.bw, self.graph = load_topology(TOPOLOGY_FILE)

        self.paths: Dict[MacPair, List[Path]] = create_paths(self.graph, self.mac_name, self.host_switch_port, self.adjacency)
        self.bottlenecks: Dict[MacPair, List[float]] = get_paths_bottlenecks(self.graph, self.paths)

        self.switch_datapath: Dict[int, Datapath] = {}

        self.prev_available_bw: Dict[EndpointPair, float] = defaultdict(lambda: 0.0)
        self.available_bw: Dict[EndpointPair, float] = defaultdict(lambda: 0.0)
        self.used_bw: Dict[EndpointPair, float] = defaultdict(lambda: 0.0)
        self.tx_bytes: Dict[EndpointPair, int] = defaultdict(lambda: 0)
        self.clock: Dict[EndpointPair, float] = defaultdict(lambda: 0.0)

        self.done_switches: List[int] = []
        self.bw_connection: socket = None

        self.topology_api_app = self
        self.monitor_bw_thread = hub.spawn(self.monitor_bw)

    def monitor_bw(self) -> None:
        bw_server_socket: socket.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        bw_server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        bw_server_socket.bind(('127.0.0.1', 6654))
        bw_server_socket.listen()
        self.bw_connection, _ = bw_server_socket.accept()

        while True:
            print(datetime.now().strftime("\n\n%H:%M:%S\n"))
            for switch, datapath in self.switch_datapath.items():
                request_stats(datapath)
            hub.sleep(UPDATE_PERIOD)

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

    @set_ev_cls(ofp_event.EventOFPStateChange, MAIN_DISPATCHER)
    def state_change_handler(self, ev):
        datapath = ev.datapath
        self.switch_datapath[datapath.id] = datapath
        if len(self.switch_datapath) == len(set(s1 for (s1, s2) in self.adjacency.keys())):  # after all switches register
            bottlenecks: Dict[MacPair, List[float]] = get_paths_bottlenecks(self.graph, self.paths)
            paths: Dict[MacPair, Path] = select_best_paths(self.paths, bottlenecks)
            for src in self.host_switch_port.keys():
                for dst in self.host_switch_port.keys():
                    if src != dst:
                        install_path(src, dst, paths[src, dst], self.switch_datapath)

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
            for (src, dst), bw in self.available_bw.items():
                self.graph[src][dst]['weight'] = bw

            bottlenecks: Dict[MacPair, List[float]] = get_paths_bottlenecks(self.graph, self.paths)

            data: str = ''
            for (src, dst), bottleneck_list in bottlenecks.items():
                data += f'{self.mac_name[src]}_{self.mac_name[dst]}=' \
                        f'{",".join(str(bottleneck) for bottleneck in bottleneck_list)}\n'
            self.bw_connection.sendall(len(data).to_bytes(16, byteorder))
            self.bw_connection.sendall(data.encode('utf-8'))

            # TODO: Update best path for each (src, dst) pair
            bottlenecks: Dict[MacPair, List[float]] = get_paths_bottlenecks(self.graph, self.paths)
            paths: Dict[MacPair, Path] = select_best_paths(self.paths, bottlenecks)

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
