from ryu.base import app_manager
from ryu.controller import ofp_event
from ryu.controller.handler import CONFIG_DISPATCHER, MAIN_DISPATCHER
from ryu.controller.handler import set_ev_cls
from ryu.ofproto import ofproto_v1_3
from ryu.controller.controller import Datapath
from ryu.lib import hub

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
BASE_STATIONS = 4
COMPUTING_STATIONS = 4
PATHS = 4
UPDATE_PERIOD = 5   # seconds

# Custom types
SwitchPair = Tuple[int, int]  # (1, 2)
SwitchPort = Tuple[int, int]  # (1, 2)
MacPair = Tuple[str, str]  # ("00:00:00:00:00:01", "00:00:00:00:00:02")
Path = List[Tuple[int, int, int]]  # [(1, 1, 4), (6, 1, 2), (2, 4, 1)]


def int_to_mac(n: int) -> str:
    hexadecimal: str = f'{n:012X}'
    return ':'.join(hexadecimal[i:i + 2] for i in range(0, 12, 2))


def load_topology(file: str
                  ) -> (Dict[str, str], Dict[str, str], Dict[str, SwitchPort], Dict[SwitchPair, int], Dict[SwitchPair, float], nx.Graph):
    mac_name: Dict[str, str] = {}
    ip_mac: Dict[str, str] = {}
    switch_ports: Dict[int, int] = {}
    host_switch_port: Dict[str, SwitchPort] = {}
    adjacency: DefaultDict[SwitchPair, int] = defaultdict(lambda: 0)
    link_bw: Dict[SwitchPair, float] = {}
    graph: nx.Graph = nx.Graph()

    with open(file, 'r') as topology:
        for line in topology.readlines()[2:]:
            cols: List[str] = line.split()
            if cols[0][0] != 'S':  # host connects to
                host_mac: str = int_to_mac(len(host_switch_port) + 1)
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
                    graph.add_edge(s1_idx, s2_idx, weight=float(cols[2]) * 1000)
                else:  # a host
                    host_mac: str = int_to_mac(len(host_switch_port) + 1)
                    mac_name[host_mac] = cols[1]
                    if host_mac not in graph:
                        graph.add_node(host_mac)
                    ip_mac[f'10.0.0.{len(host_switch_port) + 1}'] = host_mac
                    switch_ports[s1_idx] = switch_ports[s1_idx] + 1 if switch_ports.get(s1_idx) else 1
                    host_switch_port[host_mac] = (s1_idx, switch_ports[s1_idx])
                    graph.add_edge(host_mac, s1_idx, weight=float(cols[2]) * 1000)
    return mac_name, ip_mac, host_switch_port, adjacency, link_bw, graph


def create_paths(graph: nx.Graph, mac_name: Dict[str, str], host_switch_port: Dict[str, SwitchPort], adjacency: Dict[SwitchPair, int]
                 ) -> Dict[MacPair, List[Path]]:
    base_stations: List = [node for node in list(graph.nodes) if mac_name.get(node) and mac_name[node][0] == 'B']
    computing_stations: List = [node for node in list(graph.nodes) if mac_name.get(node) and mac_name[node][0] == 'C']

    all_paths = {}
    for bs in base_stations:
        for cs in computing_stations:
            all_paths[bs, cs] = sorted(list(nx.all_simple_paths(graph, bs, cs)), key=lambda x: len(x))[:PATHS]
            all_paths[cs, bs] = sorted(list(nx.all_simple_paths(graph, cs, bs)), key=lambda x: len(x))[:PATHS]

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
    bottlenecks: Dict[MacPair, Union[List[None], List[float]]] = defaultdict(lambda: PATHS * [None])
    for (src, dst), path_list in paths.items():
        for path_idx, path in enumerate(path_list):
            switches = [switch for (switch, in_port, out_port) in path]
            if len(switches) > 1:
                pairs = [(switches[i], switches[i + 1]) for i in range(len(switches) - 1)]
                bottlenecks[src, dst][path_idx] = min(graph.get_edge_data(*pair)['weight'] for pair in pairs)
            else:
                bottlenecks[src, dst][path_idx] = 0
    return bottlenecks


def select_best_paths(paths: Dict[MacPair, List[Path]], bottlenecks: Dict[MacPair, List[float]], active_paths: Dict[MacPair, int]
                      ) -> (Dict[MacPair, Path], Dict[MacPair, int]):
    best_paths: Dict[MacPair, Path] = {}
    paths_in_use: Dict[MacPair, int] = active_paths.copy()
    for (src, dst), path_list in paths.items():
        if paths_in_use[src, dst] != -1:  # if a path is in use, don't change it
            best_paths[src, dst] = paths[src, dst][paths_in_use[src, dst]]
        else:
            best_bottleneck: float = float("-Inf")
            for path_idx, path in enumerate(path_list):
                if bottlenecks[src, dst][path_idx] > best_bottleneck:
                    best_bottleneck = bottlenecks[src, dst][path_idx]
                    best_paths[src, dst] = path
                    paths_in_use[src, dst] = path_idx
    return best_paths, paths_in_use


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
        self.adjacency: Dict[SwitchPair, int]
        self.bw: Dict[SwitchPair, float]
        self.graph: nx.Graph
        self.mac_name, self.ip_mac, self.host_switch_port, self.adjacency, \
            self.bw, self.graph = load_topology(TOPOLOGY_FILE)

        self.paths: Dict[MacPair, List[Path]] = create_paths(self.graph, self.mac_name, self.host_switch_port, self.adjacency)
        self.bottlenecks: Dict[MacPair, List[float]] = get_paths_bottlenecks(self.graph, self.paths)
        self.active_paths: DefaultDict[MacPair, int] = defaultdict(lambda: -1)
        _, self.active_paths = select_best_paths(self.paths, self.bottlenecks, self.active_paths)

        self.switch_datapath: Dict[int, Datapath] = {}

        self.available_bw: Dict[SwitchPair, float] = defaultdict(lambda: 0.0)
        self.used_bw: Dict[SwitchPair, float] = defaultdict(lambda: 0.0)
        self.tx_bytes: Dict[SwitchPair, int] = defaultdict(lambda: 0)
        self.clock: Dict[SwitchPair, float] = defaultdict(lambda: 0.0)

        self.done_switches: List[int] = []
        self.bottlenecks_socket: socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.paths_connection: socket = None

        self.topology_api_app = self
        self.monitor_bw_thread = hub.spawn(self.monitor_bw)
        self.monitor_paths_thread = hub.spawn(self.monitor_paths)

    def monitor_bw(self) -> None:
        self.bottlenecks_socket.connect(('127.0.0.1', 6654))

        while True:
            print(datetime.now().strftime("\n\n%H:%M:%S\n"))
            for switch, datapath in self.switch_datapath.items():
                request_stats(datapath)
            hub.sleep(UPDATE_PERIOD)

    def monitor_paths(self) -> None:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as paths_socket:
            paths_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            paths_socket.bind(('127.0.0.1', 6655))
            paths_socket.listen()
            self.paths_connection, _ = paths_socket.accept()

        while True:
            new_paths = self.active_paths.copy()
            size = int.from_bytes(self.paths_connection.recv(16), byteorder=byteorder)
            try:
                data = self.paths_connection.recv(size).decode('utf-8').split(',')
                if len(data) == BASE_STATIONS * COMPUTING_STATIONS:
                    for idx, path_idx in enumerate(data):
                        client: str = int_to_mac(idx // BASE_STATIONS + 1)
                        server: str = int_to_mac(idx % BASE_STATIONS + BASE_STATIONS + 1)
                        new_paths[client, server] = int(path_idx) if int(path_idx) != -1 else 0
                        if new_paths[client, server] != self.active_paths[client, server]:
                            uninstall_path(client, server, self.paths[client, server][self.active_paths[client, server]], self.switch_datapath)
                            install_path(client, server, self.paths[client, server][new_paths[client, server]], self.switch_datapath)
                            self.active_paths[client, server] = new_paths[client, server]
            except OverflowError:
                print("\n\n\n\n\n\t\t\t\t\t\t\t\tOVERFLOW ERROR\n\n\n\n\n")
            except MemoryError:
                print("\n\n\n\n\n\t\t\t\t\t\t\t\tMEMORY ERROR\n\n\n\n\n")

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
            self.bottlenecks = get_paths_bottlenecks(self.graph, self.paths)
            best_paths, self.active_paths = select_best_paths(self.paths, self.bottlenecks, self.active_paths)
            for (src, dst), path in best_paths.items():
                install_path(src, dst, best_paths[src, dst], self.switch_datapath)

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
        if len(set(self.done_switches)) == len(self.switch_datapath.keys()):  # all switches recalculated links' bw
            for (src, dst), bw in sorted(self.available_bw.items()):
                self.graph[src][dst]['weight'] = min(self.available_bw[src, dst], self.available_bw[dst, src])

            self.bottlenecks = get_paths_bottlenecks(self.graph, self.paths)

            data: str = ''
            for (src, dst), bottleneck_list in self.bottlenecks.items():
                if self.mac_name[src][0] == 'B':
                    data += f'{",".join(str(bottleneck) for bottleneck in bottleneck_list)}\n'
            self.bottlenecks_socket.sendall(len(data).to_bytes(16, byteorder))
            self.bottlenecks_socket.sendall(data.encode('utf-8'))

            self.done_switches = []
