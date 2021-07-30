import networkx as nx

from functools import reduce
from typing import Dict, Iterator, List, Tuple, Union


# Custom types
EndpointPair = Tuple[int, int]
SwitchPortPair = Tuple[int, int]
MacPair = Tuple[str, str]
Path = List[Tuple[int, int, int]]


def create_graph(host_switch_port: Dict[str, SwitchPortPair], adjacency: Dict[SwitchPortPair, int],
                 available_bw: Dict[EndpointPair, float]) -> nx.Graph:
    graph: nx.Graph = nx.Graph()
    for mac, (switch, port) in host_switch_port.items():
        if mac not in graph:
            graph.add_node(mac)
        if switch not in graph:
            graph.add_node("S%s" % switch)
        graph.add_edge(mac, switch, weight=100000)
    for s1, s2 in adjacency.keys():
        if s1 not in graph:
            graph.add_node(s1)
        if s2 not in graph:
            graph.add_node(s2)
        if adjacency[s1, s2]:
            graph.add_edge(s1, s2, weight=available_bw[s1, s2])
    return graph


def get_all_paths(graph: nx.Graph, src: str, dst: str) -> List:
    return list(nx.all_simple_paths(graph, src, dst))


def create_installable_paths(paths: List[Union[str, int]], host_switch_port: Dict[str, SwitchPortPair],
                             adjacency: Dict[SwitchPortPair, int]) -> List[Path]:
    installable_paths: List[Path] = []
    for path in paths:
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
        installable_paths.append(installable_path)
    return installable_paths


def get_bottleneck(graph: nx.graph, path: List) -> float:
    bottleneck: float = float('Inf')
    for hop in path:
        if type(hop[0]) is not str and type(hop[1]) is not str:
            curr: float = graph.get_edge_data(*hop).get('weight', 0.0)
            bottleneck = curr if curr < bottleneck else bottleneck
        # print(hop, bottleneck)
    return bottleneck


def calculate_paths_bottlenecks(graph: nx.Graph, paths: List[Union[str, int]]) -> List[float]:
    paths_bottlenecks: List[float] = []
    paths_pairwise: Iterator = map(nx.utils.pairwise, paths)
    for path_iterator in paths_pairwise:
        path: List = list(path_iterator)
        path_bottleneck: float = get_bottleneck(graph, path)
        paths_bottlenecks.append(path_bottleneck)
    return paths_bottlenecks


def select_best_installable_path(installable_paths: List[Path], paths_bottlenecks: List[float]) -> Path:
    max_bottleneck: float = float("-Inf")
    best_path: Path = []
    for path in zip(installable_paths, paths_bottlenecks):
        if path[1] > max_bottleneck:
            best_path = path[0]
            max_bottleneck = path[1]
    return best_path


def possible_and_best_paths(host_switch_port: Dict[str, SwitchPortPair], adjacency: Dict[SwitchPortPair, int],
                            available_bw: Dict[EndpointPair, float]) -> (List[List[Path]], List[Path]):
    paths: Dict[MacPair, List[Path]] = {}
    best_paths: Dict[MacPair, Path] = {}
    graph: nx.Graph = create_graph(host_switch_port, adjacency, available_bw)
    for src in host_switch_port.keys():
        for dst in host_switch_port.keys():
            if src != dst:
                possible_paths = get_all_paths(graph, src, dst)
                paths_bottlenecks = calculate_paths_bottlenecks(graph, possible_paths)
                paths[src, dst] = create_installable_paths(possible_paths, host_switch_port, adjacency)
                best_paths[src, dst] = select_best_installable_path(paths[src, dst], paths_bottlenecks)
    return paths, best_paths
