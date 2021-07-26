import networkx as nx

from functools import reduce
from typing import Dict, Iterator, List, Tuple


def create_graph(host_switch_port: Dict[str, Tuple[int, int]], adjacency: Dict[Tuple[int, int], int],
                 available_bw: Dict[Tuple[int, int], float]) -> nx.Graph:
    graph: nx.Graph = nx.Graph()
    for mac, (switch, port) in host_switch_port.items():
        if mac not in graph:
            graph.add_node(mac)
        if switch not in graph:
            graph.add_node("S%s" % switch)
        graph.add_edge(mac, switch)
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


def calculate_paths_bw(graph: nx.Graph, paths: List[List[Tuple[int, int, int]]]) -> List[float]:
    paths_bw: List[float] = []
    paths_pairwise: Iterator = map(nx.utils.pairwise, paths)
    for path_iterator in paths_pairwise:
        path: list = list(path_iterator)
        path_bw: float = reduce(lambda acc, curr: acc + graph.get_edge_data(*curr).get('weight', 0.0),
                                path, 0.0)
        paths_bw.append(path_bw)
    return paths_bw


def create_installable_paths(paths: List[List[Tuple[int, int, int]]],
                             host_switch_port: Dict[str, Tuple[int, int]],
                             adjacency: Dict[Tuple[int, int], int]) -> List[List]:
    installable_paths: List[List[Tuple[int, int, int]]] = []
    for path in paths:
        installable_path: List[Tuple[int, int, int]] = []
        (switch, in_port) = host_switch_port[path[0]]
        out_port: int = adjacency[path[1], path[2]]
        installable_path.append((switch, in_port, out_port))
        for i in range(1, len(path) - 3):
            section = path[i:i + 3]
            switch = section[1]
            in_port = adjacency[switch, section[0]]
            out_port = adjacency[switch, section[2]]
            installable_path.append((switch, in_port, out_port))
        (switch, out_port) = host_switch_port[path[-1]]
        in_port = adjacency[path[1], path[2]]
        installable_path.append((switch, in_port, out_port))
        installable_paths.append(installable_path)
    return installable_paths


def select_best_path(paths: List[List[Tuple[int, int, int]]], paths_bw: List[float]) -> List[Tuple[int, int, int]]:
    pass
