from mininet.net import Containernet
from mininet.node import Controller, Host, Switch, OVSSwitch, OVSKernelSwitch
from mininet.link import TCLink

from typing import Dict, List, Optional, Union
import os
import time

# current image: iperf:latest, can change to ubuntu:trusty
DOCKER_VOLUMES = ["/home/pmsdoliveira/workspace/containers/vol1/:/home/vol1"]


def index_from_name(name: str) -> int:
    return int(name[1:]) - 1


def add_host(network: Containernet, name: str, log: bool = False) -> None:
    if log:
        print("* LOG: Cleanup of container mn.%s" % name)
    os.system('sudo docker rm -f mn.%s' % name)  # cleanup of previous docker containers with the same name
    network.addDocker(name=name, dimage="iperf:latest", volumes=DOCKER_VOLUMES)
    if log:
        print("* LOG: Created host %s\n" % name)


def add_switch(network: Containernet, name: str, log: bool = False) -> None:
    network.addSwitch(name=name, cls=OVSKernelSwitch, protocols="OpenFlow13")
    if log:
        print("* LOG: Created switch %s" % name)


def add_nodes_from_graph(network: Containernet, graph: Dict[str, List[str]], log: bool = False) -> None:
    for vertex in graph:
        if vertex[0] == 'h':  # name starts with 'h', it's a host
            add_host(network, vertex, log)
        elif vertex[0] == 's':  # starts with 's', it's a switch
            add_switch(network, vertex, log)
    if log:
        print("* LOG: Added a total of %s hosts and %s switches" % (len(network.hosts), len(network.switches)))


def add_host_switch_link(network: Containernet, host: Host, switch: OVSKernelSwitch,
                         switches_ports_map: List[List[str]], log: bool = False) -> None:
    if not network.linksBetween(host, switch):
        network.addLink(host, switch, cls=TCLink)
        switches_ports_map[index_from_name(switch.name)].append(host.name)
        if log:
            print("* LOG: Created link between host %s and switch %s" % (host.name, switch.name))


def add_switch_switch_link(network: Containernet, switch1: OVSKernelSwitch, switch2: OVSKernelSwitch,
                           switches_ports_map: List[List[str]], log: bool = False) -> None:
    if not network.linksBetween(switch1, switch2):
        network.addLink(switch1, switch2, cls=TCLink)
        switches_ports_map[index_from_name(switch1.name)].append(switch2.name)
        switches_ports_map[index_from_name(switch2.name)].append(switch1.name)
        if log:
            print("* LOG: Created link between switch %s and switch %s" % (switch1.name, switch2.name))


def add_link(network: Containernet, origin: str, destination: str, switches_ports_map: List[List[str]], log: bool = False) -> None:
    if origin[0] == 'h':
        add_host_switch_link(network, network.hosts[index_from_name(origin)],
                             network.switches[index_from_name(destination)], switches_ports_map, log)
    elif origin[0] == 's':
        if destination[0] == 'h':
            add_host_switch_link(network, network.hosts[index_from_name(destination)],
                                 network.switches[index_from_name(origin)], switches_ports_map, log)
        elif destination[0] == 's':
            add_switch_switch_link(network, network.switches[index_from_name(origin)],
                                   network.switches[index_from_name(destination)], switches_ports_map, log)


def add_links_from_graph(network: Containernet, graph: Dict[str, List[str]], log: bool = False) -> List[List[str]]:
    switches_ports_map: List[List[str]] = [[] for _ in range(len(network.switches))]
    for vertex in graph:
        for edge in graph[vertex]:
            add_link(network, vertex, edge, switches_ports_map, log)
    if log:
        print("* LOG: Added a total of %s links" % len(network.links))
        for switch_idx, switch_ports in enumerate(switches_ports_map):
            print("* LOG: s%s -> %s" % (switch_idx + 1, switch_ports))
    return switches_ports_map


def find_shortest_path(graph: Dict[str, List[str]], start: str, end: str, path: List[str] = [],
                       log: bool = False) -> List[str]:
    if start not in graph or end not in graph:
        return None
    path = path + [start]
    if start == end:
        return path
    shortest: List[str] = None
    for node in graph[start]:
        if node not in path:
            new_path = find_shortest_path(graph, node, end, path)
            if new_path:
                if not shortest or len(new_path) < len(shortest):
                    shortest = new_path
    if log:
        print("* LOG: %s -> %s: %s" % (start, end, shortest))
    return shortest


def find_all_shortest_paths_from_graph(network: Containernet, graph: Dict[str, List[str]],
                                       log: bool = False) -> List[List[str]]:
    shortest_paths: List[List[str]] = []
    for origin in range(len(network.hosts)):
        for destination in range(origin, len(network.hosts)):
            shortest_paths.append(find_shortest_path(graph, 'h%s' % (origin + 1), 'h%s' % (destination + 1), log=log))
    return shortest_paths


def choose_output_port(switch_name: str, host_name: str, switches_ports_map: List[List[str]],
                       shortest_paths: List[List[str]]) -> int:
    output_port: int = None
    if host_name in switches_ports_map[index_from_name(switch_name)]:  # direct connection between host and switch
        output_port = switches_ports_map[index_from_name(switch_name)].index(host_name) + 1
    else:
        # get only the shortest paths that have the specific host and switch in them
        paths = [path for path in shortest_paths if host_name in path and switch_name in path]
        if len(paths) > 0:
            path = paths[0] # any path will do
            # reverse the path if needed to ensure the specific host is always the path's destination
            path = path if host_name == path[-1] else path[::-1]
            next_switch = path[path.index(switch_name) + 1]
            output_port = switches_ports_map[index_from_name(switch_name)].index(next_switch) + 1
    return output_port


def add_flow(network: Containernet, switch_name: str, ip_destination: str, output_port: int, log: bool = False) -> None:
    cmd: str = 'ovs-ofctl --protocols=OpenFlow13 add-flow %s ' % switch_name
    for package in ['arp', 'ip']:
        if output_port:
            details: str = '%s,nw_dst=%s,actions=output:%s' % (package, ip_destination, output_port)
            network.switches[index_from_name(switch_name)].cmd(cmd + details)
            if log:
                print("* LOG: %s" % (cmd + details))


def add_flows(network: Containernet, switches_ports_map: List[List[str]], shortest_paths: List[List[str]],
              log: bool = False) -> None:
    for switch in range(len(network.switches)):
        switch_name: str = 's%s' % (switch + 1)
        for host in range(len(network.hosts)):
            host_name: str = 'h%s' % (host + 1)
            host_ip: str = network.hosts[host].IP()
            output_port: int = choose_output_port(switch_name, host_name, switches_ports_map, shortest_paths)
            add_flow(network, switch_name, host_ip, output_port, log)


def create_containernet_from_graph(graph: Dict[str, List[str]], log: bool = False) -> Containernet:
    os.system("sudo mn -c")  # cleanup of previously open Mininet topologies
    network: Containernet = Containernet(listenPort=6633, ipBase='10.0.0.0/8', controller=Controller)
    network.addController('c0')
    add_nodes_from_graph(network, graph, log)
    switches_ports_map: List[List[str]] = add_links_from_graph(network, graph, log)
    shortest_paths: List[List[str]] = find_all_shortest_paths_from_graph(network, graph, log)
    network.start()
    add_flows(network, switches_ports_map, shortest_paths, log)
    return network


if __name__ == '__main__':
    topology: Dict[str, List[str]] = {
        'h1': ['s1'],
        'h2': ['s10'],
        'h3': ['s9'],
        'h4': ['s8'],
        'h5': ['s3'],
        'h6': ['s2'],
        'h7': ['s7'],
        'h8': ['s6'],
        's1': ['h1', 's2', 's3'],
        's2': ['h6', 's1', 's3', 's4', 's6'],
        's3': ['h5', 's1', 's2', 's4', 's8'],
        's4': ['s2', 's3', 's5', 's6', 's7'],
        's5': ['s4', 's7', 's8', 's9', 's10'],
        's6': ['h8', 's2', 's4', 's7'],
        's7': ['h7', 's4', 's5', 's6', 's10'],
        's8': ['h4', 's3', 's5', 's9'],
        's9': ['h3', 's5', 's8'],
        's10': ['h2', 's5', 's7']
    }

    start_time: time = time.time()
    containernet: Containernet = create_containernet_from_graph(graph=topology, log=True)
    containernet.pingAll()
    containernet.stop()
    print("Time spent: %.2fs" % (time.time() - start_time))
