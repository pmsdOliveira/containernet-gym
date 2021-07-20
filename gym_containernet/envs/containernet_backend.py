from mininet.net import Containernet
from mininet.node import RemoteController, Host, OVSSwitch, Controller
from mininet.cli import CLI
from mininet.log import setLogLevel
from mininet.link import TCLink

from functools import partial
from typing import Dict, List, Tuple, Union
import os


DOCKER_LOCAL_FOLDER: str = "/home/pmsdoliveira/workspace/containernet-gym/containers/vol1"
DOCKER_CONTAINER_FOLDER: str = "/home/vol1"


def index_from_name(name: str) -> int:
    return int(name[1:]) - 1


def read_links_options(filepath: str) -> Dict[Tuple[int, int], Dict]:
    links: Dict[Tuple[int, int], Dict] = {}
    with open(filepath, 'r') as bw_file:
        for line in bw_file.readlines()[2:]:
            split: List[str] = line.split()
            links[int(split[0]), int(split[1])] = dict(bw=float(split[2]), delay='%dms' % float(split[3]),
                                                       loss=float(split[4]))
    return links


def add_host(network: Containernet, name: str, log: bool = False) -> None:
    if log:
        print("* LOG: Cleanup of container mn.%s" % name)
    os.system('sudo docker rm -f mn.%s' % name)  # cleanup of previous docker containers with the same name
    network.addDocker(name=name, dimage="iperf:latest",
                      volumes=["%s:%s" % (DOCKER_LOCAL_FOLDER, DOCKER_CONTAINER_FOLDER)])
    if log:
        print("* LOG: Created host %s\n" % name)


def add_switch(network: Containernet, name: str, log: bool = False) -> None:
    network.addSwitch(name=name, cls=OVSSwitch, protocols="OpenFlow13")
    if log:
        print("* LOG: Created switch %s" % name)


def add_nodes_from_graph(network: Containernet, graph: Dict[str, List[str]], log: bool = False) -> None:
    for vertex in graph:
        if vertex[0] == 'h':  # name starts with 'h', it's a host
            add_host(network, vertex, log)
        elif vertex[0] == 's':  # starts with 's', it's a switch
            add_switch(network, vertex, log)
    if log:
        print("* LOG: Added a total of %s hosts and %s switches\n" % (len(network.hosts), len(network.switches)))


def add_link(network: Containernet, source: str, destination: str, link_options: Dict, log: bool = False) -> None:
    src = network.hosts[index_from_name(source)] if source[0] == 'h' else network.switches[index_from_name(source)]
    dst = network.hosts[index_from_name(destination)] if destination[0] == 'h' else network.switches[index_from_name(destination)]
    if not network.linksBetween(src, dst):
        network.addLink(src, dst, cls=TCLink, **link_options)
        if log:
            print("* LOG: Created link between %s and %s" % (src.name, dst.name))


def add_links_from_graph(network: Containernet, graph: Dict[str, List[str]],
                         links_options: Dict[Tuple[int, int], Dict], log: bool = False) -> None:
    for v1 in graph:
        for v2 in graph[v1]:
            if v1[0] == 's' and v2[0] == 's':
                link_options = links_options.get((int(v1[1:]), int(v2[1:])))
            else:
                link_options = dict(bw=100, delay='1ms', loss=0)
            add_link(network, v1, v2, link_options, log)
    if log:
        print("* LOG: Added a total of %s links\n" % len(network.links))


def start_controller(network: Containernet, controller: Controller) -> None:
    controller.start()
    for s in network.switches:
        s.start([controller])


def start_containernet(graph: Dict[str, List[str]], links_options_file: str, log: bool = False) -> None:
    setLogLevel('info')
    os.system("sudo mn -c")
    network = Containernet(controller=RemoteController, switch=partial(OVSSwitch, protocols="OpenFlow13"),
                           link=TCLink, autoSetMacs=True, ipBase='10.0.0.0/8')
    controller = network.addController('c0', controller=RemoteController, ip='127.0.0.1', port=6653)
    add_nodes_from_graph(network, graph, log=log)
    links_options: Dict[Tuple[int, int], Dict] = read_links_options(links_options_file)
    add_links_from_graph(network, graph, links_options, log=log)
    network.build()
    start_controller(network, controller)
    CLI(network)
    network.stop()


if __name__ == '__main__':
    topology: Dict[str, List[str]] = {
        'h1': ['s1'],
        'h2': ['s1'],
        'h3': ['s2'],
        'h4': ['s2'],
        'h5': ['s3'],
        'h6': ['s3'],
        'h7': ['s4'],
        'h8': ['s4'],
        's1': ['h1', 'h2', 's5', 's6'],
        's2': ['h3', 'h4', 's5', 's6'],
        's3': ['h5', 'h6', 's7', 's8'],
        's4': ['h7', 'h8', 's7', 's8'],
        's5': ['s1', 's2', 's9'],
        's6': ['s1', 's2', 's10'],
        's7': ['s3', 's4', 's9'],
        's8': ['s3', 's4', 's10'],
        's9': ['s5', 's7'],
        's10': ['s6', 's8']
    }

    start_containernet(graph=topology, links_options_file="bw.txt", log=True)
