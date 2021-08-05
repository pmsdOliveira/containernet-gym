from mininet.net import Containernet
from mininet.node import RemoteController, Host, OVSSwitch
from mininet.cli import CLI
from mininet.log import setLogLevel
from mininet.link import TCLink

from typing import Dict, List
import os


TOPOLOGY_FILE: str = "topology.txt"
DOCKER_LOCAL_FOLDER: str = "/home/pmsdoliveira/workspace/containernet-gym/containers/vol1"
DOCKER_CONTAINER_FOLDER: str = "/home/vol1"


def add_host(network: Containernet, name: str) -> None:
    if name not in network.keys():
        os.system('sudo docker rm -f mn.%s' % name)
        network.addDocker(name=name, dimage="iperf:latest", volumes=["%s:%s" % (DOCKER_LOCAL_FOLDER, DOCKER_CONTAINER_FOLDER)])


def add_switch(network: Containernet, name: str) -> None:
    if name not in network.keys():
        network.addSwitch(name)


def add_link(network: Containernet, source: str, destination: str, link_options: Dict) -> None:
    if not network.linksBetween(network.get(source), network.get(destination)):
        network.addLink(network.get(source), network.get(destination), **link_options)


def load_topology(network: Containernet, file: str) -> None:
    with open(file, 'r') as topology:
        for line in topology.readlines()[2:]:
            cols: List[str] = line.split()
            for node in cols[:2]:
                if node[0] == 'S':
                    add_switch(network, node)
                else:
                    add_host(network, node)
            link_options: Dict = dict(bw=int(cols[2]), delay="{}ms".format(cols[3]), loss=float(cols[4]))
            add_link(network, cols[0], cols[1], link_options)


def start_containernet(topology_file: str) -> Containernet:
    os.system("clear")
    setLogLevel('info')
    os.system("sudo mn -c")
    network: Containernet = Containernet(controller=RemoteController, switch=OVSSwitch, link=TCLink, autoSetMacs=True, ipBase='10.0.0.0/8')
    load_topology(network, topology_file)
    network.addController('c0', controller=RemoteController, ip='127.0.0.1', port=6653)
    network.start()
    return network


if __name__ == '__main__':
    containernet: Containernet = start_containernet(TOPOLOGY_FILE)
    CLI(containernet)
    containernet.stop()
