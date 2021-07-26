from mininet.net import Containernet
from mininet.node import RemoteController, OVSSwitch
from mininet.cli import CLI
from mininet.log import setLogLevel
from mininet.link import TCLink

from functools import partial
from typing import Dict, List, Tuple, Union
import os

DOCKER_LOCAL_FOLDER: str = "/home/pmsdoliveira/workspace/containernet-gym/containers/vol1"
DOCKER_CONTAINER_FOLDER: str = "/home/vol1"


def add_host(network: Containernet, name: str, log: bool = False) -> None:
    if name in [host.name for host in network.hosts]:
        if log:
            print("* LOG: Host %s already exists" % name)
        return
    if log:
        print("* LOG: Cleanup of container mn.%s" % name)
    os.system('sudo docker rm -f mn.%s' % name)  # cleanup of previous docker containers with the same name
    network.addDocker(name=name, dimage="iperf:latest", volumes=["%s:%s" %
                                                                 (DOCKER_LOCAL_FOLDER, DOCKER_CONTAINER_FOLDER)])
    if log:
        print("* LOG: Created host %s" % name)


def add_switch(network: Containernet, name: str, log: bool = False) -> None:
    if name in [switch.name for switch in network.switches]:
        if log:
            print("* LOG: Switch %s already exists" % name)
        return
    network.addSwitch(name, cls=OVSSwitch, protocols="OpenFlow13")
    if log:
        print("* LOG: Created switch %s" % name)


def add_link(network: Containernet, source: str, destination: str, link_options: Dict, log: bool = False) -> None:
    if source[0] == 'S':
        src = network.switches[[str(switch) for switch in network.switches].index(source)]
    else:
        src = network.hosts[[str(host) for host in network.hosts].index(source)]
    if destination[0] == 'S':
        dst = network.switches[[str(switch) for switch in network.switches].index(destination)]
    else:
        dst = network.hosts[[str(host) for host in network.hosts].index(destination)]

    if not network.linksBetween(src, dst):
        network.addLink(src, dst, **link_options)
        if log:
            print("* LOG: Created link between %s and %s" % (src.name, dst.name))
    elif log:
        print("* LOG: Link between %s and %s already exists" % (src.name, dst.name))


def topology_from_file(network: Containernet, file: str, log: bool = False) -> None:
    with open(file, 'r') as topology:
        for line in topology.readlines()[2:]:
            atts: List[str] = line.split()
            for i in range(0, 2):  # runs twice (for both link nodes)
                node = atts[i]  # node name
                if node[0] == 'S':
                    add_switch(network, node, log)
                else:
                    add_host(network, node, log)
            link_options = dict(bw=int(atts[2]), delay="%sms" % int(atts[3]), loss=float(atts[4]))
            add_link(network, atts[0], atts[1], link_options, log)
    if log:
        print("* LOG: Added a total of %s hosts, %s switches and %s links\n" %
              (len(network.hosts), len(network.switches), len(network.links)))


def start_containernet(file: str, log: bool = False) -> None:
    setLogLevel('info')
    os.system("sudo mn -c")
    network: Containernet = Containernet(controller=RemoteController, switch=OVSSwitch,
                                         link=TCLink, autoSetMacs=True, ipBase='10.0.0.0/8')
    topology_from_file(network, file, log=log)
    network.addController('c0', controller=RemoteController, ip='127.0.0.1', port=6653)
    network.start()
    CLI(network)
    network.stop()


if __name__ == '__main__':
    start_containernet(file="topology.txt", log=True)
