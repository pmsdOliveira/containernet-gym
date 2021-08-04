from mininet.net import Containernet
from mininet.node import RemoteController, OVSSwitch
from mininet.cli import CLI
from mininet.log import setLogLevel
from mininet.link import TCLink

from typing import Dict, List
import os


TOPOLOGY_FILE: str = "topology.txt"
DOCKER_LOCAL_FOLDER: str = "/home/pmsdoliveira/workspace/containernet-gym/containers/vol1"
DOCKER_CONTAINER_FOLDER: str = "/home/vol1"


class ContainernetBackend:
    def __init__(self, topology_file):
        os.system("clear")
        setLogLevel('info')
        os.system("sudo mn -c")
        self.network: Containernet = Containernet(controller=RemoteController, switch=OVSSwitch, link=TCLink, autoSetMacs=True, ipBase='10.0.0.0/8')
        self.topology_from_file(topology_file, log=True)
        self.network.addController('c0', controller=RemoteController, ip='127.0.0.1', port=6653)
        self.network.start()

        # for server in self.network.hosts[-4:]:
        #    server.cmd("iperf3 -s -i 1 -p 6666")

    def add_host(self, name: str, log: bool = False) -> None:
        if name in [host.name for host in self.network.hosts]:
            if log:
                print("* LOG: Host %s already exists" % name)
            return
        if log:
            print("* LOG: Cleanup of container mn.%s" % name)
        os.system('sudo docker rm -f mn.%s' % name)  # cleanup of previous docker containers with the same name
        self.network.addDocker(name=name, dimage="iperf:latest", volumes=["%s:%s" % (DOCKER_LOCAL_FOLDER, DOCKER_CONTAINER_FOLDER)])
        if log:
            print("* LOG: Created host %s" % name)

    def add_switch(self, name: str, log: bool = False) -> None:
        if name in [switch.name for switch in self.network.switches]:
            if log:
                print("* LOG: Switch %s already exists" % name)
            return
        self.network.addSwitch(name, cls=OVSSwitch, protocols="OpenFlow13")
        if log:
            print("* LOG: Created switch %s" % name)

    def add_link(self, source: str, destination: str, link_options: Dict, log: bool = False) -> None:
        if source[0] == 'S':
            src = self.network.switches[[str(switch) for switch in self.network.switches].index(source)]
        else:
            src = self.network.hosts[[str(host) for host in self.network.hosts].index(source)]
        if destination[0] == 'S':
            dst = self.network.switches[[str(switch) for switch in self.network.switches].index(destination)]
        else:
            dst = self.network.hosts[[str(host) for host in self.network.hosts].index(destination)]

        if not self.network.linksBetween(src, dst):
            self.network.addLink(src, dst, **link_options)
            if log:
                print("* LOG: Created link between %s and %s" % (src.name, dst.name))
        elif log:
            print("* LOG: Link between %s and %s already exists" % (src.name, dst.name))

    def topology_from_file(self, file: str, log: bool = False) -> None:
        with open(file, 'r') as topology:
            for line in topology.readlines()[2:]:
                atts: List[str] = line.split()
                for i in range(0, 2):  # runs twice (for both link nodes)
                    node: str = atts[i]  # node name
                    if node[0] == 'S':
                        self.add_switch(node, log)
                    else:
                        self.add_host(node, log)
                link_options: Dict = dict(bw=int(atts[2]), delay="%sms" % int(atts[3]), loss=float(atts[4]))
                self.add_link(atts[0], atts[1], link_options, log)
        if log:
            print("* LOG: Added a total of %s hosts, %s switches and %s links\n"
                  % (len(self.network.hosts), len(self.network.switches), len(self.network.links)))

    def cli(self) -> None:
        CLI(self.network)
        self.network.stop()


if __name__ == '__main__':
    backend: ContainernetBackend = ContainernetBackend(TOPOLOGY_FILE)
    backend.cli()
