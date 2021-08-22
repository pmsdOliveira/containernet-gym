from mininet.net import Containernet
from mininet.node import RemoteController, Host, OVSSwitch
from mininet.link import TCLink

from os import system
from time import sleep
from typing import Dict, List


TOPOLOGY_FILE: str = "topology.txt"
DOCKER_VOLUME: str = "/home/pmsdoliveira/workspace/gym-containernet/docker-volume"


class TopologyManager:
    def __init__(self) -> None:
        system("clear")
        system("sudo mn -c")
        self.clear_logs()
        self.network: Containernet = Containernet(controller=RemoteController, switch=OVSSwitch, link=TCLink, autoSetMacs=True, ipBase='10.0.0.0/8')
        self.load_topology(TOPOLOGY_FILE)
        self.network.addController('c0', controller=RemoteController, ip='127.0.0.1', port=6653)
        self.network.start()

    def clear_logs(self) -> None:
        system(f"rm -f {DOCKER_VOLUME}/*.log")

    def add_host(self, name: str) -> None:
        if name not in self.network.keys():
            system(f"sudo docker rm -f mn.{name}")
            self.network.addDocker(name=name, dimage="iperf:latest", volumes=[f"{DOCKER_VOLUME}:/home/volume"])

    def add_switch(self, name: str) -> None:
        if name not in self.network.keys():
            self.network.addSwitch(name)

    def add_link(self, source: str, destination: str, link_options: Dict) -> None:
        if not self.network.linksBetween(self.network.get(source), self.network.get(destination)):
            self.network.addLink(self.network.get(source), self.network.get(destination), **link_options)

    def load_topology(self, file: str) -> None:
        with open(file, 'r') as topology:
            for line in topology.readlines()[2:]:
                cols: List[str] = line.split()
                for node in cols[:2]:
                    if node[0] == 'S':
                        self.add_switch(node)
                    else:
                        self.add_host(node)
                link_options: Dict = dict(bw=int(cols[2]), delay=f"{cols[3]}ms", loss=float(cols[4]))
                self.add_link(cols[0], cols[1], link_options)

    def slice(self, source: str, destination: str, port: int, duration: int, bw: float) -> None:
        src: Host = self.network.get(source)
        dst: Host = self.network.get(destination)
        if src and dst:
            dst.cmd(f"iperf3 -s -p {port} -i 1 &")
            src.cmd(f"iperf3 -c {dst.IP()} -p {port} -t {duration} -b {bw}M -J >& /home/volume/{source}_{destination}.log &")
            sleep(1)
