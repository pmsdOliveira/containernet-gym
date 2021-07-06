from mininet.net import Containernet
from mininet.node import Controller, Host, OVSSwitch, OVSKernelSwitch
from mininet.link import TCLink

from typing import List, Dict, Union
import os, time

# current image: iperf:latest, can change to ubuntu:trusty
DOCKER_VOLUMES = ["/home/pmsdoliveira/workspace/containers/vol1/:/home/vol1"]


class ContainernetBackend(object):
    def add_host(self, name: str, mac: str) -> Host:
        os.system('sudo docker rm -f mn.%s' % name)
        return self.net.addDocker(name=name, mac=mac, dimage="iperf:latest", volumes=DOCKER_VOLUMES)

    def add_switch(self, name: str) -> OVSSwitch:
        return self.net.addSwitch(name=name, cls=OVSKernelSwitch, protocols="OpenFlow13")

    def add_link(self, origin: str, destination: str) -> None:
        origin_idx: int = int(origin[1:]) - 1
        destination_idx: int = int(destination[1:]) - 1
        node1: Union[Host, OVSSwitch]
        node2: Union[Host, OVSSwitch]

        if origin[0] == 'h':
            node1 = self.net.hosts[origin_idx]
            node2 = self.net.switches[destination_idx]
            self.switch_ports[destination_idx].append(origin)
        elif origin[0] == 's':
            node1 = self.net.switches[origin_idx]
            if destination[0] == 'h':
                node2 = self.net.hosts[destination_idx]
            elif destination[0] == 's':
                node2 = self.net.switches[destination_idx]
                self.switch_ports[origin_idx].append(destination)

        if not self.net.linksBetween(node1, node2):
            self.net.addLink(node1, node2, cls=TCLink)

    def find_shortest_path(self, start: str, end: str, path: List[str] = []) -> List[str]:
        if start not in self.graph or end not in self.graph:
            return None
        path = path + [start]
        if start == end:
            return path

        shortest: List[str] = None
        for node in self.graph[start]:
            if node not in path:
                new_path = self.find_shortest_path(node, end, path)
                if new_path:
                    if not shortest or len(new_path) < len(shortest):
                        shortest = new_path
        return shortest

    def __init__(self, graph: dict = None):
        os.system("sudo mn -c")

        self.net: Containernet = Containernet(listenPort=6633, ipBase='10.0.0.0/8', controller=Controller)
        self.net.addController('c0')

        self.graph: Dict[str, str] = {}
        if graph is None:
            return
        self.graph = graph

        for vertex in self.graph:
            if vertex[0] == 'h':
                self.add_host(vertex, "00:00:00:00:%s" % hex(int(vertex[1:])).zfill(2))
            elif vertex[0] == 's':
                self.add_switch(vertex)

        n_hosts: int = len(self.net.hosts)
        n_switches: int = len(self.net.switches)
        print("Created %s hosts and %s switches." % (n_hosts, n_switches))

        self.switch_ports: List[List[str]] = [[] for _ in range(n_switches)]

        for vertex in self.graph:
            for edge in self.graph[vertex]:
                self.add_link(vertex, edge)
        print("Created %s links." % len(self.net.links))

        shortest_paths: List[List[str]] = []
        for origin in range(1, n_hosts + 1):
            for destination in range(origin, n_hosts + 1):
                shortest_paths.append(self.find_shortest_path('h%s' % origin, 'h%s' % destination))

        self.net.start()

        for switch in range(n_switches):
            switch_name: str = 's%s' % (switch + 1)
            cmd: str = 'ovs-ofctl --protocols=OpenFlow13 add-flow  %s priority=10,' % switch_name
            for host in range(n_hosts):
                host_name: str = 'h%s' % (host + 1)
                host_ip: str = self.net.hosts[host].IP()
                output_port: int = None
                if host_name in self.switch_ports[switch]:  # direct connection between host and switch
                    output_port = self.switch_ports[switch].index(host_name) + 1
                else:
                    # get only the shortest paths that have the specific host and switch in them
                    paths = [path for path in shortest_paths if host_name in path and switch_name in path]
                    if len(paths) > 0:
                        path = paths[0]
                        # reverse the path if needed to ensure the specific host is always the path's destination
                        path = path if host_name == path[-1] else path[::-1]
                        next_switch = path[path.index(switch_name) + 1]
                        output_port = self.switch_ports[switch].index(next_switch) + 1
                for package in ['arp', 'ip']:
                    if output_port is not None:
                        details: str = '%s,nw_dst=%s,actions=output:%s' % (package, host_ip, output_port)
                        self.net.switches[switch].cmd(cmd + details)

        self.net.pingAll()


if __name__ == '__main__':
    test = {
        'h1': ['s1'],
        'h2': ['s1'],
        'h3': ['s5'],
        'h4': ['s5'],
        's1': ['h1', 'h2', 's2'],
        's2': ['s1', 's3'],
        's3': ['s2', 's4'],
        's4': ['s3', 's5'],
        's5': ['h3', 'h4', 's4']
    }

    sdwan = {
        'h1': ['s1'],
        'h2': ['s2'],
        'h3': ['s1'],
        'h4': ['s2'],
        's1': ['h1', 'h3', 's3', 's4'],
        's2': ['h2', 'h4', 's3', 's4'],
        's3': ['s1', 's2'],
        's4': ['s1', 's2']
    }

    graph_description = {
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
    be = ContainernetBackend(sdwan)
    print("Time spent: %.2fs" % (time.time() - start_time))

