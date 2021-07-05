from mininet.net import Containernet
from mininet.node import Controller, Host, OVSSwitch, OVSKernelSwitch
from mininet.link import TCLink, Link

import os

# current image: iperf:latest, can change to ubuntu:trusty
DOCKER_VOLUMES = ["/home/pmsdoliveira/workspace/containers/vol1/:/home/vol1"]


class ContainernetBackEnd(object):
    def add_host(self, name: str, mac: str) -> Host:
        os.system('sudo docker rm -f mn.%s' % name)
        return self.net.addDocker(name=name, mac=mac, dimage="iperf:latest", volumes=DOCKER_VOLUMES)

    def add_switch(self, name: str) -> OVSSwitch:
        return self.net.addSwitch(name=name, cls=OVSKernelSwitch, protocols="OpenFlow13")

    def create_host_switch_links(self, host_switch_links: list) -> None:
        for (host_idx, switch_idx) in enumerate(host_switch_links):
            self.net.addLink(self.hosts[host_idx], self.switches[switch_idx], cls=Link)

    def create_switch_switch_links(self, switch_switch_links: dict) -> None:
        for (switch_idx, neighbours) in enumerate(switch_switch_links):
            for neighbour_idx in neighbours:
                self.net.addLink(self.switches[switch_idx], self.switches[neighbour_idx], cls=TCLink)

    def create_flows(self, switch_flows: dict) -> None:
        for (switch_idx, switch) in enumerate(switch_flows):
            for flow in switch:
                cmd = 'ovs-ofctl --protocols=OpenFlow13 add-flow  s%s priority=%s,' % (switch_idx + 1, flow['prio'])
                details = ''
                if flow['type'] == 'edge':
                    for package in ['ip', 'arp']:
                        details = '%s,nw_dst=%s,actions=output:%s' \
                                  % (package, flow['dst'], flow['actions'])
                        self.switches[switch_idx].cmd(cmd + details)
                elif flow['type'] == 'core':
                    details = 'in_port=%s,actions=output:%s' \
                              % (flow['in_port'], flow['actions'])
                    self.switches[switch_idx].cmd(cmd + details)

    def __init__(self, graph: dict = None):
        self.net: Containernet = Containernet(topo=None, listenPort=6633, ipBase='10.0.0.0/8', controller=Controller)
        self.net.addController('c0')

        self.graph: dict[str, str] = {}
        if graph is None:
            return

        self.graph = graph

        for vertex_name in self.graph:
            if vertex_name[0] == 'h':
                self.add_host(vertex_name,
                              "00:00:00:00:%s" % hex(int(vertex_name[1:])).zfill(2))
            elif vertex_name[0] == 's':
                self.add_switch(vertex_name)

        for vertex in self.graph:
            if vertex[0] == 'h':
                origin: Host = self.net.hosts[int(vertex[1:]) - 1]
                destination: OVSSwitch = self.net.switches[int(self.graph[vertex][0][1:]) - 1]
                if not self.net.linksBetween(origin, destination):
                    self.net.addLink(origin, destination)
            elif vertex[0] == 's':
                origin: OVSSwitch = self.net.switches[int(vertex[1:]) - 1]
                destinations: list[OVSSwitch]
                for destination in self.graph[vertex]:
                    destinations.append(self.net.switches[int(destination[1:]) - 1])
        print(self.net.links)


        # self.create_host_switch_links(host_switch_links)
        # self.create_switch_switch_links(switch_switch_links)
        # self.net.start()
        # self.create_flows(switch_flows)


if __name__ == '__main__':
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

    be = ContainernetBackEnd(graph_description)

