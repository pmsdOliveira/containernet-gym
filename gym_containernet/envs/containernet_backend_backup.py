from mininet.net import Containernet
from mininet.node import Controller, Host, OVSSwitch, OVSKernelSwitch
from mininet.link import TCLink, Link
from mininet.log import setLogLevel

import os

N_HOSTS = 4  # hosts from h1 to hN
N_SWITCHES = 4  # switches from s1 to sN
HOST_SWITCH_LINKS = [0, 1, 0, 1]  # host index connects to switch value
SWITCH_SWITCH_LINKS = [[2, 3], [2, 3]]  # switch index connects to list of switches
SWITCH_FLOWS = [
    [
        {'type': 'edge', 'prio': 10, 'dst': '10.0.0.1', 'actions': '1'},
        {'type': 'edge', 'prio': 10, 'dst': '10.0.0.2', 'actions': '4'},
        {'type': 'edge', 'prio': 10, 'dst': '10.0.0.3', 'actions': '2'},
        {'type': 'edge', 'prio': 10, 'dst': '10.0.0.4', 'actions': '4'}
    ],
    [
        {'type': 'edge', 'prio': 10, 'dst': '10.0.0.1', 'actions': '4'},
        {'type': 'edge', 'prio': 10, 'dst': '10.0.0.2', 'actions': '1'},
        {'type': 'edge', 'prio': 10, 'dst': '10.0.0.4', 'actions': '2'},
        {'type': 'edge', 'prio': 10, 'dst': '10.0.0.3', 'actions': '4'}
    ],
    [
        {'type': 'core', 'prio': 10, 'in_port': 1, 'actions': '2'},
        {'type': 'core', 'prio': 10, 'in_port': 2, 'actions': '1'}
    ],
    [
        {'type': 'core', 'prio': 10, 'in_port': 1, 'actions': '2'},
        {'type': 'core', 'prio': 10, 'in_port': 2, 'actions': '1'}
    ]
]


class ContainernetBackEnd(object):
    def create_host(self, h: int) -> Host:
        os.system('sudo docker rm -f mn.h%s' % (h + 1))
        return self.net.addDocker('h%s' % (h + 1), mac='00:00:00:00:00:0%s' % (h + 1), ip='10.0.0.%s' % (h + 1),
                                  dimage='iperf:latest',
                                  volumes=["/home/pmsdoliveira/workspace/containers/vol1/:/home/vol1"])

    def create_switch(self, s: int) -> OVSSwitch:
        return self.net.addSwitch('s%s' % (s + 1), cls=OVSKernelSwitch, protocols='OpenFlow13')

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
                print("Switch %s: %s%s" % (switch_idx + 1, cmd, details))

    def __init__(self, n_hosts, n_switches, host_switch_links, switch_switch_links, switch_flows):
        self.net = Containernet(topo=None, listenPort=6633, ipBase='10.0.0.0/8', controller=Controller)
        self.net.addController('c0')
        self.hosts = [self.create_host(h) for h in range(n_hosts)]
        self.switches = [self.create_switch(s) for s in range(n_switches)]
        self.create_host_switch_links(host_switch_links)
        self.create_switch_switch_links(switch_switch_links)
        self.net.start()
        self.create_flows(switch_flows)


if __name__ == '__main__':
    os.system('sudo mn -c')
    setLogLevel('info')
    be = ContainernetBackEnd(n_hosts=N_HOSTS, n_switches=N_SWITCHES, host_switch_links=HOST_SWITCH_LINKS,
                             switch_switch_links=SWITCH_SWITCH_LINKS, switch_flows=SWITCH_FLOWS)
    be.net.pingAll()
    be.net.stop()
