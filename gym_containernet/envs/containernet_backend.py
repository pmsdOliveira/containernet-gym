from mininet.net import Containernet
from mininet.node import Controller, Host, OVSSwitch, OVSKernelSwitch
from mininet.link import TCLink, Link

import os


class ContainernetBackEnd(object):
    def create_host(self, name: str, mac: str, ip: str, image: str, volumes: list) -> Host:
        os.system('sudo docker rm -f mn.%s' % name)
        return self.net.addDocker(name=name, mac=mac, ip=ip, dimage=image, volumes=volumes)

    def create_switch(self, name: str, protocol: str) -> OVSSwitch:
        return self.net.addSwitch(name=name, cls=OVSKernelSwitch, protocols=protocol)

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

    def __init__(self):
        self.net = Containernet(topo=None, listenPort=6633, ipBase='10.0.0.0/8', controller=Controller)
        self.net.addController('c0')
        # self.create_host_switch_links(host_switch_links)
        # self.create_switch_switch_links(switch_switch_links)
        # self.net.start()
        # self.create_flows(switch_flows)
