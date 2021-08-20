from topology_manager import start_containernet, TOPOLOGY_FILE, DOCKER_VOLUME

from mininet.net import Containernet
from mininet.node import Host

from os import system
import random
import socket
import time
from typing import List, Tuple


class TopologyManagerAPI:
    def __init__(self) -> None:
        self.network: Containernet = start_containernet(TOPOLOGY_FILE)

        self.active_pairs: List[Tuple[str, str]] = []
        self.active_ports: List[int] = [6653, 6654]

        self.active_pairs_server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.active_pairs_server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.active_pairs_server_socket.bind(('127.0.0.1', 6654))
        self.active_pairs_server_socket.listen()
        self.active_pairs_connection, _ = self.active_pairs_server_socket.accept()

    def reset(self) -> None:
        system(f"rm -f {DOCKER_VOLUME}/*.log")
        self.active_pairs: List[Tuple[str, str]] = []
        self.active_ports: List[int] = [6653, 6654]
        self.active_pairs_connection.sendall("reset".encode('utf-8'))

    def create_slice(self, source: str, destination: str, duration: int, bw: float) -> None:
        src: Host = self.network.get(source)
        dst: Host = self.network.get(destination)
        if src and dst:
            self.active_pairs += [(source, destination)]
            self.active_pairs_connection.sendall(','.join(f"{pair[0]}_{pair[1]}" for pair in self.active_pairs).encode('utf-8'))

            port: int = random.choice([port for port in range(1024, 10000) if port not in self.active_ports])
            self.active_ports += [port]

            dst.cmd(f"iperf3 -s -p {port} -i 1 &")
            src.cmd(f"iperf3 -c {dst.IP()} -p {port} -t {duration} -b {bw}M -J >& /home/volume/{source}_{destination}.log &")
            time.sleep(1)


if __name__ == '__main__':
    api = TopologyManagerAPI()
    time.sleep(5)
