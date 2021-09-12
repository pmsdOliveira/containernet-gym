from topology_manager import TopologyManager, DOCKER_VOLUME
from template_generator import DURATION_TEMPLATES

from bisect import bisect_left
from gym import Env
from gym.spaces import Box, Discrete
import json
from math import ceil
import numpy as np
from queue import Queue
import random
import socket
from sys import byteorder
from threading import Thread
from time import sleep
from typing import Dict, List


ELASTIC_ARRIVAL_AVERAGE = 5
INELASTIC_ARRIVAL_AVERAGE = 10
DURATION_AVERAGE = 15

BASE_STATIONS = 4
COMPUTING_STATIONS = 4
PATHS = 4

CONNECTIONS_OFFSET = 4 + BASE_STATIONS * COMPUTING_STATIONS
STATE_DIM = 6 + BASE_STATIONS * COMPUTING_STATIONS * (1 + PATHS)


def closest(values: List, number: float) -> int:
    pos: int = bisect_left(values, number)
    if pos == 0:
        return values[0]
    if pos == len(values):
        return values[-1]
    before: int = values[pos - 1]
    after: int = values[pos]
    if after - number < number - before:
        return after
    return before


def json_from_log(client: str, server: str) -> Dict:
    data: Dict = {}
    while not data:
        try:
            with open(f"{DOCKER_VOLUME}/{client}_{server}.log", 'r') as f:
                data = json.load(f)
        except (FileNotFoundError, json.decoder.JSONDecodeError):
            sleep(0.1)
    return data


def evaluate_elastic_slice(bw: float, price: float, data: Dict) -> float:
    average_bitrate: float = data["end"]["streams"][0]["sender"]["bits_per_second"] / 1000000.0
    if average_bitrate >= bw - bw * 0.05:
        print(f"Finished elastic slice {average_bitrate} >= {bw}")
        return 0.0
    print(f"Failed elastic slice {average_bitrate} < {bw}")
    return - price / 2


def evaluate_inelastic_slice(bw: float, price: float, data: Dict) -> float:
    worst_bitrate = min(interval["streams"][0]["bits_per_second"] for interval in data["intervals"]) / 1000000.0
    if worst_bitrate >= bw - bw * 0.05:
        print(f"Finished inelastic slice {worst_bitrate} >= {bw}")
        return 0.0
    print(f"Finished inelastic slice {worst_bitrate} < {bw}")
    return - price


class SliceAdmissionEnv(Env):
    def __init__(self):
        self.backend: TopologyManager = TopologyManager()

        self.observation_space: Box = Box(low=np.zeros(STATE_DIM, dtype=np.float32), high=np.full(STATE_DIM, 1000.0, dtype=np.float32),
                                          dtype=np.float32)
        self.action_space: Discrete = Discrete(2)
        self.state: np.ndarray = np.zeros(STATE_DIM, dtype=np.float32)

        self.bottlenecks: List[float] = []

        self.requests: int = 0
        self.max_requests: int = 12
        self.requests_queue: Queue = Queue(maxsize=self.max_requests)
        self.active_requests_queue: Queue = Queue(maxsize=self.max_requests)
        self.departed_info_queue: Queue = Queue(maxsize=self.max_requests)

        self.elastic_request_templates = []
        self.inelastic_request_templates = []
        with open('request_templates.txt', 'r') as request_templates:
            for template in request_templates.readlines()[2:]:
                slice_type, duration, bw, price = template.split()
                if slice_type == 'e':
                    self.elastic_request_templates += [(int(duration), float(bw), float(price))]
                else:
                    self.inelastic_request_templates += [(int(duration), float(bw), float(price))]

        self.active_ports: List[int] = []

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as bottlenecks_socket:
            bottlenecks_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            bottlenecks_socket.bind(('127.0.0.1', 6654))
            bottlenecks_socket.listen()
            self.bottlenecks_connection, _ = bottlenecks_socket.accept()
            Thread(target=self.receive_bottlenecks).start()

        sleep(10)  # give the controller time to build starting paths

    def reset(self) -> object:
        self.backend.clear_logs()
        self.state = np.zeros(86, dtype=np.float32)

        self.requests = 0
        self.requests_queue = Queue(maxsize=self.max_requests)
        self.active_requests_queue = Queue(maxsize=self.max_requests)
        self.departed_info_queue = Queue(maxsize=self.max_requests)

        self.active_ports = []

        Thread(target=self.request_generator, args=(1,)).start()
        Thread(target=self.request_generator, args=(2,)).start()

        request = self.requests_queue.get(block=True)
        self.state[0] = request["type"]
        self.state[1] = request["duration"]
        self.state[2] = request["bw"]
        self.state[3] = request["price"]
        self.state[4:CONNECTIONS_OFFSET] = request["connections"]
        self.state[CONNECTIONS_OFFSET + 2:] = self.bottlenecks

        print(f'Request: {"elastic" if self.state[0] == 1 else "inelastic"}, {self.state[1]}s, {self.state[2]}Mb/s, {self.state[3]}€/s')
        return self.state

    def step(self, action) -> (object, float, bool, dict):
        reward: float = 0.0
        done: bool = False

        if self.state[0] != 0:  # slice arrival
            self.requests += 1
            print(action)
            if action == 1:
                print(f"Accepted request")

                connections = self.state[4:CONNECTIONS_OFFSET]
                parsed_connections = [connections[i:i + BASE_STATIONS] for i in range(0, len(connections), BASE_STATIONS)]
                for bs_idx, base_station in enumerate(parsed_connections):
                    for cs_idx, connected in enumerate(base_station):
                        if connected:
                            self.create_slice(f'BS{bs_idx + 1}', f'CS{cs_idx + 1}')

                if self.state[0] == 1:  # elastic slice
                    self.state[CONNECTIONS_OFFSET] += 1
                elif self.state[0] == 2:  # inelastic slice
                    self.state[CONNECTIONS_OFFSET + 1] += 1

                reward: float = self.state[1] * self.state[3]

            else:
                print("Rejected request")
                if self.requests >= self.max_requests:  # if all requests are rejected
                    done = True

        else:  # slice departure
            departed = self.departed_info_queue.get()
            self.state[CONNECTIONS_OFFSET + departed["type"] - 1] -= 1
            reward += departed["reward"]

            if self.requests >= self.max_requests:
                done = True

        if self.requests < self.max_requests:
            request = self.requests_queue.get(block=True)
            self.state[0] = request["type"]
            self.state[1] = request["duration"]
            self.state[2] = request["bw"]
            self.state[3] = request["price"]
            self.state[4:CONNECTIONS_OFFSET] = request["connections"]
            self.state[CONNECTIONS_OFFSET + 2:] = self.bottlenecks
        else:
            done = True

        print(f'Request: {"elastic" if self.state[0] == 1 else "inelastic"}, {self.state[1]}s, {self.state[2]}Mb/s, {self.state[3]}€/s\n'
              f'Elastic/inelastic slices: {self.state[CONNECTIONS_OFFSET]}/{self.state[CONNECTIONS_OFFSET + 1]}')
        return self.state, reward, done, {}

    def render(self, mode='human') -> None:
        pass

    def receive_bottlenecks(self) -> None:
        while True:
            bottlenecks = []
            size = int.from_bytes(self.bottlenecks_connection.recv(16), byteorder=byteorder)
            data = self.bottlenecks_connection.recv(size).decode('utf-8')
            for line in data.split('\n')[:-1]:
                bottlenecks += [float(bottleneck) for bottleneck in line.split(',')]
            self.bottlenecks = bottlenecks

    def create_slice(self, client: str, server: str) -> None:
        port: int = random.choice([port for port in range(1024, 2049) if port not in self.active_ports])
        self.active_ports += [port]
        self.backend.slice(client, server, port, self.state[1], self.state[2])
        Thread(target=self.slice_evaluator,
               args=(client, server, self.state[0], self.state[1], self.state[2], self.state[1] * self.state[3])).start()

    def request_generator(self, slice_type: int) -> None:
        if slice_type not in [1, 2]:
            return

        while self.requests < self.max_requests:
            arrival: float = np.random.poisson(ELASTIC_ARRIVAL_AVERAGE if slice_type == 1 else INELASTIC_ARRIVAL_AVERAGE)
            sleep(arrival)

            if self.requests < self.max_requests:  # ensures req isn't created if new req is created while inside loop
                duration: int = closest(DURATION_TEMPLATES, np.random.exponential(DURATION_AVERAGE))

                if slice_type == 1:
                    _, bw, price = random.choice([template for template in self.elastic_request_templates if template[0] == duration])
                else:
                    _, bw, price = random.choice([template for template in self.inelastic_request_templates if template[0] == duration])

                number_connections = min(ceil(np.random.exponential(BASE_STATIONS / 2)), BASE_STATIONS)
                base_stations = random.sample(range(BASE_STATIONS), number_connections)
                computing_stations = random.sample(range(COMPUTING_STATIONS), number_connections)

                connections = np.zeros((BASE_STATIONS, COMPUTING_STATIONS))
                for (bs, cs) in zip(base_stations, computing_stations):
                    connections[bs][cs] = 1

                self.requests_queue.put(dict(type=slice_type, duration=int(duration), bw=float(bw),
                                             price=float(price), connections=connections.flatten()))

    def slice_evaluator(self, client: str, server: str, slice_type: int, duration: int, bw: float, price: float) -> None:
        if slice_type not in [1, 2]:
            return
        sleep(duration)
        data: Dict = json_from_log(client, server)
        reward: float = evaluate_elastic_slice(bw, price, data) if slice_type == 1 else evaluate_inelastic_slice(bw, price, data)
        self.departed_info_queue.put(dict(type=1 if slice_type == 1 else 2, reward=reward, client=client, server=server))
        self.requests_queue.put(dict(type=0, duration=0, bw=0.0, price=0.0))

    # NOTA: os container podem ser usados no futuro para client-server apps por exemplo
    # TODO: pré-computar 4 caminhos mais curtos entre cada par
    # TODO: um iperf ter uma LB flutuante
