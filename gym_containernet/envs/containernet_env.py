from topology_manager import TopologyManager, DOCKER_VOLUME

import datetime
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
from typing import Dict, List, Tuple


CLIENT_SERVER_PAIRS = [('BS1', 'CU1'), ('BS1', 'CU2'), ('BS1', 'CU3'), ('BS1', 'CU4'),
                       ('BS2', 'CU1'), ('BS2', 'CU2'), ('BS2', 'CU3'), ('BS2', 'CU4'),
                       ('BS3', 'CU1'), ('BS3', 'CU2'), ('BS3', 'CU3'), ('BS3', 'CU4'),
                       ('BS4', 'CU1'), ('BS4', 'CU2'), ('BS4', 'CU3'), ('BS4', 'CU4')]
ELASTIC_ARRIVAL_AVERAGE = 5
INELASTIC_ARRIVAL_AVERAGE = 10
DURATION_AVERAGE = 15


def json_from_log(client: str, server: str) -> Dict:
    data: Dict = {}
    while not data:
        try:
            with open(f"{DOCKER_VOLUME}/{client}_{server}.log", 'r') as f:
                data = json.load(f)
        except (FileNotFoundError, json.decoder.JSONDecodeError):
            sleep(0.2)
    return data


def evaluate_elastic_slice(client: str, server: str, bw: float, price: float, data: Dict) -> float:
    average_bitrate: float = data["end"]["streams"][0]["sender"]["bits_per_second"] / 1000000.0
    if average_bitrate >= bw:
        # print(f"--- REQUEST: Elastic | {average_bitrate} >= {bw} | {client} -> {server}")
        return 0.0

    # print(f"--- REQUEST: Elastic | {average_bitrate} < {bw} | {client} -> {server}")
    return - price / 4


def evaluate_inelastic_slice(client: str, server: str, bw: float, price: float, data: Dict) -> float:
    worst_bitrate = min(interval["streams"][0]["bits_per_second"] for interval in data["intervals"]) / 1000000.0
    if worst_bitrate >= bw:
        # print(f"--- REQUEST: Inelastic | {worst_bitrate} >= {bw} | {client} -> {server}")
        return 0.0

    # print(f"--- REQUEST: Inelastic | {worst_bitrate} < {bw} | {client} -> {server}")
    return - price / 2


class ContainernetEnv(Env):
    def __init__(self):
        self.backend: TopologyManager = TopologyManager()

        self.observation_space: Box = Box(low=np.zeros(6, dtype=np.float32), high=np.full(6, 100.0, dtype=np.float32), dtype=np.float32)
        self.action_space: Discrete = Discrete(2)
        self.state: np.ndarray = np.array([0.0, 0.0, 0.0, 0.0, 0.0, 0.0], dtype=np.float32)  # (n_elastic, n_inelastic, type, duration, bw, price/s)

        self.semaphore = False
        self.requests: int = 0
        self.max_requests: int = 16
        self.requests_queue: Queue = Queue(maxsize=self.max_requests)

        self.active_ports: List[int] = []
        self.active_pairs: List[Tuple[str, str]] = []
        self.active_pairs_server_socket: socket.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.active_pairs_server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.active_pairs_server_socket.bind(('127.0.0.1', 6654))
        self.active_pairs_server_socket.listen()
        self.active_pairs_connection, _ = self.active_pairs_server_socket.accept()
        sleep(5)  # give the controller time to build starting paths

    def reset(self) -> object:
        self.backend.clear_logs()
        self.state = np.array([0.0, 0.0, 0.0, 0.0, 0.0, 0.0], dtype=np.float32)

        self.semaphore = True
        self.requests = 0
        self.requests_queue: Queue = Queue(maxsize=self.max_requests)

        self.active_ports: List[int] = []
        self.active_pairs: List[Tuple[str, str]] = []
        self.active_pairs_connection.sendall(len('none').to_bytes(4, byteorder))
        self.active_pairs_connection.sendall('none'.encode('utf-8'))

        Thread(target=self.request_generator, args=(1,)).start()
        Thread(target=self.request_generator, args=(2,)).start()

        print(self.state)
        return self.state

    def step(self, action) -> (object, float, bool, dict):
        # print(datetime.datetime.now().strftime("\n%H:%M:%S:%f"))
        sleep(1)
        request = self.requests_queue.get(block=True)

        self.state = list(self.state)  # convert ndarray to list
        self.state[2] = request["type"]
        self.state[3] = request["duration"]
        self.state[4] = request["bw"]
        self.state[5] = request["price"]

        reward: float = 0.0
        done: bool = False

        if self.state[2] == 0:  # slice departure
            self.state[request["departed"]["type"] - 1] -= 1
            reward += request["departed"]["reward"]

            self.active_pairs.remove((request["departed"]["client"], request["departed"]["server"]))
            # print(f"Active pairs: {self.active_pairs}")

            data: str = ','.join(f"{pair[0]}_{pair[1]}" for pair in self.active_pairs) if len(self.active_pairs) > 0 else 'none'
            self.active_pairs_connection.sendall(len(data).to_bytes(4, byteorder))
            self.active_pairs_connection.sendall(data.encode('utf-8'))

            if self.requests >= self.max_requests and len(self.active_pairs) == 0:
                done = True
                #print("Finishing episode...")
        else:
            self.requests += 1
            if action == 1 and len(self.active_pairs) < len(CLIENT_SERVER_PAIRS):
                client, server = random.choice([pair for pair in CLIENT_SERVER_PAIRS if pair not in self.active_pairs])
                print(f"Accepted request")
                reward: float = self.state[3] * self.state[5]
                self.create_slice(client, server, reward)

                if self.state[2] == 1:
                    self.state[0] += 1
                elif self.state[2] == 2:
                    self.state[1] += 1
            else:
                print("Rejected request")
                if self.requests >= self.max_requests and len(self.active_pairs) == 0:  # for when all requests are rejected
                    done = True
                    # print("Finishing episode...")

        print(np.array(self.state, dtype=np.float32))
        return np.array(self.state, dtype=np.float32), reward, done, {}

    def render(self, mode='human') -> None:
        pass

    def create_slice(self, client: str, server: str, price: float) -> None:
        self.active_pairs += [(client, server)]
        data: str = ','.join(f"{pair[0]}_{pair[1]}" for pair in self.active_pairs)
        self.active_pairs_connection.sendall(len(data).to_bytes(4, byteorder))
        self.active_pairs_connection.sendall(data.encode('utf-8'))
        # print(f"Active pairs: {self.active_pairs}")
        port: int = random.choice([port for port in range(1024, 2049) if port not in self.active_ports])
        self.active_ports += [port]
        self.backend.slice(client, server, port, self.state[3], self.state[4])
        Thread(target=self.slice_evaluator, args=(client, server, self.state[2], self.state[3], self.state[4], price)).start()

    def request_generator(self, type: int) -> None:
        if type not in [1, 2]:
            return
        average: float = ELASTIC_ARRIVAL_AVERAGE if type == 1 else INELASTIC_ARRIVAL_AVERAGE
        while self.requests < self.max_requests:
            arrival: float = np.random.poisson(average)
            sleep(arrival)
            duration: int = ceil(np.random.exponential(DURATION_AVERAGE))
            bw: float = np.random.uniform(low=1.0, high=100.0)
            price: float = np.random.uniform(low=0.01, high=0.5) if type == 1 else np.random.uniform(low=0.5, high=1.0)
            if self.requests < self.max_requests:  # ensures req isn't created if semaphore switched inside loop
                # print(f"+++ REQUEST ({self.requests + 1}): {'Elastic' if type == 1 else 'Inelastic'} | {duration}s | {bw:.3f} Mb/s | {price:.2f} euros/s")
                self.requests_queue.put(dict(type=type, duration=duration, bw=bw, price=price, departed={}))

    def slice_evaluator(self, client: str, server: str, type: int, duration: int, bw: float, price: float) -> None:
        if type not in [1, 2]:
            return
        sleep(duration)
        data: Dict = json_from_log(client, server)
        reward: float = evaluate_elastic_slice(client, server, bw, price, data) if type == 1 else evaluate_inelastic_slice(client, server, bw, price, data)
        departed: Dict = dict(type=1 if type == 1 else 2, reward=reward, client=client, server=server)
        self.requests_queue.put(dict(type=0, duration=0, bw=0.0, price=0.0, departed=departed))
