from topology_manager import TopologyManager, DOCKER_VOLUME

from bisect import bisect_left
from gym import Env
from gym.spaces import Box, Discrete
import json
import numpy as np
from queue import Queue
import random
import socket
from sys import byteorder
from threading import Thread
from time import sleep, time
from typing import Dict, List

from parameters import BASE_STATIONS, COMPUTING_STATIONS, PATHS, CONNECTIONS_OFFSET, INPUT_DIM, OUTPUT_DIM
from parameters import ELASTIC_ARRIVAL_AVERAGE, INELASTIC_ARRIVAL_AVERAGE, DURATION_AVERAGE, CONNECTIONS_AVERAGE
from parameters import MAX_REQUESTS, STARTUP_TIME, LOG_TIMEOUT, PORT_RANGE


def read_templates(file: str) -> (List[Dict], List[Dict]):
    elastic: List[Dict] = []
    inelastic: List[Dict] = []
    with open(file, 'r') as request_templates:
        for template in request_templates.readlines():
            slice_type, bw, price = template.split()
            if slice_type == 'e':
                elastic += [(float(bw), float(price))]
            else:
                inelastic += [(float(bw), float(price))]
    return elastic, inelastic


def slice_connections_from_array(connections: List) -> (List[str], List[str]):
    parsed_connections = [connections[i:i + COMPUTING_STATIONS] for i in range(0, len(connections), COMPUTING_STATIONS)]

    clients: List[str] = []
    servers: List[str] = []
    for bs_idx, base_station in enumerate(parsed_connections):
        for cs_idx, connected in enumerate(base_station):
            if connected:
                clients += [f'BS{bs_idx + 1}']
                servers += [f'{"MECS" if cs_idx < COMPUTING_STATIONS // 2 else "CS"}'
                            f'{cs_idx + 1 if (cs_idx < COMPUTING_STATIONS // 2) else (cs_idx - COMPUTING_STATIONS // 2 + 1)}']
    return clients, servers


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


def json_from_log(client: str, server: str, port: int) -> Dict:
    data: Dict = {}
    start_time: time = time()
    current_time: time = time()
    while not data and current_time - start_time < LOG_TIMEOUT:
        try:
            with open(f"{DOCKER_VOLUME}/{client}_{server}_{port}.log", 'r') as f:
                data = json.load(f)
        except (FileNotFoundError, json.decoder.JSONDecodeError):
            sleep(0.2)
        current_time = time()
    return data


def evaluate_elastic_slice(bw: float, full_price: float, data: List[Dict]) -> float:
    averages: List[float] = [connection["end"]["streams"][0]["receiver"]["bits_per_second"] / 1000000.0 for connection in data]
    total_average: float = sum(averages) / len(averages)
    if total_average >= bw - bw * .1:
        print(f"Finished elastic slice {total_average} >= {bw}")
        return 0.0
    print(f"Failed elastic slice {total_average} < {bw}")
    return - full_price / 2


def evaluate_inelastic_slice(bw: float, price: float, data: List[Dict]) -> float:
    worst: float = min(interval["streams"][0]["bits_per_second"] / 1000000.0 for connection in data for interval in connection["intervals"])
    if worst >= bw - bw * .1:
        print(f"Finished inelastic slice {worst} >= {bw}")
        return 0.0
    print(f"Finished inelastic slice {worst} < {bw}")
    return - price


class SliceAdmissionEnv(Env):
    def __init__(self):
        self.backend: TopologyManager = TopologyManager()

        low = np.zeros(INPUT_DIM, dtype=np.float32)
        high = np.array([2.0, 60.0, 100.0, 2.0] + [1.0] * BASE_STATIONS * COMPUTING_STATIONS +
                        [float(MAX_REQUESTS)] * 2 + [750.0] * BASE_STATIONS * COMPUTING_STATIONS * PATHS,
                        dtype=np.float32)
        self.observation_space: Box = Box(low=low, high=high, dtype=np.float32)
        self.action_space: Discrete = Discrete(OUTPUT_DIM)
        self.state: np.ndarray = np.zeros(INPUT_DIM, dtype=np.float32)

        self.requests: int = 0
        self.requests_queue: Queue = Queue(maxsize=MAX_REQUESTS)
        self.departed_queue: Queue = Queue(maxsize=MAX_REQUESTS)

        self.generator_semaphore: bool = False
        self.elastic_generator = None
        self.inelastic_generator = None
        self.evaluators: List[Thread] = []

        self.elastic_request_templates: List[Dict] = []
        self.inelastic_request_templates: List[Dict] = []
        self.elastic_request_templates, self.inelastic_request_templates = read_templates("request_templates.txt")

        self.active_ports: List[int] = []
        self.active_connections: List[str] = []
        self.active_paths: List[int] = BASE_STATIONS * COMPUTING_STATIONS * [-1]
        self.bottlenecks: List[float] = []

        self.paths_socket: socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as bottlenecks_socket:
            bottlenecks_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            bottlenecks_socket.bind(('127.0.0.1', 6654))
            bottlenecks_socket.listen()
            self.bottlenecks_connection, _ = bottlenecks_socket.accept()
            Thread(target=self.receive_bottlenecks).start()

        sleep(STARTUP_TIME)  # give the controller time to build starting paths
        self.paths_socket.connect(('127.0.0.1', 6655))

    def reset(self) -> object:
        self.backend.clear_logs()
        self.state = np.zeros(INPUT_DIM, dtype=np.float32)

        self.requests = 0
        self.requests_queue = Queue(maxsize=MAX_REQUESTS)
        self.departed_queue = Queue(maxsize=MAX_REQUESTS)

        self.active_ports = []
        self.active_paths = BASE_STATIONS * COMPUTING_STATIONS * [-1]
        self.active_connections = []

        self.generator_semaphore = True
        self.elastic_generator = Thread(target=self.request_generator, args=(1, ))
        self.inelastic_generator = Thread(target=self.request_generator, args=(2, ))
        self.elastic_generator.start()
        self.inelastic_generator.start()
        self.evaluators = []

        self.send_paths()
        self.state_from_request(self.requests_queue.get(block=True))

        # print(self.state)
        return self.state

    def step(self, action) -> (object, float, bool, dict):
        reward: float = 0.0
        done: bool = False

        if self.state[0]:
            self.requests += 1
            if action:
                print(f"ACCEPT")
                self.create_slice(*slice_connections_from_array(self.state[4:CONNECTIONS_OFFSET]))
                if self.state[0] == 1:  # elastic slice
                    self.state[CONNECTIONS_OFFSET] += 1
                elif self.state[0] == 2:  # inelastic slice
                    self.state[CONNECTIONS_OFFSET + 1] += 1
                reward = self.state[1] * self.state[3]
            else:
                print("REJECT")

        if self.requests < MAX_REQUESTS:
            self.state_from_request(self.requests_queue.get(block=True))
            if self.state[0] == 0:  # slice departure
                departure = self.departed_queue.get()
                self.state[CONNECTIONS_OFFSET + departure["type"] - 1] -= 1
                reward += departure["reward"]
        else:
            if self.generator_semaphore:
                self.stop_generators()
            for evaluator in self.evaluators:
                if evaluator.is_alive():  # might get stuck if a second evaluator finishes before this one
                    evaluator.join()
                    reward += self.state_from_departure(self.departed_queue.get())
                    # print(self.state)
                    return self.state, reward, done, {}
            while not self.departed_queue.empty():  # prevent the previous error
                reward += self.state_from_departure(self.departed_queue.get())
                # print(self.state)
                return self.state, reward, done, {}
            done = True

        # print(self.state)
        return self.state, reward, done, {}

    def render(self, mode='human') -> None:
        pass

    def receive_bottlenecks(self) -> None:
        while True:
            bottlenecks = []
            size = int.from_bytes(self.bottlenecks_connection.recv(16), byteorder=byteorder)
            try:
                data = self.bottlenecks_connection.recv(size).decode('utf-8').split('\n')[:-1]
                if len(data) == BASE_STATIONS * COMPUTING_STATIONS:
                    for line in data:
                        bottlenecks += [float(bottleneck) for bottleneck in line.split(',')]
                    self.bottlenecks = bottlenecks
            except OverflowError:
                print("\n\n\n\n\n\t\t\t\t\t\t\t\tOVERFLOW ERROR\n\n\n\n\n")
            except MemoryError:
                print("\n\n\n\n\n\t\t\t\t\t\t\t\tMEMORY ERROR\n\n\n\n\n")

    def send_paths(self) -> None:
        data: str = ','.join(str(path) for path in self.active_paths)
        self.paths_socket.sendall(len(data).to_bytes(16, byteorder))
        self.paths_socket.sendall(data.encode('utf-8'))

    def state_from_request(self, request: Dict) -> None:
        self.state[0] = request["type"]
        self.state[1] = request["duration"]
        self.state[2] = request["bw"]
        self.state[3] = request["price"]
        self.state[4:CONNECTIONS_OFFSET] = request["connections"]
        self.state[CONNECTIONS_OFFSET + 2:] = self.bottlenecks

    def state_from_departure(self, departure: Dict) -> float:
        self.state[:CONNECTIONS_OFFSET] = np.zeros(4 + BASE_STATIONS * COMPUTING_STATIONS, dtype=np.float32)
        self.state[CONNECTIONS_OFFSET + departure["type"] - 1] -= 1
        self.state[CONNECTIONS_OFFSET + 2:] = self.bottlenecks
        return departure["reward"]

    def create_slice(self, clients: List[str], servers: List[str]) -> None:
        ports: List[int] = []
        for (client, server) in zip(clients, servers):
            connection_idx: int = (int(client[2:]) - 1) * COMPUTING_STATIONS + \
                                  (int(server[4:]) - 1 if server[0] == 'M' else int(server[2:]) + BASE_STATIONS - 1)
            bottleneck_idx: int = connection_idx * BASE_STATIONS
            self.active_paths[connection_idx] = np.argmax(self.bottlenecks[bottleneck_idx:bottleneck_idx + PATHS]) \
                if self.active_paths[connection_idx] == -1 else self.active_paths[connection_idx]

        self.send_paths()

        for (client, server) in zip(clients, servers):
            port: int = random.choice([port for port in range(*PORT_RANGE) if port not in self.active_ports])
            ports += [port]
            self.active_ports += [port]
            self.active_connections += [f'{client}_{server}_{port}']
            self.backend.slice(client, server, port, self.state[1], self.state[2])

        evaluator = Thread(target=self.slice_evaluator,
                           args=(clients, servers, ports, self.state[0], self.state[1], self.state[2], self.state[1] * self.state[3]))
        self.evaluators += [evaluator]
        evaluator.start()

    def stop_generators(self) -> None:
        self.generator_semaphore = False
        if self.elastic_generator.is_alive():
            self.elastic_generator.join()
        if self.inelastic_generator.is_alive():
            self.inelastic_generator.join()

    def request_generator(self, slice_type: int) -> None:
        if slice_type not in [1, 2]:
            return

        while self.generator_semaphore:
            arrival: float = np.random.poisson(ELASTIC_ARRIVAL_AVERAGE if slice_type == 1 else INELASTIC_ARRIVAL_AVERAGE)
            sleep(arrival)

            if self.generator_semaphore:  # ensures req isn't created if new req is created while inside loop
                duration: int = min(max(int(np.random.exponential(DURATION_AVERAGE)), 1), 60)
                bw, price = random.choice(self.elastic_request_templates if slice_type == 1 else self.inelastic_request_templates)

                number_connections = min(max(int(np.random.exponential(CONNECTIONS_AVERAGE)), 1), BASE_STATIONS)
                base_stations = random.sample(range(BASE_STATIONS), number_connections)
                computing_stations = random.sample(range(COMPUTING_STATIONS), number_connections)

                connections = np.zeros((BASE_STATIONS, COMPUTING_STATIONS), dtype=np.float32)
                for (bs, cs) in zip(base_stations, computing_stations):
                    connections[bs][cs] = 1

                self.requests_queue.put(dict(type=slice_type, duration=int(duration), bw=float(bw),
                                             price=float(price), connections=connections.flatten()))

    def slice_evaluator(self, clients: List[str], servers: List[str], ports: List[int], slice_type: int, duration: int, bw: float, price: float
                        ) -> None:
        if slice_type not in [1, 2]:
            return

        sleep(duration)

        data: List[Dict] = []
        for (client, server, port) in zip(clients, servers, ports):
            result = json_from_log(client, server, port)
            if result:
                data += [result]

            self.active_connections.remove(f'{client}_{server}_{port}')
            if not sum(1 for connection in self.active_connections if f'{client}_{server}' in connection):  # if no one else is using this path
                connection_idx: int = (int(client[2:]) - 1) * COMPUTING_STATIONS + \
                                      (int(server[4:]) - 1 if server[0] == 'M' else int(server[2:]) + BASE_STATIONS - 1)
                self.active_paths[connection_idx] = -1

        self.send_paths()

        reward: float = evaluate_elastic_slice(bw, price, data) if slice_type == 1 else evaluate_inelastic_slice(bw, price, data)
        self.departed_queue.put(dict(type=1 if slice_type == 1 else 2, reward=reward))
        self.requests_queue.put(dict(type=0, duration=0, bw=0.0, price=0.0,
                                     connections=np.zeros(BASE_STATIONS * COMPUTING_STATIONS, dtype=np.float32)))
