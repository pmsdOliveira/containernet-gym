from topology_manager_api import TopologyManagerAPI, DOCKER_VOLUME

from gym import Env, spaces
from gym.spaces import Box, Discrete
import json
from math import ceil
import numpy as np
from queue import Queue
import random
import tensorflow as tf
from threading import Thread
from time import sleep
from typing import Dict, Tuple


CLIENT_SERVER_PAIRS = [('BS1', 'CU1'), ('BS2', 'CU2'), ('BS3', 'CU3'), ('BS4', 'CU4')]
ELASTIC_ARRIVAL_AVERAGE = 10
INELASTIC_ARRIVAL_AVERAGE = 20
DURATION_AVERAGE = 5


class ContainernetEnv(Env):
    def __init__(self):
        self.backend = TopologyManagerAPI()

        self.observation_space = spaces.Tuple((Discrete(len(CLIENT_SERVER_PAIRS)), Discrete(len(CLIENT_SERVER_PAIRS)), spaces.Tuple(
            (Discrete(3), Discrete(61), Box(low=1.0, high=100.0, shape=(1,)), Box(low=0.1, high=1.0, shape=(1,))))))
        self.action_space = Discrete(2)
        self.state = tf.nest.flatten((0, 0, (0, 0, 0.0, 0.0)))  # (n_elastic, n_inelastic, (type, duration, bw, price/t))

        self.requests_queue = Queue()
        self.requests = 0
        self.max_requests = 8

        Thread(target=self.request_generator, args=(1,)).start()
        Thread(target=self.request_generator, args=(2,)).start()

    def reset(self) -> object:
        self.backend.reset()
        self.state = tf.nest.flatten((0, 0, (0, 0, 0.0, 0.0)))
        self.requests_queue = Queue()
        self.requests = 0
        return self.state

    def step(self, action) -> (object, float, bool, dict):
        request = self.requests_queue.get(block=True)
        self.state[2] = request["type"]
        self.state[3] = request["duration"]
        self.state[4] = request["bw"]
        self.state[5] = request["price"]

        reward = 0
        done = False
        info = {}

        if self.state[2] == 0:  # slice departure
            self.state[request["departed"] - 1] -= 1
            reward += request["reward"]
        else:
            self.requests += 1
            if action == 1 and len(self.backend.active_pairs) < len(CLIENT_SERVER_PAIRS):
                print("Accepted request")
                client_server = random.choice([pair for pair in CLIENT_SERVER_PAIRS if pair not in self.backend.active_pairs])
                self.backend.create_slice(client_server[0], client_server[1], self.state[3], self.state[4])
                Thread(target=self.slice_evaluator, args=(self.state[2], self.state[3], self.state[4], client_server)).start()

                reward = self.state[3] * self.state[5]  # duration * price/t
                if self.state[2] == 1:
                    self.state[0] += 1
                elif self.state[2] == 2:
                    self.state[1] += 1
            else:
                print("Rejected request")

            if self.requests == self.max_requests:
                done = True
                print("Finishing episode...")
                while len(self.backend.active_pairs):
                    sleep(0.5)

        return self.state, reward, done, info

    def render(self, mode='human') -> None:
        pass

    def request_generator(self, type: int) -> None:
        if type not in [1, 2]:
            return
        while True:
            average: float = ELASTIC_ARRIVAL_AVERAGE if type == 1 else INELASTIC_ARRIVAL_AVERAGE
            arrival: float = np.random.poisson(average)
            sleep(arrival)

            duration: int = ceil(np.random.exponential(DURATION_AVERAGE))
            bw: float = np.random.uniform(low=1.0, high=100.0)
            if type == 1:
                price = np.random.uniform(low=0.01, high=0.5)
            else:
                price = np.random.uniform(low=0.5, high=1.0)
            print(f"{'Elastic' if type == 1 else 'Inelastic'} request lasting {duration} seconds, "
                  f"consuming {bw:.3f} Mb/s and paying {price:.3f} euros/s")
            self.requests_queue.put(dict(type=type, duration=duration, bw=bw, price=price))

    def slice_evaluator(self, type: int, duration: int, bw: float, hosts: Tuple[str, str]) -> None:
        if type not in [1, 2]:
            return

        sleep(duration)
        reward: float = 0.0
        data: Dict = {}
        while not data:
            try:
                with open(f"{DOCKER_VOLUME}/{hosts[0]}_{hosts[1]}.log", 'r') as f:
                    data = json.load(f)
            except json.decoder.JSONDecodeError:
                print("JSONDecodeError: reopening file and loading JSON...")

        if type == 1:
            average_bitrate: float = data["end"]["streams"][0]["sender"]["bits_per_second"] / 1000000.0
            if average_bitrate >= bw:
                print(f"Finished elastic request with success: {average_bitrate} >= {bw}")
            else:
                print(f"Finished elastic request without success: {average_bitrate} < {bw}")
                reward = -0.5
        else:
            worst_bitrate = min(interval["streams"][0]["bits_per_second"] for interval in data["intervals"]) / 1000000.0
            if worst_bitrate >= bw:
                print(f"Finished inelastic request with success: {worst_bitrate} >= {bw}")
            else:
                print(f"Finished inelastic request without success: {worst_bitrate} < {bw}")
                reward = -1.0

        self.backend.active_pairs.remove(hosts)
        self.backend.active_pairs_connection.sendall(','.join(f"{pair[0]}_{pair[1]}" for pair in self.backend.active_pairs).encode('utf-8'))
        self.requests_queue.put(dict(type=0, duration=0, bw=0.0, price=0.0, departed=1 if type == 1 else 2, reward=reward))
