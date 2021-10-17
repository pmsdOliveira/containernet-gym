from typing import Tuple

# TOPOLOGY MANAGER

TOPOLOGY_FILE: str = 'topology.txt'
DOCKER_VOLUME: str = '/home/pmsdoliveira/workspace/gym-containernet/docker-volume'


# CONTROLLER

UPDATE_PERIOD: int = 5   # seconds


# ENVIRONMENT

BASE_STATIONS: int = 7
COMPUTING_STATIONS: int = 14
PATHS: int = 7
PORT_RANGE: Tuple[int, int] = (1024, 4097)

CONNECTIONS_OFFSET: int = 4 + BASE_STATIONS * COMPUTING_STATIONS

ELASTIC_ARRIVAL_AVERAGE: int = 4
INELASTIC_ARRIVAL_AVERAGE: int = 8
DURATION_AVERAGE: int = 15
CONNECTIONS_AVERAGE: int = 2

MAX_REQUESTS: int = 50
STARTUP_TIME: int = 20
LOG_TIMEOUT: int = 90


# AGENT

INPUT_DIM: int = 6 + BASE_STATIONS * COMPUTING_STATIONS * (1 + PATHS)
HL1: int = 1000
HL2: int = 800
OUTPUT_DIM: int = 2

GAMMA: float = 0.9
EPSILON: float = 0.3
LEARNING_RATE: float = 1e-3

EPOCHS: int = 5000
MEM_SIZE: int = 1000
BATCH_SIZE: int = 200
SYNC_FREQ: int = 500
