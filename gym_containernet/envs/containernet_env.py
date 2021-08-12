import gym_containernet.envs.topology_manager as containernet

from gym import Env, spaces


TOPOLOGY_FILE = "topology.txt"


class ContainernetEnv(Env):
    def __init__(self):
        self.backend = containernet.start_containernet(TOPOLOGY_FILE)

        self.observation_space = spaces.Tuple((spaces.Discrete(5), spaces.Discrete(5), spaces.Discrete(3)))
        self.action_space = spaces.Discrete(2)

        self.requests = 0

    def reset(self):
        self.backend = containernet.start_containernet(TOPOLOGY_FILE)
        self.requests = 0

    def step(self, action) -> (object, float, bool, dict):
        pass

    def render(self, mode='human'):
        pass

    def close(self):
        pass
