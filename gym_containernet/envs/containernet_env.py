import gym4. Understanding the basic Layer 2 Switch application



import gym_containernet.envs.topology_manager as backend


class ContainernetEnv(gym.Env):
    def __init__(self):
        self.backend = backend

    def seed(self, seed=None):
        pass

    def reset(self):
        self.backend = self.backend.start_containernet()

    def state(self):
        pass

    def step(self, action) -> (object, float, bool, dict):
        pass

    def render(self, mode='human'):
        pass
