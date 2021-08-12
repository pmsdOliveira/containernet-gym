import logging
from gym.envs.registration import register

logger = logging.getLogger(__name__)

register(
    id='containernet-v0',
    entry_point='gym_containernet.envs:ContainernetEnv',
)
