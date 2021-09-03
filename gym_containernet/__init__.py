import logging
from gym.envs.registration import register

logger = logging.getLogger(__name__)

register(
    id='slice-admission-v0',
    entry_point='gym_containernet.envs:SliceAdmissionEnv',
)
register(
    id='path-selection-v0',
    entry_point='gym_containernet.envs:PathSelectionEnv',
)
