import random
import math
import numpy as np
from waterfall import WaterfallRunner
import logging

logger = logging.getLogger(__name__)
np.random.seed(seed=2019)


def gen_start():
    x = np.random.normal(size=100)
    # x[0] will be the work per stage
    return x


def run(stage, start_state, start_weight):
    new_state = np.random.normal(size=100)
    # state[0] is the work per stage
    new_state[0] = start_state[0]
    weight_factor = start_state[0] + np.random.normal(scale=0.2)
    return new_state, start_weight + weight_factor


n_stages = 20
n_traj = 2000
n_seed_traj = 2000
max_queue_size = 20_000


waterfall = WaterfallRunner(n_stages, max_queue_size, n_seed_traj, n_traj)
waterfall.gen_starting_structure = gen_start
waterfall.run_traj_segment = run
waterfall.run()
