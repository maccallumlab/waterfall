import random
import math
import numpy as np
import time
from waterfall.waterfall_runner import WaterfallRunner

np.random.seed(seed=2019)


def iter_start():
    results = []
    for i in range(100):
        x = np.random.normal(size=100)
        results.append(x)
    return results


def run(stage, start_state):
    new_state = np.random.normal(size=100)
    # state[0] is the work per stage
    new_state[0] = start_state[0]
    new_state[0] = np.random.normal(loc=start_state[0], scale=0.10, size=1)[0]
    return new_state[0], new_state


waterfall = WaterfallRunner(iter_start, run)
waterfall.run()
