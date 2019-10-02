import random
import math
import numpy as np
import time
from waterfall.waterfall_runner import WaterfallRunner

np.random.seed(seed=2019)


#
# This function should return a list (or other iterable)
# of potential starting structures.
#
def iter_start():
    results = []
    for i in range(100):
        x = np.random.normal(size=100)
        results.append(x)
    return results


#
# This function should take a stage and a starting state and
# returns the log weight and the new state.
#
# Note that the previous code returned the work, i.e. -log weight.
#
def run(stage, start_state):
    new_state = np.random.normal(size=100)
    # state[0] is the work per stage
    new_state[0] = start_state[0]
    new_state[0] = np.random.normal(loc=start_state[0], scale=0.10, size=1)[0]
    return new_state[0], new_state


#
# You would run this script at least three times.
#
# First, to setup the run:
#
# > python driver.py init --n_stages=20 --n_seed=500 --n_traj=500 --n_batch=50
#
# This would setup a new run with 20 stages, 500 seed structures, a target of
# 500 completed trajectories, and with new trajectories started in batches of
# 50 if the initial 500 seeds does not produce 500 completed trajectories.
#
#
# Next, we run the server:
#
# > python driver.py server --host [host] --port [port] > server.log 2>&1 &
#
# This will start the server in the background and redirect its output to
# server.log.
# [port] can 50000, 50001, etc. Each server needs its own port.
# [host] should be the ip address of the server's internal network adapter.
#        You can usually figure this out from the output of ifconfig.
#        For glados, this will be something like "172.25.0.2".
#
# Next, inside of a job script, we run the client. This will typically be done
# in an array job. You can run many instances of the client to work on sampling
# in parallel.
#
# > python driver.py client --n_work=[n]
#
# [n] should be the number of work units to run. Ideally, this should be a
#     number that fits into the alloted runtime so that the client terminates
#     normally, rather than being killed by the queuing system.
#
# Note that there are a few files that are used to communicate between the
# client and server:
#     waterfall.lock: a new server will not start if this file exists. If you
#                     are sure that the previous server has died, you can delete
#                     this file.
#     waterfall.url: this file tells the clients where to find the server.
#     waterfall.complete: this file is created by the server when a run has
#                         completed. When clients see it, they will terminate.
#
waterfall = WaterfallRunner(iter_start, run)
waterfall.run()
