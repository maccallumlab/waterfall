import waterfall_runner
import numpy as np
import time

n_stages = 50
n_traj = 100
n_init = 10
n_atoms = 1000
target_queue_size = 100


# This function will be called n_init times
# to generate an ensemble of initial states.
# Each call should generate an initial state
# which is returned.
def populate():
    x = np.random.normal(size=(n_atoms, 3))
    return x


# This function will bel called to generate
# segments of a trajectory. It should
# take the stage, start_state, start_weight
# and use them to generate a final_state and
# final weight, which are returned.
def run(stage, start_state, start_weight):
    # Do something useful with stage, start_state, and start weight.
    # Here, we're just gnerating random numbers
    end_state = np.random.normal(size=(n_atoms, 3))
    weight_fact = np.random.lognormal(sigma=4)
    return end_state, start_weight * weight_fact


waterfall = waterfall_runner.WaterfallRunner(
    n_stages, n_traj, n_init, target_queue_size
)
waterfall.populate_method = populate
waterfall.run_method = run

waterfall.populate()
waterfall.run()
