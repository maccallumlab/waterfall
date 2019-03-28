import state
import numpy as np
import logging

N = 1000
s = state.State(10)

with s.transact():
    for i in range(N):
        x = np.random.normal(size=(1000, 3))
        print(s.add_initial_structure(x))