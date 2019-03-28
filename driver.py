import state
import logging
import random
import time
import numpy as np

logger = logging.getLogger(__name__)

# create our state and setup logging
s = state.State(10)
state.init_logging(s, console=True, level=logging.INFO)

with s.transact():
    num_completed = s.get_num_completed()

step = 0

f = open("temp.out", "w")

while num_completed < 100:
    step += 1
    with s.transact():
        for i in range(s.n_stages):
            print(
                "{}\t{}\t".format(s._visits[i].value, s._adds[i].value), file=f, end=""
            )
        print("", file=f)
        num_completed = s.get_num_completed()
        logger.info(
            "Running step %d, %d completed trajectories of %d", step, num_completed, 100
        )

        tasks = s.list_simulation_tasks()
        n = len(tasks)

        if n == 0:
            logger.info("Starting new trajectory from top")
            lineage = s.gen_new_lineage()
            parent = None
            stage = 0
            struct_id_start = s.get_random_initial_structure()
            x_start = s.load_structure(struct_id_start)
            w_start = 1.0
        else:
            choice = random.randrange(0, n)
            task = tasks[choice]
            logger.info("Continuing trajectory %s at copy %d", task.parent, task.copy)
            lineage = task.lineage
            parent = task.parent
            stage = task.stage
            struct_id_start = task.struct_hash
            x_start = s.load_structure(task.struct_hash)
            w_start = task.start_weight
            s.remove_simulation_task(task)

    # pretend to simulate
    logger.info("Running MD at stage %d", stage)
    time.sleep(0.01)
    x_end = np.random.normal(size=(1000, 3))
    w_end = w_start * np.random.lognormal(1.0, 1.0)
    logger.info("MD at stage %d complete", stage)

    with s.transact():
        struct_id_end = s.save_structure(x_end)
        s.add_weight_observation(stage, w_end)
        copies = s.get_copies_for_weight(stage, w_end)
        logger.info(
            "Adding %d copies starting from structure %s", copies, struct_id_end
        )

        provenance = state.ProvenanceEntry(
            lineage=lineage,
            parent=parent,
            struct_start=struct_id_start,
            weight_start=w_start,
            stage=stage,
            struct_end=struct_id_end,
            weight_end=w_end,
            copies=copies,
        )
        prov_hash = s.add_provenance(provenance)

        for i in range(copies):
            task = state.SimulationTask(
                lineage, prov_hash, struct_id_end, w_end / copies, stage + 1, i
            )
            s.add_simulation_task(task)

        # if we hit bottom, then add in a new traj at top
        # if stage == s.n_stages - 1:
        #     logger.info("Trajectory completed, starting new task from top")
        #     lineage = s.gen_new_lineage()
        #     parent = None
        #     stage = 0
        #     struct_id_start = s.get_random_initial_structure()
        #     x_start = s.load_structure(struct_id_start)
        #     w_start = 1.0
        #     task = state.SimulationTask(
        #         lineage, parent, struct_id_start, w_start, stage, 0
        #     )


logger.info("Worker finished normally")