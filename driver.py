import state
import logging
import random
import time
import numpy as np
import collections

logger = logging.getLogger(__name__)

# create our state and setup logging
N_STAGES = 50
s = state.State(N_STAGES)
state.init_logging(s, console=True, level=logging.INFO)
step = 0
N_COMPLETE = 500
active_task = None

with s.transact():
    num_completed = s.get_num_completed()

while num_completed < N_COMPLETE:
    step += 1
    with s.transact():
        tasks = s.list_simulation_tasks()
        total_queued = 0
        for t in tasks:
            total_queued += t.copies
        num_completed = s.get_num_completed()
        logger.info(
            "Running step %d, %d completed trajectories of %d",
            step,
            num_completed,
            N_COMPLETE,
        )

        # choose a task
        tasks = s.list_simulation_tasks()
        n = len(tasks)
        if not active_task:
            if n == 0:
                continue_queued = False
            else:
                if total_queued < 10:
                    choice = random.randint(0, n)
                    if choice == n:
                        # we're going to start a new simulation
                        continue_queued = False
                    else:
                        # we're going to continue an existing trajectory
                        continue_queued = True
                else:
                    # we're going to continue an existing trajectory
                    choice = random.randrange(0, n)
                    continue_queued = True

            if continue_queued:
                # we're going to continue one of the existing trajectories
                parent_task = tasks[choice]
                logger.info("Continuing trajectory from parent %s", parent_task.parent)
                s.remove_simulation_task(parent_task)
                active_task = state.SimulationTask(
                    lineage=parent_task.lineage,
                    parent=parent_task.parent,
                    struct_hash=parent_task.struct_hash,
                    start_weight=parent_task.start_weight,
                    stage=parent_task.stage,
                    copies=1,
                )
                # if there are still copies left, add them back to the queue
                if parent_task.copies > 1:
                    logger.info("Still %d copies left, requeuing", parent_task.copies - 1)
                    requeue_task = state.SimulationTask(
                        lineage=parent_task.lineage,
                        parent=parent_task.parent,
                        struct_hash=parent_task.struct_hash,
                        start_weight=parent_task.start_weight,
                        stage=parent_task.stage,
                        copies=parent_task.copies - 1,
                    )
                    s.add_simulation_task(requeue_task)
            else:
                # we're going to start a new simulation
                active_task = state.SimulationTask(
                    lineage=s.gen_new_lineage(),
                    parent=None,
                    struct_hash=s.get_random_initial_structure(),
                    start_weight=1.0,
                    stage=0,
                    copies=1,
                )

        # We either just picked a new active task, or have one left from the last round.
        # So, we load the structure and prepare to simulate it.
        x_start = s.load_structure(active_task.struct_hash)

    # Do the simulation.
    # This is currently faked.
    # Done outside of a transaction.
    logger.info("Running MD at stage %d", active_task.stage)
    time.sleep(0.01)
    x_end = np.random.normal(size=(1000, 3))
    x_end[0, 0] = x_start[0, 0]
    weight_fact = x_end[0, 0] ** (1.0/50.0)
    w_end = active_task.start_weight * weight_fact
    logger.info("MD at stage %d complete", active_task.stage)

    with s.transact():
        # save the structure and update weights
        struct_id_end = s.save_structure(x_end)
        s.add_weight_observation(active_task.stage, w_end)
        s.mark_visit(active_task.stage)

        # decide on how many copies
        copies, new_weight = s.get_copies_and_weight(active_task.stage, w_end)

        # record the provenance
        provenance = state.ProvenanceEntry(
            lineage=active_task.lineage,
            parent=active_task.parent,
            struct_start=active_task.struct_hash,
            weight_start=active_task.start_weight,
            stage=active_task.stage,
            struct_end=struct_id_end,
            weight_end=new_weight,
            copies=copies,
        )
        prov_hash = s.add_provenance(provenance)

        if copies == 0:
            logger.info("Trajectory terminated")
            active_task = None
        else:
            s.mark_add(active_task.stage, copies)
            task = state.SimulationTask(
                lineage=active_task.lineage,
                parent=prov_hash,
                struct_hash=struct_id_end,
                start_weight=new_weight,
                stage=active_task.stage + 1,
                copies=1,
            )
            if copies > 1:
                logger.info("Propagating one copy and queuing %d more", copies - 1)
                queue_task = state.SimulationTask(
                    lineage=active_task.lineage,
                    parent=prov_hash,
                    struct_hash=struct_id_end,
                    start_weight=new_weight,
                    stage=active_task.stage + 1,
                    copies=copies - 1,
                )
                s.add_simulation_task(queue_task)
            else:
                logger.info("Propagating single copy.")
            active_task = task


logger.info("Worker finished normally")
