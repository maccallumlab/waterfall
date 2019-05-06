from waterfall import state
import logging
import random
import time
import collections


logger = logging.getLogger(__name__)


class WaterfallRunner:
    def __init__(self, n_stages, n_traj, n_init, target_queue_size):
        self.step = 0
        self.n_stages = n_stages
        self.n_traj = n_traj
        self.n_init = n_init
        self.target_queue_size = target_queue_size
        self._step = 0
        self._active_task = None
        self._in_progress = None
        self._state = state.State(self.n_stages)
        state.init_logging(self._state, console=False, level=logging.INFO)

        self.populate_method = None
        self.run_method = None

    def populate(self):
        with self._state.transact():
            n = self._state.get_num_initial_structures()
        while n < self.n_init:
            structure = self.populate_method()
            with self._state.transact():
                self._state.add_initial_structure(structure)
                n = self._state.get_num_initial_structures()

    def _choose_task(self):
        with self._state.transact():
            total_queued = self._state.get_total_queued()
            self.num_completed = self._state.get_num_completed()
            tasks = self._state.list_simulation_tasks()
            logger.info(
                "Running step %d, %d completed trajectories of %d",
                self.step,
                self.num_completed,
                self.n_traj,
            )
            n = len(tasks)
            # If we have don't have an active task, we need to decide
            # what to do.
            if not self._active_task:
                orphan = self._state.get_orphan_task()
                if orphan:
                    logger.info("Restarting orphaned task.")
                    self._active_task = orphan
                else:
                    if n == 0:
                        continue_queued = False
                    else:
                        if total_queued < self.target_queue_size:
                            choice = random.randint(0, n)
                            if choice == n:
                                # We're going to start a new lineage
                                continue_queued = False
                            else:
                                continue_queued = True
                        else:
                            choice = random.randrange(0, n)
                            continue_queued = True

                    if continue_queued:
                        # We're going to continue one of the existing trajectories.
                        parent_task = tasks[choice]
                        logger.info(
                            "Continuing trajectory from parent %s", parent_task.parent
                        )
                        self._state.remove_simulation_task(parent_task)
                        self._active_task = state.SimulationTask(
                            lineage=parent_task.lineage,
                            parent=parent_task.parent,
                            struct_hash=parent_task.struct_hash,
                            start_weight=parent_task.start_weight,
                            stage=parent_task.stage,
                            copies=1,
                        )
                        if parent_task.copies > 1:
                            logger.info(
                                "Still %d copies left, requeuing",
                                parent_task.copies - 1,
                            )
                            requeue_task = state.SimulationTask(
                                lineage=parent_task.lineage,
                                parent=parent_task.parent,
                                struct_hash=parent_task.struct_hash,
                                start_weight=parent_task.start_weight,
                                stage=parent_task.stage,
                                copies=parent_task.copies - 1,
                            )
                            self._state.add_simulation_task(requeue_task)
                    else:
                        # we're going to start a new simulation
                        self._active_task = state.SimulationTask(
                            lineage=self._state.gen_new_lineage(),
                            parent=None,
                            struct_hash=self._state.get_random_initial_structure(),
                            start_weight=1.0,
                            stage=0,
                            copies=1,
                        )
            # Once we get here, we have an active task.
            self._in_progress = self._state.add_to_in_progress_queue(self._active_task)
            start_struct = self._state.load_structure(self._active_task.struct_hash)
            start_weight = self._active_task.start_weight
            stage = self._active_task.stage
        return start_struct, start_weight, stage

    def _persist_run(self, struct_end, weight_end):
        with self._state.transact():
            # we completed this work unit, so remove it
            self._state.remove_from_in_progress_queue(self._in_progress)
            self._in_progress = None

            # save the structure and update weights
            struct_id_end = self._state.save_structure(struct_end)
            self._state.add_weight_observation(self._active_task.stage, weight_end)
            self._state.mark_visit(self._active_task.stage)

            # decide on how many copies
            copies, new_weight = self._state.get_copies_and_weight(
                self._active_task.stage, weight_end
            )

            # record the provenance
            provenance = state.ProvenanceEntry(
                lineage=self._active_task.lineage,
                parent=self._active_task.parent,
                struct_start=self._active_task.struct_hash,
                weight_start=self._active_task.start_weight,
                stage=self._active_task.stage,
                struct_end=struct_id_end,
                weight_end=new_weight,
                copies=copies,
            )
            prov_hash = self._state.add_provenance(provenance)

            if copies == 0:
                logger.info("Trajectory terminated")
                self._active_task = None
            else:
                self._state.mark_add(self._active_task.stage, copies)
                task = state.SimulationTask(
                    lineage=self._active_task.lineage,
                    parent=prov_hash,
                    struct_hash=struct_id_end,
                    start_weight=new_weight,
                    stage=self._active_task.stage + 1,
                    copies=1,
                )
                if copies > 1:
                    logger.info("Propagating one copy and queuing %d more", copies - 1)
                    queue_task = state.SimulationTask(
                        lineage=self._active_task.lineage,
                        parent=prov_hash,
                        struct_hash=struct_id_end,
                        start_weight=new_weight,
                        stage=self._active_task.stage + 1,
                        copies=copies - 1,
                    )
                    self._state.add_simulation_task(queue_task)
                else:
                    logger.info("Propagating single copy.")
                self._active_task = task

    def run(self):
        with self._state.transact():
            self.num_completed = self._state.get_num_completed()

        while self.num_completed < self.n_traj:
            self.step += 1

            start_struct, start_weight, stage = self._choose_task()
            logger.info("Running MD at stage %d", self._active_task.stage)
            result = self.run_method(stage, start_struct, start_weight)
            struct_end, weight_end = result
            logger.info("MD at stage %d complete", self._active_task.stage)

            self._persist_run(struct_end, weight_end)

        logger.info("Worker finished normally")
