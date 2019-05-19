from waterfall import state
import uuid
import logging
import random
import time
import collections


logger = logging.getLogger(__name__)


class WaterfallRunner:
    def __init__(self, n_stages, max_queue_size):
        self.step = 0
        self.n_stages = n_stages
        self.max_queue_size = max_queue_size
        self._step = 0
        self._active_task = None
        self._in_progress = None
        self._state = state.State(self.n_stages)
        state.init_logging(self._state, console=False, level=logging.INFO)

        self.populate_method = None
        self.run_method = None

    def populate_starting_structures(self, n_starting_structures):
        with self._state.transact():
            n = self._state.get_num_initial_structures()
        while n < n_starting_structures:
            structure = self.populate_method()
            with self._state.transact():
                self._state.add_initial_structure(structure)
                n = self._state.get_num_initial_structures()

    def populate_init_trajectories(self, n_trajectories):
        with self._state.transact():
            for _ in range(n_trajectories):
                task = state.SimulationTask(
                    lineage=self._state.gen_new_lineage(),
                    parent=None,
                    struct_hash=self._state.get_random_initial_structure(),
                    start_log_weight=0.0,
                    stage=0,
                    multiplicity=1,
                    child_id=uuid.uuid4().int,
                )
                self._state.add_simulation_task(task)

    def _choose_task(self):
        with self._state.transact():
            total_queued = self._state.get_total_queued()
            self.num_completed = self._state.get_num_completed()
            tasks = self._state.list_simulation_tasks()
            logger.info(
                "Running step %d, %d completed trajectories with %d queued",
                self.step,
                self.num_completed,
                total_queued,
            )
            n = len(tasks)

            # First we check for orphaned trajectories.
            orphan = self._state.get_orphan_task()
            if orphan:
                logger.info("Restarting orphaned task.")
                self._active_task = orphan
            # If there is no orphan, then we check the queue. If there is
            # nothing queued, we start a new trajectory.
            elif n == 0:
                logger.info("Starting a new trajectory")
                self._active_task = state.SimulationTask(
                    lineage=self._state.gen_new_lineage(),
                    parent=None,
                    struct_hash=self._state.get_random_initial_structure(),
                    start_log_weight=0.0,
                    stage=0,
                    multiplicity=1,
                    child_id=uuid.uuid4().int,
                )
            # If there are trajectories queued, we extend one of them at random.
            else:
                choice = random.randrange(0, n)
                self._active_task = tasks[choice]
                logger.info(
                    "Continuing trajectory from parent %s", self._active_task.parent
                )
                self._state.remove_simulation_task(self._active_task)

            # Once we get here, we have an active task.
            self._in_progress = self._state.add_to_in_progress_queue(self._active_task)
            start_struct = self._state.load_structure(self._active_task.struct_hash)
            start_log_weight = self._active_task.start_log_weight
            stage = self._active_task.stage
        return start_struct, start_log_weight, stage

    def _persist_run(self, struct_end, log_weight_end):
        with self._state.transact():
            # we completed this work unit, so remove it
            self._state.remove_from_in_progress_queue(self._in_progress)
            self._in_progress = None

            # save the structure and update weights
            struct_id_end = self._state.save_structure(struct_end)
            for _ in range(self._active_task.multiplicity):
                self._state.add_log_weight_observation(
                    self._active_task.stage, log_weight_end
                )
            self._state.mark_visit(self._active_task.stage)

            # decide on how many copies
            copies, new_log_weight = self._state.get_copies_and_log_weight(
                self._active_task.stage, log_weight_end
            )
            logger.info(
                "New log weight %f. Trying to add %d copies", new_log_weight, copies
            )

            # record the provenance
            provenance = state.ProvenanceEntry(
                lineage=self._active_task.lineage,
                parent=self._active_task.parent,
                struct_start=self._active_task.struct_hash,
                weight_start=self._active_task.start_log_weight,
                stage=self._active_task.stage,
                struct_end=struct_id_end,
                weight_end=new_log_weight,
                multiplicity=self._active_task.multiplicity,
                copies=copies,
            )
            prov_hash = self._state.add_provenance(provenance)

            if copies == 0:
                logger.info("Trajectory terminated")
            else:
                self._state.mark_add(self._active_task.stage, copies)

                # Check to see how many copies we can add without exceeding queue size
                total_queued = self._state.get_total_queued()
                normal_copies, merged, merged_multiplicity = _calculate_merged_copies(
                    copies, total_queued, self.max_queue_size
                )
                for _ in range(normal_copies):
                    task = state.SimulationTask(
                        lineage=self._active_task.lineage,
                        parent=prov_hash,
                        struct_hash=struct_id_end,
                        start_log_weight=new_log_weight,
                        stage=self._active_task.stage + 1,
                        multiplicity=self._active_task.multiplicity,
                        child_id=uuid.uuid4().int,
                    )
                    self._state.add_simulation_task(task)
                if merged:
                    task = state.SimulationTask(
                        lineage=self._active_task.lineage,
                        parent=prov_hash,
                        struct_hash=struct_id_end,
                        start_log_weight=new_log_weight,
                        stage=self._active_task.stage + 1,
                        multiplicity=merged_multiplicity
                        * self._active_task.multiplicity,
                        child_id=uuid.uuid4().int,
                    )
                    self._state.add_simulation_task(task)

            self._active_task = None

    def run(self, n_traj):
        with self._state.transact():
            self.num_completed = self._state.get_num_completed()

        while self.num_completed < n_traj:
            self.step += 1

            start_struct, start_weight, stage = self._choose_task()
            logger.info("Running MD at stage %d", self._active_task.stage)
            result = self.run_method(stage, start_struct, start_weight)
            struct_end, weight_end = result
            logger.info("MD at stage %d complete", self._active_task.stage)

            self._persist_run(struct_end, weight_end)

        logger.info("Worker finished normally")


def _calculate_merged_copies(requested_copies, total_queued, max_queue_size):
    if requested_copies + total_queued <= max_queue_size:
        # We don't exceed the queue size, so we can add all of the copies.
        logger.info("Not exceeding queue size, adding %d copies", requested_copies)
        return requested_copies, False, None
    else:
        # We will exceed the queue size, so we only add some of the copies,
        # and condense the rest into a single copy with higher multiplicity.
        merged = True
        merged_multiplicity = requested_copies + total_queued - max_queue_size
        normal_copies = requested_copies - merged_multiplicity
        logger.info(
            "Exceeding queue size, adding %d copies and 1 copy with %d multiplicity",
            normal_copies,
            merged_multiplicity,
        )
        return normal_copies, merged, merged_multiplicity
