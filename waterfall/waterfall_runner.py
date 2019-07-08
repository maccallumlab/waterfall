from waterfall import datamanager
import argparse
import uuid
import logging
import random
import time
import collections
import sys
import math


logger = logging.getLogger(__name__)


class WaterfallRunner:
    def __init__(
        self,
        n_stages,
        max_queue_size,
        n_seed_traj,
        n_to_complete,
        console_logging=False,
        max_duration=3600,
    ):
        self.step = 0
        self.n_stages = n_stages
        self.max_queue_size = max_queue_size
        self.n_seed_traj = n_seed_traj
        self.n_to_complete = n_to_complete
        self.console_logging = console_logging
        self.max_duration = max_duration
        self._step = 0
        self._active_task = None
        self._in_progress = None
        self._datamanager = None
        self.gen_starting_structure = None
        self.run_traj_segment = None
        self.trajectory_writer = None
        self._logger = None

    def run(self):
        parser = argparse.ArgumentParser(
            description="Setup or run a waterfall simulation."
        )
        parser.add_argument(
            "command", help="command to execute", choices=["init", "run"]
        )
        parser.add_argument(
            "--console-logging",
            help="log output to console instead of file",
            default=False,
            action="store_true",
        )
        args = parser.parse_args()

        self.console_logging = args.console_logging
        self._setup_logging()

        if args.command == "init":
            self._logger.info("Executing `init` command.")
            self._initialize()
        else:
            self._logger.info("Executing `run` command.")
            self._run()

    def _initialize(self):
        self._logger.info("Initializing DataManager with %d stages.", self.n_stages)
        datamanager.DataManager.initialize(
            self.n_stages, self.trajectory_writer, console_logging=self.console_logging
        )
        self._datamanager = datamanager.DataManager.activate()
        self._logger.info("Seeding %d initial trajectories.", self.n_seed_traj)
        for _ in range(self.n_seed_traj):
            task = self._gen_new_traj_start_task()
            self._datamanager.queue_task(task)

    def _gen_new_traj_start_task(self):
        self._logger.info("Generating new starting trajectory task.")
        task_id = self._datamanager.gen_random_id()
        lineage = self._datamanager.gen_random_id()
        self._logger.info("Created new lineage %s", lineage)
        parent = None
        struct = self.gen_starting_structure()
        struct_id = self._datamanager.save_structure(struct)
        log_weight = 0.0
        stage = 0
        multiplicity = 1
        task = datamanager.SimulationTask(
            task_id, lineage, parent, struct_id, log_weight, stage, multiplicity
        )
        return task

    def _run(self):
        self._datamanager = datamanager.DataManager.activate(
            console_logging=self.console_logging
        )
        while self._datamanager.n_complete < self.n_to_complete:
            self._logger.info(
                "Running with %d complete of %d.",
                self._datamanager.n_complete,
                self.n_to_complete,
            )
            self._do_work()

    def _do_work(self):
        # first check if there are any orphan tasks
        task = self._datamanager.get_orphan_task()
        if task:
            self._logger.info("Restarting orphan task.")
        # if there isn't an orphan, then choose a random task from the queue
        else:
            task = self._datamanager.get_random_task()
        if task:
            self._logger.info("Running queued task.")
        # if there isn't a task to do, then we start a new one
        else:
            task = self._gen_new_traj_start_task()
            self._logger.info("Starting new trajectory.")

        # If we get here, we have a task. We add it to the in progress
        # tasks and then we run the user supplied run function.
        in_progress = self._datamanager.add_in_progress(task, self.max_duration)
        struct = self._datamanager.load_structure(task.start_struct)
        self._logger.info("Starting run of trajectory segment at stage %d.", task.stage)
        new_struct, new_log_weight = self.run_traj_segment(
            task.stage, struct, task.start_log_weight
        )
        self._logger.info("Finished run of trajectory segment.")

        # We're done. Remove from in progress queue and store structure.
        self._datamanager.remove_in_progress(in_progress)
        struct_id = self._datamanager.save_structure(new_struct)

        if task.stage == self.n_stages - 1:
            logger.info("Trajectory completed.")
            self._datamanager.mark_complete()
            requested_copies = 0
            requested_weight = new_log_weight
        else:
            # Update the weight and compute the number of copies
            self._datamanager.log_average[task.stage].update_weight(
                new_log_weight, task.multiplicity
            )
            requested_copies, requested_weight = get_copies(
                self.n_stages,
                task.stage,
                new_log_weight,
                self._datamanager.log_average[task.stage].log_average_weight(),
            )

        # Choose a number of regular copies and merged copies
        current_queued = (
            self._datamanager.n_queued_tasks + self._datamanager.n_in_progress
        )
        normal_copies, merged_copies = get_merged_copies(
            self.max_queue_size, current_queued, requested_copies
        )
        self._logger.info("Propogating %d requested copies.", requested_copies)
        self._logger.info("Currently %d tasks queued.", current_queued)
        if merged_copies == 0:
            self._logger.info("Using %d normal copies.", normal_copies)
        else:
            self._logger.info(
                "Using %d normal copies and a merged copy with %d multiplicity.",
                normal_copies,
                merged_copies,
            )

        # record the provenance
        prov_id = datamanager.gen_random_id()
        prov = datamanager.Provenance(
            id=prov_id,
            lineage=task.lineage,
            parent=task.parent,
            stage=task.stage,
            start_struct=task.start_struct,
            start_log_weight=task.start_log_weight,
            end_struct=struct_id,
            end_log_weight=requested_weight,
            mult=task.multiplicity,
            copies=requested_copies,
        )
        self._datamanager.save_provenance(prov)

        # Add the appropriate copies to the task queue
        if requested_copies == 0:
            self._logger.info("Trajectory terminated at stage %d.", task.stage)
        for _ in range(normal_copies):
            new_task = datamanager.SimulationTask(
                id=datamanager.gen_random_id(),
                lineage=task.lineage,
                parent=prov_id,
                start_struct=struct_id,
                start_log_weight=requested_weight,
                stage=task.stage + 1,
                multiplicity=task.multiplicity,
            )
            self._datamanager.queue_task(new_task)
        if merged_copies:
            new_task = datamanager.SimulationTask(
                id=datamanager.gen_random_id(),
                lineage=task.lineage,
                parent=prov_id,
                start_struct=struct_id,
                start_log_weight=requested_weight,
                stage=task.stage + 1,
                multiplicity=task.multiplicity * merged_copies,
            )
            self._datamanager.queue_task(new_task)

    def _setup_logging(self):
        self._logger = logging.getLogger(__name__)


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


def get_copies(n_stages, stage, log_weight, log_average_weight):
    if stage == n_stages - 1:
        return 0, log_weight
    elif stage >= 0 and stage < n_stages - 1:
        ratio = math.exp(log_weight - log_average_weight)
        lower = math.floor(ratio)
        upper = math.ceil(ratio)
        frac = ratio - lower
        if random.random() < frac:
            return int(upper), log_average_weight
        else:
            return int(lower), log_average_weight
    else:
        raise ValueError("stage > (n_stages - 1)")


def get_merged_copies(max_queued, current_queued, requested_copies):
    if current_queued + requested_copies <= max_queued:
        return requested_copies, 0
    else:
        merged_copies = requested_copies + current_queued - max_queued
        normal_copies = requested_copies - merged_copies
    return normal_copies, merged_copies
