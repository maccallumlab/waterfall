from . import vault
import random
import pickle
import uuid
import math
import time
import os
import logging

logger = logging.getLogger(__name__)

__version__ = "0.0.1"


class Waterfall:
    def __init__(self, n_stages, n_traj, max_queue, max_duration=3600):
        self.n_stages = n_stages
        self.n_traj = n_traj
        self.n_completed_traj = 0
        self.max_queue = max_queue
        self.max_duration = max_duration
        self.work_queue = []
        self.in_progress = {}
        self.start_states = []
        self.weights = [LogAverageWeight() for _ in range(self.n_stages)]
        self.store = None
        self._create_dirs()

    def save(self):
        with open("Data/waterfall.dat", "wb") as f:
            f.write(pickle.dumps(self))

    def __getstate__(self):
        state = self.__dict__.copy()
        del state["store"]
        return state

    @classmethod
    def load(cls):
        with open("Data/waterfall.dat", "rb") as f:
            w = pickle.loads(f.read())
        w.store = vault.connect_db()
        return w

    @property
    def n_queued(self):
        return len(self.work_queue)

    def add_start_state(self, start_states):
        self.start_states.append(start_states)

    def get_random_start_state(self):
        return random.choice(self.start_states)

    def add_task(self, task):
        self.work_queue.append(task)

    def get_task(self):
        task = self._get_orphan_task()
        if task:
            logger.info("Restarting orphan task.")
        else:
            # There wasn't an orphaned task, so we check the task queue
            task = self._get_random_task()
        if task:
            logger.info("Running queued task.")
        else:
            # There were no tasks in the queue, so we start a new simulation
            # from scratch.
            logger.info("Starting new trajectory.")
            task = self.get_new_start_task()
        self._add_in_progress(task)
        return task

    def _add_in_progress(self, task):
        now = time.time()
        self.in_progress[task.id] = task, now

    def _remove_in_progress(self, task_id):
        del self.in_progress[task_id]

    def _get_orphan_task(self):
        now = time.time()
        for task_id, (task, start) in self.in_progress.items():
            if now - start > self.max_duration:
                self._remove_in_progress(task_id)
                return task
        return None

    def _get_random_task(self):
        if self.work_queue:
            return self.work_queue.pop(random.randrange(len(self.work_queue)))
        else:
            return None

    def get_new_start_task(self):
        # Choose a random starting state
        state = self.get_random_start_state()
        # Add it to store
        parent_id = self.store.save_structure(-1, 0.0, 1, 1, None, state)
        # Create a new task
        task = WaterfallTask(0, parent_id, 0.0, 1, state)
        logger.info("Created new start task.")
        return task

    def add_new_start_task(self):
        task = self.get_new_start_task()
        logger.info("Added new start task to work queue.")
        self.add_task(task)

    def add_result(self, result):
        task_id, log_weight, new_state = result
        task = self.in_progress[task_id][0]  # 0 is task, 1 is start time
        self._remove_in_progress(task_id)
        new_log_weight = task.log_weight + log_weight

        if task.stage == self.n_stages - 1:
            logger.info("Trajectory completed.")
            self.n_completed_traj += 1
            requested_copies = 0
            requested_log_weight = new_log_weight
        else:
            # Update the weight and compute the number of copies
            self.weights[task.stage].update(new_log_weight, task.multiplicity)
            requested_copies, requested_log_weight = get_copies(
                new_log_weight, self.weights[task.stage].log_average
            )
        logger.info(f"{self.n_completed_traj} of {self.n_traj} completed.")

        # decide how many tasks to add
        normal_copies, merged_copies = get_merged_copies(
            self.max_queue, self.n_queued, requested_copies
        )
        logger.info("Propogating %d requested copies.", requested_copies)
        logger.info("Currently %d tasks queued.", self.n_queued)
        if merged_copies == 0:
            logger.info("Using %d normal copies.", normal_copies)
        else:
            logger.info(
                "Using %d normal copies and a merged copy with %d multiplicity.",
                normal_copies,
                merged_copies,
            )

        # store the results in the database
        parent_id = self.store.save_structure(
            task.stage,
            requested_log_weight,
            requested_copies,
            task.multiplicity,
            task.parent_id,
            new_state,
        )

        # queue new tasks
        if requested_copies == 0:
            logger.info("Trajectory terminated at stage %d.", task.stage)
        for _ in range(normal_copies):
            new_task = WaterfallTask(
                stage=task.stage + 1,
                parent_id=parent_id,
                log_weight=requested_log_weight,
                multiplicity=task.multiplicity,
                state=new_state,
            )
            self.add_task(new_task)
        if merged_copies:
            new_task = WaterfallTask(
                stage=task.stage + 1,
                parent_id=parent_id,
                log_weight=requested_log_weight,
                multiplicity=task.multiplicity * merged_copies,
                state=new_state
            )
            self.add_task(new_task)

    #
    # private methods
    #
    def _create_dirs(self):
        if os.path.exists("Data"):
            raise RuntimeError('"Data" directory already exists')
        os.mkdir("Data")
        os.mkdir("Data/Logs")


class WaterfallTask:
    def __init__(self, stage, parent_id, log_weight, multiplicity, state):
        self.id = uuid.uuid4().hex
        self.stage = stage
        self.parent_id = parent_id
        self.log_weight = log_weight
        self.multiplicity = multiplicity
        self.state = state

    def __repr__(self):
        return (
            f"WaterfallTask(id={self.id}, stage={self.stage}, "
            "parent_id={self.parent_id}, log_weight={self.log_weight}, "
            "multiplicity={self.multiplicity}, state=<...>)"
        )


def _calculate_merged_copies(requested_copies, total_queued, max_queue_size):
    if requested_copies + total_queued <= max_queue_size:
        # We don't exceed the queue size, so we can add all of the copies.
        # logger.info("Not exceeding queue size, adding %d copies", requested_copies)
        return requested_copies, False, None
    else:
        # We will exceed the queue size, so we only add some of the copies,
        # and condense the rest into a single copy with higher multiplicity.
        merged = True
        merged_multiplicity = requested_copies + total_queued - max_queue_size
        normal_copies = requested_copies - merged_multiplicity
        # logger.info(
        #     "Exceeding queue size, adding %d copies and 1 copy with %d multiplicity",
        #     normal_copies,
        #     merged_multiplicity,
        # )
        return normal_copies, merged, merged_multiplicity


def get_copies(log_weight, log_average_weight):
    ratio = math.exp(log_weight - log_average_weight)
    lower = math.floor(ratio)
    upper = math.ceil(ratio)
    frac = ratio - lower
    if random.random() < frac:
        return int(upper), log_average_weight
    else:
        return int(lower), log_average_weight


def get_merged_copies(max_queued, current_queued, requested_copies):
    if current_queued + requested_copies <= max_queued:
        return requested_copies, 0
    else:
        merged_copies = requested_copies + current_queued - max_queued
        normal_copies = requested_copies - merged_copies
    return normal_copies, merged_copies


class LogAverageWeight:
    def __init__(self):
        self.logsum = -math.inf
        self.count = 0

    def update(self, log_weight, multiplicity):
        self.logsum = log_sum_exp(self.logsum, log_weight + math.log(multiplicity))
        self.count += multiplicity

    @property
    def log_average(self):
        return self.logsum - math.log(self.count)


def log_sum_exp(log_w1, log_w2):
    "Return ln(exp(log_w1) + exp(log_w2)) avoiding overflow."
    log_max = max(log_w1, log_w2)
    return log_max + math.log(math.exp(log_w1 - log_max) + math.exp(log_w2 - log_max))
