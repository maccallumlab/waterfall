from . import vault
import random
import pickle
import uuid
import math
import time
import os
import logging
import numpy as np

logger = logging.getLogger(__name__)

__version__ = "0.0.1"


class Waterfall:
    def __init__(
        self,
        n_stages,
        n_traj,
        batch_size,
        max_queue,
        control_width=False,
        max_duration=3600,
    ):
        self.n_stages = n_stages
        self.n_traj = n_traj
        self.n_completed_traj = 0
        self.max_queue = max_queue
        self.batch_size = batch_size
        self.max_duration = max_duration
        self.control_width = control_width

        self.work_queue = []
        for _ in range(n_stages):
            self.work_queue.append([])

        self.in_progress = {}
        self.start_states = []
        self.weights = WeightManager(self.n_stages)
        self.counts = [0 for _ in range(self.n_stages)]
        self.store = None
        self.drain = False
        self.kill = False

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
        w.store = vault.connect_db(w)
        return w

    @property
    def n_queued(self):
        return sum(len(self.work_queue[i]) for i in range(self.n_stages))

    def add_start_state(self, start_states):
        self.start_states.append(start_states)

    def get_random_start_state(self):
        return random.choice(self.start_states)

    def add_task(self, stage, task):
        if isinstance(task, SolidTask):
            self.counts[stage] += 1
        self.work_queue[stage].append(task)

    def get_task(self):
        task = self._get_orphan_task()
        if task:
            logger.info("Restarting orphan task.")
        else:
            # There wasn't an orphaned task, so we check the task queue
            task = self._get_next_task()
        if task:
            logger.info("Running queued task.")
        else:
            # There were no tasks in the queue, so we start a new batch of
            # simulations from scratch.
            logger.info("Starting new trajectory batch.")
            self.gen_new_start_batch()
            task = self.work_queue[0].pop()
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

    def _get_next_task(self):
        for stage in range(self.n_stages):
            # Skip over empty levels.
            if not self.work_queue[stage]:
                continue
            # Make sure all tasks are solid.
            self._solidify_queue(stage)
            # Skip over level if there are no tasks after solidifying.
            if not self.work_queue[stage]:
                continue
            # Return a random task from this level.
            return self.work_queue[stage].pop(
                random.randrange(len(self.work_queue[stage]))
            )
        # If we get here, there was nothing for us to do.
        # We will end up generting a new start task
        return None

    def _solidify_queue(self, stage):
        average_weight = self.weights.get_log_average_weight(stage - 1)
        logger.info(f"Solidifying queue level {stage}.")
        tasks = self.work_queue[stage]
        # We loop through the tasks in a random order.
        random.shuffle(tasks)
        new_tasks = []
        for task in tasks:
            if isinstance(task, SolidTask):
                new_tasks.append(task)
            else:
                # Compute the number of copies by comparing the
                # weight to the average weight.
                requested_copies, requested_log_weight, log_contribution = get_copies(
                    task.log_weight, average_weight
                )

                # Decide how many merged copies to create, either to
                # control the number of trajectories or to control
                # the size of the queue.
                normal_copies_q, merged_copies_q = get_merged_copies(
                    self.max_queue, self.n_queued, requested_copies
                )
                normal_copies_t, merged_copies_t = get_merged_copies(
                    self.counts[0], self.counts[task.stage], requested_copies
                )
                if self.control_width and (merged_copies_t > merged_copies_q):
                    logger.info("Copies merged to maintain the number of trajectories.")
                    normal_copies = normal_copies_t
                    merged_copies = merged_copies_t
                else:
                    if merged_copies_q:
                        logger.info("Copies merged to control queue size.")
                    normal_copies = normal_copies_q
                    merged_copies = merged_copies_q
                logger.info(
                    f"Adding {normal_copies} normal copies and {merged_copies} merged copies."
                )

                # Update the number of copies in the database.
                self.store.update_copies(task.parent_id, requested_copies)

                # add the solid tasks
                for _ in range(normal_copies):
                    new_task = SolidTask(
                        task.stage,
                        task.parent_id,
                        requested_log_weight,
                        task.multiplicity,
                        task.state,
                        log_contribution,
                    )
                    new_tasks.append(new_task)
                    self.counts[task.stage] += 1
                if merged_copies:
                    new_task = SolidTask(
                        task.stage,
                        task.parent_id,
                        requested_log_weight,
                        task.multiplicity * merged_copies,
                        task.state,
                        log_contribution,
                    )
                    new_tasks.append(new_task)
                    self.counts[task.stage] += 1
        logger.info(f"Level {stage} has {len(new_tasks)} tasks.")
        self.work_queue[stage] = new_tasks

    def gen_new_start_batch(self):
        logger.info(f"Adding batch of {self.batch_size} start tasks.")
        for _ in range(self.batch_size):
            self.add_new_start_task()

    def add_new_start_task(self):
        # Choose a random starting state
        state = self.get_random_start_state()
        # Add it to store
        parent_id = self.store.save_structure(-1, 0.0, 1, 1, None, state)
        # Create a new task
        task = SolidTask(0, parent_id, 0.0, 1, state, 0)
        self.add_task(0, task)
        logger.info("Added new start task to work queue.")

    def add_result(self, result):
        task_id, log_weight, new_state = result
        task = self.in_progress[task_id][0]  # 0 is task, 1 is start time
        self._remove_in_progress(task_id)
        new_log_weight = task.log_weight + log_weight

        if task.stage == self.n_stages - 1:
            # Handle the trajectories that have completed.
            logger.info("Trajectory completed.")
            self.n_completed_traj += 1
        else:
            # Update the weights.
            self.weights.add_sample(
                task.stage, log_weight, task.log_contribution, task.multiplicity
            )

        logger.info(f"{self.n_completed_traj} of {self.n_traj} completed.")

        # Store structure in the database.
        parent_id = self.store.save_structure(
            task.stage,
            new_log_weight,
            -1,  # This will be set to the correct value when the child task is solidified.
            task.multiplicity,
            task.parent_id,
            new_state,
        )

        # Create a liquid task and put it into the queue.
        if task.stage != self.n_stages - 1:
            new_task = LiquidTask(
                stage=task.stage + 1,
                parent_id=parent_id,
                log_weight=new_log_weight,
                multiplicity=task.multiplicity,
                state=new_state,
            )
            self.work_queue[task.stage + 1].append(new_task)

        # If we've completed enough tasks, start draining the queue.
        if self.n_completed_traj >= self.n_traj:
            self.drain = True

        self._check_kill()

    def _check_kill(self):
        # We only check to kill if we are draining.
        if self.drain:
            # If nothing is queued, then we're done.
            if self.n_queued == 0:
                self.kill = True

            else:
                # If we are draining and there are only liquid tasks left,
                # then we are done if the total number of tasks that would
                # be created is zero.
                n_tasks = 0
                for i in range(self.n_stages):
                    for task in self.work_queue[i]:
                        if isinstance(task, SolidTask):
                            return  # If we have a solid task, we can't be done

                        requested_copies, _ = get_copies(
                            task.log_weight, self.weights[task.stage - 1].log_average
                        )
                        n_tasks += requested_copies
                if n_tasks == 0:
                    self.kill = True

            if self.kill:
                logger.info("Completed!!!!")

    #
    # private methods
    #
    def _create_dirs(self):
        if os.path.exists("Data"):
            raise RuntimeError('"Data" directory already exists')
        os.mkdir("Data")
        os.mkdir("Data/Logs")


class SolidTask:
    def __init__(
        self, stage, parent_id, log_weight, multiplicity, state, log_contribution
    ):
        self.id = uuid.uuid4().hex
        self.stage = stage
        self.parent_id = parent_id
        self.log_weight = log_weight
        self.multiplicity = multiplicity
        self.state = state
        self.log_contribution = log_contribution

    def __repr__(self):
        return (
            f"SolidTask(id={self.id}, stage={self.stage}, "
            f"parent_id={self.parent_id}, log_weight={self.log_weight}, "
            f"multiplicity={self.multiplicity}, log_contribution={self.log_contribution}, "
            f"state=<...>)"
        )


class LiquidTask:
    def __init__(self, stage, parent_id, log_weight, multiplicity, state):
        self.stage = stage
        self.parent_id = parent_id
        self.log_weight = log_weight
        self.multiplicity = multiplicity
        self.state = state

    def __repr__(self):
        return (
            f"LiquidTask(stage={self.stage}, "
            f"parent_id={self.parent_id}, log_weight={self.log_weight}, "
            f"multiplicity={self.multiplicity}, state=<...>)"
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
    upper = lower + 1
    frac = ratio - lower

    if random.random() < frac:
        copies = int(upper)
    else:
        copies = int(lower)

    if copies == 0:
        log_contribution = -math.inf
    else:
        log_contribution = log_weight - math.log(copies)

    return copies, log_average_weight, log_contribution


def get_merged_copies(max_queued, current_queued, requested_copies):
    if current_queued + requested_copies <= max_queued:
        return requested_copies, 0
    elif current_queued > max_queued:
        return 0, requested_copies
    else:
        merged_copies = requested_copies + current_queued - max_queued
        normal_copies = max(0, requested_copies - merged_copies)
    return normal_copies, merged_copies


def log_sum_exp(log_w1, log_w2):
    "Return ln(exp(log_w1) + exp(log_w2)) avoiding overflow."
    log_max = max(log_w1, log_w2)
    return log_max + math.log(math.exp(log_w1 - log_max) + math.exp(log_w2 - log_max))


class WeightManager:
    def __init__(self, n_levels):
        self.n_levels = n_levels
        self._average_log_deltas = -np.inf * np.ones(n_levels)
        self._sum_log_contributions = -np.inf * np.ones(n_levels)

    def add_sample(self, level, delta_log_weight, log_contribution, multiplicity):
        new_contribution = log_sum_exp(
            math.log(multiplicity) + log_contribution,
            self._sum_log_contributions[level],
        )
        new_log_average = (
            log_sum_exp(
                math.log(multiplicity) + log_contribution + delta_log_weight,
                self._sum_log_contributions[level] + self._average_log_deltas[level],
            )
            - new_contribution
        )
        self._average_log_deltas[level] = new_log_average
        self._sum_log_contributions[level] = new_contribution

    def get_log_average_weight(self, level):
        return np.sum(self._average_log_deltas[: (level + 1)])
        # return self._average_log_deltas[level]
