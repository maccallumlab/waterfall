import time
import uuid
import math
import os
import pickle
import shutil
import contextlib
import hashlib
from collections import namedtuple
import random
import logging
from waterfall import crdt

logger = logging.getLogger(__name__)


def init_logging(s, console=True, level=None):
    if console:
        logging.basicConfig(
            level=level or logging.INFO,
            format="%(asctime)s %(levelname)s %(name)s: %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
    else:
        logging.basicConfig(
            filename=s.log_file,
            level=level or logging.INFO,
            format="%(asctime)s %(levelname)s %(name)s: %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
    logger.info("Worker started")
    logger.info(f"Created State(n_stages={s.n_stages})")
    logger.info(f"Client_id=f{s.client_id}")


SimulationTask = namedtuple(
    "SimulationTask",
    "lineage parent struct_hash start_log_weight stage multiplicity child_id",
)
SimulationTask.__doc__ = """\
A namedtuple to hold a simulation task.

Attributes
----------
lineage : string
    Each simulation started from the top is given a unique random uuid.
    This is maintained by all children.
parent : string
    The hash of the parent ProvenanceEntry. This is `None` for simulations
    started from the top stage.
struct_hash : string
    The hash of the starting structure.
start_log_weight : float
    The initial weight of this structure.
stage : int
    The stage of the calculation: 0 for the top stage, n_stages-1 for the bottom.
multiplicity : int
    The number of copies this task represents.
child_id: int
    Unique identifier for each child
"""

StructureEntry = namedtuple("StructureEntry", "file offset length")
StructureEntry.__doc__ = """\
A namedtuple to hold information about a structure stored on disk.

Attributes
----------
file : string
    The path to the file containing this structure.
offset : int
    The offset in bytes where the structure starts.
length : int
    The length of the structure in bytes.
"""

ProvenanceEntry = namedtuple(
    "ProvenanceEntry",
    "id lineage parent struct_start weight_start stage struct_end weight_end multiplicity copies",
)
ProvenanceEntry.__doc__ = """\
A namedtuple to hold information about the provenance of structures.

Attributes
----------
id: string
    Unique id for this entry.
lineage : string
    Each simulation started from the top is given a unique random uuid.
    This is maintained by all children.
parent : string
    The hash of the parent ProvenanceEntry. This is `None` for simulations
    started from the top stage.
struct_start : string
    The hash of the starting structure.
weight_start : float
    The initial weight of this structure.
stage : int
    The stage of the calculation: 0 for the top stage, n_stages-1 for the bottom.
struct_end : string
    The hash of the ending structure.
weight_end : float
    The weight of the final structure.
multiplicity : int
    The number of copies this structure represents.
copies : int
    The number of children spawned.
"""

InProgressEntry = namedtuple("InProgressEntry", "task timestamp max_duration")
InProgressEntry.__doc__ = """\
A namedtuple to hold information about tasks that are in progress.

Attributes
----------
task : SimulationTask
    The SimulationTask that is in progress.
timestamp : float
    The time when the task was started, in seconds from the epoch.
    started from the top stage.
max_duration : float
    The maximum duration of task. If it takes longer than this, we
    assume the worker executing the task died or was killed.
"""


def gen_random_client_id():
    "Generate a random uuid string"
    return uuid.uuid4().hex


class State:
    def __init__(self, n_stages, client_id=None):
        self._in_transaction = False
        self.n_stages = n_stages
        self.client_id = client_id if client_id else gen_random_client_id()
        self._averages = [
            crdt.LogAverageWeight(self.client_id) for _ in range(self.n_stages)
        ]
        self._visits = [crdt.GCounter(self.client_id) for _ in range(self.n_stages)]
        self._adds = [crdt.GCounter(self.client_id) for _ in range(self.n_stages)]
        self._work_queue = crdt.TombstoneSet(self.client_id)
        self._structures = crdt.UUIDMap(self.client_id)
        self._current_structure_offset = 0
        self._init_structs = crdt.GSet(self.client_id)
        self._provenances = crdt.UUIDMap(self.client_id)
        self._lineages = crdt.GSet(self.client_id)
        self._started = crdt.GCounter(self.client_id)
        self._completed = crdt.GCounter(self.client_id)
        self._in_progress = crdt.TombstoneSet(self.client_id)
        self.task_duration = 3600  # default task duration is 1 hour
        self.log_file = None
        self._state_file = None
        self._struct_file = None
        self._update_times = {}
        self._setup_files()
        self._update_from_disk()
        self._persist_state()

    @contextlib.contextmanager
    def transact(self):
        logger.debug("Transaction started")
        self._in_transaction = True
        self._update_from_disk()
        yield
        self._persist_state()
        self._in_transaction = False
        logger.debug("Transaction ended")

    def mark_visit(self, stage_index):
        self._ensure_in_transaction()
        self._visits[stage_index].increment()

    def mark_add(self, stage_index, copies):
        self._ensure_in_transaction()
        self._adds[stage_index].increment(copies)

    def get_num_completed(self):
        self._ensure_in_transaction()
        return self._completed.value

    def add_log_weight_observation(self, stage_index, log_weight):
        logger.debug("Added log_weight obs of %f to stage %d", log_weight, stage_index)
        self._ensure_in_transaction()
        self._averages[stage_index].add_observation(log_weight)

    def get_log_average_weight(self, stage_index):
        self._ensure_in_transaction()
        return self._averages[stage_index].value

    def get_copies_and_log_weight(self, stage_index, log_weight):
        self._ensure_in_transaction()
        if stage_index == self.n_stages - 1:
            self._completed.increment()
            return 0, log_weight

        log_average = self.get_log_average_weight(stage_index)
        R = math.exp(log_weight - log_average)
        low = math.floor(R)
        high = math.ceil(R)
        frac = R - low
        if low == 0:
            if random.random() < frac:
                return_copy = 1
                return_log_average = log_average
            else:
                return_copy = 0
                return_log_average = -99999
        else:
            if random.random() < frac:
                return_copy = int(high)
                return_log_average = log_average
            else:
                return_copy = int(low)
                return_log_average = log_average
        logger.info(
            "Stage %d log_average %f ratio %f low %d high %d copies %d",
            stage_index,
            log_average,
            R,
            low,
            high,
            return_copy,
        )
        return return_copy, return_log_average

    def add_simulation_task(self, task):
        logger.debug("Added simulation task %s to queue", task)
        self._ensure_in_transaction()
        self._work_queue.add(task)

    def list_simulation_tasks(self):
        self._ensure_in_transaction()
        return self._work_queue.items

    def get_total_queued(self):
        return len(self._work_queue.items) + len(self._in_progress.items)

    def remove_simulation_task(self, task):
        logger.debug("Removed simulation task %s from queue", task)
        self._ensure_in_transaction()
        self._work_queue.remove(task)

    def add_to_in_progress_queue(self, task):
        self._ensure_in_transaction()
        timestamp = time.time()
        duration = self.task_duration
        entry = InProgressEntry(task, timestamp, duration)
        self._in_progress.add(entry)
        return entry

    def remove_from_in_progress_queue(self, entry):
        self._ensure_in_transaction()
        self._in_progress.remove(entry)

    def get_orphan_task(self):
        self._ensure_in_transaction()
        current = time.time()
        for entry in self._in_progress.items:
            if current > entry.timestamp + entry.max_duration:
                # This entry has been orphaned, so we will remove the
                # old entry and restart it.
                self._in_progress.remove(entry)
                return entry.task
        # Return None if none of the entries have expired
        return None

    def save_structure(self, structure):
        self._ensure_in_transaction()
        data = pickle.dumps(structure)
        length = len(data)
        hash = hashlib.sha256(data).hexdigest()
        assert hash not in self._structures
        with open(self._struct_file, "ab") as f:
            f.seek(0, os.SEEK_END)
            f.write(data)
        entry = StructureEntry(
            self._struct_file, self._current_structure_offset, length
        )
        self._structures[hash] = entry
        self._current_structure_offset += length
        logger.debug("Saved structure with hash %s to file %s", hash, self._struct_file)
        return hash

    def load_structure(self, hash):
        logger.debug("Loading structure with hash %s", hash)
        self._ensure_in_transaction()
        entry = self._structures[hash]
        logger.debug("Opening file %s", entry.file)
        with open(entry.file, "rb") as f:
            logger.debug("Seeking offset %d", entry.offset)
            f.seek(entry.offset)
            logger.debug("Reading %d bytes", entry.length)
            data = f.read(entry.length)
        struct = pickle.loads(data)
        return struct

    def add_initial_structure(self, structure):
        self._ensure_in_transaction()
        hash = self.save_structure(structure)
        self._init_structs.add(hash)
        logger.debug("Added initial structure with hash %s", hash)
        return hash

    def get_random_initial_structure(self):
        self._ensure_in_transaction()
        items = self._init_structs.items
        if not items:
            raise RuntimeError("There are no ititial structures")
        self._started.increment()
        choice = random.choice(items)
        s = self.load_structure(choice)
        return choice

    def get_num_initial_structures(self):
        self._ensure_in_transaction()
        items = self._init_structs.items
        return len(items)

    def gen_new_lineage(self):
        self._ensure_in_transaction()
        lineage_id = gen_random_client_id()
        self._lineages.add(lineage_id)
        logger.info("Created new lineage %s", lineage_id)
        return lineage_id

    def add_provenance(self, prov_entry):
        self._ensure_in_transaction()
        hash = hashlib.sha256(pickle.dumps(prov_entry)).hexdigest()
        self._provenances[hash] = prov_entry
        return hash

    def get_provenance(self, hash):
        self._ensure_in_transaction()
        return self._provenances[hash]

    def _setup_files(self):
        os.makedirs("Data/States", exist_ok=True)
        os.makedirs("Data/Coords", exist_ok=True)
        os.makedirs("Data/Logs", exist_ok=True)
        self._state_file = f"Data/States/{self.client_id}.dat"
        self._struct_file = f"Data/Coords/{self.client_id}.dat"
        self.log_file = f"Data/Logs/{self.client_id}.log"
        if os.path.exists(self._state_file):
            raise RuntimeError(f"State file {self._state_file} already exists")
        if os.path.exists(self._struct_file):
            raise RuntimeError(f"Struct file {self._struct_file} already exists")

    def _persist_state(self):
        logger.info("Persisting state to %s", self._state_file)
        with open(f"Data/States/{self.client_id}_temp.dat", "wb") as f:
            pickle.dump(self, f)
        shutil.move(f"Data/States/{self.client_id}_temp.dat", self._state_file)

    def _update_from_disk(self):
        logger.info("Updating from disk")
        filenames = [
            os.path.join("Data/States", f)
            for f in os.listdir("Data/States")
            if os.path.isfile(os.path.join("Data/States", f))
        ]
        filenames = [f for f in filenames if f.endswith(".dat") and "temp" not in f]
        for filename in filenames:
            timestamp = os.path.getmtime(filename)
            if filename not in self._update_times:
                update = True
                self._update_times[filename] = timestamp
            else:
                if timestamp == self._update_times[filename]:
                    update = False
                else:
                    update = True
            if update:
                logger.debug("Updating from %s", filename)
                f = open(filename, "rb")
                x = pickle.load(f)
                for i in range(self.n_stages):
                    self._averages[i].merge(x._averages[i])
                    self._visits[i].merge(x._visits[i])
                    self._adds[i].merge(x._adds[i])
                self._work_queue.merge(x._work_queue)
                self._structures.merge(x._structures)
                self._provenances.merge(x._provenances)
                self._lineages.merge(x._lineages)
                self._init_structs.merge(x._init_structs)
                self._started.merge(x._started)
                self._completed.merge(x._completed)
                self._in_progress.merge(x._in_progress)
            else:
                logger.debug("Skipping %s", filename)
        logger.debug("Done updating")

    def _ensure_in_transaction(self):
        if not self._in_transaction:
            raise RuntimeError("Tried to operate on State outside of a transaction.")

    def __getstate__(self):
        # only pickle our own observations, not all observations
        state = self.__dict__.copy()
        del state["_update_times"]
        return state

    def __setstate__(self, state):
        self.__dict__ = state
        self._update_times = {}
