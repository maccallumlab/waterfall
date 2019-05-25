import uuid
import pickle
import os
import glob
import random
import time
import math
import logging
from collections import namedtuple


InProgress = namedtuple("InProgress", "task deadline")

StructureId = namedtuple("StructureId", "id file offset size")

Provenance = namedtuple(
    "Provenance",
    "id lineage parent stage start_struct start_log_weight end_struct end_log_weight mult copies",
)
Provenance.__doc__ = """\
A namedtuple to hold information about the provenance of structures.

Attributes
----------
id: string
    A unique id for this provenance entry.
lineage : string
    Each simulation started from the top is given a unique random uuid.
    This is maintained by all children.
parent : string
    The hash of the parent Provenance. This is `None` for simulations
    started from the top stage.
stage : int
    The stage of the calculation: 0 for the top stage, n_stages-1 for the bottom.
start_struct : StructureId
    The starting structure.
start_log_weight : float
    The initial log weight of this structure.
end_struct : StructureId
    The ending structure.
end_log_weight : float
    The log weight of the final structure.
mult : int
    The number of copies this structure represents.
copies : int
    The number of children spawned.
"""

SimulationTask = namedtuple(
    "SimulationTask",
    "id lineage parent start_struct start_log_weight stage multiplicity",
)
SimulationTask.__doc__ = """\
A namedtuple to hold a simulation task.

Attributes
----------
id: string
    A unique id for this task.
lineage : string
    Each simulation started from the top is given a unique random uuid.
    This is maintained by all children.
parent : string
    The id of the parent Provenance. This is `None` for simulations
    started from the top stage.
start_struct : StructureId
    The starting structure.
start_log_weight : float
    The initial log weight of this structure.
stage : int
    The stage of the calculation: 0 for the top stage, n_stages-1 for the bottom.
multiplicity : int
    The number of copies this task represents.
"""


class TaskNotFoundError(Exception):
    pass


def gen_random_id():
    "Generate a random uuid string"
    return uuid.uuid4().hex


class DataManager:
    _filename = "Data/datamanager"

    def __init__(self):
        raise RuntimeError(
            "Do not create a DataManager directly. Call DataManager.activate() instead."
        )
        self._logger = None
        self._console_logging = False

    def __getstate__(self):
        odict = self.__dict__.copy()
        del odict["_logger"]
        return odict

    @classmethod
    def initialize(cls, n_stages, console_logging=False):
        d = cls.__new__(cls)
        d.n_stages = n_stages
        d.client_id = None
        d._traj_data = None
        d._prov_data = None
        d._console_logging = console_logging
        d._setup_logging()
        if os.path.exists(d._filename):
            raise RuntimeError(f"Path {d._filename} already exists.")
        d._create_dirs()
        with open(d._filename, "wb") as f:
            pickle.dump(d, f)

    @classmethod
    def activate(cls, read_only=False, client_id=None, console_logging=False):
        with open(cls._filename, "rb") as f:
            d = pickle.load(f)
        d.read_only = read_only
        d.client_id = client_id if client_id is not None else gen_random_id()
        d.log_average = [
            LogAverageWeight(d.client_id, i) for i in range(d.n_stages - 1)
        ]
        d._traj_data = TrajectoryStore(d.client_id)
        d._prov_data = ProvStore(d.client_id)
        d._my_complete = 0
        d._console_logging = console_logging
        d._setup_logging()
        return d

    def gen_random_id(self):
        return gen_random_id()

    def _create_dirs(self):
        os.makedirs("Data/")
        os.makedirs("Data/Queue")
        os.makedirs("Data/Run")
        os.makedirs("Data/Weight")
        os.makedirs("Data/Log")
        os.makedirs("Data/Traj")
        os.makedirs("Data/Prov")
        os.makedirs("Data/Count")

    def _setup_logging(self):
        if self._console_logging or self.read_only:
            logging.basicConfig(
                level=logging.INFO,
                format="%(asctime)s %(levelname)s %(name)s: %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
            )
        else:
            logging.basicConfig(
                filename=f"Data/Log/{self.client_id}.log",
                level=logging.INFO,
                format="%(asctime)s %(levelname)s %(name)s: %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
            )
        self._logger = logging.getLogger(__name__)

    #
    # Track completed
    #
    @property
    def n_complete(self):
        filenames = glob.glob("Data/Count/*_completed.dat")
        # exclude our file
        filenames = [
            f for f in filenames if f != f"Data/Count/{self.client_id}_completed.dat"
        ]
        count = self._my_complete
        for fn in filenames:
            count += int(open(fn).read())
        return count

    def mark_complete(self):
        basename = f"Data/Count/{self.client_id}_completed"
        self._my_complete += 1
        with open(basename + ".tmp", "w") as f:
            f.write(f"{self._my_complete}")
        os.rename(basename + ".tmp", basename + ".dat")

    #
    # Task queuing
    #
    @property
    def n_queued_tasks(self):
        return len(self._get_queued_task_ids())

    def queue_task(self, task):
        if self.read_only:
            assert RuntimeError("DataManager was activated as read only.")
        id = gen_random_id()
        basename = f"Data/Queue/{id}"
        with open(basename + ".tmp", "wb") as f:
            pickle.dump(task, f)
        os.rename(basename + ".tmp", basename + ".dat")

    def get_random_task(self, retries=50):
        if self.read_only:
            assert RuntimeError("DataManager was activated as read only.")
        if retries < 0:
            raise RuntimeError("Exceed retries while in `get_random_task`.")
        try:
            task_ids = self._get_queued_task_ids()
            if not task_ids:
                return None
            else:
                task_id = random.choice(task_ids)
                return self._dequeue_task(task_id)
        except TaskNotFoundError:
            return self.get_random_task(retries - 1)

    def _get_queued_task_ids(self):
        files = glob.glob("Data/Queue/*.dat")
        return files

    def _dequeue_task(self, task_name):
        if self.read_only:
            assert RuntimeError("DataManager was activated as read only.")
        try:
            with open(task_name, "rb") as f:
                result = pickle.load(f)
            os.unlink(task_name)
            return result
        except FileNotFoundError:
            raise TaskNotFoundError(f"Task {task_name} not found.")

    #
    # In progress
    #
    @property
    def n_in_progress(self):
        return len(self._get_in_progress())

    def _get_in_progress(self):
        files = glob.glob("Data/Run/*.dat")
        return files

    def add_in_progress(self, task, max_duration):
        if self.read_only:
            assert RuntimeError("DataManager was activated as read only.")
        basename = f"Data/Run/{gen_random_id()}"
        deadline = time.time() + max_duration
        in_progress = InProgress(task, deadline)
        with open(basename + ".tmp", "wb") as f:
            pickle.dump(in_progress, f)
        os.rename(basename + ".tmp", basename + ".dat")
        return basename + ".dat"

    def remove_in_progress(self, in_progress_id):
        if self.read_only:
            assert RuntimeError("DataManager was activated as read only.")
        os.unlink(in_progress_id)

    def get_orphan_task(self, retries=50):
        if self.read_only:
            assert RuntimeError("DataManager was activated as read only.")
        if retries < 0:
            raise RuntimeError("Exceed retries while in `get_orphan_task`.")
        filenames = self._get_in_progress()
        random.shuffle(filenames)
        for filename in filenames:
            try:
                with open(filename, "rb") as f:
                    in_progress = pickle.load(f)
                    if time.time() > in_progress.deadline:
                        self.remove_in_progress(filename)
                        return in_progress.task
            except (FileNotFoundError, EOFError):
                return self.get_orphan_task(retries - 1)
        return None

    #
    # Structures
    #
    def save_structure(self, structure):
        if self.read_only:
            assert RuntimeError("DataManager was activated as read only.")
        return self._traj_data.save_structure(structure)

    def load_structure(self, structure_id):
        return self._traj_data.load_structure(structure_id)

    def get_structure_ids(self):
        return self._traj_data.get_structure_ids()

    #
    # Provenances
    #
    def save_provenance(self, prov):
        if self.read_only:
            assert RuntimeError("DataManager was activated as read only.")
        self._prov_data.save_provenance(prov)

    def get_provenances(self):
        return self._prov_data.get_provenances()


#
# Log Average
#
def log_sum_exp(log_w1, log_w2):
    "Return ln(exp(log_w1) + exp(log_w2)) avoiding overflow."
    log_max = max(log_w1, log_w2)
    return log_max + math.log(math.exp(log_w1 - log_max) + math.exp(log_w2 - log_max))


class LogAverageWeight:
    def __init__(self, client_id, stage):
        self.client_id = client_id
        self.stage = stage
        self._log_sum = -math.inf
        self._count = 0
        self._basename = f"Data/Weight/{client_id}_stage_{stage}"
        self._pickle()

    def update_weight(self, log_weight, multiplicity):
        """Updates the weight and returns the log-average-weight and weight ratio."""
        self._log_sum = log_sum_exp(self._log_sum, log_weight + math.log(multiplicity))
        self._count += multiplicity
        self._pickle()

    def log_average_weight(self):
        log_sum = self._log_sum
        count = self._count
        filenames = glob.glob(f"Data/Weight/*_stage_{self.stage}.dat")
        # Skip our own file
        filenames = [f for f in filenames if not f == self._basename + ".dat"]
        for fn in filenames:
            with open(fn, "rb") as f:
                law = pickle.load(f)
            log_sum = log_sum_exp(log_sum, law._log_sum)
            count += law._count
        return log_sum - math.log(count)

    def _pickle(self):
        with open(self._basename + ".tmp", "wb") as f:
            pickle.dump(self, f)
        os.rename(self._basename + ".tmp", self._basename + ".dat")


#
# Trajectories
#
class TrajectoryStore:
    def __init__(self, client_id):
        self.client_id = client_id
        self._datafile = f"Data/Traj/{self.client_id}.dat"
        self._ledgerfile = f"Data/Traj/{self.client_id}.txt"
        self._offset = 0

    def save_structure(self, structure):
        id = gen_random_id()
        data = pickle.dumps((id, structure))
        size = len(data)
        # update the data file
        with open(self._datafile, "ab") as f:
            f.seek(0, os.SEEK_END)
            pos = f.tell()
            assert pos == self._offset
            f.write(data)
        structure_id = StructureId(id, self._datafile, self._offset, size)
        self._offset += size
        # update the ledger
        with open(self._ledgerfile, "a") as f:
            f.seek(0, os.SEEK_END)
            print(
                f"{structure_id.id} {structure_id.file} {structure_id.offset} {structure_id.size}",
                file=f,
            )
        return structure_id

    def load_structure(self, structure_id):
        with open(structure_id.file, "rb") as f:
            f.seek(structure_id.offset)
            data = f.read(structure_id.size)
        id, structure = pickle.loads(data)
        assert id == structure_id.id
        return structure

    def get_structure_ids(self):
        filenames = glob.glob("Data/Traj/*.txt")
        ids = []
        for fn in filenames:
            with open(fn) as f:
                for line in f:
                    cols = line.split()
                    ids.append(
                        StructureId(cols[0], cols[1], int(cols[2]), int(cols[3]))
                    )
        return ids


#
# Provenances
#
class ProvStore:
    def __init__(self, client_id):
        self.client_id = client_id
        self._ledgerfile = f"Data/Prov/{self.client_id}.txt"

    def save_provenance(self, p):
        with open(self._ledgerfile, "a") as f:
            f.seek(0, os.SEEK_END)
            print(
                f"{p.id} {p.lineage} {p.parent} {p.stage} "
                f"{p.start_struct.id} {p.start_struct.file} {p.start_struct.offset} {p.start_struct.size} {p.start_log_weight} "
                f"{p.end_struct.id} {p.end_struct.file} {p.end_struct.offset} {p.end_struct.size} {p.end_log_weight} "
                f"{p.mult} {p.copies}",
                file=f,
            )

    def get_provenances(self):
        filenames = glob.glob("Data/Prov/*.txt")
        provs = []
        for fn in filenames:
            with open(fn) as f:
                for line in f:
                    cols = line.split()
                    id = cols[0]
                    lineage = cols[1]
                    parent = cols[2]
                    if parent == "None":
                        parent = None
                    stage = int(cols[3])
                    start_struct = StructureId(
                        cols[4], cols[5], int(cols[6]), int(cols[7])
                    )
                    start_weight = float(cols[8])
                    end_struct = StructureId(
                        cols[9], cols[10], int(cols[11]), int(cols[12])
                    )
                    end_weight = float(cols[13])
                    mult = int(cols[14])
                    copies = int(cols[15])
                    prov = Provenance(
                        id,
                        lineage,
                        parent,
                        stage,
                        start_struct,
                        start_weight,
                        end_struct,
                        end_weight,
                        mult,
                        copies,
                    )
                    provs.append(prov)
        return provs
