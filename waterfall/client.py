import requests
import jsonpickle
import logging
import itertools
import time
import uuid as uuid_mod


class Client:
    def __init__(self, runner, uuid=None, n_work_units=None):
        self._runner = runner
        if not uuid:
            self.uuid = uuid_mod.uuid4().hex
        else:
            self.uuid = uuid
        self.n_work_units = n_work_units
        self._server = None
        self._logger = None
        self._setup_logging()

    def run(self):
        self._logger.info("Starting run.")
        self._connect_to_server()
        if self.n_work_units:
            loop = range(self.n_work_units)
            max_work = f"{self.n_work_units}"
        else:
            loop = itertools.count(0)
            max_work = "infinity"
        for i in loop:
            self._logger.info(f"Starting task {i+1} of {max_work}.")
            task_id, stage, state = self._get_work_unit()
            self._logger.info(f"Starting simulation at stage {stage}.")
            start = time.time()
            log_weight, new_state = self._run_work_unit(stage, state)
            end = time.time()
            self._logger.info(f"Finished simulation with log_weight {log_weight}.")
            self._logger.info(f"Simulation took {end - start:.2f}s.")
            self._post_result(task_id, log_weight, new_state)
        self._logger.info(f"Completed {i+1} of {max_work} tasks. Exiting")

    def _setup_logging(self):
        print(f"Starting client with uuid {self.uuid}.")
        filename = f"Data/Logs/{self.uuid}.log"
        logging.basicConfig(level=logging.INFO, filename=filename)
        self._logger = logging.getLogger(__name__)
        self._logger.info(f"Started client with uiid {self.uuid}.")

    def _connect_to_server(self):
        try:
            with open("waterfall.url") as f:
                self._server = f.read().strip()
                self._logger.info(f"Connected to server {self._server}.")
        except FileNotFoundError:
            raise RuntimeError(
                "The file waterfall.url does not exist. Is the server running?"
            )

        response = requests.get(f"{self._server}/ping")
        if not response:
            raise RuntimeError(
                f"Could not connect to server. Response code was {response}."
            )
        self._logger.info(f"Succesfully pinged server {self._server}.")

    def _get_work_unit(self):
        try:
            response = requests.get(f"{self._server}/work_unit")
        except:
            print("fucky")
            time.sleep(0.5)
            response = requests.get(f"{self._server}/work_unit")
        if not response:
            raise RuntimeError(
                f"Unable to get work unit from server. Response code was {response}."
            )
        task = jsonpickle.decode(response.text)
        task_id = task.id
        stage = task.stage
        state = task.state
        return task_id, stage, state

    def _run_work_unit(self, stage, state):
        return self._runner(stage, state)

    def _post_result(self, task_id, log_weight, new_state):
        data = jsonpickle.encode((task_id, log_weight, new_state))
        try:
            response = requests.post(f"{self._server}/post_result", data=data)
        except:
            print("fucky")
            time.sleep(0.5)
            response = requests.post(f"{self._server}/post_result", data=data)
        if not response:
            raise RuntimeError(
                f"Unable to post work unit to server. Response code: {response}."
            )
