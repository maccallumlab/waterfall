import jsonpickle
import logging
import itertools
import time
import uuid as uuid_mod
import sys
import os
from http.client import BadStatusLine
from xmlrpc.client import ServerProxy


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
        if os.path.exists("waterfall.complete"):
            self._logger.info("The file waterfall.complete exists, indicating that the run has completed. Terminating client.")
            print("The file waterfall.complete exists, indicating that the run has completed. Terminating client.")
            sys.exit()
        try:
            with open("waterfall.url") as f:
                self._server = f.read().strip()
                self._logger.info(f"Connected to server {self._server}.")
        except FileNotFoundError:
            raise RuntimeError(
                "The file waterfall.url does not exist. Is the server running?"
            )

        with ServerProxy(self._server) as proxy:
            response = self._retry(proxy.ping)

        if response != "Ping":
            raise RuntimeError(
                f"Could not connect to server. Response was {response}."
            )
        self._logger.info(f"Succesfully pinged server {self._server}.")

    def _get_work_unit(self):
        with ServerProxy(self._server) as proxy:
            response = self._retry(proxy.get_work_unit)
        task = jsonpickle.decode(response)
        task_id = task.id
        stage = task.stage
        state = task.state
        return task_id, stage, state

    def _run_work_unit(self, stage, state):
        return self._runner(stage, state)

    def _post_result(self, task_id, log_weight, new_state):
        data = jsonpickle.encode((task_id, log_weight, new_state))
        with ServerProxy(self._server) as proxy:
            result = self._retry(proxy.post_result, data)
        if result == "Kill":
            self._logger.info("Server indicates calculation is finished. Terminating.")
            print("Server indicates calculation is finished. Terminating.")
            sys.exit()

    def _retry(self, func, *args):
        """Work around bug where we sometimes get BadStatusLine."""
        for _ in range(10):
            try:
                if args:
                    result = func(*args)
                else:
                    result = func()
                return result
            except BadStatusLine:
                self._logger("Got BadStatusLine from server, retrying.")
                time.sleep(0.5)
        raise RuntimeError("Unsuccessfully retried 10 times.")

