import argparse
import random
import pickle
import uuid
import os
from waterfall import Waterfall, vault
import requests
import logging


class WaterfallRunner:
    def __init__(self, iter_start, run_traj):
        self.iter_start = iter_start
        self.run_traj = run_traj

    def run(self):
        parser = argparse.ArgumentParser(
            description="Setup or run a waterfall simulation."
        )
        subparsers = parser.add_subparsers(dest="command")
        subparsers.required = True
        init_parser = subparsers.add_parser("init", help="initialize system")
        init_parser.add_argument("--n_stages", type=int, required=True)
        init_parser.add_argument("--n_seed", type=int, required=True)
        init_parser.add_argument("--n_traj", type=int, required=True)
        init_parser.add_argument("--n_batch", type=int, required=True)
        init_parser.add_argument("--max_queue", type=int, default=20000)

        server_parser = subparsers.add_parser("server", help="run as server")
        server_parser.add_argument("--host", default="localhost")
        server_parser.add_argument("--port", type=int, default=50000)
        client_parser = subparsers.add_parser("client", help="run as client")
        client_parser.add_argument(
            "--n_work", type=int, default=None, help="number of work units to run"
        )
        kill_parser = subparsers.add_parser("kill", help="kill the server")
        args = parser.parse_args()

        if args.command == "init":
            self._initialize(
                n_stages=args.n_stages,
                n_seed=args.n_seed,
                n_traj=args.n_traj,
                batch_size=args.n_batch,
                max_queue=args.max_queue,
            )
        elif args.command == "server":
            host = args.host
            port = args.port
            self._serve(host, port)
        elif args.command == "client":
            from waterfall import client

            c = client.Client(self.run_traj, n_work_units=args.n_work)
            c.run()
        elif args.command == "kill":
            with open("waterfall.url") as f:
                server_address = f.read().strip()

            response = requests.get(f"{server_address}/kill")
            if not response:
                raise RuntimeError(
                    f"Could not connect to server. Response code was {response}."
                )

    def _initialize(self, n_stages, n_seed, n_traj, batch_size, max_queue):
        w = Waterfall(n_stages, n_traj, batch_size, max_queue)
        store = vault.create_db(w)
        w.store = store
        logging.basicConfig(level=logging.INFO, filename="Data/Logs/server.log")
        logger = logging.getLogger(__name__)
        logger.info("Initializing waterfall.")
        for state in self.iter_start():
            w.add_start_state(state)
        for _ in range(n_seed):
            w.add_new_start_task()
        w.save()

    def _serve(self, host, port):
        from waterfall import server

        server.run(host=host, port=port)
