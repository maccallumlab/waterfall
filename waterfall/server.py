import os
import waterfall
import logging
import jsonpickle
import argparse
import numpy as np
from flask import Flask, request
from flask_restful import Resource, Api


class Ping(Resource):
    def get(self):
        return None


class WorkUnit(Resource):
    def get(self):
        t = w.get_task()
        return jsonpickle.encode(t)


class PostResult(Resource):
    def post(self):
        result = jsonpickle.decode(request.data)
        w.add_result(result)
        if w.n_completed_traj >= w.n_traj:
            _cleanup()
            request.environ.get("werkzeug.server.shutdown")()


class Kill(Resource):
    def get(self):
        _cleanup()
        request.environ.get("werkzeug.server.shutdown")()
        return "Server shutting down"


def _cleanup():
    w.save()
    os.unlink("waterfall.url")
    os.unlink("waterfall.lock")


# setup logging
logging.basicConfig(level=logging.INFO, filename="Data/Logs/server.log")
logger = logging.getLogger(__name__)

# load global waterfall state
w = waterfall.Waterfall.load()

def _check_lock():
    if os.path.exists("waterfall.lock"):
        raise RuntimeError()
    open("waterfall.lock", "w").close()

def _write_url(host, port):
    with open("waterfall.url", "w") as outfile:
        print(f"http://{host}:{port}", file=outfile)

def run(host, port):
    _check_lock()

    # wire up flask
    app = Flask(__name__)
    api = Api(app)
    api.add_resource(Ping, "/ping")
    api.add_resource(WorkUnit, "/work_unit")
    api.add_resource(PostResult, "/post_result")
    api.add_resource(Kill, "/kill")

    # hide werkzeug logging messages
    import logging
    log = logging.getLogger('werkzeug')
    log.setLevel(logging.ERROR)

    logger.info(f"Starting server on {host}:{port}")
    _write_url(host, port)
    app.run(host=host, port=port)
