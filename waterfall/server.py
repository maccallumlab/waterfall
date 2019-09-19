import os
import waterfall
import logging
import jsonpickle
import argparse
import numpy as np
from flask import Flask, request
import logging

# hide werkzeug logging messages
# log = logging.getLogger('werkzeug')
# log.setLevel(logging.ERROR)
app = Flask(__name__)


@app.route("/ping", methods=["GET"])
def ping():
    return "Ping"


@app.route("/work_unit", methods=["GET"])
def get_work_unit():
    t = w.get_task()
    return jsonpickle.encode(t)


@app.route("/post_result", methods=["POST"])
def post_result():
    result = jsonpickle.decode(request.data)
    w.add_result(result)
    if w.kill:
        _cleanup()
        request.environ.get("werkzeug.server.shutdown")()
    return "Result posted"


@app.route("/kill", methods=["GET"])
def kill():
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

    logger.info(f"Starting server on {host}:{port}")
    _write_url(host, port)
    app.run(host=host, port=port, threaded=False)

    @app.teardown_appcontext
    def shutdown_session(exception=None):
        w.store.DBSession.remove()
