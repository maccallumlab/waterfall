import os
import waterfall
import logging
import jsonpickle
import argparse
import numpy as np
import logging
from xmlrpc.server import SimpleXMLRPCServer


should_kill = False


def ping():
    return "Ping"


def get_work_unit():
    t = w.get_task()
    return jsonpickle.encode(t)


def post_result(data):
    global should_kill
    result = jsonpickle.decode(data)
    w.add_result(result)
    if w.kill:
        _cleanup()
        open("waterfall.complete", "w").close()
        should_kill = True

    if should_kill:
        return "Kill"
    else:
        return "Result posted"


def kill():
    global should_kill
    _cleanup()
    should_kill = True
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
    global should_kill
    _check_lock()

    logger.info(f"Starting server on {host}:{port}")
    _write_url(host, port)

    server = SimpleXMLRPCServer((host, port), logRequests=False)
    server.register_function(ping, "ping")
    server.register_function(kill, "kill")
    server.register_function(kill, "kill")
    server.register_function(get_work_unit, "get_work_unit")
    server.register_function(post_result, "post_result")
    while not should_kill:
        server.handle_request()