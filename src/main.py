import os
import sys
import logging
from flask import Flask
from datetime import datetime
from util.misc import printer

import config
from routes.kvs import kvs_router

from util.scheduler import Scheduler
from util.misc import printer


def uncaught_exception_handler(type, value, tb):
    printer("Uncaught exception: {0}".format(str(value)))


# Install exception handler
sys.excepthook = uncaught_exception_handler


app = Flask(__name__)
app.register_blueprint(kvs_router, url_prefix="/kvs")

if __name__ == "__main__":
    app.run(port=config.PORT, host=config.HOST, debug=True, use_reloader=False)
