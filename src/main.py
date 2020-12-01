import os
from flask import Flask
import config
from routes.kvs import kvs_router

from util.scheduler import Scheduler
from util.misc import printer

app = Flask(__name__)
app.register_blueprint(kvs_router, url_prefix="/kvs")

if __name__ == "__main__":
    app.run(port=config.PORT, host=config.HOST, debug=True)
