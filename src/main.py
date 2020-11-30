import os
from flask import Flask
import config
from util import KVSDistributor

app = Flask(__name__)

if __name__ == "__main__":
    app.run(port=config.PORT, host=config.HOST, debug=True)