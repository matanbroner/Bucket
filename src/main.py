import os
from flask import Flask
import config

app = Flask(__name__)

if __name__ == "__main__":
    app.run(port=config.PORT, host=config.HOST, debug=True)