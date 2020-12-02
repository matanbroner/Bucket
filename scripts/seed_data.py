# Use this script to add N key value pairs to the network

import requests
import random
import string

letters = list(string.ascii_lowercase)


def generate_keys(num_keys: int = 500):
    keys = []
    for i in range(num_keys):
        keys.append("{}{}{}{}{}".format(*[random.choice(letters) for _ in range(5)]))
    return keys


def generate_official_keys(num_keys: int = 100):
    keys = []
    for i in range(num_keys):
        keys.append("test_2_%d" % i)
    return keys


URI = "http://127.0.0.1:13801/kvs/keys"


def send_key_value(key, value):
    # key, value = get_random_string(), get_random_string()
    url = URI + f"/{key}"
    headers = {"Content-Type": "application/json"}
    response = requests.put(url, headers=headers, json={"value": value})
    if response.status_code > 201:
        print(response.json())


for index, key in enumerate(generate_keys()):
    send_key_value(key, index)
