import os
import sys
import requests
from flask import Blueprint, jsonify, request
from util.distributor import KVSDistributor
from util.misc import printer

address = os.getenv("ADDRESS")
# list of IPs defaults to node's own IP
ips = os.getenv("VIEW", address).split(",")
# replication factor
repl_factor = int(os.getenv("REPL_FACTOR", 1))

kvs_router = Blueprint("kvs", __name__)

kvs_distributor = KVSDistributor(ips, address, repl_factor)


@kvs_router.route("/view-change-propagate", methods=["PUT"])
def propogate_view_change():
    """Recieved by a follower node from a leader propogating a view change

    JSON:
        views (list): IP addresses of each node in the network

    Returns:
        tuple: json, status code
    """
    json = request.get_json()
    view = json.get("view")
    repl_factor = json.get("repl_factor")
    shard = kvs_distributor.change_view(
        ips=view, repl_factor=repl_factor, propogate=False
    )
    return shard, 200


@kvs_router.route("/view-change", methods=["PUT"])
def client_view_change():
    """Client interface to perform a view change

    JSON:
        view (str): comma delimited IP addresses of each node in the network

    Returns:
        tuple: json, status code
    """
    json = request.get_json()
    view = json.get("view").split(",")
    repl_factor = json.get("repl-factor")
    template = kvs_distributor.change_view(
        ips=view, repl_factor=repl_factor, propogate=True
    )
    return {"shards": template}, 200


@kvs_router.route("/shard", methods=["PUT"])
def accept_shard():
    """Absorb an incoming shard from another node

    JSON:
        kvs (dict): key-value pairs to absorb

    Returns:
        tuple: json, status code
    """
    json = request.get_json()
    shard = json.get("kvs")
    kvs_distributor.merge_shard(shard)
    return "Success", 200


@kvs_router.route("/gossip", methods=["PUT"])
def accept_gossip():
    """Absorb gossip from another node

    JSON:
        kvs (dict): key-value pairs to absorb

    Returns:
        tuple: json, status code
    """
    printer("Got gossip")
    json = request.get_json()
    shard = json.get("kvs")
    kvs_distributor.merge_gossip(shard)
    return "Success", 200


@kvs_router.route("/key-count", methods=["GET"])
def key_count():
    """Get number of keys in KVS

    Returns:
        tuple: json, status code
    """
    count = kvs_distributor.key_count()
    return count, 200


@kvs_router.route("/keys/<key>", methods=["GET", "PUT", "DELETE"])
def dynamic_key_route(key):
    """Handles all key adding, updating, and deleting in KVS

    Args:
        key (str): key to add, update, or delete

    JSON:
        value: if PUT request, value to update key with in KVS

    Returns:
        tuple: json, status code
    """
    global address
    json = request.get_json()
    context = json.get("causal-context", {})
    res = None
    if request.method == "GET":
        res = kvs_distributor.get(key, context)
    elif request.method == "PUT":
        res = kvs_distributor.put(key, json.get("value"), context)

    printer(res.__dict__)
    return res.to_flask_response(include_address=res.address != address)


# Dev Routes - Delete Before Submission


@kvs_router.route("/all-keys", methods=["GET"])
def all_keys():
    """Returns all keys in KVS

    Returns:
        tuple: json, status code
    """
    return jsonify(kvs_distributor.kvs.json()), 200


@kvs_router.route("/info", methods=["GET"])
def info():
    """Returns KVS Distributor metadata

    Returns:
        tuple: json, status code
    """
    return (
        jsonify({"view": kvs_distributor.view.all_ips, "ip": address}),
        200,
    )
