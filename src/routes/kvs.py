import os
import sys
import requests
from flask import Blueprint, jsonify, request
from util.distributor import KVSDistributor
from constants.responses import (
    key_count_response,
    all_shards_info_response,
    single_shard_info_response,
)
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
    id = kvs_distributor.shard_id()
    return key_count_response(count, id)


@kvs_router.route("/shards/", defaults={"shard_id": None})
@kvs_router.route("/shards/<shard_id>")
def shard_info(shard_id):
    if shard_id:
        shard_id = int(shard_id)
        key_count = kvs_distributor.key_count(bucket_index=shard_id)
        replicas = kvs_distributor.bucket(id=shard_id)
        return single_shard_info_response(key_count, shard_id, replicas)
    else:
        all_shards = kvs_distributor.all_bucket_ids()
        return all_shards_info_response(all_shards)


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
    json = request.get_json() or {}
    context = json.get("causal-context", {})
    res = None
    if request.method == "GET":
        res = kvs_distributor.get(key, context)
    elif request.method == "PUT":
        res = kvs_distributor.put(key, json.get("value"), context)

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
