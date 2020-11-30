import os
import sys
import requests
from flask import Blueprint, jsonify, request
from util.distributor import KVSDistributor
from util.responses import *
from util.misc import printer

ip = os.getenv("ADDRESS")
views = os.getenv("VIEW").split(",")

kvs_router = Blueprint("kvs", __name__)

kvs_distributor = KVSDistributor(ip, views)


def _handle_get(key: str) -> tuple:
    """Interface for handling GET requests

    Args:
        key (str)

    Returns:
        tuple: json, code
    """
    value, view = kvs_distributor.get(key)
    if not value:
        return nonexistent_key_response("GET")
    return read_key_response(value) if view == ip else read_key_response(value, view)


def _handle_put(key: str, value: str) -> tuple:
    """Interface for handling PUT requests

    Args:
        key (str)
        value (str)

    Returns:
        tuple: json, code
    """
    if not kvs_distributor.key_valid(key):
        return invalid_key_response()
    view, is_update = kvs_distributor.put(key, value)
    if view == ip:
        # withhold address since key stored locally
        return insert_key_response() if not is_update else update_key_response()
    else:
        return (
            insert_key_response(address=view)
            if not is_update
            else update_key_response(address=view)
        )


def _handle_delete(key: str) -> tuple:
    """Interface for handling DELETE requests

    Args:
        key (str)

    Returns:
        tuple: json, code
    """
    view = kvs_distributor.delete(key)
    if not view:
        return nonexistent_key_response("DELETE")
    else:
        # withold address if equal to local ip
        return (
            delete_key_response(address=view) if view != ip else delete_key_response()
        )


@kvs_router.route("/view-change-propogated", methods=["PUT"])
def propogate_view_change():
    """Recieved by a follower node from a leader propogating a view change

    JSON:
        views (list): IP addresses of each node in the network

    Returns:
        tuple: json, status code
    """
    json = request.get_json()
    views = json.get("views")
    key_count = kvs_distributor.change_view(views, propogate=False)
    return propogated_view_change_response(count=key_count)


@kvs_router.route("/view-change", methods=["PUT"])
def client_view_change():
    """Client interface to perform a view change

    JSON:
        view (str): comma delimited IP addresses of each node in the network

    Returns:
        tuple: json, status code
    """
    json = request.get_json()
    views = json.get("view").split(",")
    key_count = kvs_distributor.change_view(views, propogate=True)
    return view_change_response(key_count)


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
    return success_response()


@kvs_router.route("/key-count", methods=["GET"])
def key_count():
    """Get number of keys in KVS

    Returns:
        tuple: json, status code
    """
    count = kvs_distributor.key_count()
    return key_count_response(count)




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
    if request.method == "GET":
        return _handle_get(key)
    elif request.method == "DELETE":
        return _handle_delete(key)
    elif request.method == "PUT":
        json = request.get_json()
        if "value" not in json:
            return value_missing_response()
        return _handle_put(key, json.get("value"))


@kvs_router.route("/shards", methods=["GET"])
def shards():
    """Get information for all shards. 

    Returns:
        "message" : "Shard membership retrieved successfully"
        "shards"  : ["1","2"]
    """
    return 

@kvs_router.route("/shards/<id>", methods=["GET"])
def shards(id):
    """Get information for specific shard

    Returns:
        "message" : "Shard membership retrieved successfully"
        "shard-id"  : "2"
        "key-count" : 4
        "replicas" : ["10.10.0.2:13800","10.10.0.3"]
    """
    return 