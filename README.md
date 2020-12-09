# 🪣 Sharded KVS

`UCSC CSE138 Fall 2020`
</br>
A causally consistent sharded key value store with relatively even distribution of keys per "bucket".

# Mechanics

The following is a description on how some of the cooler parts of this application work.

## Sharding

For even distribution of keys among shards, the KVS uses an implementation of [MurmurHash](https://en.wikipedia.org/wiki/MurmurHash) to designate each key a "bucket" (shard):

    hashed = mmh3.hash128(key, signed=False)
    p = hashed / float(2 ** 128)
    for bucket_index in range(num_buckets):
        if (
    	bucket_index / float(num_buckets) <= p
    	and (bucket_index + 1) / float(num_buckets) > p
        ):

    	    return bucket_index
    return num_buckets - 1

Keys are generally distributed evenly, and shards' key counts are generally within a 30% difference or less.

## Causal Consistency

Simply put, causal consistency states that all causally related operations should be read in the same order accross a system. The KVS network tracks causality through a propagated causal consistency structure.
The structure of a casual consistency object is as follows:

    [
        ["a", {
        "cause": [["b", 1594370977.537462]],
        "deleted": false,
        "last-write": 1607370977.5734642
        }]
    ]

Causal consistency is a series of write events and the causal writes they are dependent on. In this case, key `a` was written as a result of the write of `b` in the given timestamp. Note that `a` being depedent on `b` indicates that either `b` was written before writing `a`, read before writing `a`, or deleted before writing `a`. The KVS's mechanism will verify that during the reading of a key, each causal write in each item in the given causal consistency is available in the given replica. If the key in question does not belong to the shard in which that replica is placed, the replica will query the correct shard to which that key is hashed, and will verify that a timstamp for the last write of said key is later or equal to the one in the given cause.

In short, if `b` belongs to the replica recieiving the request, the replica willl verify that it has seen an equal to or later write to `b`, and if not the replica will ask `b`'s corresponding shard if it can provide an equal to or later write for `b`. Failing to fulfill the correct case will result in a `400` being returned to the client, indicating a causal consistency error.

# API

A Docker subnet can be used to provide inter-node communication, though any hosting platform is usable, so long as each node is publicly exposed through its given host and port. To create a subnet, use:

    docker network create --subnet=10.10.0.0/16 <subnet-name>

To build the KVS node image, use:

    docker build -t kvs <path-to-dockerfile-directory>

To create a KVS node, use the provided Dockerfile. Host and port can be edited in `src/config.py`.

    docker run -p 13800:13800 \
             --net=<subnet-name> --ip=10.10.0.2 --name="node1" \
             -e ADDRESS="10.10.0.2:13800" \
             -e VIEW="10.10.0.2:13800,10.10.0.3:13800,10.10.0.4:13800,10.10.0.5:13800" \
             -e REPL_FACTOR=2 \
             kvs


Note the arguments passed to each container:

- `ADDRESS` (required): IP address of node
- `VIEW` (required): current view of the network, meaning in scope nodes
- `REPL_FACTOR` (required): replication factor of shards. Note that the number of nodes **must** be evenly divisible by the replication factor.

Each request returns a `causal-context` in its response. This context represents the causality created through a chain of requests, such that writes can be labled as causally dependent on this context. Note that for the KVS nodes to remain causally consistent, **`causal-context` must be propagated from each request to the next**.

Note that in addition to the below return values, a node may return status code `503` in the case of a request timeout or if it is unable to satisfy a request due to an entire shard being down.

## Read a key

    curl --request   GET \
       --header    "Content-Type: application/json" \
       --data      '{"causal-context":causal-context-object}' \
       http://127.0.0.1:13800/kvs/keys/key

Return values:

- `200`: read successful, returns value
- `404`: key does not exist
- `400`: causality error, requested replica cannot satify causal consitency

## Write a key

Must include a `value` key in request body.

    curl --request   PUT \
       --header    "Content-Type: application/json" \
       --data      '{"value":"foo_bar","causal-context":causal-context-object}' \
       http://127.0.0.1:13800/kvs/keys/key

Return values:

- `200`: updated successfully
- `201`: wrote successfully
- `400`: invalid request (value missing or invalid key)

## Delete a key

    curl --request   DELETE \
       --header    "Content-Type: application/json" \
       --data      '{"causal-context":causal-context-object}' \
       http://127.0.0.1:13800/kvs/keys/key

Return values:

- `200`: deleted successfully
- `404`: key does not exist

## Change view

Used to update the current view, allows nodes to see additions or removals of nodes from the network, as well as the ability to change replication factors. Keys are automatically redistributed between the nodes of the new view. Note that **causal context is not preserved between views of the network**. View changes require that request body have a `view` and `repl-factor` key.

    curl --request   PUT \
       --header    "Content-Type: application/json" \
       --data '{"view":"10.10.0.2:13800,10.10.0.3:13800","repl-factor":1}' \
       http://127.0.0.1:13800/kvs/view-change

Return values:

- `200`: successfully changed view

## Get node key count

    curl --request   GET \
       --header    "Content-Type: application/json" \
       http://127.0.0.1:13800/kvs/key-count

Return values:

- `200`: successfully got key count of node

## Get all shard IDs

    curl --request   GET \
       --header    "Content-Type: application/json" \
       http://127.0.0.1:13800/kvs/shards

Return values:

- `200`: successfully got all shard IDs

## Get shard information

    curl --request   GET \
       --header    "Content-Type: application/json" \
       --write-out "%{http_code}\n" \
       http://127.0.0.1:13800/kvs/shards/<id>

Return values:

- `200`: successfully got shard information

# Notes

- This application is an assignment for a course, and is not robust in its error checking nor its configuration options. All features work well under certain assumptions, such as at least one replica in each shard staying up. Failiure to uphold valid input or assumptions of system will lead to a bad time using this project...
