import sys
import mmh3
import asyncio
import aiohttp
import collections
import requests

from util.kvs import KVS
from util.view import View
from util.misc import request, printer


class KVSDistributor:
    def __init__(self, ips: list, address: str, repl_factor: int):
        self.view = View(ips, address, repl_factor)
        self.kvs = KVS()

    # Private Functions

    def _request_multiple_ips(
        self, ips: list, url: str, method: str, headers: dict = {}, json=None
    ) -> list:
        """Performs a number of sequential requests to multiple IP addresses

        Args:
            ips (list). List of addresses to request to.
            url (str)
            method (str)
            headers (dict, optional). Defaults to {}.
            json (list/dict, optional). Allows for unique json to each ip (list) or identical json (dict). Defaults to None.

        Returns:
            list: results of each request
        """
        if not json:
            json = [{} for ip in ips]  # default empty json
        elif isinstance(json, dict):
            json = [json for ip in ips]  # reuse same json n-1 times
        responses = []
        for index, ip in enumerate(ips):
            if ip != self.view.address:
                url_complete = ip + url
                responses.append(request(url_complete, method, headers, json[index]))
        return responses

    def _request_bucket(
        self, bucket: list, url: str, method: str, headers: dict = {}, json={}
    ) -> requests.Response:
        for ip in bucket:
            url_complete = ip + url
            response = request(url_complete, method, headers, json)
            if response.status_code >= 200 and response.status_code <= 300:
                return response
        # Entire bucket is down
        # TODO: Figure out if this use case needs to be handled...
        return None

    def _assign_key_bucket(self, key: str, num_buckets: int = None) -> int:
        """Determines which replica bucket is assigned a key based on number of buckets and Murmurhash

        Args:
            key (str)
            num_buckets (int, optional): number of buckets to consider when hashing. Defaults to None.

        Returns:
            int: index in self.view.buckets
        """
        if not num_buckets:
            num_buckets = self.view.num_buckets()
        hashed = mmh3.hash128(key, signed=False)
        p = hashed / float(2 ** 128)
        for bucket_index in range(0, num_buckets):
            if (
                bucket_index / float(num_buckets) <= p
                and (bucket_index + 1) / float(num_buckets) > p
            ):
                return bucket_index
        return num_buckets - 1

    def _shard_keys(self, kvs: dict) -> list:
        distributed_keys = [{} for bucket in self.view.buckets]
        for key in kvs:
            bucket_index = self._assign_key_bucket(key)
            distributed_keys[bucket_index]["kvs"][key] = kvs.get(key)
        return distributed_keys

    def _generate_replica_template(self, bucket_shards: dict) -> list:
        return [
            {
                "shard-id": index,
                "key_count": len(bucket_shards[index]),
                "replicas": self.view.buckets[index],
            }
            for index in enumerate(bucket_shards)
        ]

    # Public Functions

    def change_view(self, ips: list, repl_factor: int, propagate: bool = False) -> dict:
        # set up default return as a node's shard
        return_template = self.kvs.json()
        # get all current + legacy ips as set to allow for dropped nodes
        ips_union = list(set(ips + self.views.all_ips))
        # set new view -> new buckets
        self.view = View(ips, self.view.address, repl_factor)
        if propagate:
            central_kvs = {}

            # get all sub-kvs's (ie. shards) from each node
            # note that there will be duplicate shards, but we do this
            # to mitigate potential missed gossip between nodes in buckets
            url = "/kvs/view-change-propagate"
            json = {"view": ips, "repl-factor": repl_factor}
            shards = [
                response.json().get("kvs")
                for response in self._request_multiple_ips(
                    ips=ips_union, url=url, method="PUT", json=json
                )
            ]
            # use a mitigation function to combine all shards
            # this function will pick a more recent value in an identical key conflict
            for shard in shards:
                if isinstance(shard, dict):
                    central_kvs = KVS.combine_conflicting_shard(
                        central_kvs, shard, reset_clock=True, as_dict=True
                    )

            # assign new shard to each bucket
            bucket_shards = self._shard_keys(central_kvs)
            for shard, bucket in zip(bucket_shards, self.view.buckets):
                # send each node in each bucket its shard
                # prevents need for immediate gossip
                url = "/kvs/shard"
                json = {"kvs": shard}
                # if a node fails to get the shard, gossip will handle it
                self._request_multiple_ips(ips=bucket, url=url, method="PUT", json=json)

            # generate return template
            return_template = self._generate_replica_template(bucket_shards)
        return return_template
