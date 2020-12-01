import sys
import mmh3
import requests

from util.kvs import KVS
from util.view import View
from util.misc import request, printer, status_code_success

from constants.errors import (
    INVALID_CAUSAL_CONTEXT,
    KEY_TOO_LONG,
    KEY_NOT_EXIST,
    VALUE_MISSING,
)
from constants.messages import GET_SUCCESS, PUT_NEW_SUCCESS, PUT_UPDATE_SUCCESS
from constants.responses import GetResponse, PutResponse


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
        """Request nodes in a bucket until a valid response is returned

        Args:
            bucket (list): [list of IP addresses in bucket
            url (str)
            method (str)
            headers (dict, optional). Defaults to {}.
            json (dict, optional) . Defaults to {}.

        Returns:
            requests.Response
        """
        for ip in bucket:
            try:
                url_complete = ip + url
                response = request(url_complete, method, headers, json)
                if response.status_code != 500:
                    return response
            except requests.exceptions.ConnectionError:
                pass
        # Entire bucket is down
        # TODO: Figure out if this use case needs to be handled...
        return None

    def _causal_context_ahead(self, key: str, context: dict = {}) -> bool:
        """Check if given causal context is ahead of local KVS for a given key

        Args:
            key (str)
            context (dict, optional). Defaults to {}.

        Returns:
            bool: True if given context is ahead of local context
        """
        key_context = context.get(key)
        if not key_context:
            # context not aware of key
            return False
        return self.kvs.compare(key, key_context["timestamp"]) == 1

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
        """Shard keys of given KVS to all available buckets

        Args:
            kvs (dict)

        Returns:
            list: list of dicts with length number of buckets. Each dict is a KVS shard.
        """
        distributed_keys = [{} for bucket in self.view.buckets]
        for key in kvs:
            bucket_index = self._assign_key_bucket(key)
            distributed_keys[bucket_index]["kvs"][key] = kvs.get(key)
        return distributed_keys

    def _generate_replica_template(self, bucket_shards: dict) -> list:
        """Creates expected tamplate for a view change response to client

        Args:
            bucket_shards (dict): response from _shard_keys

        Returns:
            list: shards in expected format
        """
        return [
            {
                "shard-id": index,
                "key_count": len(bucket_shards[index]),
                "replicas": self.view.buckets[index],
            }
            for index in enumerate(bucket_shards)
        ]

    def _key_valid(self, key: str) -> bool:
        """Check if key is valid for insertion/update

        Args:
            key (str)

        Returns:
            bool
        """
        return isinstance(key, str) and len(key) <= 50

    # Public Functions

    def change_view(self, ips: list, repl_factor: int, propagate: bool = False) -> dict:
        """Public interface for a view change

        Args:
            ips (list): list of all IP addresses in new view
            repl_factor (int): replication factor of new view
            propagate (bool, optional): should node propagate view change to remaining nodes. Defaults to False.

        Returns:
            dict: returned template, depending on propagation flag
        """
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
                    central_kvs = KVS.combine_conflicting_shards(
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

    def merge_shard(self, shard: dict):
        """Sets KVS to be a recieved shard

        Args:
            shard (dict): key-value pairs
        """
        self.kvs = KVS(shard)

    def key_count(self) -> int:
        """Returns number of keys in KVS

        Returns:
            int
        """
        return len(self.kvs)

    def get(self, key: str, context: str = {}) -> GetResponse:
        """Public interface for completing GET requests

        Args:
            key (str)
            context (str, optional): causal context. Defaults to {}.

        Returns:
            GetResponse
        """
        bucket_index = self._assign_key_bucket(key)
        if self.view.is_own_bucket_index(bucket_index):
            entry = self.kvs.get(key)
            # key not in local KVS
            if not entry:
                return GetResponse(
                    status_code=404,
                    value=None,
                    context=context,
                    address=self.view.address,
                    error=KEY_NOT_EXIST,
                )
            # given context is ahead of local KVS
            if self._causal_context_ahead(key, context):
                return GetResponse(
                    status_code=400,
                    value=None,
                    context=context,
                    address=self.view.address,
                    error=INVALID_CAUSAL_CONTEXT,
                )
            # successful fetch
            return GetResponse(
                status_code=200,
                value=entry["value"],
                context=self.kvs.context(),
                address=self.view.address,
                error=None,
                message=GET_SUCCESS,
            )
        else:
            # proxy request to another bucket
            bucket = self.view.buckets[bucket_index]
            url = f"/kvs/keys/{key}"
            json = {"causal-context": context}
            proxy_response = self._request_bucket(
                bucket=bucket, url=url, method="GET", json=json
            )
            if proxy_response:
                return GetResponse.from_flask_response(proxy_response)
            # if entire bucket fails to respond, unlikely use case
            return GetResponse(
                status_code=503,
                value=None,
                context=context,
                address=self.view.address,
                error=INVALID_CAUSAL_CONTEXT,
            )

    def put(self, key: str, value: str = None, context: dict = {}) -> PutResponse:
        """Public interface for completing PUT requests

        Args:
            key (str)
            value (str)
            context (str, optional): causal context. Defaults to {}.

        Returns:
            GetResponse
        """
        bucket_index = self._assign_key_bucket(key)
        if self.view.is_own_bucket_index(bucket_index):
            # key invalid
            if not self._key_valid(key):
                return PutResponse(
                    status_code=400,
                    error=KEY_TOO_LONG,
                    address=self.view.address,
                    context=context,
                )
            elif not value:
                # value missing
                return PutResponse(
                    status_code=400,
                    error=VALUE_MISSING,
                    address=self.view.address,
                    context=context,
                )
            entry = self.kvs.get(key)
            if entry:
                # update key-value
                self.kvs.update(key, value)
                return PutResponse(
                    status_code=200,
                    context=self.kvs.context(),
                    address=self.view.address,
                    message=PUT_UPDATE_SUCCESS,
                )
            else:
                # inserty key-value
                self.kvs.insert(key, value)
                return PutResponse(
                    status_code=201,
                    context=self.kvs.context(),
                    address=self.view.address,
                    message=PUT_NEW_SUCCESS,
                )
        else:
            # proxy request to another bucket
            bucket = self.view.buckets[bucket_index]
            url = f"/kvs/keys/{key}"
            json = {"causal-context": context, "value": value}
            proxy_response = self._request_bucket(
                bucket=bucket, url=url, method="PUT", json=json
            )
            if proxy_response:
                return PutResponse.from_flask_response(proxy_response)
            # if entire bucket fails to respond, unlikely use case
            return PutResponse(
                status_code=503,
                value=None,
                context=context,
                address=self.view.address,
                error=INVALID_CAUSAL_CONTEXT,
            )
