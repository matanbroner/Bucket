import sys
import mmh3
import requests

from util.kvs import KVS
from util.view import View
from util.misc import (
    request,
    printer,
    status_code_success,
    get_request_first_success,
    key_count_max,
)
from util.scheduler import Scheduler

from constants.errors import (
    INVALID_CAUSAL_CONTEXT,
    KEY_TOO_LONG,
    KEY_NOT_EXIST,
    VALUE_MISSING,
)
from constants.messages import GET_SUCCESS, PUT_NEW_SUCCESS, PUT_UPDATE_SUCCESS
from constants.responses import GetResponse, PutResponse

GOSSIP_INTERVAL = 5


class KVSDistributor:
    """Distributor of underlying KVS structure for multiple replicated shards

    Args:
        ips (list): list of IP addresses in current view
        address (str): IP address of node
        repl_factor (int): replication factor of shards
    """

    def __init__(self, ips: list, address: str, repl_factor: int):
        self.view = View(ips, address, repl_factor)
        self.kvs = KVS(replicas=repl_factor, replica_index=self.view.replica_index)
        # schedule repeated gossip in bucket
        # self._start_gossiping()

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
            list: tuples with each item being of type (response, IP address of response origin)
        """
        if not json:
            json = [{} for ip in ips]  # default empty json
        elif isinstance(json, dict):
            json = [json for ip in ips]  # reuse same json n-1 times
        responses = []
        for index, ip in enumerate(ips):
            if ip != self.view.address:
                url_complete = ip + url
                try:
                    responses.append(
                        (request(url_complete, method, headers, json[index]), ip)
                    )
                except requests.exceptions.ConnectionError:
                    printer(f"Connection error to IP: {ip}")
                    pass
        return responses

    def _request_bucket(
        self, bucket: list, url: str, method: str, headers: dict = {}, json={}
    ) -> tuple:
        """Request nodes in a bucket until a valid response is returned

        Args:
            bucket (list): [list of IP addresses in bucket
            url (str)
            method (str)
            headers (dict, optional). Defaults to {}.
            json (dict, optional) . Defaults to {}.

        Returns:
            tuple: requests.Response, IP of request
        """
        for ip in bucket:
            try:
                url_complete = ip + url
                response = request(url_complete, method, headers, json)
                if response.status_code != 500:
                    return response, ip
            except requests.exceptions.ConnectionError:
                pass
        # Entire bucket is down
        # TODO: Figure out if this use case needs to be handled...
        return None, None

    def _causal_context_ahead(
        self, key: str, bucket_index: int, context: dict = {}
    ) -> bool:
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
        return self.kvs.compare(key, key_context["clock"]) == 1

    def _update_relevant_context(self, context: dict):
        # Get only keys belnging to shard
        return {
            key: self.kvs.combine_contexts(
                context_a={key: self.kvs.get_key_context(key)},
                context_b={key: context.get(key)},
            )[key]
            for key in context
            if self.view.is_own_bucket_index(self._assign_key_bucket(key))
        }

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
            distributed_keys[bucket_index][key] = kvs.get(key)
        return distributed_keys

    def _generate_replica_template(self, bucket_shards: list) -> list:
        """Creates expected tamplate for a view change response to client

        Args:
            bucket_shards (list): response from _shard_keys

        Returns:
            list: shards in expected format
        """
        return [
            {
                "shard-id": index,
                "key-count": len(bucket_shards[index]),
                "replicas": self.view.buckets[index],
            }
            for index, _ in enumerate(bucket_shards)
        ]

    def _key_valid(self, key: str) -> bool:
        """Check if key is valid for insertion/update

        Args:
            key (str)

        Returns:
            bool
        """
        return isinstance(key, str) and len(key) <= 50

    def _start_gossiping(self):
        if self.view.repl_factor > 1:
            Scheduler.add_job(
                function=self._send_gossip,
                seconds=GOSSIP_INTERVAL,
                id="send_gossip",
            )

    def _send_gossip(self):
        bucket = self.view.self_replication_bucket(own_ip=False)
        url = "/kvs/gossip"
        json = {"kvs": self.kvs.json(), "context": self.kvs.context()}
        self._request_multiple_ips(ips=bucket, url=url, method="PUT", json=json)

    def _append_key_context(self, key, context):
        context.update({key: self.kvs.context().get(key)})
        return context

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
        return_template = {"kvs": self.kvs.json(), "context": self.kvs.context()}
        # get all current + legacy ips as set to allow for dropped nodes
        ips_union = [
            ip for ip in list(set(ips + self.view.all_ips)) if ip != self.view.address
        ]
        # set new view -> new buckets
        self.view = View(ips, self.view.address, repl_factor)
        Scheduler.clear_jobs()
        # init gossip again with new view, needed to force refresh scheduler underlying class
        self._start_gossiping()
        if propagate:
            central_kvs = {}
            central_context = {}

            # get all sub-kvs's (ie. shards) from each node
            # note that there will be duplicate shards, but we do this
            # to mitigate potential missed gossip between nodes in buckets
            url = "/kvs/view-change-propagate"
            json = {"view": ips, "repl-factor": repl_factor}
            shards = [
                (response.json().get("kvs"), response.json().get("context"))
                for response, _ in self._request_multiple_ips(
                    ips=ips_union, url=url, method="PUT", json=json
                )
                if status_code_success(response.status_code)
            ]
            # use a mitigation function to combine all shards
            # this function will pick a more recent value in an identical key conflict

            # include own shard
            shards.append((self.kvs.json(), self.kvs.context()))
            for shard, context in shards:
                if isinstance(shard, dict):
                    central_kvs, central_context = KVS.combine_conflicting_shards(
                        kvs_args=(central_kvs, central_context, shard, context),
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

            # set own shard if still in view
            if self.view.address in self.view.all_ips:
                self.kvs = KVS(
                    replicas=self.view.repl_factor,
                    replica_index=self.view.replica_index,
                    kvs=bucket_shards[self.view.bucket_index],
                )

            # generate return template
            return_template = self._generate_replica_template(bucket_shards)
        return return_template

    def merge_shard(self, shard: dict):
        """Sets KVS to be a recieved shard

        Args:
            shard (dict): key-value pairs
        """
        self.kvs = KVS(
            replicas=self.view.repl_factor,
            replica_index=self.view.replica_index,
            kvs=shard,
        )

    def merge_gossip(self, shard: dict, context: dict):
        """Accepts gossip from replicas in same bucket

        Args:
            shard (dict): key-value structure
        """
        kvs, context = self.kvs.combine_conflicting_shards(
            kvs_args=(self.kvs.json(), self.kvs.context(), shard, context),
        )
        self.kvs = KVS(
            replicas=self.view.repl_factor,
            replica_index=self.view.replica_index,
            kvs=kvs,
            context_store=context,
        )

    def key_count(self, bucket_index: int = None) -> int:
        """Returns number of keys in KVS
        Args:
            bucket_index (int): shard ID for key count. Defaults to None (ie. own shard ID)
        Returns:
            int
        """
        if bucket_index == None or bucket_index == self.view.bucket_index:
            return len(self.kvs.json())
        else:
            url = "/kvs/key-count"
            bucket = self.view.buckets[bucket_index]
            responses = self._request_multiple_ips(ips=bucket, url=url, method="GET")
            return key_count_max(responses)

    def shard_id(self) -> int:
        """Return shard ID of own node

        Returns:
            int
        """
        return self.view.bucket_index

    def bucket(self, id: int = None) -> list:
        """Abstraction of View's self_replication_bucket

        Returns:
            list: all IP addresses in node's bucket
        """
        id = self.view.bucket_index if id == None else id
        return self.view.buckets[id]

    def all_bucket_ids(self) -> list:
        """Return ID's of all buckets

        Returns:
            list
        """
        return [id for id, _ in enumerate(self.view.buckets)]

    def get(self, key: str, context: dict = {}) -> GetResponse:
        """Public interface for completing GET requests

        Args:
            key (str)
            context (str, optional): causal context. Defaults to {}.

        Returns:
            GetResponse
        """
        bucket_index = self._assign_key_bucket(key)
        if self.view.is_own_bucket_index(bucket_index):
            # given context is ahead of local KVS
            if self._causal_context_ahead(key, bucket_index, context):
                return GetResponse(
                    status_code=400,
                    value=None,
                    context=context,
                    address=self.view.address,
                    error=INVALID_CAUSAL_CONTEXT,
                )
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
            # get up to date context for each key in context
            self.kvs.update_context(self._update_relevant_context(context))
            # assign updated context locally
            # self.kvs.update_context(context)
            # increment clock for read events
            self.kvs.increment_key_clock(key)
            # add key to context for propagation
            context.update({key: self.kvs.get_key_context(key)})
            # successful fetch
            return GetResponse(
                status_code=200,
                value=entry,
                context=context,
                address=self.view.address,
                error=None,
                message=GET_SUCCESS,
            )
        else:
            # proxy request to another bucket
            bucket = self.view.buckets[bucket_index]
            url = f"/kvs/keys/{key}"
            json = {"causal-context": context}
            responses = self._request_multiple_ips(
                ips=bucket, url=url, method="GET", json=json
            )
            # ensures that a 200 can be obtained even if not all replicas have a value yet
            best_reponse = get_request_first_success(responses)
            if best_reponse[0] != None:
                return GetResponse.from_flask_response(
                    best_reponse[0], manual_address=best_reponse[1]
                )
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
            # update context where needed
            # self.kvs = self.kvs.combine_conflicting_shards(
            #     context, self.kvs.json(), as_dict=False
            # )
            # key invalid
            if not self._key_valid(key):
                return PutResponse(
                    status_code=400,
                    error=KEY_TOO_LONG,
                    address=self.view.address,
                    context=context,
                )
            # value missing
            elif value == None:
                return PutResponse(
                    status_code=400,
                    error=VALUE_MISSING,
                    address=self.view.address,
                    context=context,
                )
            entry = self.kvs.get(key)
            status_code = None
            message = None
            if entry:
                # update key-value
                self.kvs.update(key, value)
                status_code = 200
                message = PUT_UPDATE_SUCCESS
            else:
                # insert key-value
                self.kvs.insert(key, value)
                status_code = 201
                message = PUT_NEW_SUCCESS
            # get up to date context for each key in context
            self.kvs.update_context(self._update_relevant_context(context))
            # assign updated context locally
            # self.kvs.update_context(context)
            # add key to context for propagation
            context.update({key: self.kvs.get_key_context(key)})
            return PutResponse(
                status_code=status_code,
                address=self.view.address,
                message=message,
                context=context,
            )

        else:
            # proxy request to another bucket
            bucket = self.view.buckets[bucket_index]
            url = f"/kvs/keys/{key}"
            json = {"causal-context": context, "value": value}
            proxy_response, ip = self._request_bucket(
                bucket=bucket, url=url, method="PUT", json=json
            )
            if proxy_response != None:
                return PutResponse.from_flask_response(
                    proxy_response, manual_address=ip
                )
            # if entire bucket fails to respond, unlikely use case
            return PutResponse(
                status_code=503,
                context=context,
                address=self.view.address,
                error=INVALID_CAUSAL_CONTEXT,
            )
