import time
from util.misc import printer
from typing import NamedTuple
from constants.terms import KEY, VALUE, TIMESTAMP, CAUSE, CONTEXT


class KVSItem:
    def __init__(self, value: str, last_write: float = None, cause: dict = {}):
        self[VALUE] = value
        self[TIMESTAMP] = last_write or time.time()
        self[CAUSE] = cause

    def update(self, key: str, value: str, last_write: float = None, cause: dict = {}):
        self[VALUE] = value
        self[TIMESTAMP] = last_write or time.time()
        self[CAUSE] = cause

    def json(self):
        return {
            VALUE: self[VALUE],
            TIMESTAMP: self[TIMESTAMP],
            CAUSE: self[CAUSE],
        }

    def context(self):
        return {TIMESTAMP: self[TIMESTAMP], CAUSE: self[CAUSE]}

    def reset_context(self, timestamp: float = None):
        if not timestamp:
            timestamp = time.time()
        self[TIMESTAMP] = timestamp
        self[CAUSE] = {}

    @staticmethod
    def from_json(cls, json: dict):
        value, last_write, cause = (
            json.get(VALUE),
            json.get(TIMESTAMP, time.time()),
            json.get(CAUSE, {}),
        )
        if not value:
            raise RuntimeError(
                f"Value not provided for last_write [{last_write}] and cause [{cause}]"
            )
        return cls(value=value, last_write=last_write, cause=cause)


class KVS:
    def __init__(self, kvs: dict = {}):
        self.kvs = kvs

    def __iter__(self):
        return iter(self.kvs.items())

    def __len__(self):
        return len(self.kvs)

    def clear(self):
        """Reset KVS"""
        self.kvs = {}

    def json(self) -> dict:
        """Return JSON serializable version of KVS

        Returns:
            dict: KVS underlying dict
        """
        return {key: entry.json() for key, entry in self.kvs.items()}

    def context(self) -> dict:
        """Return valueless causal context of each key in KVS

        Returns:
            dict: key with dict value having keys and "timestamp"
        """
        return {key: entry.context() for key, entry in self.kvs.items()}

    def reset_context(self, key: str):
        timestamp = time.time()
        for entry in self.kvs.values():
            entry.reset_context(timestamp=timestamp)

    def get(self, key, return_value=False):
        entry = self.kvs.get(key)
        if entry:
            return entry[VALUE] if return_value else entry
        return None

    def insert(self, key: str, value: str, cause: dict = {}):
        """Insert new key-value pair into KVS

        Args:
            key (str)
            value (str)
        """
        self.kvs[key] = KVSItem(value, cause=cause)

    def context_was_witnessed(self, context: {}) -> int:
        """Compares two KVS entries for a key to determine which is more recent

        Args:
            key (str)
            timestamp (float): latest write timestamp of entry

        Returns:
            int:
                if passed in entry more recent, return 1
                else, return -1
        """
        for key in context:
            entry = self.kvs.get(key)
            if not entry or entry[TIMESTAMP] < context[key][TIMESTAMP]:
                return False
        return True

    @staticmethod
    def from_shard(cls, shard: dict):
        kvs = cls()
        for key, entry in shard.items():
            self.kvs[key] = KVSItem.from_json(entry)
        return kvs

    @classmethod
    def combine_conflicting_shards(cls, shard_a: dict, shard_b: dict):
        """Merges two shards (ie. dicts) which may have conflicting values for keys

        Args:
            kvs_a (dict)
            kvs_b (dict)
            reset_clock (bool, optional): Reset timestamp for each key in returned shard. Defaults to False.
            as_dict (bool, optional): return shard as dict rather than a new KVS instance. Defaults to True.

        Returns:
            KVS: [description]
        """
        kvs_a, kvs_b = cls(shard_a), cls(shard_b)
        all_keys = set().union(shard_a.keys(), shard_b.keys())
        for all_keys:
            # mitigate any conflicts between keys existing in both kvs's
            if kvs_a.compare(key, entry["timestamp"]) == 1:
                kvs_a.kvs[key] = entry
            if reset_clock:
                kvs_a.reset_context(key, start_timestamp)
        return kvs_a.json() if as_dict else kvs_a
