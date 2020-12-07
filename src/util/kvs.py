import time
from util.misc import printer
from typing import NamedTuple
from constants.terms import KEY, VALUE, TIMESTAMP, CAUSE, CONTEXT


class KVSItem:
    def __init__(self, value: str, last_write: float = None, cause: list = []):
        self[VALUE] = value
        self[TIMESTAMP] = last_write or time.time()
        self[CAUSE] = cause
        self.is_deleted = False

    def __getitem__(self, key):
        return getattr(self, key)

    def __setitem__(self, key, value):
        return setattr(self, key, value)

    def update(self, key: str, value: str, last_write: float = None, cause: list = []):
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

    def last_write(self):
        return self[TIMESTAMP]

    def reset_context(self, timestamp: float = None):
        if not timestamp:
            timestamp = time.time()
        self[TIMESTAMP] = timestamp
        self[CAUSE] = []

    @classmethod
    def from_json(cls, json: dict):
        value, last_write, cause = (
            json.get(VALUE),
            json.get(TIMESTAMP, time.time()),
            json.get(CAUSE, []),
        )
        if value == None:
            raise RuntimeError(f"Value not provided in {json}")
        return cls(value=value, last_write=last_write, cause=cause)


class KVS:
    def __init__(self):
        self.kvs = {}

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

    def reset_context(self):
        timestamp = time.time()
        for entry in self.kvs.values():
            entry.reset_context(timestamp=timestamp)

    def get(self, key, return_value=False):
        entry = self.kvs.get(key)
        if entry:
            return entry[VALUE] if return_value else entry
        return None

    def upsert(self, key: str, value: str, cause: dict = []):
        """Insert new key-value pair into KVS

        Args:
            key (str)
            value (str)
        """
        inserted = key not in self.kvs
        self.kvs[key] = KVSItem(value, cause=cause)
        return inserted

    @classmethod
    def from_shard(cls, shard: dict):
        instance = cls()
        for key, entry in shard.items():
            instance.kvs[key] = KVSItem.from_json(entry)
        return instance

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
        kvs_a, kvs_b = cls.from_shard(shard_a), cls.from_shard(shard_b)
        all_keys = set().union(shard_a.keys(), shard_b.keys())
        final_shard = {}
        for key in all_keys:
            entry_a, entry_b = kvs_a.get(key), kvs_b.get(key)
            if entry_a and entry_b:
                final_shard[key] = (
                    entry_a.json()
                    if entry_a[TIMESTAMP] > entry_b[TIMESTAMP]
                    else entry_b.json()
                )
            else:
                final_shard[key] = entry_a.json() if entry_a else entry_b.json()
        return final_shard
