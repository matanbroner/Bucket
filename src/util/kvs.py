import time
from util.misc import printer


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
        return self.kvs

    def context(self) -> dict:
        """Return valueless causal context of each key in KVS

        Returns:
            dict: key with dict value having keys and "timestamp"
        """
        return {
            key: {"timestamp": entry["timestamp"]} for key, entry in self.kvs.items()
        }

    def reset_context(self, key: str, timestamp: float = None):
        if not timestamp:
            timestamp = time.time()
        if self.kvs.get(key):
            self.kvs[key]["timestamp"] = timestamp

    def get(self, key, default=None):
        return self.kvs.get(key, default)

    def insert(self, key: str, value: str):
        """Insert new key-value pair into KVS

        Args:
            key (str)
            value (str)
        """
        self.kvs[key] = {"value": value, "timestamp": time.time()}

    def update(self, key: str, value: str, timestamp: float = None):
        """Update KVS entry with new value and metadata

        Args:
            key (str)
            value (str)
            timestamp (float, optional): [description]. Defaults to time.time().
        """
        if not timestamp:
            timestamp = time.time()
        self.kvs[key] = {"value": value, "timestamp": timestamp}

    def compare(self, key: str, timestamp: float) -> int:
        """Compares two KVS entries for a key to determine which is more recent

        Args:
            key (str)
            timestamp (float): latest write timestamp of entry

        Returns:
            int:
                if passed in entry more recent, return 1
                else, return -1
        """
        entry = self.kvs.get(key, None)
        if not entry:
            return 1
        if entry["timestamp"] > timestamp:
            return -1
        else:
            return 1

    @classmethod
    def combine_conflicting_shards(
        cls, kvs_a: dict, kvs_b: dict, reset_clock: bool = False, as_dict: bool = True
    ):
        """Merges two shards (ie. dicts) which may have conflicting values for keys

        Args:
            kvs_a (dict)
            kvs_b (dict)
            reset_clock (bool, optional): Reset timestamp for each key in returned shard. Defaults to False.
            as_dict (bool, optional): return shard as dict rather than a new KVS instance. Defaults to True.

        Returns:
            KVS: [description]
        """
        start_timestamp = time.time()
        kvs_a, kvs_b = cls(kvs_a), cls(kvs_b)
        for key, entry in kvs_b:
            # mitigate any conflicts between keys existing in both kvs's
            if kvs_a.compare(key, entry["timestamp"]) == 1:
                kvs_a.kvs[key] = entry
            if reset_clock:
                kvs_a.reset_context(key, start_timestamp)
        return kvs_a.json() if as_dict else kvs_a
