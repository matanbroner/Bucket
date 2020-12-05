import time
from util.misc import printer

CLOCK = "clock"
TIMESTAMP = "last_write"


class KVS:
    def __init__(
        self,
        replicas: int,
        replica_index: int,
        kvs: dict = {},
        context_store: dict = {},
    ):
        self.kvs = kvs
        self.context_store = context_store
        self.replicas = replicas
        self.replica_index = replica_index

        # set a default clock value for all keys if no context given
        if len(kvs) > 0 and len(context_store) == 0:
            self.reset_context()

    def __iter__(self):
        return iter(self.kvs.items())

    def __len__(self):
        return len(self.kvs)

    def clear(self):
        """Reset KVS"""
        self.kvs = {}
        self.context_store = {}

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
        return self.context_store

    def reset_context(self):
        timestamp = time.time()
        self.context_store = {key: self.new_context(self.replicas) for key in self.kvs}

    def get_key_context(self, key: str):
        return self.context_store.get(key)

    def increment_key_clock(self, key: str, update_timestamp: bool = False):
        self.context_store[key][CLOCK][self.replica_index] += 1
        if update_timestamp:
            self.context_store[key][TIMESTAMP] = time.time()

    def get(self, key, default=None):
        return self.kvs.get(key, default)

    def insert(self, key: str, value: str):
        """Insert new key-value pair into KVS

        Args:
            key (str)
            value (str)
        """
        self.kvs[key] = value
        self.context_store[key] = self.new_context(self.replicas)
        self.context_store[key][CLOCK][self.replica_index] = 1

    def update(self, key: str, value: str, clock_args: tuple = None):
        """Update KVS entry with new value and metadata

        Args:
            key (str)
            value (str)
            timestamp (float, optional): [description]. Defaults to time.time().
        """
        replica_index, clock_count, timestamp = (
            self.replica_index,
            self.context_store[key][CLOCK][self.replica_index] + 1,
            time.time(),
        )
        if clock_args:
            replica_index, clock_count, timestamp = clock_args
        self.context_store[key][CLOCK][replica_index] = clock_count
        self.context_store[key][TIMESTAMP] = timestamp
        self.kvs[key] = value

    def update_context(self, context: dict):
        self.context_store.update(context)

    def local_context_ahead(self, key: str) -> int:
        """Compares two KVS entries for a key to determine which is more recent

        Args:
            key (str)
            timestamp (float): latest write timestamp of entry

        Returns:
            int:
                if passed in entry more recent, return 1
                else, return -1
        """
        clock = self.context_store.get(key, self.new_context(self.replicas))[CLOCK]
        # Cj[i] = VC[i] - 1
        # Cj[k] >= VC[k] for k=i
        if clock[self.replica_index] < clock[self.replica_index] - 1 or True in [
            v_clock[p] < clock[p]
            for p in range(self.replicas)
            if p != self.replica_index
        ]:
            printer("Returning 1")
            return 1
        else:
            return -1

    @staticmethod
    def combine_clocks(clock_a: list, clock_b: list):
        return [max(a, b) for a, b in zip(clock_a, clock_b)]

    @classmethod
    def new_context(cls, replicas: int):
        return {CLOCK: [0 for _ in range(replicas)], TIMESTAMP: time.time()}

    @classmethod
    def combine_contexts(
        cls, context_a: dict, context_b: dict, use_intersection: bool = False
    ):
        # intersections of contexts' keys
        keys = set().union(context_a.keys(), context_b.keys())
        if use_intersection:
            keys = [key for key in keys if key in context_a and key in context_b]
        context = {}
        for key in keys:
            entry_a, entry_b = context_a.get(key), context_b.get(key)
            if entry_a and entry_b:
                clock_a, clock_b = entry_a[CLOCK], entry_b[CLOCK]
                ts_a, ts_b = (
                    context_a.get(key)[TIMESTAMP],
                    context_b.get(key)[TIMESTAMP],
                )
                context.update(
                    {
                        key: {
                            CLOCK: cls.combine_clocks(clock_a, clock_b),
                            TIMESTAMP: max(ts_a, ts_b),
                        }
                    }
                )
            else:
                valid_context = context_a.get(key) or context_b.get(key)
                context.update(
                    {
                        key: {
                            CLOCK: valid_context[CLOCK],
                            TIMESTAMP: valid_context[TIMESTAMP],
                        }
                    }
                )
        return context

    @classmethod
    def combine_conflicting_shards(cls, kvs_args: tuple):
        """Merges two shards (ie. dicts) which may have conflicting values for keys

        Args:
            kvs_a (dict)
            kvs_b (dict)
            reset_clock (bool, optional): Reset timestamp for each key in returned shard. Defaults to False.
            as_dict (bool, optional): return shard as dict rather than a new KVS instance. Defaults to True.

        Returns:
            KVS: [description]
        """
        kvs_a, context_a, kvs_b, context_b = kvs_args
        all_keys = set().union(kvs_a.keys(), kvs_b.keys())
        for key in all_keys:
            # mitigate any conflicts between keys existing in both kvs's
            val_a, val_b = (
                kvs_a.get(key),
                kvs_b.get(key),
            )
            entry_a, entry_b = context_a.get(key), context_b.get(key)
            if val_a and val_b:
                value = (
                    kvs_a.get(key)
                    if entry_a[TIMESTAMP] > entry_b[TIMESTAMP]
                    else kvs_b.get(key)
                )
                kvs_a[key] = value
                context_a[key] = cls.combine_contexts({key: entry_a}, {key: entry_b})
            else:
                valid_context, value = (
                    (entry_a, val_a) if val_a else (entry_b, val_b),
                )
                kvs_a[key] = value
                context_a[key] = valid_context
        return kvs_a, context_a
