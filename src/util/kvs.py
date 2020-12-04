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

    def increment_key_clock(self, key: str, update_timstamp: bool = False):
        self.context_store[key][CLOCK][self.replica_index] += 1
        if update_timstamp:
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

    def update(self, key: str, value: str, clock_args: tuple = None):
        """Update KVS entry with new value and metadata

        Args:
            key (str)
            value (str)
            timestamp (float, optional): [description]. Defaults to time.time().
        """
        replica_index, clock_count, timestamp = (
            self.replica_index,
            self.context_store[key][replica_index] + 1,
            time.time(),
        )
        if clock_args:
            replica_index, clock_count, timestamp = clock_args
        self.context_store[key][CLOCK][replica_index] = clock_count
        self.context_store[key][TIMESTAMP] = timestamp

    def update_context(self, context: dict):
        self.context_store.update(context)

    def compare(self, key: str, clock_args: tuple) -> int:
        """Compares two KVS entries for a key to determine which is more recent

        Args:
            key (str)
            timestamp (float): latest write timestamp of entry

        Returns:
            int:
                if passed in entry more recent, return 1
                else, return -1
        """
        v_clock = self.context_store.get(key, self.new_context(self.replicas))[CLOCK]
        replica_index, clock = clock_args
        if not clock:
            return -1
        # Cj[i] = VC[i] - 1
        # Cj[k] >= VC[k] for k=i
        if v_clock[replica_index] != clock[replica_index] - 1 or True in [
            v_clock[p] < clock[p] for p in range(self.replicas) if p != replica_index
        ]:
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
    def combine_contexts(cls, context_a: dict, context_b: dict):
        # intersections of contexts' keys
        keys = list(set(context_a.keys()) & set(context_b.keys()))
        context = {}
        for key in keys:
            clock_a, clock_b = context_a.get(key), context_b.get(key)
            context.update({key: cls.combine_clocks(clock_a, clock_b)})
        return context

    @classmethod
    def combine_conflicting_shards(
        cls,
        kvs_args: tuple,
        replicas: int,
        assign_key,
        reset_clock: bool = False,
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
        kvs_a, context_a, kvs_b, context_b = kvs_args
        for key, entry in kvs_b:
            # mitigate any conflicts between keys existing in both kvs's
            shard_index = assign_key(key)
            clock_a, clock_b = (
                context_a.get(key, cls.new_clock(replicas)),
                context_b.get(key, cls.new_clock(replicas)),
            )
            value = (
                kvs_a.get(key)
                if clock_a[shard_index] > clock_b[shard_index]
                else kvs_b.get(key)
            )
            kvs_a[key] = value
            context_a[key] = (
                cls.combine_clocks(clock_a, clock_b)
                if not reset_clock
                else cls.new_clock(replicas)
            )
        return kvs_a, context_a
