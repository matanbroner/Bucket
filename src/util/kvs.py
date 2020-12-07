import time
from util.misc import printer
from typing import NamedTuple
from constants.terms import KEY, VALUE, TIMESTAMP, CAUSE, CONTEXT, DELETED


class KVSItem:
    """Data structure to represent item in KVS. Stores value and causal context

    Args:
        value (str): entry value
        last_write (float, optional): timestamp of last write of entry. Defaults to None.
        cause (list, optional): causal writes for a given entry. Defaults to None.
        is_deleted (bool, optional): indicates whether entry is deleted from public view of KVS. Defaults to False.
    """

    def __init__(
        self,
        value: str,
        last_write: float = None,
        cause: list = [],
        is_deleted: bool = False,
    ):
        self[VALUE] = value
        self[TIMESTAMP] = last_write or time.time()
        self[CAUSE] = cause
        self[DELETED] = is_deleted

    def __getitem__(self, key):
        """Allows bracket get of attribute"""
        return getattr(self, key)

    def __setitem__(self, key, value):
        """Allows bracket set of attribute"""
        return setattr(self, key, value)

    def update(self, key: str, value: str, last_write: float = None, cause: list = []):
        """Update an entry

        Args:
            key (str)
            value (str)
            last_write (float, optional) Defaults to None.
            cause (list, optional): Defaults to [].
        """
        self[VALUE] = value
        self[TIMESTAMP] = last_write or time.time()
        self[CAUSE] = cause

    def delete(self, cause: list = []):
        """Delete entry from public view of KVS

        Args:
            cause (list, optional) Defaults to []
        """
        self[DELETED] = True
        self[CAUSE] = cause
        self[TIMESTAMP] = time.time()

    def json(self) -> dict:
        """JSON serializable view of entry

        Returns:
            dict
        """
        return {
            VALUE: self[VALUE],
            TIMESTAMP: self[TIMESTAMP],
            CAUSE: self[CAUSE],
            DELETED: self[DELETED],
        }

    def context(self) -> dict:
        """JSON serializable context of entry

        Returns:
            dict
        """
        return {TIMESTAMP: self[TIMESTAMP], CAUSE: self[CAUSE], DELETED: self[DELETED]}

    def last_write(self) -> float:
        """Get last write timestamp of entry

        Returns:
            float
        """
        return self[TIMESTAMP]

    def is_deleted(self) -> bool:
        """Is entry visible in public KVS view.

        Returns:
            bool
        """
        return self[DELETED]

    def reset_context(self, timestamp: float = None):
        """Remove all causal context from entry. Reset timestamp.

        Args:
            timestamp (float, optional): manual new timestamp for entry. Defaults to None.
        """
        if not timestamp:
            timestamp = time.time()
        self[TIMESTAMP] = timestamp
        self[CAUSE] = []

    @classmethod
    def from_json(cls, json: dict):
        """Create KVSItem from JSON entry

        Args:
            json (dict): identical format from KVSItem.json()

        Raises:
            RuntimeError: value not provided

        Returns:
            KVSItem
        """
        value, last_write, cause, is_deleted = (
            json.get(VALUE),
            json.get(TIMESTAMP, time.time()),
            json.get(CAUSE, []),
            json.get(DELETED, False),
        )
        if value == None:
            raise RuntimeError(f"Value not provided in {json}")
        return cls(
            value=value, last_write=last_write, cause=cause, is_deleted=is_deleted
        )


class KVS:
    """KVS data strucutre for storing key value pairs with causal context"""

    def __init__(self):
        self.kvs = {}

    def __iter__(self):
        """Allows using 'for ... in ...' on KVS"""
        return iter(self.kvs.items())

    def __len__(self):
        return len(self.kvs)

    def clear(self):
        """Reset KVS"""
        self.kvs = {}

    def json(self, include_deleted=True) -> dict:
        """Return JSON serializable version of KVS

        Args:
            include_deleted (bool, optional): should deleted items be included. Defaults to True.

        Returns:
            dict
        """
        return (
            {key: entry.json() for key, entry in self.kvs.items()}
            if include_deleted
            else {
                key: entry.json()
                for key, entry in self.kvs.items()
                if not entry.is_deleted()
            }
        )

    def reset_context(self):
        """Reset causal context for all entries in KVS. Delete any items with deleted flag set."""
        timestamp = time.time()
        to_delete = []
        for key, entry in self.kvs.items():
            if not entry.is_deleted():
                entry.reset_context(timestamp=timestamp)
            else:
                to_delete.append(key)
        for key in to_delete:
            # safe delete from dict
            self.kvs.pop(key, None)

    def get(self, key, return_value=False):
        """Retrieve entry/value from KVS

        Args:
            key ([type])
            return_value (bool, optional): only retrun value instead of KVSItem. Defaults to False.

        Returns:
            [type]: [description]
        """
        entry = self.kvs.get(key)
        if entry:
            return entry[VALUE] if return_value else entry
        return None

    def upsert(self, key: str, value: str, cause: dict = []) -> bool:
        """Update or Insert entry into KVS

        Args:
            key (str)
            value (str)
            cause (dict, optional): causal writes of entry. Defaults to [].

        Returns:
            bool: was key inserted (ie. did not exist)
        """
        entry = self.kvs.get(key)
        inserted = not entry or entry.is_deleted()
        self.kvs[key] = KVSItem(value, cause=cause)
        return inserted

    def create_cause_from_context(self, context: list):
        return [[key, entry[TIMESTAMP]] for key, entry in context]

    @classmethod
    def from_shard(cls, shard: dict):
        """Create KVS from JSON serialized shard

        Args:
            shard (dict)

        Returns:
            KVS
        """
        instance = cls()
        for key, entry in shard.items():
            instance.kvs[key] = KVSItem.from_json(entry)
        return instance

    @classmethod
    def combine_conflicting_shards(cls, shard_a: dict, shard_b: dict) -> dict:
        """Merges two shards (ie. dicts) which may have conflicting values for keys

        Args:
            shard_a (dict): JSON serialized KVS
            shard_b (dict): JSON serialized KVS

        Returns:
            dict: combined shard in dict format
        """
        kvs_a, kvs_b = cls.from_shard(shard_a), cls.from_shard(shard_b)
        all_keys = set().union(shard_a.keys(), shard_b.keys())
        final_shard = {}
        for key in all_keys:
            entry_a, entry_b = kvs_a.get(key), kvs_b.get(key)
            if entry_a and entry_b:
                final_shard[key] = (
                    entry_a.json()
                    # mitigate based on last write timestamp
                    if entry_a[TIMESTAMP] > entry_b[TIMESTAMP]
                    else entry_b.json()
                )
            else:
                # choose valid of two entries
                final_shard[key] = entry_a.json() if entry_a else entry_b.json()
        return final_shard
