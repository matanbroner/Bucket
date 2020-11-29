import time


class KVS:
    def __init__(self):
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
            dict: key with dict value having keys "lamport" and "timestamp"
        """
        return {
            key: {"lamport": entry["lamport"], "timestamp": entry["timestamp"]}
            for key, entry in self.kvs.items()
        }

    def insert(self, key: str, value: str):
        """Insert new key-value pair into KVS

        Args:
            key (str)
            value (str)
        """
        self.kvs[key] = {"value": value, "lamport": 1, "timestamp": time.time()}

    def update(self, key: str, value: str, lamport: int, timestamp: float):
        """Update KVS entry with new value and metadata

        Args:
            key (str)
            value (str)
            lamport (int): updated lamport clock of entry
            timestamp (float): latest write timestamp of entry
        """
        self.kvs[key] = {"value": value, "lamport": lamport, "timestamp": timestamp}

    def compare(self, key: str, lamport: int, timestamp: float) -> int:
        """Compares two KVS entries for a key to determine which is more recent

        Args:
            key (str)
            lamport (int): lamport clock of entry
            timestamp (float): latest write timestamp of entry

        Returns:
            int:
                if passed in entry more recent, return 1
                else, return -1
        """
        entry = self.kvs.get(key, None)
        if not entry:
            return 1
        if entry["lamport"] >= lamport:
            if entry["timestamp"] > timestamp:
                return -1
            else:
                return 1
        else:
            if entry["timestamp"] < timestamp:
                return 1
            else:
                return -1
