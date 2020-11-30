class View:
    def __init__(self, ips: list, address: str, repl_factor: int):
        self.all_ips = ips
        self.address = address
        self.repl_factor = repl_factor
        self._test_class_inputs_valid()
        self._create_buckets()

    def _test_class_inputs_valid(self):
        """Validates view inputs"""
        assert self.address in self.all_ips
        assert len(self.all_ips) % self.repl_factor == 0

    def _create_buckets(self):
        """Designates a set of replica buckets based on a replication factor"""
        self.buckets = [
            # evenly split ips (ex. [1, 2, 3, 4] with factor 2 -> [[1, 2], [3, 4]])
            self.all_ips[x : x + self.repl_factor]
            for x in range(0, len(self.all_ips), self.repl_factor)
        ]
        for index, bucket in enumerate(self.buckets):
            if bucket.index(self.address) != -1:
                self.bucket_index = index
                break

    def num_buckets(self) -> int:
        """Returns number of replica buckets

        Returns:
            int
        """
        return len(self.buckets)

    def includes_own_address(self):
        return self.address in self.all_ips

    def is_own_bucket_index(self, index: int) -> bool:
        """Determine if given index is node's own replica bucket's index

        Args:
            index (int)

        Returns:
            bool
        """
        return index == self.bucket_index

    def node_is_replica_leader(self) -> bool:
        """Determine if node is replica bucket leader

        Returns:
            bool
        """
        return self.buckets[self.bucket_index][0] == self.address

    def self_replication_bucket(self, own_ip: bool = True) -> list:
        """Get the IP addresses in a node's own replication bucket

        Args:
            own_ip (bool, optional): Include node's own IP. Defaults to True.

        Returns:
            list: IP addresses of all replicas
        """
        bucket = self.buckets[self.bucket_index]
        if not own_ip:
            bucket = bucket.filter(lambda ip: ip != self.address, bucket)
        return bucket

    def bucket_leaders(self, own_leader: bool = False) -> list:
        """Get a list of replica leaders for easier communication in proxied requests.
            Leaders are defaultred as the first IP in any given bucket.

        Args:
            own_leader (bool, optional): Return node's own replica bucket leader. Defaults to False.

        Returns:
            list: IP addresses of all leaders
        """
        leaders = [bucket[0] for index, bucket in enumerate(self.buckets)]
        if not own_leader:
            leaders = leaders.filter(
                lambda ip: ip != self.buckets[self.bucket_index][0], leaders
            )
        return leaders
