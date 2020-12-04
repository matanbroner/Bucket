from util.misc import printer


class View:
    def __init__(self, ips: list, address: str, repl_factor: int):
        printer(
            "Creating view {}, address {}, repl {}".format(ips, address, repl_factor)
        )
        self.all_ips = ips
        self.address = address
        self.repl_factor = repl_factor
        self._test_class_inputs_valid()
        self._create_buckets()

    def _test_class_inputs_valid(self):
        """Validates view inputs"""
        try:
            assert (
                len(self.all_ips) % self.repl_factor == 0
            ), f"Length of view {self.all_ips} not divisible by {self.repl_factor}"
        except AssertionError:
            raise

    def _create_buckets(self):
        """Designates a set of replica buckets based on a replication factor"""
        self.buckets = [
            # evenly split ips (ex. [1, 2, 3, 4] with factor 2 -> [[1, 2], [3, 4]])
            self.all_ips[x : x + self.repl_factor]
            for x in range(0, len(self.all_ips), self.repl_factor)
        ]
        for index, bucket in enumerate(self.buckets):
            if self.address in bucket:
                self.bucket_index = index
                self.replica_index = bucket.index(self.address)
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

    def self_replication_bucket(self, own_ip: bool = True) -> list:
        """Get the IP addresses in a node's own replication bucket

        Args:
            own_ip (bool, optional): Include node's own IP. Defaults to True.

        Returns:
            list: IP addresses of all replicas
        """
        bucket = self.buckets[self.bucket_index]
        if not own_ip:
            bucket = filter(lambda ip: ip != self.address, bucket)
        return list(bucket)
