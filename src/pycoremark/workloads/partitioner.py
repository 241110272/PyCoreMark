"""Data partitioner utility for dividing benchmark datasets.

TODO(Member 3): Add hash-partitioning, range-partitioning, and skew-simulation partitioning.
"""

from typing import Any, List, Sequence


class DataPartitioner:
    """Partitions collections of data into disjoint chunks for distributed execution."""

    @staticmethod
    def partition_range(total_records: int, partitions: int) -> List[tuple[int, int]]:
        """Divide a range [0, total_records) into equal segment boundaries.

        Args:
            total_records: Total count of items to slice.
            partitions: Number of buckets.

        Returns:
            List of (start_idx, end_idx) tuples.
        """
        # TODO(Member 3): Handle uneven partitions and residual remainder offsets
        if partitions <= 0:
            raise ValueError("Partitions must be > 0")
        chunk_size = total_records // partitions
        ranges = []
        start = 0
        for i in range(partitions):
            end = total_records if i == partitions - 1 else start + chunk_size
            ranges.append((start, end))
            start = end
        return ranges

    @staticmethod
    def partition_items(items: Sequence[Any], partitions: int) -> List[List[Any]]:
        """Divide item sequence across partitions.

        Args:
            items: Source sequence of items.
            partitions: Target partition count.

        Returns:
            List of chunked item lists.
        """
        # TODO(Member 3): Implement round-robin and key-based hashing partitioners
        ranges = DataPartitioner.partition_range(len(items), partitions)
        return [list(items[start:end]) for start, end in ranges]

