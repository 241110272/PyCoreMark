"""Dataset partitioning owned by Filbert Matthew (Anggota 2)."""

from typing import Any, List, Sequence


class DataPartitioner:
    """Split records into disjoint, contiguous, balanced half-open ranges."""

    @staticmethod
    def partition_range(total_records: int, partitions: int) -> List[tuple[int, int]]:
        """Keep all records; partition sizes differ by at most one, including empties."""
        if total_records < 0:
            raise ValueError("total_records must be >= 0")
        if partitions <= 0:
            raise ValueError("partitions must be > 0")
        size, remainder = divmod(total_records, partitions)
        ranges = []
        start = 0
        for index in range(partitions):
            end = start + size + (index < remainder)
            ranges.append((start, end))
            start = end
        return ranges

    @staticmethod
    def partition_items(items: Sequence[Any], partitions: int) -> List[List[Any]]:
        """Preserve input order without dropping remainder records."""
        return [list(items[start:end]) for start, end in
                DataPartitioner.partition_range(len(items), partitions)]
