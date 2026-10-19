"""Communication and orchestration overhead calculation.

TODO(Member 6): Disaggregate serialization delay, network RTT, and queue waiting time.
"""

from typing import List

from pycoremark.models.task import TaskResult


class OverheadCalculator:
    """Calculates network and scheduling overhead versus raw compute time."""

    @staticmethod
    def calculate_communication_overhead(
        total_experiment_duration: float,
        task_results: List[TaskResult],
        num_workers: int = 1,
    ) -> float:
        """Estimate cluster communication and scheduling overhead time.

        Overhead = wall_time - (total_compute_time / num_workers).

        Args:
            total_experiment_duration: Durasi wall-clock seluruh eksperimen (detik).
            task_results: Daftar hasil task.
            num_workers: Jumlah worker/node yang mengeksekusi paralel.
                Default 1 agar kompatibel dengan pemanggil lama.
        """
        if not task_results or total_experiment_duration <= 0:
            return 0.0

        total_compute_time = sum(r.execution_time_seconds for r in task_results)
        effective_workers = max(1, num_workers)
        parallel_compute_time = total_compute_time / effective_workers
        overhead = max(0.0, total_experiment_duration - parallel_compute_time)
        return round(overhead, 4)