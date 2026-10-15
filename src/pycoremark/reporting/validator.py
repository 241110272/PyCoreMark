"""Result and output data validator.

TODO(Member 7): Add checksum integrity validation and anomaly detection in benchmark results.
"""

from pycoremark.models.event import ValidationResult
from pycoremark.models.experiment import ExperimentResult


class ResultValidator:
    """Validates experiment outputs against expected thresholds and consistency rules."""

    @staticmethod
    def validate_experiment_result(result: ExperimentResult) -> ValidationResult:
        """Validate whether an experiment completed with sane metrics.

        Args:
            result: ExperimentResult to evaluate.

        Returns:
            ValidationResult with boolean validity status and warnings/errors.
        """
        errors = []
        warnings = []

        if result.failed_tasks > 0:
            warnings.append(f"{result.failed_tasks} tasks encountered execution failure.")

        if result.duration_seconds <= 0:
            errors.append("Invalid duration: duration must be > 0.")

        if result.speedup <= 0:
            errors.append("Invalid speedup calculation: speedup must be > 0.")

        return ValidationResult(
            is_valid=(len(errors) == 0),
            errors=errors,
            warnings=warnings,
        )

