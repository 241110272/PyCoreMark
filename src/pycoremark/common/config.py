
"""Configuration schema and loader for PyCoreMark cluster nodes and services."""

from dataclasses import asdict, dataclass, field
import math
import os
from pathlib import Path
import tomllib
from typing import Any, Dict


@dataclass(frozen=True)
class ClusterConfig:
    """Cluster-wide runtime configuration parameters."""

    coordinator_host: str = "127.0.0.1"
    coordinator_port: int = 8000
    grpc_port: int = 50051
    transport: str = "grpc"
    worker_id: str = "worker-0"
    worker_cores: int = 4
    worker_memory_mb: int = 4096
    worker_failure_rate: float = 0.0
    simulated_network_delay_ms: float = 0.0
    network_failure_rate: float = 0.0
    broker_url: str = "memory://"
    database_url: str = "sqlite:///pycoremark.db"
    heartbeat_interval: float = 2.0
    failure_timeout: float = 6.0
    max_retries: int = 3
    metrics_interval: float = 1.0
    log_level: str = "INFO"
    registration_auth_token: str | None = field(default=None, repr=False)

    def __post_init__(self) -> None:
        string_fields = (
            "coordinator_host",
            "worker_id",
            "broker_url",
            "database_url",
            "log_level",
            "transport",
        )

        for name in string_fields:
            value = getattr(self, name)

            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"{name} must be a non-empty string.")

            object.__setattr__(self, name, value.strip())

        integer_fields = (
            "coordinator_port",
            "grpc_port",
            "worker_cores",
            "worker_memory_mb",
            "max_retries",
        )

        for name in integer_fields:
            value = getattr(self, name)

            if isinstance(value, bool) or not isinstance(value, int):
                raise ValueError(f"{name} must be an integer.")

        if not 1 <= self.coordinator_port <= 65535:
            raise ValueError(
                "coordinator_port must be between 1 and 65535."
            )
        if not 1 <= self.grpc_port <= 65535:
            raise ValueError("grpc_port must be between 1 and 65535.")

        normalized_transport = self.transport.lower()
        if normalized_transport not in {"grpc", "http"}:
            raise ValueError("transport must be either 'grpc' or 'http'.")
        object.__setattr__(self, "transport", normalized_transport)

        if self.worker_cores <= 0:
            raise ValueError("worker_cores must be positive.")

        if self.worker_memory_mb <= 0:
            raise ValueError("worker_memory_mb must be positive.")

        if self.max_retries < 0:
            raise ValueError("max_retries cannot be negative.")

        numeric_fields = (
            "heartbeat_interval",
            "failure_timeout",
            "metrics_interval",
        )

        for name in numeric_fields:
            value = getattr(self, name)

            if (
                isinstance(value, bool)
                or not isinstance(value, (int, float))
                or not math.isfinite(value)
                or value <= 0
            ):
                raise ValueError(
                    f"{name} must be a positive finite number."
                )

        for name in (
            "worker_failure_rate", "simulated_network_delay_ms", "network_failure_rate"
        ):
            value = getattr(self, name)
            if (
                isinstance(value, bool)
                or not isinstance(value, (int, float))
                or not math.isfinite(value)
                or value < 0
            ):
                raise ValueError(f"{name} must be a finite nonnegative number.")
        if self.worker_failure_rate > 1:
            raise ValueError("worker_failure_rate must be in [0, 1].")
        if self.network_failure_rate > 1:
            raise ValueError("network_failure_rate must be in [0, 1].")

        allowed_log_levels = {
            "DEBUG",
            "INFO",
            "WARNING",
            "ERROR",
            "CRITICAL",
        }

        normalized_log_level = self.log_level.upper()

        if normalized_log_level not in allowed_log_levels:
            raise ValueError(
                f"log_level must be one of "
                f"{', '.join(sorted(allowed_log_levels))}."
            )

        object.__setattr__(self, "log_level", normalized_log_level)

        token = self.registration_auth_token

        if token is not None:
            if not isinstance(token, str) or not token.strip():
                raise ValueError(
                    "registration_auth_token must be None "
                    "or a non-empty string."
                )

            object.__setattr__(
                self,
                "registration_auth_token",
                token.strip(),
            )


def load_config_from_file(path: str | Path) -> ClusterConfig:
    """Load and validate configuration from a TOML or YAML file."""

    if not str(path).strip():
        raise ValueError("Configuration file path must not be empty.")

    config_path = Path(path)

    if not config_path.is_file():
        raise FileNotFoundError(
            f"Configuration file not found: {config_path}"
        )

    suffix = config_path.suffix.lower()

    if suffix not in {".toml", ".yaml", ".yml"}:
        raise ValueError(
            "Unsupported configuration format. "
            "Use .toml, .yaml, or .yml."
        )

    try:
        if suffix == ".toml":
            with config_path.open("rb") as file:
                data = tomllib.load(file)
        else:
            try:
                import yaml
            except ImportError as exc:
                raise ImportError(
                    "YAML support requires PyYAML. "
                    "Install it with: python -m pip install PyYAML"
                ) from exc

            with config_path.open("r", encoding="utf-8") as file:
                data = yaml.safe_load(file)

    except (OSError, ValueError, ImportError) as exc:
        raise ValueError(
            f"Unable to load configuration from "
            f"'{config_path}': {exc}"
        ) from exc

    if data is None:
        data = {}

    if not isinstance(data, dict):
        raise ValueError(
            "Configuration file must contain a mapping of field names "
            "to values."
        )

    defaults = asdict(ClusterConfig())
    unknown_fields = set(data) - set(defaults)

    if unknown_fields:
        raise ValueError(
            "Unknown configuration fields: "
            + ", ".join(sorted(str(name) for name in unknown_fields))
        )

    try:
        return ClusterConfig(**data)
    except TypeError as exc:
        raise ValueError(
            f"Invalid configuration schema: {exc}"
        ) from exc


def load_config_from_env() -> ClusterConfig:
    """Load configuration from an optional file and environment variables.

    Configuration file values provide the base settings. Environment
    variables override matching values. Missing values use defaults.
    """

    config_path = os.getenv("PYCOREMARK_CONFIG_FILE")

    if config_path:
        config = load_config_from_file(config_path)
        values: Dict[str, Any] = asdict(config)
    else:
        values = asdict(ClusterConfig())

    defaults = asdict(ClusterConfig())

    for name, default in defaults.items():
        environment_name = f"PYCOREMARK_{name.upper()}"
        raw_value = os.getenv(environment_name)

        if raw_value is None:
            continue
        if name == "registration_auth_token" and not raw_value.strip():
            continue

        try:
            if isinstance(default, bool):
                raise ValueError("Boolean configuration is unsupported.")

            if isinstance(default, int):
                values[name] = int(raw_value)
            elif isinstance(default, float):
                values[name] = float(raw_value)
            else:
                values[name] = raw_value

        except ValueError as exc:
            raise ValueError(
                f"Invalid value for environment variable "
                f"{environment_name}: {raw_value!r}"
            ) from exc

    return ClusterConfig(**values)
