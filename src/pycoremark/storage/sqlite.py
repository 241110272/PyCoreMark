"""SQLite-backed experiment history for local and single-host runs."""

import json
from dataclasses import asdict
from pathlib import Path
import sqlite3
from threading import RLock
from typing import Any, List, Optional

from pycoremark.models.experiment import ExperimentConfig, ExperimentResult, ExperimentStatus
from pycoremark.models.workload import WorkloadConfig, WorkloadType
from pycoremark.storage.base import ExperimentRepository


def _json_default(value: Any) -> Any:
    if isinstance(value, Path):
        return str(value)
    if hasattr(value, "isoformat"):
        return value.isoformat()
    if hasattr(value, "value"):
        return value.value
    raise TypeError(f"Cannot serialize {type(value).__name__}")


class SQLiteExperimentRepository(ExperimentRepository):
    """Persist experiment configurations and results in a local SQLite DB."""

    def __init__(self, database_path: str | Path) -> None:
        self.database_path = Path(database_path).expanduser()
        if str(self.database_path) != ":memory:":
            self.database_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = RLock()
        self._connection = sqlite3.connect(
            str(self.database_path), timeout=15, check_same_thread=False
        )
        with self._connection:
            connection = self._connection
            connection.execute("""
                CREATE TABLE IF NOT EXISTS experiment_configs (
                    experiment_id TEXT PRIMARY KEY,
                    payload TEXT NOT NULL
                )
            """)
            connection.execute("""
                CREATE TABLE IF NOT EXISTS experiment_results (
                    experiment_id TEXT PRIMARY KEY,
                    payload TEXT NOT NULL
                )
            """)

    def save_config(self, config: ExperimentConfig) -> None:
        payload = json.dumps(asdict(config), default=_json_default)
        with self._lock, self._connection:
            connection = self._connection
            connection.execute(
                "INSERT INTO experiment_configs VALUES (?, ?) "
                "ON CONFLICT(experiment_id) DO UPDATE SET payload=excluded.payload",
                (config.experiment_id, payload),
            )

    def get_config(self, experiment_id: str) -> Optional[ExperimentConfig]:
        with self._lock, self._connection:
            connection = self._connection
            row = connection.execute(
                "SELECT payload FROM experiment_configs WHERE experiment_id=?",
                (experiment_id,),
            ).fetchone()
        if row is None:
            return None
        data = json.loads(row[0])
        workload = dict(data["workload"])
        workload["workload_type"] = WorkloadType(workload["workload_type"])
        data["workload"] = WorkloadConfig(**workload)
        return ExperimentConfig(**data)

    def save_result(self, result: ExperimentResult) -> None:
        payload = json.dumps(asdict(result), default=_json_default)
        with self._lock, self._connection:
            connection = self._connection
            connection.execute(
                "INSERT INTO experiment_results VALUES (?, ?) "
                "ON CONFLICT(experiment_id) DO UPDATE SET payload=excluded.payload",
                (result.experiment_id, payload),
            )

    def get_result(self, experiment_id: str) -> Optional[ExperimentResult]:
        with self._lock, self._connection:
            connection = self._connection
            row = connection.execute(
                "SELECT payload FROM experiment_results WHERE experiment_id=?",
                (experiment_id,),
            ).fetchone()
        if row is None:
            return None
        from datetime import datetime
        data = json.loads(row[0])
        data["status"] = ExperimentStatus(data["status"])
        data["start_time"] = datetime.fromisoformat(data["start_time"])
        if data.get("end_time"):
            data["end_time"] = datetime.fromisoformat(data["end_time"])
        return ExperimentResult(**data)

    def list_all(self) -> List[ExperimentResult]:
        with self._lock, self._connection:
            connection = self._connection
            rows = connection.execute(
                "SELECT payload FROM experiment_results ORDER BY rowid DESC"
            ).fetchall()
        results = []
        for (payload,) in rows:
            data = json.loads(payload)
            data["status"] = ExperimentStatus(data["status"])
            from datetime import datetime
            data["start_time"] = datetime.fromisoformat(data["start_time"])
            if data.get("end_time"):
                data["end_time"] = datetime.fromisoformat(data["end_time"])
            results.append(ExperimentResult(**data))
        return results
