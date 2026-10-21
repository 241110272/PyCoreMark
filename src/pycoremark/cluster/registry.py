
import json
from abc import ABC, abstractmethod
from dataclasses import asdict
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from threading import RLock
from typing import Any, Dict, List, Optional

from pycoremark.common.exceptions import (
    NodeAlreadyRegisteredError,
    NodeNotFoundError,
    NodeRegistryPersistenceError,
)
from pycoremark.models.node import (
    GPUResource,
    NetworkInterface,
    NodeInfo,
    NodeResources,
    NodeStatus,
)


def _json_default(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Enum):
        return value.value
    raise TypeError(f"Unsupported type: {type(value).__name__}")


def _deserialize_node(data: Dict[str, Any]) -> NodeInfo:
    data = dict(data)
    data["status"] = NodeStatus(data["status"])

    for field in ("registered_at", "last_heartbeat_at"):
        value = datetime.fromisoformat(data[field])
        data[field] = (
            value if value.tzinfo else value.replace(tzinfo=timezone.utc)
        )

    resources = dict(data["resources"])
    resources["gpus"] = [
        GPUResource(**gpu) for gpu in resources.get("gpus", [])
    ]
    data["resources"] = NodeResources(**resources)

    data["network_interfaces"] = [
        NetworkInterface(**interface)
        for interface in data.get("network_interfaces", [])
    ]

    return NodeInfo(**data)


class NodeRegistry(ABC):

    @abstractmethod
    def register(self, node: NodeInfo) -> None:
        raise NotImplementedError

    @abstractmethod
    def unregister(self, node_id: str) -> None:
        raise NotImplementedError

    @abstractmethod
    def get(self, node_id: str) -> Optional[NodeInfo]:
        raise NotImplementedError

    @abstractmethod
    def list_all(self) -> List[NodeInfo]:
        raise NotImplementedError

    @abstractmethod
    def list_by_status(self, status: NodeStatus) -> List[NodeInfo]:
        raise NotImplementedError

    @abstractmethod
    def update_status(self, node_id: str, status: NodeStatus) -> None:
        raise NotImplementedError

    @abstractmethod
    def update_node(self, node: NodeInfo) -> None:
        raise NotImplementedError


class InMemoryNodeRegistry(NodeRegistry):

    def __init__(self) -> None:
        self._nodes: Dict[str, NodeInfo] = {}
        self._lock = RLock()

    def _persist(self) -> None:
        pass

    def register(self, node: NodeInfo) -> None:
        with self._lock:
            if node.node_id in self._nodes:
                raise NodeAlreadyRegisteredError(
                    f"Node '{node.node_id}' is already registered."
                )

            self._nodes[node.node_id] = node
            try:
                self._persist()
            except Exception:
                self._nodes.pop(node.node_id, None)
                raise

    def unregister(self, node_id: str) -> None:
        with self._lock:
            previous = self._nodes.pop(node_id, None)
            if previous is None:
                return

            try:
                self._persist()
            except Exception:
                self._nodes[node_id] = previous
                raise

    def get(self, node_id: str) -> Optional[NodeInfo]:
        with self._lock:
            return self._nodes.get(node_id)

    def list_all(self) -> List[NodeInfo]:
        with self._lock:
            return list(self._nodes.values())

    def list_by_status(self, status: NodeStatus) -> List[NodeInfo]:
        with self._lock:
            return [
                node for node in self._nodes.values()
                if node.status == status
            ]

    def update_status(self, node_id: str, status: NodeStatus) -> None:
        with self._lock:
            node = self._nodes.get(node_id)
            if node is None:
                return

            previous = node.status
            node.status = status

            try:
                self._persist()
            except Exception:
                node.status = previous
                raise

    def update_node(self, node: NodeInfo) -> None:
        with self._lock:
            previous = self._nodes.get(node.node_id)
            if previous is None:
                raise NodeNotFoundError(
                    f"Node '{node.node_id}' is not registered."
                )

            self._nodes[node.node_id] = node
            try:
                self._persist()
            except Exception:
                self._nodes[node.node_id] = previous
                raise


class PersistentNodeRegistry(InMemoryNodeRegistry):

    def __init__(self, storage_path: str | Path) -> None:
        if not str(storage_path).strip():
            raise ValueError("storage_path must not be empty.")

        self._storage_path = Path(storage_path)
        super().__init__()
        self._load()

    def _load(self) -> None:
        if not self._storage_path.exists():
            return

        try:
            with self._storage_path.open("r", encoding="utf-8") as file:
                data = json.load(file)

            if not isinstance(data, list):
                raise ValueError("Registry cache must be a JSON list.")

            nodes = [_deserialize_node(item) for item in data]
            loaded = {node.node_id: node for node in nodes}

            if len(loaded) != len(nodes):
                raise ValueError("Registry cache contains duplicate node IDs.")

            self._nodes = loaded

        except (OSError, ValueError, TypeError, KeyError) as exc:
            raise NodeRegistryPersistenceError(
                f"Unable to load registry cache: {exc}"
            ) from exc

    def _persist(self) -> None:
        temporary_path = self._storage_path.with_suffix(
            self._storage_path.suffix + ".tmp"
        )

        try:
            self._storage_path.parent.mkdir(parents=True, exist_ok=True)

            with temporary_path.open("w", encoding="utf-8") as file:
                json.dump(
                    [asdict(node) for node in self._nodes.values()],
                    file,
                    ensure_ascii=False,
                    indent=2,
                    default=_json_default,
                )
                file.flush()

            temporary_path.replace(self._storage_path)

        except Exception as exc:
            temporary_path.unlink(missing_ok=True)
            raise NodeRegistryPersistenceError(
                f"Unable to save registry cache: {exc}"
            ) from exc


def create_node_registry(
    storage_path: Optional[str | Path] = None,
) -> NodeRegistry:
    if storage_path is None:
        return InMemoryNodeRegistry()
    return PersistentNodeRegistry(storage_path)
