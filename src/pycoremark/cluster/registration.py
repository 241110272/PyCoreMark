
"""Service handling dynamic worker registration and admission control."""

import hashlib
import hmac
import secrets
from threading import RLock
from typing import Dict, Optional

from pycoremark.cluster.registry import NodeRegistry
from pycoremark.common.exceptions import (
    NodeAlreadyRegisteredError,
    NodeNotFoundError,
)
from pycoremark.common.logging import get_logger
from pycoremark.models.node import NodeInfo, NodeStatus

logger = get_logger("cluster.registration")


class NodeRegistrationService:

    def __init__(
        self,
        registry: NodeRegistry,
        auth_token: Optional[str] = None,
    ) -> None:
        if auth_token is not None:
            if not isinstance(auth_token, str) or not auth_token.strip():
                raise ValueError("auth_token must be a non-empty string.")

        self._registry = registry
        self._auth_token = auth_token
        self._pending_challenges: Dict[str, str] = {}
        self._lock = RLock()

    def issue_handshake_challenge(self, node_id: str) -> str:
        if not isinstance(node_id, str) or not node_id.strip():
            raise ValueError("node_id must be a non-empty string.")

        if self._auth_token is None:
            raise RuntimeError(
                "Authentication must be configured before issuing challenges."
            )

        challenge = secrets.token_urlsafe(32)

        with self._lock:
            self._pending_challenges[node_id] = challenge

        return challenge

    @staticmethod
    def create_handshake_response(
        node_id: str,
        challenge: str,
        auth_token: str,
    ) -> str:
        if not isinstance(node_id, str) or not node_id.strip():
            raise ValueError("node_id must be a non-empty string.")

        if not isinstance(challenge, str) or not challenge:
            raise ValueError("challenge must be a non-empty string.")

        if not isinstance(auth_token, str) or not auth_token:
            raise ValueError("auth_token must be a non-empty string.")

        message = f"{node_id}:{challenge}".encode("utf-8")

        return hmac.new(
            auth_token.encode("utf-8"),
            message,
            hashlib.sha256,
        ).hexdigest()

    def _verify_credentials(
        self,
        node_id: str,
        auth_token: Optional[str],
        handshake_response: Optional[str],
    ) -> None:
        if self._auth_token is None:
            if auth_token is not None or handshake_response is not None:
                raise PermissionError(
                    "Registration authentication is not configured."
                )
            return

        with self._lock:
            challenge = self._pending_challenges.pop(node_id, None)

        if challenge is None:
            raise PermissionError(
                "A valid registration handshake challenge is required."
            )

        if (
            not isinstance(auth_token, str)
            or not hmac.compare_digest(auth_token, self._auth_token)
        ):
            raise PermissionError("Invalid registration authentication token.")

        expected_response = self.create_handshake_response(
            node_id,
            challenge,
            self._auth_token,
        )

        if (
            not isinstance(handshake_response, str)
            or not hmac.compare_digest(
                handshake_response,
                expected_response,
            )
        ):
            raise PermissionError("Registration handshake verification failed.")

    def register_worker(
        self,
        node: NodeInfo,
        *,
        auth_token: Optional[str] = None,
        handshake_response: Optional[str] = None,
    ) -> bool:
        if not isinstance(node, NodeInfo):
            raise TypeError("node must be a NodeInfo instance.")

        if not isinstance(node.node_id, str) or not node.node_id.strip():
            raise ValueError("node.node_id must be a non-empty string.")

        self._verify_credentials(
            node.node_id,
            auth_token,
            handshake_response,
        )

        with self._lock:
            existing = self._registry.get(node.node_id)

            if existing is not None:
                if existing.status in (
                    NodeStatus.READY,
                    NodeStatus.BUSY,
                ):
                    raise NodeAlreadyRegisteredError(
                        f"Node {node.node_id} is already active."
                    )

                if existing.active_tasks > 0:
                    raise NodeAlreadyRegisteredError(
                        f"Node {node.node_id} still has active tasks."
                    )

                if existing.status not in (
                    NodeStatus.INITIALIZING,
                    NodeStatus.DRAINING,
                    NodeStatus.SUSPECTED,
                    NodeStatus.DEAD,
                    NodeStatus.OFFLINE,
                ):
                    raise NodeAlreadyRegisteredError(
                        f"Node {node.node_id} cannot rejoin from "
                        f"status {existing.status}."
                    )

                self._registry.unregister(node.node_id)

                logger.info(
                    "Removed inactive registration for node %s before rejoining.",
                    node.node_id,
                )

            from datetime import datetime, timezone

            now = datetime.now(timezone.utc)
            node.status = NodeStatus.READY
            node.registered_at = now
            node.last_heartbeat_at = now

            self._registry.register(node)

        logger.info(
            "Worker node %s successfully admitted to cluster.",
            node.node_id,
        )

        return True

    def deregister_worker(self, node_id: str) -> bool:
        with self._lock:
            node = self._registry.get(node_id)

            if node is None:
                raise NodeNotFoundError(
                    f"Node {node_id} is not registered."
                )

            if node.active_tasks > 0:
                if node.status != NodeStatus.DRAINING:
                    self._registry.update_status(
                        node_id,
                        NodeStatus.DRAINING,
                    )

                logger.info(
                    "Worker %s is draining with %d active tasks.",
                    node_id,
                    node.active_tasks,
                )
                return False

            self._registry.unregister(node_id)

        logger.info("Worker node %s deregistered.", node_id)
        return True
