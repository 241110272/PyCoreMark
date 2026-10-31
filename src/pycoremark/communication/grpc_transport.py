"""gRPC control/data RPCs for coordinator-worker communication.

The wire schema uses protobuf ``Struct`` so this module works without checked-in
generated Python files; ``proto/cluster.proto`` documents the RPC contract.
"""

import hashlib
import hmac
import json
import random
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict

import grpc
from google.protobuf.json_format import MessageToDict, ParseDict
from google.protobuf.struct_pb2 import Struct

from pycoremark.communication.http_transport import (
    _event_from_dict,
    _heartbeat_from_dict,
    _json_default,
    _metric_from_dict,
    _node_from_dict,
    _result_from_dict,
    _task_from_dict,
)
from pycoremark.models.node import NodeStatus


def _struct(value: dict) -> Struct:
    return ParseDict(json.loads(json.dumps(value, default=_json_default)), Struct())


def _dict(value: Struct) -> dict:
    return MessageToDict(value, preserving_proto_field_name=True)


class GrpcCoordinatorServer:
    """Coordinator-side gRPC server for worker membership and RPC control."""

    def __init__(
        self,
        host: str,
        port: int,
        *,
        registration_service,
        cluster_manager,
        metrics_handler,
        event_handler,
        result_queue,
        task_getter,
        result_handler=None,
        cancel_handler=None,
        node_event_handler=None,
        auth_token: str | None = None,
    ) -> None:
        self.host = host
        self.port = port
        self._registration = registration_service
        self._manager = cluster_manager
        self._metrics_handler = metrics_handler
        self._event_handler = event_handler
        self._result_queue = result_queue
        self._result_handler = result_handler
        self._task_getter = task_getter
        self._cancel_handler = cancel_handler or (lambda task_id: False)
        self._node_event_handler = node_event_handler or (lambda event_type, node_id: None)
        self._auth_token = auth_token
        self._server = grpc.server(ThreadPoolExecutor(max_workers=16))
        self._started = False
        self._lock = threading.Lock()
        self._install_handlers()

    def _install_handlers(self) -> None:
        def invoke(fn):
            def handler(request, context):
                if self._auth_token:
                    metadata = dict(context.invocation_metadata())
                    supplied = metadata.get("authorization", "")
                    if not hmac.compare_digest(supplied, f"Bearer {self._auth_token}"):
                        context.abort(grpc.StatusCode.UNAUTHENTICATED, "authentication required")
                try:
                    return _struct(fn(_dict(request)))
                except PermissionError as exc:
                    context.abort(grpc.StatusCode.UNAUTHENTICATED, str(exc))
                except KeyError as exc:
                    context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(exc))
                except (ValueError, TypeError) as exc:
                    context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(exc))
                except Exception as exc:
                    context.abort(grpc.StatusCode.INTERNAL, str(exc))
            return handler

        handlers = {
            "HealthCheck": grpc.unary_unary_rpc_method_handler(
                invoke(lambda _: {
                    "status": "ok",
                    "active_nodes": len(self._manager.get_active_nodes()),
                }),
                request_deserializer=Struct.FromString,
                response_serializer=Struct.SerializeToString,
            ),
            "IssueChallenge": grpc.unary_unary_rpc_method_handler(
                invoke(lambda data: {
                    "challenge": self._registration.issue_handshake_challenge(data["node_id"])
                }),
                request_deserializer=Struct.FromString,
                response_serializer=Struct.SerializeToString,
            ),
            "RegisterNode": grpc.unary_unary_rpc_method_handler(
                invoke(self._register), request_deserializer=Struct.FromString,
                response_serializer=Struct.SerializeToString,
            ),
            "DeregisterNode": grpc.unary_unary_rpc_method_handler(
                invoke(self._deregister), request_deserializer=Struct.FromString,
                response_serializer=Struct.SerializeToString,
            ),
            "Heartbeat": grpc.unary_unary_rpc_method_handler(
                invoke(self._heartbeat), request_deserializer=Struct.FromString,
                response_serializer=Struct.SerializeToString,
            ),
            "PublishMetrics": grpc.unary_unary_rpc_method_handler(
                invoke(self._metrics), request_deserializer=Struct.FromString,
                response_serializer=Struct.SerializeToString,
            ),
            "PublishEvent": grpc.unary_unary_rpc_method_handler(
                invoke(self._event), request_deserializer=Struct.FromString,
                response_serializer=Struct.SerializeToString,
            ),
            "SubmitResult": grpc.unary_unary_rpc_method_handler(
                invoke(self._result), request_deserializer=Struct.FromString,
                response_serializer=Struct.SerializeToString,
            ),
            "GetTask": grpc.unary_unary_rpc_method_handler(
                invoke(self._get_task), request_deserializer=Struct.FromString,
                response_serializer=Struct.SerializeToString,
            ),
            "GetNode": grpc.unary_unary_rpc_method_handler(
                invoke(self._get_node), request_deserializer=Struct.FromString,
                response_serializer=Struct.SerializeToString,
            ),
            "CancelTask": grpc.unary_unary_rpc_method_handler(
                invoke(self._cancel), request_deserializer=Struct.FromString,
                response_serializer=Struct.SerializeToString,
            ),
        }
        generic = grpc.method_handlers_generic_handler(
            "pycoremark.ClusterControl", handlers
        )
        self._server.add_generic_rpc_handlers((generic,))
        if not self._server.add_insecure_port(f"{self.host}:{self.port}"):
            raise OSError(f"Unable to bind gRPC coordinator port {self.host}:{self.port}")

    def _register(self, payload: dict) -> dict:
        node = _node_from_dict(payload["node"])
        result = self._registration.register_worker(
            node,
            auth_token=payload.get("auth_token"),
            handshake_response=payload.get("handshake_response"),
        )
        self._node_event_handler("NODE_REGISTERED", node.node_id)
        return {"registered": result}

    def _deregister(self, payload: dict) -> dict:
        node_id = str(payload["node_id"])
        node = self._manager.registry.get(node_id)
        if node is not None:
            node.active_tasks = 0
            self._manager.registry.update_status(node_id, NodeStatus.OFFLINE)
            self._manager.heartbeat_service.forget_node(node_id)
            self._node_event_handler("NODE_DEREGISTERED", node_id)
        return {"deregistered": node is not None}

    def _heartbeat(self, payload: dict) -> dict:
        accepted = self._manager.process_heartbeat(_heartbeat_from_dict(payload))
        return {"accepted": accepted}

    def _metrics(self, payload: dict) -> dict:
        self._metrics_handler(_metric_from_dict(payload))
        return {"accepted": True}

    def _event(self, payload: dict) -> dict:
        self._event_handler(_event_from_dict(payload))
        return {"accepted": True}

    def _result(self, payload: dict) -> dict:
        result = _result_from_dict(payload)
        if self._result_handler is not None:
            self._result_handler(result)
        else:
            self._result_queue.put(result)
        return {"accepted": True}

    def _cancel(self, payload: dict) -> dict:
        return {"cancelled": bool(self._cancel_handler(str(payload["task_id"])))}

    def _get_task(self, payload: dict) -> dict:
        worker_id = str(payload["worker_id"])
        timeout = min(20.0, max(0.0, float(payload.get("timeout", 0.5))))
        task = self._task_getter(worker_id, timeout)
        return {"task": task}

    def _get_node(self, payload: dict) -> dict:
        from dataclasses import asdict
        node = self._manager.registry.get(str(payload["node_id"]))
        return {"node": asdict(node) if node is not None else None}

    def start(self) -> None:
        with self._lock:
            if not self._started:
                self._server.start()
                self._started = True

    def stop(self) -> None:
        with self._lock:
            if self._started:
                self._server.stop(grace=2).wait(timeout=3)
                self._started = False


class GrpcWorkerBroker:
    """Worker broker adapter using gRPC for direct coordinator RPCs."""

    def __init__(
        self,
        coordinator_host: str,
        coordinator_port: int,
        *,
        worker_id: str,
        auth_token: str | None = None,
        timeout_seconds: float = 5.0,
        simulated_latency_ms: float = 0.0,
        simulated_failure_rate: float = 0.0,
    ) -> None:
        self.worker_id = worker_id
        self.auth_token = auth_token
        self._registered_node = None
        self.timeout_seconds = timeout_seconds
        self.simulated_latency_ms = max(0.0, simulated_latency_ms)
        self.simulated_failure_rate = min(1.0, max(0.0, simulated_failure_rate))
        self._channel = grpc.insecure_channel(f"{coordinator_host}:{coordinator_port}")

    def _call(self, name: str, payload: dict | None = None) -> dict:
        if self.simulated_latency_ms:
            time.sleep(self.simulated_latency_ms / 1000.0)
        if random.random() < self.simulated_failure_rate:
            raise ConnectionError("Injected gRPC message loss")
        method = self._channel.unary_unary(
            f"/pycoremark.ClusterControl/{name}",
            request_serializer=lambda data: _struct(data).SerializeToString(),
            response_deserializer=lambda data: _dict(Struct.FromString(data)),
        )
        metadata = (("authorization", f"Bearer {self.auth_token}"),) if self.auth_token else None
        try:
            return method(payload or {}, timeout=self.timeout_seconds, metadata=metadata)
        except grpc.RpcError as exc:
            code = exc.code()
            if code == grpc.StatusCode.UNAUTHENTICATED:
                raise PermissionError(exc.details()) from exc
            if code in (grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.DEADLINE_EXCEEDED):
                raise ConnectionError(exc.details()) from exc
            raise ValueError(exc.details()) from exc

    def publish(self, topic: str, message) -> None:
        payload = asdict(message) if hasattr(message, "__dataclass_fields__") else message
        from pycoremark.communication.http_transport import _json_default
        import json
        payload = json.loads(json.dumps(payload, default=_json_default))
        calls = {
            "cluster.heartbeats": "Heartbeat",
            "cluster.metrics": "PublishMetrics",
            "cluster.events": "PublishEvent",
            "cluster.task_results": "SubmitResult",
        }
        if topic not in calls:
            raise ValueError(f"Unsupported gRPC topic {topic!r}")
        response = self._call(calls[topic], payload)
        if topic == "cluster.heartbeats" and not response.get("accepted", True):
            if self._registered_node is None:
                raise ConnectionError("Coordinator rejected heartbeat for unregistered worker")
            self.register_node(self._registered_node)
            self._call(calls[topic], payload)

    def subscribe(self, topic: str):
        return None

    def poll(self, topic: str, timeout: float | None = None):
        parts = topic.split(".")
        if len(parts) != 3 or parts[0] != "worker" or parts[2] != "tasks":
            return None
        from pycoremark.communication.http_transport import _task_from_dict
        result = self._call("GetTask", {
            "worker_id": self.worker_id,
            "timeout": timeout if timeout is not None else 0.5,
        })
        return _task_from_dict(result["task"]) if result.get("task") else None

    def register_node(self, node) -> None:
        self._registered_node = node
        from pycoremark.communication.http_transport import _json_default
        import json
        node_data = json.loads(json.dumps(asdict(node), default=_json_default))
        auth_token = self.auth_token
        challenge = None
        response = None
        if auth_token:
            challenge = self._call("IssueChallenge", {"node_id": node.node_id})["challenge"]
            message = f"{node.node_id}:{challenge}".encode("utf-8")
            response = hmac.new(auth_token.encode("utf-8"), message, hashlib.sha256).hexdigest()
        self._call("RegisterNode", {
            "node": node_data,
            "auth_token": auth_token,
            "handshake_response": response,
        })

    def deregister_node(self, node_id: str) -> None:
        self._call("DeregisterNode", {"node_id": node_id})

    def health_check(self) -> dict:
        return self._call("HealthCheck")

    def get_node(self, node_id: str):
        from pycoremark.communication.http_transport import _node_from_dict
        payload = self._call("GetNode", {"node_id": node_id}).get("node")
        return _node_from_dict(payload) if payload else None

    def cancel_task(self, task_id: str) -> bool:
        return bool(self._call("CancelTask", {"task_id": task_id}).get("cancelled"))

    def close(self) -> None:
        self._channel.close()
