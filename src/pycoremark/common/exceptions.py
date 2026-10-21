
"""Common domain exceptions for PyCoreMark."""


class PyCoreMarkError(Exception):
    """Base exception for all PyCoreMark domain errors."""


class NodeError(PyCoreMarkError):
    """Raised when a node operation encounters an error."""


class NodeNotFoundError(NodeError):
    """Raised when a queried node is not present in the registry."""


class NodeAlreadyRegisteredError(NodeError):
    """Raised when attempting to register a node with an existing identifier."""


class NodeRegistryPersistenceError(NodeError):
    """Raised when loading or saving the persistent node registry fails."""


class TaskError(PyCoreMarkError):
    """Raised when a task management or execution operation fails."""


class TaskNotFoundError(TaskError):
    """Raised when a task cannot be located."""


class TaskExecutionError(TaskError):
    """Raised when a worker fails during task computation."""


class BrokerError(PyCoreMarkError):
    """Raised when message broker operations fail."""


class BrokerConnectionError(BrokerError):
    """Raised when a connection to the broker cannot be established."""


class StorageError(PyCoreMarkError):
    """Raised when repository or persistence operations fail."""


class EntityNotFoundError(StorageError):
    """Raised when an entity is not found in persistent storage."""


class SchedulingError(PyCoreMarkError):
    """Raised when task scheduling cannot be resolved."""


class NoAvailableNodeError(SchedulingError):
    """Raised when no suitable worker node satisfies task resource requirements."""


class ValidationError(PyCoreMarkError):
    """Raised when configuration, workload, or result validation fails."""


class InvalidWorkloadConfigError(ValidationError):
    """Raised when a workload specification is malformed."""
