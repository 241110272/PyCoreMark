
import logging
import sys
from contextlib import contextmanager
from contextvars import ContextVar
from functools import wraps
from typing import Callable, Iterator, Optional, ParamSpec, TypeVar
from uuid import uuid4


_current_trace_id: ContextVar[Optional[str]] = ContextVar(
    "pycoremark_trace_id",
    default=None,
)

P = ParamSpec("P")
R = TypeVar("R")


def get_trace_id() -> Optional[str]:
    return _current_trace_id.get()


@contextmanager
def trace_context(trace_id: Optional[str] = None) -> Iterator[str]:
    if trace_id is not None:
        trace_id = trace_id.strip()
        if not trace_id:
            raise ValueError("trace_id must not be empty.")

    resolved_trace_id = trace_id or get_trace_id() or uuid4().hex
    token = _current_trace_id.set(resolved_trace_id)

    try:
        yield resolved_trace_id
    finally:
        _current_trace_id.reset(token)


def traced(func: Callable[P, R]) -> Callable[P, R]:
    @wraps(func)
    def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
        with trace_context():
            return func(*args, **kwargs)

    return wrapper


class TraceIdFilter(logging.Filter):

    def filter(self, record: logging.LogRecord) -> bool:
        record.trace_id = get_trace_id() or "-"
        return True


def setup_logging(level: str = "INFO") -> None:
    numeric_level = getattr(logging, level.upper(), None)

    if not isinstance(numeric_level, int):
        raise ValueError(f"Invalid logging level: {level}")

    handler = logging.StreamHandler(sys.stdout)
    handler.addFilter(TraceIdFilter())

    log_format = (
        "[%(asctime)s] [%(levelname)s] [%(name)s] "
        "[pid:%(process)d] [trace:%(trace_id)s]: %(message)s"
    )

    logging.basicConfig(
        level=numeric_level,
        format=log_format,
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[handler],
        force=True,
    )


def get_logger(name: str) -> logging.Logger:
    if not name or not name.strip():
        raise ValueError("Logger name must not be empty.")

    return logging.getLogger(f"pycoremark.{name}")
