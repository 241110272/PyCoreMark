"""Distributed tracing and span hierarchy manager.

TODO(Member 6): Implement Jaeger / W3C Trace Context propagation serialization.
"""

from datetime import datetime, timezone
from typing import Dict, List, Optional
import uuid
from pycoremark.models.metrics import TraceContext, TraceSpan


class TraceManager:
    """Manages distributed trace contexts and span lifecycles."""

    def __init__(self) -> None:
        self._spans: Dict[str, TraceSpan] = {}

    def start_trace(self) -> TraceContext:
        """Create a new root trace context."""
        trace_id = str(uuid.uuid4())
        return TraceContext(trace_id=trace_id)

    def start_span(
        self,
        name: str,
        context: TraceContext,
        node_id: str,
    ) -> TraceSpan:
        """Start a new span within an existing trace context."""
        span_id = str(uuid.uuid4())
        span = TraceSpan(
            span_id=span_id,
            trace_id=context.trace_id,
            parent_span_id=context.parent_span_id,
            name=name,
            node_id=node_id,
            start_time=datetime.now(timezone.utc),
        )
        self._spans[span_id] = span
        return span

    def finish_span(self, span_id: str) -> Optional[TraceSpan]:
        """Close a span by setting its completion timestamp."""
        span = self._spans.get(span_id)
        if span:
            span.end_time = datetime.now(timezone.utc)
        return span

    def get_spans_for_trace(self, trace_id: str) -> List[TraceSpan]:
        """Retrieve all spans belonging to a trace."""
        return [s for s in self._spans.values() if s.trace_id == trace_id]
