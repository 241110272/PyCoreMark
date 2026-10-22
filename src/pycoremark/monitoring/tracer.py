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
        parent_span: Optional[TraceSpan] = None,
    ) -> TraceSpan:
        """Start a new span within an existing trace context.

        Args:
            name: Nama operasi span.
            context: TraceContext induk (root trace).
            node_id: Node tempat span dijalankan.
            parent_span: Jika diisi, span ini menjadi child dari parent_span.
                Jika None, parent diambil dari context.parent_span_id.
        """
        span_id = str(uuid.uuid4())
        parent_span_id = (
            parent_span.span_id if parent_span is not None
            else context.parent_span_id
        )
        span = TraceSpan(
            span_id=span_id,
            trace_id=context.trace_id,
            parent_span_id=parent_span_id,
            name=name,
            node_id=node_id,
            start_time=datetime.now(timezone.utc),
        )
        self._spans[span_id] = span
        return span

    def start_child_span(
        self,
        parent_span: TraceSpan,
        name: str,
        context: TraceContext,
        node_id: str,
    ) -> TraceSpan:
        """Shortcut untuk membuat span anak dari parent_span."""
        return self.start_span(
            name=name,
            context=context,
            node_id=node_id,
            parent_span=parent_span,
        )

    def finish_span(self, span_id: str) -> Optional[TraceSpan]:
        """Close a span by setting its completion timestamp."""
        span = self._spans.get(span_id)
        if span is not None and span.end_time is None:
            span.end_time = datetime.now(timezone.utc)
        return span

    def get_spans_for_trace(self, trace_id: str) -> List[TraceSpan]:
        """Retrieve all spans belonging to a trace."""
        return [s for s in self._spans.values() if s.trace_id == trace_id]