# Copyright 2024-2025 IBM Corporation

from typing import Optional

from aiu_trace_analyzer.types import TraceEvent, TraceWarning
from aiu_trace_analyzer.pipeline import AbstractContext, AbstractHashQueueContext


class _BarrierContext(AbstractContext):
    def __init__(self) -> None:
        super().__init__()
        self.hold = []

    def collect(self, event: TraceEvent):
        self.hold.append(event)

    def drain(self) -> list[TraceEvent]:
        revents = self.hold
        self.hold = []
        return revents


_main_barrier_context = _BarrierContext()


def pipeline_barrier(event: TraceEvent, _: AbstractContext) -> list[TraceEvent]:
    bctx = _main_barrier_context
    bctx.collect(event)
    return []


class TwoPhaseWithBarrierContext(AbstractHashQueueContext):
    _COLLECTION_PHASE = 0
    _APPLICATION_PHASE = 1

    def __init__(self, warnings: Optional[list[TraceWarning]] = None) -> None:
        super().__init__(warnings=warnings)
        self.phase = self._COLLECTION_PHASE

    def collection_phase(self) -> bool:
        return self.phase == self._COLLECTION_PHASE

    def drain(self) -> list[TraceEvent]:
        if self.phase == self._COLLECTION_PHASE:
            # first drain call: switch to the application phase. Defer to the application-phase
            # drain so parent drain is not called twice and cross-phase state in self.queues
            # survives the transition.
            self.phase = self._APPLICATION_PHASE
            revents = []
        else:
            # application phase (final drain call): the cross-phase state has been consumed, so
            # it is safe to chain the parent drain, which flushes the (event-less) queues and
            # emits any additional events if needed.
            revents = super().drain()
        return revents
