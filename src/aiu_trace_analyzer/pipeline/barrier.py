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

    def drain(self):
        if self.phase == self._COLLECTION_PHASE:
            self.phase = self._APPLICATION_PHASE
        else:
            # do nothing if this is the application phase
            pass
        # the queues for these contexts don't contain events (events are held in barrier context),
        # so nothing to drain here
        return []
