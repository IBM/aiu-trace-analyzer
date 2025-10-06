# Copyright 2024-2025 IBM Corporation

import aiu_trace_analyzer.logger as aiulog
from aiu_trace_analyzer.types import TraceEvent
from aiu_trace_analyzer.pipeline import AbstractContext, EventPairDetectionContext


class EventSortingContext(EventPairDetectionContext):
    def __init__(self, event_types=None, sortkey="ts", global_sort=False) -> None:
        super().__init__()

        self.event_types = event_types
        self.sortkey = sortkey
        self.global_sort = global_sort
        self.lastidx = {}

    def queue_hash(self, pid, tid) -> int:
        if self.global_sort:
            return 1
        else:
            return super().queue_hash(pid, tid)

    def sort(self, event: TraceEvent):
        if ((self.event_types is not None) and (event["ph"] not in self.event_types)) or (self.sortkey not in event):
            return [event]

        tid = event["tid"] if "tid" in event else 0
        queue_id = self.queue_hash(event["pid"], tid)
        if queue_id not in self.queues:
            self.queues[queue_id] = []
            self.lastidx[queue_id] = 0
        aiulog.log(aiulog.TRACE, "SORT queue: ", queue_id, "from", event["pid"], tid, self.global_sort)
        self.queues[queue_id].append(event)
        return []

    def drain(self):
        drained_events = []
        for _, q in self.queues.items():
            q.sort(key=lambda x: x[self.sortkey])
        while len(self.queues.keys()) > 0:
            queue_id = list(self.queues.keys())[0]
            drained_events += self.queues.pop(queue_id)
        return drained_events


def sort_events(event: TraceEvent, context: AbstractContext) -> list[TraceEvent]:
    '''
    Collects events into queues and sorts by configured keys in context
    '''
    assert isinstance(context, EventSortingContext)
    return context.sort(event)
