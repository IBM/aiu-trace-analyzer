# Copyright 2024-2025 IBM Corporation

import aiu_trace_analyzer.logger as aiulog
from aiu_trace_analyzer.types import TraceEvent
from aiu_trace_analyzer.pipeline import AbstractContext, EventPairDetectionContext


class EventSortingContext(EventPairDetectionContext):
    def __init__(self,
                 event_types=None,
                 sortkey: str = "ts",
                 global_sort: bool = False) -> None:
        super().__init__()

        self.event_types = event_types
        self.sortkey: list[tuple[str, int]] = self._parse_sortkey(sortkey)
        self.global_sort = global_sort
        self.lastidx = {}

    def queue_hash(self, pid, tid) -> int:
        if self.global_sort:
            return 1
        else:
            return super().queue_hash(pid, tid)

    def _parse_sortkey(self, sortkey: str) -> list[tuple[str, int]]:
        keys = sortkey.split(',')
        sortkeys: list[tuple[str, int]] = []
        for k in keys:
            key = k.split(':')
            reverse = 1
            if len(key) > 1 and key[1] == 'r':
                reverse = -1
            sortkeys.append((key[0], reverse))
        return sortkeys

    def _check_keys(self, event: TraceEvent) -> bool:
        # check for primary key only, any missing secondary keys will be considered zero
        k, _ = self.sortkey[0]
        return k in event

    def sort(self, event: TraceEvent):
        if ((self.event_types is not None) and (event["ph"] not in self.event_types)) or not self._check_keys(event):
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
            q.sort(key=lambda x: tuple([float(rev) * float(x[k] if k in x else 0.0) for k, rev in self.sortkey]))
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
