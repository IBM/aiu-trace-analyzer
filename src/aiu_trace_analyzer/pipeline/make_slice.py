# Copyright 2024-2025 IBM Corporation

import aiu_trace_analyzer.logger as aiulog
from aiu_trace_analyzer.types import TraceEvent
from aiu_trace_analyzer.pipeline import AbstractContext, EventPairDetectionContext


class SliceCreationContext(EventPairDetectionContext):
    '''
    Turns all B/E events into slices using X-type
    Uses event pair detection for queues and helpers so
    that there's a search queue for each pid/tid combination.
    Once a pair of B/E events is found, B is turned into X
    by adding 'dur' and E is dropped.
    '''
    def __init__(self, make_complete=True) -> None:
        super().__init__()
        self.complete = make_complete

    def add(self, event: TraceEvent):
        assert event["ph"] in self.OPENING_EVENTS
        aiulog.log(aiulog.DEBUG, "SLICE START:", len(self.queues), event["name"], event["ts"])
        self.insert(event)

    def drain(self):
        for k, q in self.queues.items():
            if len(q):
                aiulog.log(aiulog.WARN, "SLICE_EXTRACT: Has Open/Close events without matching partners:", len(q), q[0])
            assert len(q) == 0
        return []

    def slice(self, event: TraceEvent) -> list[TraceEvent]:
        assert event["ph"] in self.CLOSING_EVENTS
        queue_id = self.queue_hash(event["pid"], event["tid"])
        parter, idx = self.find_slice_open_event_partner(event, self.queues[queue_id])
        if parter:
            self.queues[queue_id].pop(idx)
            aiulog.log(aiulog.DEBUG,
                       "SLICE POP:", idx, event["name"], event["ts"],
                       "remlen=", len(self.queues[queue_id]))
            return self.emit_as_complete([parter, event])
        aiulog.log(aiulog.WARN,
                   "SLICE END: No opening event found.", idx, event["name"], event["ts"],
                   "remlen=", len(self.queues[queue_id]))
        return []

    def emit_as_complete(self, events: list[TraceEvent]) -> list[TraceEvent]:
        if not self.complete:
            return events

        assert len(events) % 2 == 0
        rlist = []
        while True:
            a, b, rem = events[0], events[1], events[2:]
            if a["ph"] not in ["B", "E"] or b["ph"] not in ["B", "E"]:
                return events
            if a["name"] == b["name"]:
                a["ph"] = "X"
                a["dur"] = round(abs(b["ts"] - a["ts"]), 3)
                aiulog.log(aiulog.DEBUG, "SLICE CREATE: ", a)
                rlist.append(a)
            if len(rem) == 0:
                break
        return rlist


def create_slice_from_BE(event: TraceEvent, context: AbstractContext) -> list[TraceEvent]:
    '''
    Captures B and E events, matches them up and emits X events with duration
    Those Complete events are easier for detecting/resolving overlap
    '''
    assert isinstance(context, SliceCreationContext)
    if event["ph"] in "B":
        context.add(event)
        return []

    if event["ph"] in "E":
        complete = context.slice(event)
        assert len(complete) != 0
        return complete

    return [event]
