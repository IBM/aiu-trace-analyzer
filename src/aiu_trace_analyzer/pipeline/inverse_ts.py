# Copyright 2024-2025 IBM Corporation

import aiu_trace_analyzer.logger as aiulog
from aiu_trace_analyzer.types import TraceEvent
from aiu_trace_analyzer.pipeline import AbstractContext


class InversedTSDetectionContext(AbstractContext):
    '''
    Detect any begin/end event pair with timestamps that are out of order
    This also needs to cover cases where the end-event is processed before the begin event
    For that, we have:
     * 2 lists that hold events until their corresponding partner appears...
     * ...one list for begin/opening events waiting for the end to appear (stash_o)
     * ...one list for end/closing events waiting for the begin to appear (stach_c)
     * once both events are available, check for correctly ordered time stamps...
     * ...and emit both events in begin -> end order
     * ... or drop both events + issue a warning if their time stamps are out-of order
    '''

    # match those keys to identify begin/end event pairs
    MATCHKEYS = ["name", "pid", "tid"]

    def __init__(self) -> None:
        super().__init__()
        self.stash_o = []
        self.lastmatch_o = -1
        self.stash_c = []
        self.lastmatch_c = -1
        self.bad_count = 0

    def __del__(self) -> None:
        if self.bad_count > 0:
            aiulog.log(aiulog.WARN, "TS-Inversed events dropped:", self.bad_count)

    def drain(self) -> list[TraceEvent]:
        drained = super().drain()
        rval = drained + self.stash_o + self.stash_c
        self.stash_c = []
        self.stash_o = []
        return rval

    def add_open(self, event: TraceEvent):
        self.stash_o.append(event)

    def add_close(self, event: TraceEvent):
        self.stash_c.append(event)

    def search_in_open(self, event: TraceEvent) -> TraceEvent:
        for i, e in enumerate(self.stash_o):
            if self.match(e, event):
                self.lastmatch_o = i
                return e
        return None

    def search_in_close(self, event: TraceEvent) -> TraceEvent:
        for i, e in enumerate(self.stash_c):
            if self.match(e, event):
                self.lastmatch_c = i
                return e
        return None

    def drop_from_open(self) -> TraceEvent:
        assert (self.lastmatch_o >= 0 and self.lastmatch_o < len(self.stash_o))
        item = self.stash_o.pop(self.lastmatch_o)
        self.lastmatch_o = -1
        return item

    def drop_from_close(self) -> TraceEvent:
        assert (self.lastmatch_c >= 0 and self.lastmatch_c < len(self.stash_c))
        item = self.stash_c.pop(self.lastmatch_c)
        self.lastmatch_c = -1
        return item

    def match(self, a: TraceEvent, b: TraceEvent) -> bool:
        found = True
        for key in self.MATCHKEYS:
            if key in a and key in b:
                found &= (a[key] == b[key])
        return found

    def count_bads(self):
        self.bad_count += 1


# uses InversedTSDetectionContext to hold B- or E-events until their counterpart is appears
# and checks timestamps are ordered before emitting them in the B -> E order
def drop_timestamp_reversed_events(event: TraceEvent, _: AbstractContext) -> list[TraceEvent]:
    return [event]


# keep the old code in case there's a future need for events that come as Begin-End-Event pairs
def _drop_ts_old_if_BE_events(event: TraceEvent, context: AbstractContext) -> list[TraceEvent]:
    # current ingestion will create complete events, so this is no longer needed
    assert isinstance(context, InversedTSDetectionContext)

    event_list = []

    if event["ph"] in AbstractContext.CLOSING_EVENTS:
        open_event = context.search_in_open(event)
        if open_event:
            context.drop_from_open()
            event_list = [open_event, event]
        else:
            # events appeared out of order
            context.add_close(event)

    elif event["ph"] in AbstractContext.OPENING_EVENTS:
        close_event = context.search_in_close(event)
        if close_event:
            # closing event has been encountered out of order
            context.drop_from_close()
            event_list = [event, close_event]
            aiulog.log(aiulog.WARN, "Closing Event encountered before opening: ", event["name"])
        else:
            # stash until closing event appears
            context.add_open(event)

    else:
        event_list = [event]

    # check B/E correctness and then add both events back into the chain
    if len(event_list) == 2 and event_list[0]["ts"] > event_list[1]["ts"]:
        aiulog.log(aiulog.WARN, "B/E events with invalid timestamps:", event_list[0]["name"])
        context.count_bads()
        event_list = []

    return event_list
