# Copyright 2024-2025 IBM Corporation

import aiu_trace_analyzer.logger as aiulog
from aiu_trace_analyzer.types import TraceEvent, TraceWarning
from aiu_trace_analyzer.pipeline import AbstractHashQueueContext


class EventPairDetectionContext(AbstractHashQueueContext):
    '''
    scan events for opening and closing types and match them based on pid, tid, and name
    to create complete event types (X) with duration
    make sure to detect partners based on their closest subsequent timestamp in case the
    same name repeats

    provides both options:
    a) find a closing event match for a given opening event
    b) find an opening event match for a given closing event
    '''
    def __init__(self, warnings: list[TraceWarning] = None) -> None:
        super().__init__(warnings=warnings)

    def drain(self) -> list[TraceEvent]:
        return []

    def queue_hash(self, pid, tid) -> int:
        return hash((pid, tid))

    def insert(self, event: TraceEvent, queue_id=None) -> int:
        tid = event["tid"] if "tid" in event else 0
        _queue_id = queue_id if queue_id else self.queue_hash(event["pid"],
                                                              tid)
        aiulog.log(aiulog.TRACE, "PAIR INSERT:", _queue_id, event)
        if _queue_id in self.queues:
            self.queues[_queue_id].append(event)
        else:
            self.queues[_queue_id] = [event]
        return _queue_id

    # create hash of reference event and list of keys to include
    def create_reference_data(self, reference: TraceEvent, mixed_queue: bool) -> tuple[int, list]:
        # basic mapping based on just the name (assuming mixed queues first):
        include_keys = ["name"]

        # if the queues don't allow events from multiple proc/threads,
        # we need to include pid and tid entries for comparison
        if not mixed_queue:
            assert ("pid" in reference)
            include_keys += ["pid", "tid"]
        event_hash = self.event_data_hash(reference, include_keys, ignore_missing=True)
        return event_hash, include_keys

    def find_slice_close_event_partner(self, event: TraceEvent,
                                       queue: list[TraceEvent],
                                       mixed_queue: bool = False) -> tuple[TraceEvent, int]:
        if event["ph"] not in self.OPENING_EVENTS:
            return None, None

        # reference event hash and list of entries to strip from event before making pair match hash
        event_hash, include_keys = self.create_reference_data(event, mixed_queue)

        partner = None, None
        aiulog.log(aiulog.TRACE, "PAIR FINDO:", len(queue))
        for idx, b in enumerate(queue):
            if b["ph"] not in self.CLOSING_EVENTS or b == event:
                continue
            # skip anything where the end ts is before the begin ts (those events have been cleared in previous stage)
            if b["ts"] < event["ts"]:
                assert (event["name"] != b["name"])  # make sure we have not hit a skip-event bug
                continue

            # check the event data for matches (except what's listed in ignore_list)
            if event_hash == self.event_data_hash(b, include_keys):
                # find the corresponding end event with the smallest timestamp in
                # case subsequent events have the same name
                if not partner[0] or partner[0]["ts"] > b["ts"]:
                    partner = b, idx
        return partner

    def find_slice_open_event_partner(self, event: TraceEvent,
                                      queue: list[TraceEvent],
                                      mixed_queue: bool = False) -> tuple[TraceEvent, int]:
        if event["ph"] not in self.CLOSING_EVENTS:
            return None, None

        # reference event hash and list of entries to strip from event before making pair match hash
        event_hash, include_keys = self.create_reference_data(event, mixed_queue)

        partner = None, None
        aiulog.log(aiulog.TRACE, "PAIR FINDC:", len(queue))
        for idx, a in enumerate(queue):
            if a["ph"] not in self.OPENING_EVENTS or a == event:
                continue
            # skip anything where the end ts is before the begin ts (those events have been cleared in previous stage)
            if event["ts"] < a["ts"]:
                assert (event["name"] != a["name"])  # sanity check for duplicate overlapping events
                continue

            # remaining check: name
            if event_hash == self.event_data_hash(a, include_keys):
                # find the corresponding begin event with the smallest timestamp in
                # case subsequent events have the same name
                if not partner[0] or partner[0]["ts"] > a["ts"]:
                    partner = a, idx
        return partner
