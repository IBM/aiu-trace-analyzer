# Copyright 2024-2025 IBM Corporation

import copy

import aiu_trace_analyzer.logger as aiulog
from aiu_trace_analyzer.pipeline import AbstractContext,EventPairDetectionContext
from aiu_trace_analyzer.pipeline.tools import PipelineContextTool
from aiu_trace_analyzer.types import TraceEvent, GlobalIngestData

class OverlapTracking(tuple[float, bool, list[float]]):
    pass

class OverlapDetectionContext(EventPairDetectionContext):
    '''
    Management structures and functions to deal with overlapping events.
    Solves without storing the events themselves, just keeps track of
    occupied/active stream (combination of pid+tid)
     * requires incoming events per pid/tid sorted by time stamp
     * blocks each stream for the duration of an active event
     * any event that appears within that stream while an event
       is active needs an overlap resolve
     * instead of an active queue, it only holds a tuple (ts_c, bool, ts_e)
       where bool indicates whether active or inactive stream and ts
       is the time stamp indicating until what time it's active
     * need to keep a list of active end ts because conflicts might happen
       within non-critical nested overlapping events
    '''
    OVERLAP_RESOLVE_DROP=1
    OVERLAP_RESOLVE_TID=2
    OVERLAP_RESOLVE_ASYNC=3

    def __init__(self, overlap_resolve=OVERLAP_RESOLVE_DROP) -> None:
        super().__init__()
        self.overlap_resolve = overlap_resolve
        self.resolved = 0
        self.async_id = 0
        self.async_queues = {}

    def __del__(self) -> None:
        level = aiulog.WARN if self.resolved else aiulog.INFO
        aiulog.log(level, "Partial-overlap slices resolved:", self.resolved)

    # search for events within the same pid/tid
    # accumulate a queue of events for each pid/tid
    # once the queue is full, run detection and emit events that are fine
    def overlap_detection(self, event:TraceEvent) -> list[TraceEvent]:

        tid = event["tid"] if "tid" in event else 0
        queue_id = self.queue_hash(event["pid"], tid)
        if not queue_id in self.queues:
            self.queues[queue_id] = (0.0, False, [])

        current_ts, blocked, end_ts = self.queues[queue_id]

        assert current_ts <= event["ts"]  # make sure ts is monotonic increasing

        event_ts = event["ts"]
        event_end = event["ts"] + event["dur"]

        aiulog.log(aiulog.TRACE, "POD queue before: ", queue_id, "from", event["pid"], tid, self.queues[queue_id])
        assert (blocked and len(end_ts) > 0) or (not blocked and len(end_ts) == 0)
        if not blocked:
            self.queues[queue_id] = (event_ts, True, [event_end])
            revents = [event]
        else:
            # only do overlap resolution for flex events, ptorch events already have parentID etc for traceviewer to disentangle
            if PipelineContextTool.is_FLEX_event and self.check_overlap_condition(event_ts, event_end, self.queues[queue_id]):
                # actual overlap
                revents = self.handle_overlap(event,queue_id)
            else:
                # non-critical stacking: need to track the additional end-ts
                revents = [event]
                self.queues[queue_id][2].append(event_end)

        if self.overlap_resolve == self.OVERLAP_RESOLVE_ASYNC:
            aevents = self.update_async_event_queue(queue_id, None, event_ts)
            revents = aevents + revents  # prepend any async events that need to be injected
        self.update_queue_status(event_ts, queue_id)
        aiulog.log(aiulog.TRACE, "POD queue after: ", queue_id, "from", event["pid"], tid, self.queues[queue_id])
        return revents

    # run the beginning and end ts of the event through the existing list of active event ends to detect overlaps
    def check_overlap_condition(self, ts, end, qstate: OverlapTracking) -> bool:
        c,b,end_q = qstate
        overlap = False
        for e in end_q:
            aiulog.log(aiulog.TRACE, "POD overlap check", b, c, ts, e, end)
            overlap |= (ts<e and e<end)   # !!! b and c<=ts are already granted
        if overlap:
            aiulog.log(aiulog.TRACE, "POD overlap detected", qstate, ts, end)
        return overlap

    # remove only keep entries of end timestamps that are later than the new current head
    def update_queue_status(self, new_current: float, queue_id: int):
        end_q = self.queues[queue_id][2]
        new_end_q = list( filter(lambda x: x>=new_current, end_q))
        is_blocked = (len(new_end_q) > 0)  # unblock the queue if no more end-ts are remaining
        self.queues[queue_id] = (new_current, is_blocked, new_end_q)

    # solve a detected overlap between a pair of pairs
    def handle_overlap(self,
                       oevent: TraceEvent,
                       queue_id: int) -> TraceEvent:
        if self.overlap_resolve == self.OVERLAP_RESOLVE_DROP:
            aiulog.log(aiulog.WARN, "Solving overlap conflict by dropping:", oevent)
            self.resolved += 1
            return []
        elif self.overlap_resolve == self.OVERLAP_RESOLVE_TID:
            oevent["tid"] += 1
            # feed offending event back into the detector with the new TID to make sure there are no collisions there either
            rlist = self.overlap_detection(oevent)
            self.resolved += 1
            return rlist
        elif self.overlap_resolve == self.OVERLAP_RESOLVE_ASYNC:
            oevent["id"] = self.async_id
            end_ts = oevent["ts"] + oevent["dur"]
            oevent.pop("dur")
            self.async_id += 1
            self.resolved += 1

            e_event = copy.deepcopy(oevent)
            oevent["ph"] = "b"
            e_event["ph"] = "e"
            e_event["ts"] = end_ts
            alist = self.update_async_event_queue(queue_id, e_event, oevent["ts"])
            aiulog.log(aiulog.TRACE, "POD aret:", [ e["ts"] for e in alist] + [ oevent["ts"] ])
            return alist + [oevent]
        return []

    def update_async_event_queue(self, queueID, async_event, current) -> list[TraceEvent]:
        if not async_event and queueID not in self.async_queues:
            # nothing to do, no pending async events to emit or handle
            return []

        # if a new async event is provided, lets queue it:
        if async_event:
            if queueID not in self.async_queues:
                self.async_queues[queueID] = []
            self.async_queues[queueID].append(async_event)

        # otherwise: just review any existing async events to emit
        aiulog.log(aiulog.TRACE, "POD aqueue:", current, [ e["ts"] for e in self.async_queues[queueID]])
        # return every async 'e' event with a ts <= current
        rlist = list(filter(lambda x: x['ts']<=current, self.async_queues[queueID]))
        rlist.sort(key=lambda e: e['ts'])
        # keep every async 'e' event until its time has come
        remain = list(filter(lambda x: x['ts']>current, self.async_queues[queueID]))
        self.async_queues[queueID] = remain
        return rlist

    def drain(self):
        revents = []
        # make sure to drain the queue of async 'e' events that might have been hold back past the end of the last main event of a stream
        while len(self.async_queues)>0:
            _,aq = self.async_queues.popitem()
            # make sure to keep everything sorted
            aq.sort(key=lambda e: e['ts'])
            revents += aq
        return revents



# mapping function callback
def detect_partial_overlap_events(event: TraceEvent, context: AbstractContext) -> list[TraceEvent]:
    assert( isinstance(context, OverlapDetectionContext) )

    if event["ph"] in "X":
        return context.overlap_detection(event)
    else:
        return [event]


###################################################################
# Timestamps sequence checking to make sure time stamps stay sorted
class TSSequenceContext(EventPairDetectionContext):
    def __init__(self, ts3check: bool = False):
        super().__init__()
        self.TS_cmpt_end = {}
        self.ts_outsync = (0, 0)
        self.ts_total = 0
        self.ts_check = ts3check

    def __del__(self):
        if self.ts_outsync[1] > 0:
            aiulog.log(aiulog.WARN, "TS_SEQUENCE: detected cycles overlapping (TS3[n] < TS4[n-1]) between cmpt_exec events within the same PID ", self.ts_outsync[0], "/", self.ts_total, "max overlap cycles: ", self.ts_outsync[1])

    def insert(self, event: TraceEvent, queue_id=None):
        if not queue_id:
            queue_id = self.queue_hash( event["pid"], event["tid"])

        if queue_id not in self.queues:
            self.queues[queue_id] = 0.0

        if self.queues[queue_id] > event['ts']:
            aiulog.log(aiulog.ERROR, "Events out of order:", self.queues[queue_id], ">", event['ts'])

        self.queues[queue_id] = event['ts']

    def ts3insert(self, event: TraceEvent, queue_id=None):
        if "Cmpt Exec" not in event["name"]:
            return

        self.ts_total += 1
        if not queue_id:
            queue_id = event["pid"]

        if queue_id not in self.TS_cmpt_end:
            self.TS_cmpt_end[queue_id] = (0.0, 0)

        try:
            last_TS = self.TS_cmpt_end[queue_id]
            if last_TS[0] < event["ts"] and int(event["args"]["TS3"]) < last_TS[1]:
                self.ts_outsync = (self.ts_outsync[0]+1, max(self.ts_outsync[1], last_TS[1] - int(event["args"]["TS3"])))
            self.TS_cmpt_end[queue_id] = (event["ts"], int(event["args"]["TS4"]))
        except:
            print(self.TS_cmpt_end[queue_id], event)
            raise


def assert_ts_sequence(event: TraceEvent, context: AbstractContext) -> list[TraceEvent]:
    assert( isinstance(context, TSSequenceContext))

    if event["ph"] in "Xbe":
        context.insert(event)
        if context.ts_check:
            context.ts3insert(event)
    return [event]


def assert_global_ts_sequence(event: TraceEvent, context: AbstractContext) -> list[TraceEvent]:
    assert( isinstance(context, TSSequenceContext))

    if event["ph"] in "Xbe":
        context.insert(event, queue_id=1)
    return [event]

def recombine_cpu_events(event: TraceEvent, context:AbstractContext, config:dict) -> list[TraceEvent]:

    # TODO this should be moved to a tool-section because it's highly reusable
    def is_CPU_event(event:TraceEvent) -> bool:
        is_cpu = ("args" not in event)
        is_cpu |= ("args" in event and "TS1" not in event["args"])
        return is_cpu

    try:
        if GlobalIngestData.get_dialect(event["args"]["jobhash"]).get("NAME") != "FLEX":
            return [event]
    except KeyError:
        return [event]

    if event["ph"] in "X" and is_CPU_event(event) and "AIU Roundtrip" not in event["name"] and PipelineContextTool.is_FLEX_event(event):
        fixed_tid = config.get("cpu_stream_tid", 1000)  # extract the new tid from config
        event["tid"] = fixed_tid
    return [event]
