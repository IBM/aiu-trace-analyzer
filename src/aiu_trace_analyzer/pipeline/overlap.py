# Copyright 2024-2025 IBM Corporation

import copy

import aiu_trace_analyzer.logger as aiulog
from aiu_trace_analyzer.pipeline import (
    AbstractContext,
    AbstractHashQueueContext,
    TwoPhaseWithBarrierContext)
from aiu_trace_analyzer.pipeline.context import AbstractVerificationContext
from aiu_trace_analyzer.types import TraceEvent, GlobalIngestData, TraceWarning
from aiu_trace_analyzer.pipeline.tools import PipelineContextTool


class OverlapTracking(tuple[float, bool, list[float]]):
    """
    tuple of:
      float:  the ts of the currently 'active' event
      bool:   whether this event stream/queue is blocked or not (has active event)
      list[float]: list of end-ts for the stack of active events
    """
    pass


class OverlapDetectionContext(TwoPhaseWithBarrierContext):
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
    OVERLAP_RESOLVE_DROP = 1
    OVERLAP_RESOLVE_TID = 2
    OVERLAP_RESOLVE_ASYNC = 3
    OVERLAP_RESOLVE_WARN = 4
    OVERLAP_RESOLVE_SHIFT = 5

    # default event fields that define the identity of a stream/queue. Derived classes with a static
    # partitioning can override this by assignment (never in-place: the list is shared by all
    # instances). Hierarchical keys are supported, e.g. "args.stream".
    _QUEUE_ID_KEYS = ["pid", "tid"]

    def __init__(self,
                 overlap_resolve=OVERLAP_RESOLVE_DROP,
                 ts_shift_threshold=0.0,
                 max_tid_streams=5,
                 ) -> None:
        super().__init__(warnings=[
            TraceWarning(
                name="overlaps",
                text="Partial-overlap slices resolved: {d[count]}",
                data={"count": 0},
            )
        ])
        self.overlap_resolve = overlap_resolve
        self.async_id = 0
        self.async_queues = {}
        self.ts_shift_threshold = ts_shift_threshold
        self.tid_space = {}
        self.max_tid_streams = max_tid_streams
        self.queue_id_keys = None  # resolved from the first event, see _select_queue_id_keys()

    # select the event fields that define a stream/queue identity. Called only for the first event,
    # so the selection may depend on the input dialect (assumed constant for one program execution).
    # Derived classes can override, e.g. `return super()._select_queue_id_keys(event) + ["args.stream"]`
    def _select_queue_id_keys(self, _event: TraceEvent) -> list[str]:
        return self._QUEUE_ID_KEYS

    # search for events within the same pid/tid
    # accumulate a queue of events for each pid/tid
    # once the queue is full, run detection and emit events that are fine
    def overlap_detection(self, event: TraceEvent) -> list[TraceEvent]:

        tid = event["tid"] if "tid" in event else 0
        if self.queue_id_keys is None:
            self.queue_id_keys = self._select_queue_id_keys(event)
            aiulog.log(aiulog.DEBUG, "POD: stream identity keys:", self.queue_id_keys)
        queue_id = self.event_data_hash(event, self.queue_id_keys, ignore_missing=True)
        if queue_id not in self.queues:
            self.queues[queue_id] = (0.0, False, [])

        current_ts, blocked, end_ts = self.queues[queue_id]

        # make sure ts is monotonic increasing
        assert current_ts <= event["ts"], (
            f"Events out-of-order[{event['tid']}]: {current_ts},"
            f" {event['ts']}, {self.queues[queue_id]}")

        event_ts = event["ts"]
        event_end = round(event["ts"] + event["dur"], 4)

        aiulog.log(aiulog.TRACE, "POD queue before: ", queue_id, "from", event["pid"], tid, self.queues[queue_id])
        assert (blocked and len(end_ts) > 0) or (not blocked and len(end_ts) == 0)
        if not blocked:
            self.queues[queue_id] = (event_ts, True, [event_end])
            revents = [event]
        else:
            if self.check_overlap_condition(event_ts, event_end, self.queues[queue_id]):
                # actual overlap
                revents = self.handle_overlap(event, queue_id)
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
        c, b, end_q = qstate
        overlap = False
        for e in end_q:
            aiulog.log(aiulog.TRACE, "POD overlap check", b, c, ts, e, end)
            overlap |= (ts < e and e < end)   # !!! b and c<=ts are already granted
        if overlap:
            aiulog.log(aiulog.TRACE, "POD overlap detected", qstate, ts, end)
        return overlap

    # remove only keep entries of end timestamps that are later than the new current head
    def update_queue_status(self, new_current: float, queue_id: int):
        end_q = self.queues[queue_id][2]
        new_end_q = list(filter(lambda x: x >= new_current, end_q))
        is_blocked = (len(new_end_q) > 0)  # unblock the queue if no more end-ts are remaining
        self.queues[queue_id] = (new_current, is_blocked, new_end_q)

    def get_overlap_time(self, ts: float, end: float, qstate: OverlapTracking) -> float:
        _, _, end_q = qstate
        overlap_time = -1.e99
        for e in end_q:
            # consider potential overlap time only if this event is not fully embedded
            if e <= end:
                overlap_time = max(overlap_time, e - ts)
        return overlap_time

    def find_next_tid(self, event: TraceEvent) -> int:
        if self.max_tid_streams == -1:
            return event["tid"] + 1

        if event["tid"] not in self.tid_space[event["pid"]]:
            aiulog.log(
                aiulog.ERROR,
                f"POD: insufficient dynamic range for tid-based overlap resolution ({self.max_tid_streams})",
                f"of job: {event['args']['jobname']}. Increase max_tid_space.")
        new_tid = self.tid_space[event["pid"]][event["tid"]]
        return new_tid

    # single funnel for recording a detected overlap: derived classes can override this to
    # attach per-event detail to the warning (the offending event is not available in issue_warning())
    def _record_overlap(self, _oevent: TraceEvent) -> None:
        self.issue_warning("overlaps")

    # solve a detected overlap between a pair of pairs
    def handle_overlap(self,
                       oevent: TraceEvent,
                       queue_id: int) -> list[TraceEvent]:
        if self.overlap_resolve == self.OVERLAP_RESOLVE_DROP:
            aiulog.log(aiulog.WARN, "Solving overlap conflict by dropping:", oevent)
            self._record_overlap(oevent)
            return []
        elif self.overlap_resolve == self.OVERLAP_RESOLVE_WARN:
            aiulog.log(aiulog.WARN, "Detected overlap conflict: ", oevent["name"])
            self._record_overlap(oevent)
            return [oevent]
        elif self.overlap_resolve == self.OVERLAP_RESOLVE_SHIFT:
            ts_shift = self.get_overlap_time(oevent["ts"], oevent["ts"]+oevent["dur"], self.queues[queue_id])
            if ts_shift > 0.0001:
                aiulog.log(aiulog.DEBUG, "Detected overlap for event", oevent["name"],
                           "Start-shift to solve: ", ts_shift)
            if ts_shift < self.ts_shift_threshold:
                oevent["args"]["orig_ts"] = oevent["ts"]   # keep the original ts in args
                oevent["ts"] += round(ts_shift+0.0015, 3)  # round-up the required ts-shift and add 1ns
                if ts_shift > oevent["dur"]:
                    aiulog.log(aiulog.WARN, "Overlap shifting of", oevent["name"],
                               "exceeds its duration", oevent["dur"])
                else:
                    oevent["args"]["orig_dur"] = oevent["dur"]
                    oevent["dur"] -= round(ts_shift+0.0015, 3)  # reduce the duration to keep the end-time unchanged
                # feed offending event back into the detector to make sure
                # it's end time does not collide with anything else
                rlist = self.overlap_detection(oevent)
            else:
                aiulog.log(aiulog.WARN, "Detected overlap of", oevent["name"], "exceeds the threshold/limit",
                           self.ts_shift_threshold, "us. Overlap of", ts_shift,
                           "us: increase threshold or use different overlap res option.")
                rlist = [oevent]

            self._record_overlap(oevent)
            return rlist
        elif self.overlap_resolve == self.OVERLAP_RESOLVE_TID:
            oevent["tid"] = self.find_next_tid(oevent)
            # feed offending event back into the detector with the new TID to make sure
            # there are no collisions there either
            rlist = self.overlap_detection(oevent)
            self._record_overlap(oevent)
            return rlist
        elif self.overlap_resolve == self.OVERLAP_RESOLVE_ASYNC:
            # record before the event is converted to an async b/e pair (which drops its 'dur')
            self._record_overlap(oevent)
            oevent["id"] = self.async_id
            end_ts = oevent["ts"] + oevent["dur"]
            oevent.pop("dur")
            self.async_id += 1

            e_event = copy.deepcopy(oevent)
            oevent["ph"] = "b"
            e_event["ph"] = "e"
            e_event["ts"] = end_ts
            alist = self.update_async_event_queue(queue_id, e_event, oevent["ts"])
            aiulog.log(aiulog.TRACE, "POD aret:", [e["ts"] for e in alist] + [oevent["ts"]])
            return alist + [oevent]
        return []

    def update_async_event_queue(self, queue_id, async_event, current) -> list[TraceEvent]:
        if not async_event and queue_id not in self.async_queues:
            # nothing to do, no pending async events to emit or handle
            return []

        # if a new async event is provided, lets queue it:
        if async_event:
            if queue_id not in self.async_queues:
                self.async_queues[queue_id] = []
            self.async_queues[queue_id].append(async_event)

        # otherwise: just review any existing async events to emit
        aiulog.log(aiulog.TRACE, "POD aqueue:", current, [e["ts"] for e in self.async_queues[queue_id]])
        # return every async 'e' event with a ts <= current
        rlist = list(filter(lambda x: x['ts'] <= current, self.async_queues[queue_id]))
        rlist.sort(key=lambda e: e['ts'])
        # keep every async 'e' event until its time has come
        remain = list(filter(lambda x: x['ts'] > current, self.async_queues[queue_id]))
        self.async_queues[queue_id] = remain
        return rlist

    def collect_tid_space(self, event: TraceEvent) -> None:
        pid, tid = event["pid"], event["tid"]
        if pid not in self.tid_space:
            self.tid_space[pid] = {-1: set()}

        # collect all detected tids under key `-1`
        self.tid_space[pid][-1].add(tid)

        if tid not in self.tid_space[pid]:
            self.tid_space[pid][tid] = []

    def _create_tid_space(self, tid: int, exclude: list[int]) -> list[int]:
        tlist = []
        next_tid = tid
        while len(tlist) < max(self.max_tid_streams, 1):
            next_tid += 1
            if next_tid not in exclude:
                tlist.append(next_tid)
        return tlist

    def _collect_and_build_tid_space(self) -> None:
        new_tspace = {}
        for pid, tspace in self.tid_space.items():
            new_tspace = {}
            # collect candidate lists for each known tid from input
            exclude: set = tspace[-1]
            for tid in tspace.keys():
                if tid == -1:
                    continue
                tcandidates = self._create_tid_space(tid, exclude)
                self.tid_space[pid][tid] = tcandidates
                exclude.update(tcandidates)
                new_tspace[tid] = tcandidates[0]
                for src_tid, next_tid in zip(tcandidates[:-1], tcandidates[1:]):
                    new_tspace[src_tid] = next_tid

            aiulog.log(aiulog.TRACE, "POD: total tid_space:", self.tid_space[pid])
            self.tid_space[pid] = copy.deepcopy(new_tspace)
            aiulog.log(aiulog.TRACE, "POD: tid neighbors:", new_tspace)

    def drain(self):
        if self.overlap_resolve == self.OVERLAP_RESOLVE_TID:
            if self.phase == self._COLLECTION_PHASE:
                self._collect_and_build_tid_space()
            # chain to the parent drain in both phases: it advances the two-phase state and
            # propagates accumulated warnings as trace_issue events
            return super().drain()
        else:
            revents = []
            # make sure to drain the queue of async 'e' events that might have been hold
            # back past the end of the last main event of a stream
            while len(self.async_queues) > 0:
                _, aq = self.async_queues.popitem()
                # make sure to keep everything sorted
                aq.sort(key=lambda e: e['ts'])
                revents += aq
            # these modes don't use the two-phase mechanism and are drained only once, so switch
            # to the application phase before chaining up to make the parent emit the accumulated
            # warnings as trace_issue events (rather than deferring to a second drain that never comes)
            self.phase = self._APPLICATION_PHASE
            return revents + super().drain()


def detect_partial_overlap_tids(event: TraceEvent, context: AbstractContext) -> list[TraceEvent]:
    assert isinstance(context, OverlapDetectionContext)

    if event["ph"] in "X":
        context.collect_tid_space(event)
    return [event]


# mapping function callback
def detect_partial_overlap_events(event: TraceEvent, context: AbstractContext) -> list[TraceEvent]:
    assert isinstance(context, OverlapDetectionContext)

    if event["ph"] in "X":
        return context.overlap_detection(event)
    else:
        return [event]


###################################################################
# Timestamps sequence checking to make sure time stamps stay sorted
class TSSequenceContext(AbstractHashQueueContext):
    def __init__(self, ts3check: bool = False):
        super().__init__()
        self.ts_cmpt_end = {}
        self.ts_outsync = (0, 0)
        self.ts_total = 0
        self.ts_check = ts3check

    def __del__(self):
        if self.ts_outsync[1] > 0:
            aiulog.log(aiulog.WARN,
                       "TS_SEQUENCE: detected cycles overlapping (TS3[n] < TS4[n-1])"
                       " between cmpt_exec events within the same PID ", self.ts_outsync[0],
                       "/", self.ts_total,
                       "max overlap cycles: ", self.ts_outsync[1])

    def insert(self, event: TraceEvent, queue_id=None):
        if not queue_id:
            queue_id = self.event_data_hash(event, ["pid", "tid"], ignore_missing=True)

        if queue_id not in self.queues:
            self.queues[queue_id] = (-1.99, 1e99)

        if self.queues[queue_id][0] > event['ts']:
            aiulog.log(aiulog.ERROR, "Events out of order:", self.queues[queue_id], ">", event['ts'])

        if self.queues[queue_id][0] == event['ts'] and 'dur' in event:
            if self.queues[queue_id][1] < event['dur']:
                aiulog.log(aiulog.ERROR, "Secondary key (dur) out of order",
                           self.queues[queue_id], "vs.", event['ts'], event['dur'])

        self.queues[queue_id] = (event['ts'], event['dur'])
        return queue_id

    def ts3insert(self, event: TraceEvent, queue_id=None):
        if "Cmpt Exec" not in event["name"]:
            return

        self.ts_total += 1
        if not queue_id:
            queue_id = event["pid"]

        if queue_id not in self.ts_cmpt_end:
            self.ts_cmpt_end[queue_id] = (0.0, 0)

        try:
            last_ts = self.ts_cmpt_end[queue_id]
            if last_ts[0] < event["ts"] and int(event["args"]["TS3"]) < last_ts[1]:
                self.ts_outsync = (self.ts_outsync[0]+1, max(self.ts_outsync[1],
                                                             last_ts[1] - int(event["args"]["TS3"])))
            self.ts_cmpt_end[queue_id] = (event["ts"], int(event["args"]["TS4"]))
        except:  # noqa: E722
            print(self.ts_cmpt_end[queue_id], event)
            raise


def assert_ts_sequence(event: TraceEvent, context: AbstractContext) -> list[TraceEvent]:
    assert isinstance(context, TSSequenceContext)

    if event["ph"] in "Xbe":
        context.insert(event)
        if context.ts_check:
            context.ts3insert(event)
    return [event]


def assert_global_ts_sequence(event: TraceEvent, context: AbstractContext) -> list[TraceEvent]:
    assert isinstance(context, TSSequenceContext)

    if event["ph"] in "Xbe":
        context.insert(event, queue_id=1)
    return [event]


def recombine_cpu_events(event: TraceEvent, context: AbstractContext, config: dict) -> list[TraceEvent]:
    try:
        if GlobalIngestData.get_dialect(event["args"]["jobhash"]).get("NAME") != "FLEX":
            return [event]
    except KeyError:
        return [event]

    if event["ph"] in "X" and not PipelineContextTool.is_acc_event(event) and "AIU Roundtrip" not in event["name"]:
        fixed_tid = config.get("cpu_stream_tid", 1000)  # extract the new tid from config
        event["tid"] = fixed_tid
    return [event]


###################################################################
# Overlap checks for verification mode
class OverlapVerificationContext(OverlapDetectionContext, AbstractVerificationContext):
    '''
    Verification-mode variant of the overlap detection: it only reports overlapping compute
    events, it never resolves them (always OVERLAP_RESOLVE_WARN). Every detected overlap is
    recorded as an instance of an error-level finding and the accumulated findings are emitted
    as verification meta-data events by AbstractVerificationContext.drain() (reached via the MRO,
    the parent drain chains up to it; no two-phase barrier required for this context).

    Compute streams are identified per dialect: FLEX events have a single stream per pid, while
    TORCH events separate the streams within a pid by their 'args.stream' entry.
    '''
    test_name = "Compute Overlap Check"

    _OVERLAP_WARNING = "overlaps"
    _STREAM_KEY = "stream"
    _STREAM_ARG = "args." + _STREAM_KEY
    _DEFAULT_STREAM = 0  # convention: actual stream numbers start at 1, 0 means 'no stream entry'

    def __init__(self, ts_shift_threshold=0, max_tid_streams=5) -> None:
        super().__init__(self.OVERLAP_RESOLVE_WARN, ts_shift_threshold, max_tid_streams)
        # replace the resolution-oriented warning of the parent: in verification mode nothing is
        # resolved, a detected overlap is a finding that has to fail the test
        self.add_warning(
            TraceWarning(
                name=self._OVERLAP_WARNING,
                text="Overlapping compute events detected: {d[count]}",
                data={"count": 0},
                is_error=True,
            )
        )

    def _select_queue_id_keys(self, event: TraceEvent) -> list[str]:
        dialect = PipelineContextTool.get_dialect_of_event(event)
        assert dialect is not None, \
            "OVL: cannot determine the dialect of the first event." \
            " Register this stage before any stage that removes the jobhash."
        if dialect.get("NAME") == "TORCH":
            return ["pid", self._STREAM_ARG]
        return ["pid"]

    def _record_overlap(self, oevent: TraceEvent) -> None:
        super()._record_overlap(oevent)
        self.warnings[self._OVERLAP_WARNING].add_instance({
            "name": oevent["name"],
            "pid": oevent["pid"],
            "tid": oevent["tid"],
            "stream": oevent["args"].get(self._STREAM_KEY),
            "ts": oevent["ts"],
            "dur": oevent["dur"],
        })

    def add_default_stream(self, event: TraceEvent) -> bool:
        '''
        the stream-based separation of queues requires the stream entry to exist. Events without
        one are all attributed to the same default stream. Returns whether a default was added.
        '''
        if self._STREAM_KEY in event["args"]:
            return False
        event["args"][self._STREAM_KEY] = self._DEFAULT_STREAM
        return True

    def remove_default_stream(self, event: TraceEvent) -> None:
        del event["args"][self._STREAM_KEY]


def verify_kernel_overlap(event: TraceEvent, context: AbstractContext) -> list[TraceEvent]:
    assert isinstance(context, OverlapVerificationContext)

    # only compute events are checked; anything else just passes through
    if event["ph"] not in "X" or not PipelineContextTool.is_acc_event(event):
        return [event]

    # the default is only required to determine the queue/stream of this event: drop it again to
    # keep the event unchanged for any downstream stage
    default_added = context.add_default_stream(event)
    revents = context.overlap_detection(event)
    if default_added:
        context.remove_default_stream(event)
    return revents
