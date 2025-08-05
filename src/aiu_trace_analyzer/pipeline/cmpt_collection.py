# Copyright 2024-2025 IBM Corporation

import copy
import re

import aiu_trace_analyzer.logger as aiulog
from aiu_trace_analyzer.types import TraceEvent, GlobalIngestData
from aiu_trace_analyzer.pipeline import AbstractContext, AbstractHashQueueContext


class QueueingCounterContext(AbstractHashQueueContext):
    def __init__(self) -> None:
        super().__init__()

    def create_counter(self, event: TraceEvent) -> list[TraceEvent]:
        qid = event["pid"]
        if qid not in self.queues:
            self.queues[qid] = []

        event_start = event["ts"]
        event_end = event["ts"]+event["dur"]

        aiulog.log(aiulog.TRACE, "QCC: adding event: ", (event_start, event_end))
        # update all queue entries based on the event timestamps
        ready, self.queues[qid] = self.update_queues(event_start, event_end, qid)

        return self.make_events(ready, event["pid"])

    def update_queues(self, s: float, e: float, qid) -> tuple[list[tuple[float, int]], list[tuple[float, int]]]:
        ts_list = self.queues[qid]

        # per process keep a list of tuples (ts, #overlap)
        #  * gets bumped +1 at event start s and
        #  * gets dropped -1 at event end e
        if len(ts_list) == 0:
            return [], [(s, 1), (e, 0)]

        # assuming sorted inbound events, any entry starting before the current can be used to generate a counter
        ready_list = list(filter(lambda x: x[0] < s, ts_list))
        # remember the last entry of the ready list as it impacts the count for a new s
        last_ready = ready_list[-1][1] if len(ready_list) else 0
        aiulog.log(aiulog.TRACE, "QCC: ready, s/e", ready_list, s, e)

        # the rest depends on whether the timestamps are matching anything existing
        new_list = []
        # anything that overlaps with current event interval
        mid_se = list(filter(lambda x: s <= x[0] and x[0] < e, ts_list))
        # anything that's past the current event interval
        post_e = list(filter(lambda x: e <= x[0], ts_list))
        # remember the last entry of the overlap section as it impacts the count for a new e
        last_overlap = mid_se[-1][1] if len(mid_se) else 0

        if len(mid_se) == 0:
            new_list.append((s, last_ready + 1))
        else:
            if s < mid_se[0][0]:
                new_list.append((s, last_ready + 1))

        # walk the overlap-list and bump the counters
        for t, c in mid_se:
            aiulog.log(aiulog.TRACE, "QCC: OverlapBump Mid_se t,c:", t, c)
            new_list.append((t, c + 1))

        if len(post_e) == 0:
            new_list.append((e, last_overlap))
        else:
            if e < post_e[0][0]:
                new_list.append((e, last_overlap))

        new_list += post_e
        aiulog.log(aiulog.TRACE, "QCC: new_list", new_list)
        return ready_list, new_list

    def make_events(self, ready: list[tuple[float, int]], pid) -> list[TraceEvent]:
        revents = []
        for t, c in ready:
            counter = {
                "ph": "C",
                "ts": t,
                "pid": pid,
                "name": "ConcurrentPreps",
                "cat": "Pending Prep Events",
                "args": {"Concurrency": c},
            }
            revents.append(copy.deepcopy(counter))
        return revents

    def drain(self) -> list[TraceEvent]:
        revents = []
        while len(self.queues):
            pid, q = self.queues.popitem()
            revents += self.make_events(q, pid)
        return revents


def queueing_counter(event: TraceEvent, queue_coll: AbstractContext, keyval: dict) -> list[TraceEvent]:
    revents = [event]
    keep_prep = keyval.get("keep_prep", False)

    if event["ph"] in "X":
        dialect = GlobalIngestData.get_dialect(event["args"]["jobhash"])
        if re.search(dialect.get("acc_compute_prep"), event["name"]) is None:
            return revents

        if keep_prep:
            revents += queue_coll.create_counter(event)
        else:
            revents = queue_coll.create_counter(event)

    return revents
