# Copyright 2024-2025 IBM Corporation

import copy

from aiu_trace_analyzer.types import TraceEvent
from aiu_trace_analyzer.pipeline import AbstractContext, EventPairDetectionContext
import aiu_trace_analyzer.logger as aiulog


class MpTsCalibContext(EventPairDetectionContext):
    '''
    Multiple process time stamp calibration

    Input:
        events from multiple processes, with some regions has "CollGroup" arguments
        in the event to indicating participation of AllReduce (other collective) calls

    States/Contexts:
        == These are produced from the preprocess hook func: ==
        all_events: keep all events (inside or outside of collective groups).
        queues:     hashed with (pid, coll_group_name), each entry stores indices to
                    events in a CollGroup from a rank.
        proc_ids:   keeps values of ranks/proc_id
        coll_groups: keeps CollGroup names (assuming names are the same across ranks
                    for a given group)

        ==
        ts_calibs:  timestampe adjustment value per rank/proc, produced by mp_calibrate_ts
        ts_calibs_host_device: derived timestamp adjustments per PID for events without TS5 readings.

    Output:
        coded in drain with multiple steps:
        1. calculate a calibration timestamp adjustment value per rank
        2. adjust the timestamp for all events with the calibration
        3. dump all_events[] through drain func.
    '''

    def __init__(self, window_size=None) -> None:
        super().__init__()
        self.all_events = []    # gather all events into a buffer
        self.proc_ids = {}
        self.coll_groups = {}
        self.ts_calibs = {}
        self.ts_calibs_host_device = {}
        self.SYNC_TO_DEV = True

    def mp_gather_events(self, event: TraceEvent) -> list[TraceEvent]:

        self.all_events.append(copy.deepcopy(event))
        idx = len(self.all_events) - 1

        if event["ph"] in ["X", "b"] and "args" in event and "CollGroup" in event["args"]:
            aiulog.log(aiulog.TRACE, "mp_gather_events 3: ", event)
            pid = event["pid"]
            cg_name = event["args"]["CollGroup"]

            self.proc_ids[pid] = 1
            self.coll_groups[cg_name] = 1

            queue_id = self.queue_hash(pid, cg_name)
            aiulog.log(aiulog.TRACE, "mp_gather_events 3: ", queue_id)
            if queue_id in self.queues:
                self.queues[queue_id].append(idx)
            else:
                self.queues[queue_id] = [idx]

        return []

    def mp_calibrate_ts(self) -> None:

        '''
        Scan the stored events in collective-call-group (with arg CollGroup) and
        compute the minimal and maximal timestamps for each group from each rank.

        Ack:  experimentation stage
        TODO: use numpy
        '''
        ts_ranges = {}
        ts_delta_host_device = {}

        for pid in self.proc_ids:
            for cg_name in self.coll_groups:

                queue_id = self.queue_hash(pid, cg_name)
                cg_events = self.queues[queue_id]

                timestamps = []
                ts_delta_host_device[queue_id] = 10**14

                for event_idx in cg_events:

                    assert event_idx >= 0 and event_idx < len(self.all_events)
                    event = self.all_events[event_idx]
                    assert "ts" in event

                    # It has shown that TS5 from Firmware side is more reliable than the TS captured on the host side.
                    if "TS5" in event["args"]:
                        t5_event = int(event["args"]["ts_dev"][4]) \
                            if self.SYNC_TO_DEV else int(event["args"]["ts_all"][4])
                        timestamps.append(t5_event)
                        ts_delta_host_device[queue_id] = min(event["ts"] - t5_event, ts_delta_host_device[queue_id])
                        aiulog.log(aiulog.INFO,
                                   "MP_SYNC: different gaps: ",
                                   pid, event["ts"], t5_event,
                                   ts_delta_host_device[queue_id], timestamps, cg_name, event["name"])

                timestamps.sort()
                aiulog.log(aiulog.INFO, "MP_SYNC: ts sorted: ", pid, timestamps, cg_name)

                assert len(timestamps) > 0
                ts_ranges[queue_id] = (timestamps[0], timestamps[-1])

        '''
        Simple algorithm:
        1. compute |T_1_a - T_0_a|, |T_1_b - T_0_b|, .... |T_1_k - T_0_k|, with max_ts for collective-group a-k
        2. minimal value of this list is the calibration delta.
        '''
        max_stamps = {}
        for pid in self.proc_ids:
            max_stamps[pid] = []
            for cg_name in self.coll_groups:
                ts_range = ts_ranges[self.queue_hash(pid, cg_name)]
                assert len(ts_range) == 2
                max_stamps[pid].append((ts_range[-1], cg_name, ts_range[-1]-ts_range[0]))

        aiulog.log(aiulog.TRACE, "mp_gather_events max_stamps: ", max_stamps)

        for pid in self.proc_ids:
            assert 0 in max_stamps and pid in max_stamps
            # a/b[0:2],
            #   0: maximal-TS5 of all events in a given collection-call group
            #   1: name of the callGroup
            #   2: delta( min_TS5, max_TS5 ) of all events in a given collection-call group
            delta_stamps = [(b[0]-a[0], b[1], (b[0]-a[0])/a[2]) for a, b in zip(max_stamps[0], max_stamps[pid])]
            assert len(delta_stamps) > 0

            # The best reference collGroup for PID "pid" is the one with the smallest factor defined as:
            #   (TS_last[collGroup][pid] - TS_last[collGroup][0]) / TS_span[collGroup][0]
            delta_stamps.sort(key=lambda x: x[2])
            self.ts_calibs[pid] = delta_stamps[-1][0]

            aiulog.log(aiulog.TRACE, "mp_gather_events cross devices: ", self.ts_calibs)

            def get_ts_delta(pid, cg_name):
                _queue_id = self.queue_hash(pid, cg_name)
                return ts_delta_host_device[_queue_id]

            self.ts_calibs_host_device[pid] = get_ts_delta(pid, delta_stamps[0][1])
            aiulog.log(aiulog.INFO, "MP_SYNC: shift and name ", self.ts_calibs_host_device[pid], delta_stamps[0][1])

        aiulog.log(aiulog.TRACE, "mp_gather_events host   device: ", self.ts_calibs_host_device)

    def mp_alter_event_ts(self) -> None:

        for e in self.all_events:
            pid = e["pid"]
            if "TS5" in e["args"]:
                e["ts"] = e["args"]["ts_dev"][4] - self.ts_calibs[pid]
                if "dur" in e:
                    e["ts"] -= e["dur"]
            else:
                if "Flex RoundTrip" in e["name"]:
                    aiulog.log(aiulog.INFO, "MP_SYNC: new ts and shift 0 ", e["ts"], self.ts_calibs_host_device[pid])
                e["ts"] -= self.ts_calibs_host_device[pid]
                if "Flex RoundTrip" in e["name"]:
                    aiulog.log(aiulog.INFO, "MP_SYNC: new ts and shift 1 ", e["ts"], self.ts_calibs_host_device[pid])

    def drain(self) -> list[TraceEvent]:
        aiulog.log(aiulog.TRACE, "mp_gather_events drain: ")
        revents = super().drain()

        self.mp_calibrate_ts()
        self.mp_alter_event_ts()

        while len(self.all_events) > 0:
            e = self.all_events.pop()
            aiulog.log(aiulog.TRACE, "mp_gather_events drain: ", e)
            # if "TS5" in e["args"]:
            revents += [e]
        aiulog.log(aiulog.TRACE, "mp_gather_events drain: ", revents)
        return revents


def mp_ts_calibration(event: TraceEvent, context: AbstractContext) -> list[TraceEvent]:
    '''
    Gather the collective events for post processing calibration
    '''
    assert isinstance(context, MpTsCalibContext)

    # hook function is to gather events, computation happens in drain
    return context.mp_gather_events(event)
