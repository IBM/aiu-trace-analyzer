# Copyright 2024-2025 IBM Corporation

import copy

from aiu_trace_analyzer.types import TraceEvent
from aiu_trace_analyzer.pipeline import AbstractContext, EventPairDetectionContext
import aiu_trace_analyzer.logger as aiulog


class MpTsCalibV2Context(EventPairDetectionContext):
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

    Process stages:
        copy-in:    read in all events into self.all_events
                    organize collective-events along proc-id and coll-groups
        drain:
            - calibration (~ calibration stage): read wiki page
            - host-ts alteration:
                    adjust host-ts of all device-involving events.
                    leave host-ts of host-only events unchanged.

    Output:
        coded in drain with multiple steps:
        1. calculate a calibration timestamp adjustment value per rank
        2. adjust the timestamp for all events with the calibration
        3. dump all_events[] through drain func.
    '''

    def __init__(self, window_size=None) -> None:
        super().__init__()
        self.SYNC_TO_DEV = True

        self.all_events = []      # gather all events into a buffer

        self.proc_ids = {}      # index variables for loop-nest to traverse collective events (organized in 2D)
        self.coll_groups = []

        self.ref_dts_per_pid = {}   # variables produced at 'calibration stage' and used in 'drain stage'
        self.dilation_factor = {}

    def _get_ts5_in_us(self, event: TraceEvent):
        if "ts_dev" in event["args"]:
            return event["args"]["ts_dev"][4]
        return int(event["args"]["TS5"]) / 560

    def mp_gather_events(self, event: TraceEvent) -> list[TraceEvent]:

        self.all_events.append(copy.deepcopy(event))
        idx = len(self.all_events) - 1

        # assuming the collective-call groups across ranks are same
        if (event["ph"] in ["X", "b"] and
            "args" in event and
            "CollGroup" in event["args"] and
            "AllReduce" in event["args"]["CollGroup"]):   # noqa: E129

            aiulog.log(aiulog.TRACE, "MP_SYNC 3: ", event)
            pid = event["pid"]
            cg_name = event["args"]["CollGroup"]

            self.proc_ids[pid] = 1

            # keep coll-groups in order with the traces from rank-0
            if pid == 0 and cg_name not in self.coll_groups:
                self.coll_groups.append(cg_name)

            queue_id = self.queue_hash(pid, cg_name)
            aiulog.log(aiulog.TRACE, "MP_SYNC 3: ", queue_id)

            if queue_id in self.queues:
                self.queues[queue_id].append(idx)
            else:
                self.queues[queue_id] = [idx]

        return []

    # dts: device time stamp
    # hts: host time stamp
    def mp_calibrate_dts(self) -> None:

        '''
        Scan the stored events in collective-call-group (with arg CollGroup) and
        compute the minimal and maximal timestamps for each group from each rank.

        Ack:  experimentation stage
        TODO: use numpy
        '''

        # <min, max> dev-ts ranges for each coll-group on each rank/proc, <min> is not in effect in mp_sync_v2
        dts_ranges_per_pid_cg = {}
        # ref_hts, reference host-ts for host-ts dev-ts calibration
        ref_hts_per_pid_cg = {}
        for pid in self.proc_ids:
            for cg_name in self.coll_groups:

                queue_id = self.queue_hash(pid, cg_name)
                cg_events = self.queues[queue_id]
                timestamps = []
                for event_idx in cg_events:

                    assert event_idx >= 0 and event_idx < len(self.all_events)
                    event = self.all_events[event_idx]
                    assert ("ts" in event)

                    # It has shown that TS5 from Firmware side is more reliable than the TS captured on the host side.
                    if "TS5" in event["args"]:
                        t5_event = self._get_ts5_in_us(event)
                        ts_end_host = event["ts"] + event["dur"]
                        timestamps.append((t5_event, ts_end_host))
                        aiulog.log(aiulog.DEBUG, "MP_SYNC_v2: different gaps: ",
                                   pid, event["ts"], event["dur"], t5_event, timestamps, cg_name, event["name"])

                timestamps.sort(key=lambda x: x[0])     # sort incremental of dev-ts
                aiulog.log(aiulog.DEBUG, "MP_SYNC_v2: ts sorted: ", pid, timestamps, cg_name)

                assert len(timestamps) > 0
                dts_ranges_per_pid_cg[queue_id] = (timestamps[0][0], timestamps[-1][0])
                ref_hts_per_pid_cg[queue_id] = (timestamps[-1][1])

        max_dts_per_pid = {}    # max dev-ts from all collective-related ops, one per rank/proc
        for pid in self.proc_ids:
            max_dts_per_pid[pid] = []
            for cg_name in self.coll_groups:
                ts_range = dts_ranges_per_pid_cg[self.queue_hash(pid, cg_name)]
                assert len(ts_range) == 2
                max_dts_per_pid[pid].append((ts_range[-1], cg_name, ts_range[-1]-ts_range[0]))
        aiulog.log(aiulog.TRACE, "mp_gather_events max_dts_per_pid: ", max_dts_per_pid)

        # Compute the mean-diff (mse) of max_TS5 across all ranks, for each collective group
        # Formula: sum<P = 0..NP> ( max_ts5[P] - max_ts5[0] ) / NP
        # Legend:  P: rank/proc-id; NP: the number of ranks/procs
        NP = len(self.proc_ids)
        list_mse = []
        for cg_idx, cg_name in enumerate(self.coll_groups):
            mse = 0
            for pid in self.proc_ids:
                mse += (max_dts_per_pid[pid][cg_idx][0] - max_dts_per_pid[0][cg_idx][0])
            mse /= NP
            list_mse.append((cg_idx, cg_name, mse))
        list_mse.sort(key=lambda x: x[2])

        # Coll-group with the minimum MSE value is chosen as the reference coll-group, wherein the max_TS5
        # from all ranks/procs are deemed to be the same as rank0/proc0, as the center of cross-dev
        # clock calibration.
        self.ref_dts_cg = list_mse[0][1]

        self.ref_dts_cg_idx = list_mse[0][0]
        for pid in self.proc_ids:
            self.ref_dts_per_pid[pid] = max_dts_per_pid[pid][self.ref_dts_cg_idx][0]

        # Used to adjust the host-ts of device-events on all ranks/procs.
        ref_hts_p0 = ref_hts_per_pid_cg[self.queue_hash(0, self.ref_dts_cg)]
        self.dts_hts_calib_ref = ref_hts_p0 - self.ref_dts_per_pid[0]

        # Compute a dilation_factor per rank/proc (clock drifting, either expanding or shrinking)
        # The shifting factor on a rank/proc is calculated from: max_TS5[last_cg] - max_TS5[first_cg]
        delta_t5_last_cg_to_first_cg = {}
        for pid in self.proc_ids:
            # t5[cg_last] - t5[cg_0], presenting a meaningful range to sync clock dilation across devices
            list_dts = [a[0] for a in max_dts_per_pid[pid]]
            list_dts.sort()
            delta_t5_last_cg_to_first_cg[pid] = list_dts[-1] - list_dts[0]
            aiulog.log(aiulog.INFO, "mp_gather_events delta_t5: ", list_dts)
        for pid in self.proc_ids:
            self.dilation_factor[pid] = delta_t5_last_cg_to_first_cg[pid] / delta_t5_last_cg_to_first_cg[0]

        aiulog.log(aiulog.INFO, "MP_SYNC_V2 delta_t5, dilation factor:   ", self.dilation_factor)
        aiulog.log(aiulog.INFO, "MP_SYNC_V2 delta_t5, ref_dts_cg:        ", self.ref_dts_cg)
        aiulog.log(aiulog.INFO, "MP_SYNC_V2 delta_t5, ref_hts_p0:        ", ref_hts_p0)
        aiulog.log(aiulog.INFO, "MP_SYNC_V2 delta_t5, ref_dts_per_pid:   ", self.ref_dts_per_pid)
        aiulog.log(aiulog.INFO, "MP_SYNC_V2 delta_t5, dts_hts_calib_ref: ", self.dts_hts_calib_ref)

    # Calibration calculation:
    #   t_p1_syncd_to_p0 = ( t_p1 - t5_ref_p1 ) / D_p1 + t5_ref_p0
    #
    #   t_p1:                any device-time-stamp read-out from device p1
    #   t5_ref_p0/t5_ref_p1: TS calibration reference point, these 2 device-time-stamp read-outs are the same moment
    #   D_p1:                clock dilation factor, (t5_cgN_p1 - t5_cg0_p1) / (t5_cgN_p0 - t5_cg0_p0)
    #   t_p1_syncd_to_p0:    new device-ts-stamp after calibration
    #
    #   Derivation: ( t_p1_syncd_to_p0 - t5_ref_p0 )
    #                             / ( t_p1 - t5_ref_p1 ) = D_p1 = (t5_cgN_p1 - t5_cg0_p1) / (t5_cgN_p0 - t5_cg0_p0)

    # Compute the calibrated dev-ts
    def _get_calib_dts(self, t_p1, pid):
        t5_ref_p0 = self.ref_dts_per_pid[0]
        t5_ref_p1 = self.ref_dts_per_pid[pid]
        D_p1 = self.dilation_factor[pid]

        t_p1_syncd_to_p0 = (t_p1 - t5_ref_p1) / D_p1 + t5_ref_p0
        return t_p1_syncd_to_p0

    # Adjust host-ts from dev-ts
    def _get_calib_hts_from_dts(self, dev_ts):
        return dev_ts + self.dts_hts_calib_ref

    def mp_alter_event_hts(self) -> None:
        for e in self.all_events:
            pid = e["pid"]
            if "TS5" in e["args"]:
                dev_t5 = self._get_calib_dts(self._get_ts5_in_us(e), pid)
                e["ts"] = self._get_calib_hts_from_dts(dev_t5)

                if "dur" in e:
                    e["ts"] -= e["dur"]

    def drain(self) -> list[TraceEvent]:
        aiulog.log(aiulog.TRACE, "mp_gather_events drain: ")
        revents = super().drain()

        self.mp_calibrate_dts()
        self.mp_alter_event_hts()

        while len(self.all_events) > 0:
            e = self.all_events.pop()
            aiulog.log(aiulog.TRACE, "mp_gather_events drain: ", e)
            # if "TS5" in e["args"]:
            revents += [e]
        aiulog.log(aiulog.TRACE, "mp_gather_events drain: ", revents)
        return revents


def mp_ts_calibration_v2(event: TraceEvent, context: AbstractContext) -> list[TraceEvent]:
    '''
    Gather the collective events for post processing calibration
    '''
    assert isinstance(context, MpTsCalibV2Context)

    # hook function is to gather events, computation happens in drain
    return context.mp_gather_events(event)
