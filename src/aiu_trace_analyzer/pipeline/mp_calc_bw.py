# Copyright 2024-2025 IBM Corporation

import copy

from aiu_trace_analyzer.types import TraceEvent
from aiu_trace_analyzer.pipeline import AbstractContext, AbstractHashQueueContext
import aiu_trace_analyzer.logger as aiulog


class MpCalcBwContext(AbstractHashQueueContext):
    '''
    '''

    def __init__(self) -> None:
        super().__init__()
        self.all_events = []             # gather all events into a buffer
        self.all_events_w_ts_end = []    # list of (tsEnd, event_index_into_all_events)
        self.proc_ids = {}               # for compute NP to derive total communication amount: 2*(NP-1)*msgSize
        self.cnt = 0

    def mp_gather_events(self, event: TraceEvent) -> list[TraceEvent]:

        self.all_events.append(copy.deepcopy(event))
        # self.all_events.append(event)
        self.cnt += 1
        idx = len(self.all_events) - 1

        if event["ph"] in ["X", "b"]:
            ts_end = event["ts"] + event["dur"]
            self.all_events_w_ts_end.append((ts_end, idx))

        self.proc_ids[event["pid"]] = 1

        return []

    def _gen_bw_counter_events(self, t_beg, t_end, reduce_bw, coll_bw):

        bw_pid = -1
        c_events = []
        for n in ["BW allreduce", "BW all-pcie"]:
            e = {
                "ph": "C",
                "pid": bw_pid,
                "ts": t_beg,
                "cat": "",
                "name": n,
                "args": {"Unit GBps": reduce_bw if "reduce" in n else coll_bw}
            }
            c_events.append(e)
            e = {
                "ph": "C",
                "pid": bw_pid,
                "ts": t_end,
                "cat": "",
                "name": n,
                "args": {"Unit GBps": 0}
            }
            c_events.append(e)

        return c_events

    def calc_bw(self):

        # all events are sorted by the tsEnd (ts + dur)
        self.all_events_w_ts_end.sort(key=lambda x: x[0])
        NP = len(self.proc_ids)

        in_coll_gr = False
        got_bytes = False
        num_bytes = 0
        recv_count = 0
        for i, (_ts_end, e_idx) in enumerate(self.all_events_w_ts_end):
            e = self.all_events[e_idx]

            # Sweeping events in a coll group, take 2 actions:
            # (1) log the _ts_end of the event right before the first event in the coll group
            # (2) log the message size from any send event
            if (e["ph"] in ["X", "b"]
                and "args" in e
                and "CollGroup" in e["args"]
                and "AllReduce_all_reduce_" in e["args"]["CollGroup"]):  # noqa: E129

                if not in_coll_gr:
                    t_bw_beg = self.all_events_w_ts_end[i-1][0]
                    in_coll_gr = True

                # assuming same message size for all Send event (let's say that a Multicast-send has multiple sends)
                if not got_bytes and "SenRdmaSend" in e["name"] and "Bytes" in e["args"]:
                    num_bytes = int(e["args"]["Bytes"])
                    got_bytes = True

                if "SenRdmaRecv" in e["name"]:
                    recv_count += 1

            # after left a coll group, take 2 actions:
            # (1) log the tsEnd of the last event in the coll group
            # (2) generate counter events for "BW allreduce" and "BW all-pcie"
            elif (in_coll_gr and got_bytes and recv_count > NP - 1
                  and e["ph"] in ["X", "b"] and "args" in e and "CollGroup" not in e["args"]
                  and " Cmpt Exec" in e["name"]):
                t_bw_end = self.all_events_w_ts_end[i - 1][0]

                # BW unit is GB/s, Timing unit is milli-sec
                reduce_bw = round(num_bytes / (t_bw_end - t_bw_beg) / 1000, 3)
                coll_bw = 2 * (NP - 1) * reduce_bw

                counter_events = self._gen_bw_counter_events(t_bw_beg, t_bw_end, reduce_bw, coll_bw)
                self.all_events.extend(counter_events)

                in_coll_gr = False
                got_bytes = False
                num_bytes = 0
                recv_count = 0

            elif (in_coll_gr and got_bytes
                  and "args" in e
                  and "CollGroup" in e["args"]
                  and "AllGather" in e["args"]["CollGroup"]):
                break

    def drain(self) -> list[TraceEvent]:
        aiulog.log(aiulog.TRACE, "mp_gather_events drain: ")
        revents = super().drain()

        self.calc_bw()

        while len(self.all_events) > 0:
            e = self.all_events.pop()
            revents += [e]

        revents.sort(key=lambda x: x["ts"])
        return revents


def mp_calc_bw(event: TraceEvent, context: AbstractContext) -> list[TraceEvent]:
    '''
    Gather the collective events for post processing calibration
    '''
    assert isinstance(context, MpCalcBwContext)

    # hook function is to gather events, computation happens in drain
    return context.mp_gather_events(event)
