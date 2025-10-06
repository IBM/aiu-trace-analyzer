# Copyright 2024-2025 IBM Corporation

from aiu_trace_analyzer.types import TraceEvent
from aiu_trace_analyzer.pipeline import AbstractContext, AbstractHashQueueContext
import aiu_trace_analyzer.logger as aiulog


class MpCalcBwV2Context(AbstractHashQueueContext):
    def __init__(self):
        super().__init__()
        self.coll_events = []
        self.all_events = []
        self.coll_groups = {}
        self.NP = 0
        # start/end timestamp and coll group name list to keep track of overlapping collectives
        self.prev_group = (0.0, 0.0, set())

    def _create_counter(self, ts: float, value: float) -> TraceEvent:
        return {
            "ph": "C",
            "pid": -1,
            "ts": ts,
            "cat": "",
            "name": "Coll_Bandwidth",
            "args": {"GBps": value}
        }

    def _gen_bw_counter_events(self, event, t_bw_beg, t_bw_end, coll_data_size, coll_group):
        opening_ts, closing_ts, open_groups = self.prev_group

        revents = [event]

        # check if the new counter event is past the end of the previous one and insert a zero event:
        if t_bw_beg > closing_ts and closing_ts > 0.0:
            revents.append(self._create_counter(closing_ts, 0.0))
            aiulog.log(aiulog.DEBUG, f'BW: new added zero value event: {revents[-1]}')
            open_groups.clear()    # no further open group at this point
            opening_ts = t_bw_beg  # reset the opening TS
            self.NP = 0            # reset NP for the next group to start

        # append the beginning counter event
        revents.append(self._create_counter(t_bw_beg,
                                            round(int(coll_data_size) / (float(t_bw_end) - float(t_bw_beg)) / 1000, 3)))
        # debug log
        aiulog.log(aiulog.DEBUG, f'BW: generated bw counter event: {revents[-1]}')
        aiulog.log(aiulog.DEBUG, f'BW: T_bw_end: {t_bw_end}; T_bw duration: {t_bw_end-t_bw_beg}')
        aiulog.log(aiulog.DEBUG, f'BW: Coll_data_size: {coll_data_size}')

        # update the coll_group overlap tracking
        open_groups.add(coll_group)
        self.prev_group = (opening_ts, self.coll_groups[coll_group]['T_bw_end'], open_groups)
        self.coll_groups.pop(coll_group)
        return revents

    def drain(self) -> list[TraceEvent]:
        _, closing_ts, open_groups = self.prev_group
        if len(open_groups) >= 1:
            if len(open_groups) > 1:
                aiulog.log(aiulog.WARN,
                           "BW: More than one collective group without close."
                           " Could be caused by processing subset of events.")
            return [self._create_counter(closing_ts, 0.0)]
        return []

    def insert(self, event) -> list[TraceEvent]:
        # a sanity check test to make sure the incoming events are ordered
        assert event["ts"] >= self.prev_group[0], "OUT OF ORDER COLLECTIVE EVENT." \
            " Detected event with ts before the previous collective start. Cannot continue."

        coll_group = event["args"]["CollGroup"]

        # as long as there's no overlap or overlap happens with a different collective group, we go and compute bw:
        if event["ts"] >= self.prev_group[1] or coll_group not in self.prev_group[2]:
            group_size = len(event["args"]["peers"].strip('{}').split(','))

            if event["ts"] < self.prev_group[1]:
                aiulog.log(aiulog.WARN,
                           "CBW: Concurrent collective ops detected."
                           " BW counters will be wrong (BW of latest collective is shown).")

            # add event-data to bw-computation
            if self.NP != 0 and self.NP != group_size:
                # number of processes for all events are not consistent
                aiulog.log(aiulog.ERROR,
                           "Events with different numbers of processes: previous NP ", self.NP,
                           "not equal to current NP ", group_size)
                raise Exception("EVENT with DIFFERENT NUM of PROCESSES DETECTED.")
            if coll_group not in self.coll_groups:
                # first time to initiate currrent coll_group info
                self.coll_groups[coll_group] = {
                    'T_bw_beg': event["ts"],
                    'T_bw_end': event["ts"] + event["dur"],
                    'Coll_data_size': int(event["args"]["Coll_data_size"]),
                    'Procs': 1,
                }
                self.NP = group_size
                aiulog.log(aiulog.DEBUG,
                           f'BW: initialize group {self.coll_groups[coll_group]} with current event: {event}')
            else:
                # not first time to collect info for this coll group
                if self.coll_groups[coll_group]['Procs'] < self.NP:
                    self.coll_groups[coll_group]['T_bw_beg'] = min(self.coll_groups[coll_group]['T_bw_beg'],
                                                                   event["ts"])
                    self.coll_groups[coll_group]['T_bw_end'] = max(self.coll_groups[coll_group]['T_bw_end'],
                                                                   event["ts"] + event["dur"])
                    self.coll_groups[coll_group]['Procs'] += 1
                    # for debug
                    t_bw_beg_tmp = self.coll_groups[coll_group]['T_bw_beg']
                    t_bw_end_tmp = self.coll_groups[coll_group]['T_bw_end']
                    procs = self.coll_groups[coll_group]['Procs']
                    aiulog.log(aiulog.DEBUG,
                               f'BW: current event with procs {procs} < {self.NP} NP: {event}')
                    aiulog.log(aiulog.DEBUG,
                               f'BW: T_bw_beg : {t_bw_beg_tmp}; T_bw_end: {t_bw_end_tmp};'
                               f' T_bw duration: {t_bw_end_tmp-t_bw_beg_tmp}')

                    if self.coll_groups[coll_group]['Procs'] == self.NP:
                        return self._gen_bw_counter_events(event,
                                                           self.coll_groups[coll_group]['T_bw_beg'],
                                                           self.coll_groups[coll_group]['T_bw_end'],
                                                           self.coll_groups[coll_group]['Coll_data_size'],
                                                           coll_group)
        else:
            # Collective Events are out of order, so nothing can be processed.
            aiulog.log(aiulog.ERROR,
                       "Encountered overlapping collective groups with the same name:",
                       self.prev_group[1], ">", event["ts"])
            raise ValueError("OVERLAPPING COLLECTIVE GROUP.")
        return [event]


def mp_calc_bw_v2(event: TraceEvent, context: AbstractContext) -> list[TraceEvent]:
    assert isinstance(context, MpCalcBwV2Context)
    if event['ph'] == "X" \
       and "args" in event \
       and "AllReduce_all_reduce" in event["name"] \
       and "Coll_data_size" in event["args"]:
        returned_events = context.insert(event)
        return returned_events
    else:
        return [event]
