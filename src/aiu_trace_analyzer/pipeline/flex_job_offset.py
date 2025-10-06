# Copyright 2024-2025 IBM Corporation

from math import isclose

import aiu_trace_analyzer.logger as aiulog
from aiu_trace_analyzer.types import TraceEvent
from aiu_trace_analyzer.pipeline import AbstractContext
from aiu_trace_analyzer.pipeline.barrier import TwoPhaseWithBarrierContext


def _is_cpu_event(event: TraceEvent) -> bool:
    return not ("args" in event and "TS1" in event["args"])


class FlexJobStats():

    def __init__(self, jobname: str, jobhash: int):
        # initialize min-max times
        self.jobname = jobname
        self.jobhash = jobhash
        self.cpu = (1.0e99, 0)
        self.aiu = (1.0e99, 0)
        self.first_cycl = {"ts": 1.0e99, "cycle": 0}
        self.last_cycl = {"ts": 0, "cycle": 0}
        self.job_offset = 0.0

    def update(self, event: TraceEvent) -> None:
        is_cpu_event = _is_cpu_event(event)
        if is_cpu_event and event["name"] not in ["AIU Roundtrip"]:
            return

        ts_b = event["ts"]
        ts_e = event["ts"]+event["dur"]
        cycl_b = int(event["args"]["TS1"]) if not is_cpu_event else None
        cycl_e = int(event["args"]["TS5"]) if not is_cpu_event else None
        if is_cpu_event:
            self.cpu = (min(self.cpu[0], ts_b), max(self.cpu[1], ts_e))
        else:
            self.aiu = (min(self.aiu[0], ts_b), max(self.cpu[1], ts_e))
        if cycl_b is not None and cycl_e is not None:
            if cycl_e < cycl_b:
                cycl_b += (1 << 32)
            if ts_b < self.first_cycl["ts"]:
                self.first_cycl = {"ts": ts_b, "cycl": cycl_b}
            if ts_e > self.last_cycl["ts"]:
                self.last_cycl = {"ts": ts_e, "cycl": cycl_e}

    def compute_frequency(self) -> float:
        # print(self.jobhash, self.cpu, self.aiu, self.first_cycl, self.last_cycl, self.job_offset)
        return 0


class FlexJobOffsetContext(TwoPhaseWithBarrierContext):
    def __init__(self, soc_frequency: float) -> None:
        super().__init__()
        self.soc_frequency = soc_frequency

    def collect(self, event: TraceEvent) -> None:
        jid = hash(event["args"]["jobhash"])
        if jid not in self.queues:
            self.queues[jid] = FlexJobStats(event["args"]["jobname"], event["args"]["jobhash"])

        self.queues[jid].update(event)

    def detect_cpu_aiu_range_mismatch(self) -> bool:
        valid = True
        for jid, jdata in self.queues.items():
            range_match = (jdata.cpu[0] <= jdata.aiu[0]) and (jdata.cpu[1] >= jdata.aiu[1])

            if not range_match:
                jdata.job_offset = jdata.cpu[0] - jdata.aiu[0]
                if (jdata.cpu[1] - jdata.cpu[0] < jdata.aiu[1] - jdata.aiu[0]):
                    aiulog.log(aiulog.WARN,
                               f"FREQ: aiu event range of {jdata.jobname} is wider than cpu range."
                               " Will not attempt to adjust timestamps.")
                    jdata.job_offset = None
            if not range_match and valid:  # remember the first job that fails the range test
                self.out_of_bounds_job = (jdata.jobname, jdata.job_offset)
            valid &= range_match
        return not valid

    def detect_job_overlap(self) -> bool:
        valid = True
        for ref_jid, ref_jdata in self.queues.items():
            for jid, jdata in self.queues.items():
                if ref_jid == jid:
                    continue
                if ref_jdata.cpu[0] < jdata.cpu[0]:
                    valid &= (ref_jdata.cpu[1] < jdata.cpu[0])
                    valid &= (ref_jdata.first_cycl["ts"] < jdata.first_cycl["ts"])
                    valid &= (ref_jdata.last_cycl["ts"] < jdata.first_cycl["ts"])
                else:
                    continue  # no need to check both directions
                if not valid:
                    self.out_of_bounds_job = (ref_jdata.jobname, jdata.jobname)
                    break
        return not valid

    def apply(self, event: TraceEvent) -> TraceEvent:
        if not _is_cpu_event(event):
            jid = event["args"]["jobhash"]
            jdata = self.queues[jid]
            apply_new_ts = (jdata.job_offset is not None and not isclose(jdata.job_offset, 0.0, abs_tol=1e-9))
            # new_end = new_ts + event["dur"]
            # was the event too early and updating would bring it into the range (negative offset)?
            # apply_new_ts &= event["ts"] < jdata.job_offset and new_ts > self.queues[jid].cpu[0]

            if apply_new_ts:
                new_ts = event["ts"] + jdata.job_offset
                aiulog.log(aiulog.TRACE, "FREQ: event", event["name"], "applying offset:", jdata.job_offset)
                event["args"]["ts_adj"] = jdata.job_offset
                event["ts"] = new_ts

        return event

    def drain(self) -> list[TraceEvent]:
        if self.collection_phase():
            if self.detect_cpu_aiu_range_mismatch():
                aiulog.log(aiulog.WARN,
                           "FREQ: job:", self.out_of_bounds_job,
                           "has AIU events outside of corresponding CPU range."
                           " Attempting to fix might cause unreliable data.")
            if self.detect_job_overlap():
                aiulog.log(aiulog.WARN,
                           "FREQ: Detected a job timestamp overlap in the input data:", self.out_of_bounds_job,
                           "Possible cause is a frequency mismatch or input data misalignment.")
            for _, queue in self.queues.items():
                queue.compute_frequency()
        else:
            pass
        return super().drain()  # parent class flips the phases


def frequency_align_collect(event: TraceEvent, context: AbstractContext) -> list[TraceEvent]:
    assert isinstance(context, FlexJobOffsetContext)

    context.collect(event)
    # returning event here, next stage will happen after barrier
    return [event]


def frequency_align_apply(event: TraceEvent, context: AbstractContext) -> list[TraceEvent]:
    assert isinstance(context, FlexJobOffsetContext)

    return [context.apply(event)]
