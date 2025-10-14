# Copyright 2024-2025 IBM Corporation

import copy
import math
import re

import aiu_trace_analyzer.logger as aiulog
from aiu_trace_analyzer.pipeline.context import AbstractContext
from aiu_trace_analyzer.pipeline.hashqueue import AbstractHashQueueContext
from aiu_trace_analyzer.types import TraceEvent, GlobalIngestData


class NormalizationContext(AbstractHashQueueContext):

    def __init__(self, soc_frequency: float, ignore_crit: bool = False) -> None:
        super().__init__()
        self.soc_frequency = soc_frequency
        self.frequency_minmax = (1e99, 0.0, 0, 0.0, 0.0)
        self.OVERFLOW_TIME_SPAN_US = float(1 << 32) / self.soc_frequency
        self.OVERFLOW_TIME_TOLERANCE = self.OVERFLOW_TIME_SPAN_US * 0.05  # allow for some tolerance
        self.ignore_crit = ignore_crit

    def __del__(self) -> None:
        mi, ma, _, mean, madr = self.frequency_minmax
        if math.isclose(mean, 0.0, abs_tol=1e-9):
            return
        if ma-mi > mean * 0.2:
            aiulog.log(aiulog.WARN,
                       "FREQ: Min/Max of detected correct frequency is >20% of mean"
                       f" ({round(mi,3)},{round(ma,3)})."
                       " This indicates some events might have been assigned to the wrong TSx epoch.")
        elif abs(mean - self.soc_frequency) > 0.1:
            aiulog.log(aiulog.WARN,
                       "FREQ: Recommendation: to minimize event time drift"
                       f" (max: {madr}us) between CPU and Accelerator, use:"
                       f" --freq={round(mean,3)}")

    def queue_hash(self, event: TraceEvent) -> int:
        return hash(event["pid"])

    def get_overflow_count(self, qid, job: str, ts: float, cycle: int) -> tuple[float, float, float]:
        # potential start ts of epoch assuming cycle->wallclk mapping is correct
        epoch_start = ts - cycle / self.soc_frequency

        # the first computed epoch_start becomes the OVC reference point
        if qid not in self.queues:
            self.queues[qid] = {"0": (epoch_start, ts, cycle)}   # store epoch0 for this job
            aiulog.log(aiulog.INFO, "OVC: Reference Epoch for", qid, job, ts-epoch_start, epoch_start)
        # ts distance to reference point
        time_since_epoch0 = ts - self.queues[qid]["0"][0]

        elapsed_epochs = int(math.floor(time_since_epoch0 / self.OVERFLOW_TIME_SPAN_US))
        actual_freq = self.frequency_minmax[3]

        if job not in self.queues[qid]:
            abs_cycle = cycle + (elapsed_epochs * (1 << 32))
            job_drift = int(epoch_start
                            - (self.queues[qid]["0"][0] + elapsed_epochs * self.OVERFLOW_TIME_SPAN_US))
            actual_freq = (abs_cycle - self.queues[qid]["0"][2]) / (ts - self.queues[qid]["0"][1]) \
                if ts != self.queues[qid]["0"][1] else None
            self.queues[qid][job] = (epoch_start, ts, cycle)
            aiulog.log(aiulog.DEBUG, "OVC: Next job reference Epoch", qid, job, epoch_start, job_drift, actual_freq)
            mi, ma, cnt, mean, madr = self.frequency_minmax
            cnt += 1
            if actual_freq:
                self.frequency_minmax = (min(mi, actual_freq),
                                         max(ma, actual_freq),
                                         cnt,
                                         mean + (actual_freq - mean) / float(cnt),
                                         max(madr, job_drift, key=abs)
                                         )

        #   drift is: computed epoch start   differs from    actual epoch start
        drift = self.queues[qid]["0"][0] + elapsed_epochs * self.OVERFLOW_TIME_SPAN_US - epoch_start

        aiulog.log(aiulog.TRACE, "OVC: Event", qid, ts, ts-epoch_start, self.queues[qid], elapsed_epochs)

        return elapsed_epochs, drift, actual_freq

    def tsx_32bit_local_correction(self, event: TraceEvent) -> dict:
        if "TS1" in event["args"]:
            args = event["args"]
            prev = -(1 << 48)  # set something very small to cover for some negative overflow epochs to happen
            for ts in ["TS1", "TS2", "TS3", "TS4", "TS5"]:
                curr = int(args[ts], 0)
                if curr < prev:
                    if "TSxOF" not in event["args"]:
                        event["args"]["TSxOF"] = ts
                    aiulog.log(aiulog.TRACE, "OVC: intra-event TSx overflow:", event["args"])
                    curr += 1 << 32
                args[ts] = str(curr)
                prev = curr

            if event["dur"] > self.OVERFLOW_TIME_SPAN_US:
                aiulog.log(aiulog.WARN,
                           "OVC: Detected event with long duration and"
                           " thus potential undetected overflow in TSx counter.")
            return args
        return event["args"]

    def tsx_32bit_global_correction(self, qid, event: TraceEvent) -> dict:
        if "TS1" in event["args"]:
            args = event["args"]
            ovc, drift, tofix = self.get_overflow_count(qid,
                                                        str(event["args"]["jobhash"]),
                                                        event["ts"],
                                                        int(event["args"]["TS1"]))
            aiulog.log(aiulog.TRACE, "OVC: DRIFT:", event["name"], ovc, drift, tofix, self.frequency_minmax)

            prev = -(1 << 48)  # set something very small to cover for some negative overflow epochs to happen
            for ts in ["TS1", "TS2", "TS3", "TS4", "TS5"]:
                curr = int(args[ts], 0)
                curr += (ovc * 1 << 32)
                if curr < prev:
                    aiulog.log(aiulog.ERROR, "attempt of local_correction fix has missed a spot in TS-sequence.")
                    if not self.ignore_crit:
                        assert curr >= prev, "local_correction of TS-sequence incomplete."
                args[ts] = str(curr)
                prev = curr
            args["OVC"] = ovc
            return args
        return event["args"]

    def drain(self) -> list[TraceEvent]:
        return []


def _attr_to_args(event: TraceEvent) -> TraceEvent:
    '''
    Turns k/v entries made under 'attr' into k/v under args
    '''
    if "attr" in event:
        if "args" not in event:
            event["args"] = copy.deepcopy({})
        for k, v in event["attr"].items():
            event["args"][k] = copy.deepcopy(v)
        event.pop("attr")
    return event


def _hex_to_int_str(event: TraceEvent) -> TraceEvent:
    if "args" in event:
        if not isinstance(event["args"], dict):
            return event

        for k in ["TS1", "TS2", "TS3", "TS4", "TS5", "Power"]:
            if k in event["args"] and isinstance(event["args"][k], str):
                try:
                    event["args"][k] = str(int(event["args"][k], 0))
                except ValueError:
                    pass  # do nothing and leave the value alone
    return event


_unify_recv = re.compile("Receive")
_unify_rdma = re.compile("RDMA")
_jobinfo = GlobalIngestData()


# deal with the different naming schemes for the same kind of event
# instead of writing more complex detection patters, let's unify the names instead
def _name_unification(name: str) -> str:
    new_name = _unify_rdma.sub("Rdma", name)
    new_name = _unify_recv.sub("Recv", new_name)
    return new_name


def normalize_phase1(event: TraceEvent, context: AbstractContext) -> list[TraceEvent]:
    assert isinstance(context, NormalizationContext)

    # don't let anything pass that's not in X-event
    if event["ph"] not in ["X"]:
        return [event]

    event = _attr_to_args(event)
    event = _hex_to_int_str(event)
    event["name"] = _name_unification(event["name"])
    event["args"]["jobname"] = _jobinfo.get_job(event["args"]["jobhash"])
    if "args" in event and "TS1" in event["args"]:
        event["args"] = context.tsx_32bit_local_correction(event)

    assert isinstance(event, dict)
    return [event]


def normalize_phase2(event: TraceEvent, context: AbstractContext) -> list[TraceEvent]:
    assert isinstance(context, NormalizationContext)

    # don't let anything pass that's not in X-event
    if event["ph"] not in ["X"]:
        return [event]

    if "args" in event and "TS1" in event["args"]:
        qid = context.queue_hash(event)
        event["args"] = context.tsx_32bit_global_correction(qid, event)
        aiulog.log(aiulog.TRACE, "NORM after:", id(event["args"]), event)

    assert isinstance(event, dict)
    return [event]
