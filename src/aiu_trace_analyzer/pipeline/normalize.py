# Copyright 2024-2025 IBM Corporation

import copy
import math
import re

import aiu_trace_analyzer.logger as aiulog
from aiu_trace_analyzer.pipeline.context import AbstractContext
from aiu_trace_analyzer.pipeline.hashqueue import AbstractHashQueueContext
from aiu_trace_analyzer.types import TraceEvent, GlobalIngestData
from aiu_trace_analyzer.pipeline.tools import FlexEventMapToTS


class EventStats(object):
    def __init__(self,
                 cycles: tuple[int, int] = (0, 0),
                 ts_dur: tuple[float, float] = (0.0, 0.0)):
        self.cycle_start, self.cycle_end = cycles
        self.ts, self.dur = ts_dur

        self.freq_mean = 0.0
        self.freq_min = 1.0e99
        self.freq_max = 0.0
        self.count = 0

    def get_end_ts(self) -> float:
        return self.ts + self.dur

    def get_start_ts(self) -> float:
        return self.ts

    def get_end_cycle(self) -> int:
        return self.cycle_end

    def get_start_cycle(self) -> int:
        return self.cycle_start

    def update(self,
               cycles: tuple[int, int],
               ts_dur: tuple[float, float],
               freq: float):
        self.cycle_start, self.cycle_end = cycles
        self.ts, self.dur = ts_dur

        self.freq_max = max(self.freq_max, freq)
        self.freq_min = min(self.freq_min, freq)
        self.count += 1
        self.freq_mean = self.freq_mean + (freq - self.freq_mean) / float(self.count)


class NormalizationContext(AbstractHashQueueContext):

    # dictionary keys to be used for 2 different freq-detection stats
    _DURATION_KEY = "duration"
    _INTERVAL_KEY = "interval"

    # tolerance before warning of frequency issue with trace or cmdline
    _FREQ_TOLERANCE = 0.1

    def __init__(self, soc_frequency: float, ignore_crit: bool = False,
                 filterstr: str = "") -> None:
        super().__init__()
        self.soc_frequency = soc_frequency
        self.frequency_minmax = (1e99, 0.0, 0, 0.0, 0.0)
        self.OVERFLOW_TIME_SPAN_US = float(1 << 32) / self.soc_frequency
        self.OVERFLOW_TIME_TOLERANCE = self.OVERFLOW_TIME_SPAN_US * 0.05  # allow for some tolerance
        self.ignore_crit = ignore_crit
        self.prev_event_data: dict[int, dict[str, EventStats]] = {}
        self.flex_name_ts_map = FlexEventMapToTS()
        self.event_filter = self.extract_eventfilters(filterstr)

    def __del__(self) -> None:
        def _print_freq_minmax(key: str):
            freq_min = 1e99
            freq_max = freq_mean = 0.0
            for c, ev_stats in enumerate(self.prev_event_data.values()):
                freq_min = min(freq_min, ev_stats[key].freq_min)
                freq_max = max(freq_max, ev_stats[key].freq_max)
                freq_mean = freq_mean + (ev_stats[key].freq_mean - freq_mean) / float(c + 1)

            if math.isclose(freq_mean, 0.0, abs_tol=1e-9):
                # if there was no event with hw clock timestamps and thus no frequency can be computed
                return

            rel_range = round((freq_max - freq_min) / freq_mean, 3)
            inp_diff = round(self.soc_frequency / freq_mean, 3)
            if rel_range > self._FREQ_TOLERANCE or abs(1.0 - inp_diff) > self._FREQ_TOLERANCE:
                loglevel = aiulog.WARN
            else:
                loglevel = aiulog.INFO

            aiulog.log(loglevel,
                       f"FREQ: Detected Event-{key}-based frequency (min/mean/max):",
                       round(freq_min, 2), round(freq_mean, 2), round(freq_max, 2),
                       f"; rel_range={rel_range}, input_soc_freq/detected={inp_diff}"
                       )

        mi, ma, _, mean, madr = self.frequency_minmax
        if not math.isclose(mean, 0.0, abs_tol=1e-9):
            if ma-mi > mean * 0.2:
                aiulog.log(aiulog.WARN,
                           "FREQ: Min/Max of detected correct frequency is >20% of mean"
                           f" ({round(mi, 3)},{round(ma, 3)})."
                           " This indicates some events might have been assigned to the wrong TSx epoch.")
            elif abs(mean - self.soc_frequency) > self._FREQ_TOLERANCE:
                aiulog.log(aiulog.WARN,
                           "FREQ: Recommendation: to minimize event time drift"
                           f" (max: {madr}us) between CPU and Accelerator, use:"
                           f" --freq={round(mean, 3)}")

        _print_freq_minmax(self._DURATION_KEY)
        _print_freq_minmax(self._INTERVAL_KEY)

    def queue_hash(self, event: TraceEvent) -> int:
        return hash(event["pid"])

    def extract_eventfilters(self, filterstr: str) -> dict[str, re.Pattern]:
        event_filters: dict[str, re.Pattern] = {}
        if len(filterstr.strip()) == 0:
            return event_filters
        for fstr in filterstr.split(","):
            key_regex = fstr.split(":")
            if len(key_regex) != 2:
                aiulog.log(aiulog.WARN, "FLTR: key:regex pattern not found in event filter. Skipping", fstr)
                continue
            event_filters[key_regex[0]] = re.compile(rf"{key_regex[1]}")
        aiulog.log(aiulog.INFO, f"FLTR: Event filtering is active. {len(event_filters)} filters enabled.")
        return event_filters

    def event_filtered(self, event: TraceEvent) -> bool:
        for attr, regex in self.event_filter.items():
            attr_tree = attr.split('.')
            e = event
            for a in attr_tree:
                if a not in e:
                    break
                e = e[a]

            if not isinstance(e, dict) and regex.search(e) is not None:
                return True
        return False

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
                    # TODO instead of hard-coded 1 epoch, consider estimated number of epochs based on event duration
                    curr += 1 << 32
                args[ts] = str(curr)
                prev = curr

            if event["dur"] > self.OVERFLOW_TIME_SPAN_US:
                aiulog.log(aiulog.WARN,
                           "OVC: Detected event with long duration and"
                           " thus potential undetected overflow in TSx counter.")

            if "Cmpt Exec" not in event["name"]:
                return args

            # compute anticipated frequency based on duration
            qid = self.queue_hash(event)
            if qid not in self.prev_event_data:
                self.prev_event_data[qid] = {
                    self._DURATION_KEY: EventStats(),
                    self._INTERVAL_KEY: EventStats()}

            ts_a, ts_b = self.flex_name_ts_map[event["name"]]
            dur_cycles = int(event["args"][ts_b]) - int(event["args"][ts_a])
            dur_freq = float(dur_cycles) / event["dur"]
            aiulog.log(aiulog.TRACE,
                       f"{event['args'][ts_a]:10} {event['args'][ts_b]:10} {dur_cycles:10}"
                       f" {event['dur']:15} {dur_freq:12.3f} |{event['name']}")
            self.prev_event_data[qid][self._DURATION_KEY].update(
                (int(event["args"][ts_a]), int(event["args"][ts_b])),
                (event["ts"], event["dur"]),
                dur_freq)

            # compute anticipated frequency based on event interval to previous event
            if self.prev_event_data[qid][self._INTERVAL_KEY].count > 0:
                gap_cycles = int(event["args"][ts_a]) - self.prev_event_data[qid][self._INTERVAL_KEY].get_start_cycle()
                gap_time = event["ts"] - self.prev_event_data[qid][self._INTERVAL_KEY].get_start_ts()
                gap_freq = float(gap_cycles) / gap_time
            else:
                gap_freq = dur_freq
            self.prev_event_data[qid][self._INTERVAL_KEY].update(
                (int(event["args"][ts_a]), int(event["args"][ts_b])),
                (event["ts"], event["dur"]),
                gap_freq)

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

    if context.event_filtered(event):
        return []

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
