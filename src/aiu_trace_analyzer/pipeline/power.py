# Copyright 2024-2025 IBM Corporation

import re
import copy
from typing import Union

import aiu_trace_analyzer.logger as aiulog
from aiu_trace_analyzer.constants import TS_CYCLE_KEY, TS_KEYS_LIST
from aiu_trace_analyzer.types import TraceEvent
from aiu_trace_analyzer.pipeline import AbstractContext, AbstractHashQueueContext
from aiu_trace_analyzer.pipeline.timesync import get_cycle_ts_as_clock


class PowerExtractionContext(AbstractHashQueueContext):
    '''
    Power counter extraction (experimental):
    Hashes counter events into a queue per process ID and sorts events by their TS
    When emitting (during drain), counter values are computed by subtracting
    Current value from previous.
    '''
    unit = "Watts"

    def __init__(self, skip_events_flag=False, filter_pattern=None, use_ts4=False) -> None:
        super().__init__()
        self.last_TS = {}  # used for TS sequence checking
        self.ts_offsets = {}  # used to align cycle-TS with wall clock (experimental)
        self.prev = {}  # holds the previous event to compute the power delta
        self.processed_first = {}  # track whether a processed event is the first for a given PID
        self.ecount_in = 0
        self.ecount_out = 0
        self.filter = re.compile(filter_pattern) if filter_pattern else re.compile("")
        self.skip_events = skip_events_flag
        self.power_ts = 3 if not use_ts4 else 4
        self.bad_events = 0

    def __del__(self):
        if self.bad_events:
            aiulog.log(aiulog.WARN,
                       f"PEC encountered {self.bad_events} bad power events set to 0.0W."
                       " Enable DEBUG (-D 3) to display.")
        aiulog.log(aiulog.INFO, "PEC event stats o/i:", self.ecount_out, '/', self.ecount_in)

    # power is per process, so we drop the tid information
    def queue_hash(self, pid) -> int:
        return hash(pid)

    def get_prev(self, event: TraceEvent) -> TraceEvent:
        queue_id = self.queue_hash(event["pid"])
        if queue_id not in self.prev:
            self.prev[queue_id] = None
            return None
        return self.prev[queue_id]

    def update_prev(self, event: TraceEvent):
        self.prev[self.queue_hash(event["pid"])] = event

    def compute_delta(self, prev: TraceEvent, this: TraceEvent) -> Union[float, None]:
        aiulog.log(aiulog.TRACE, "PEC: Delta: ", prev, this)

        pa = int(prev["args"][self.unit])
        pb = int(this["args"][self.unit])
        # skip the previous event anything that has no power
        if pa == 0:
            self.update_prev(this)
            return None

        # skip the current event if it's 0
        if pb == 0:
            return None

        # same timestamp, drop the one with the pattern (e.g. Prep)
        if prev[TS_CYCLE_KEY] == this[TS_CYCLE_KEY]:
            # equal timestamp requires to drop at least one of the 2 events
            aiulog.log(aiulog.TRACE, "PEC: TS Equal: ", prev[TS_CYCLE_KEY], ">", prev["cat"], "<|>", this["cat"], "<")
            if self.filter.search(prev['cat']):
                # updating self.prev means: drop 'prev' by replacing with 'this'
                self.update_prev(this)
                aiulog.log(aiulog.TRACE, "prev: %d, %s, this: %d, %s" % (prev[TS_CYCLE_KEY],
                                                                         prev['cat'],
                                                                         this[TS_CYCLE_KEY],
                                                                         this['cat']))

            if self.filter.search(this['cat']):
                aiulog.log(aiulog.DEBUG, "PEC: filter match second event: ", this)
            # not updating self.prev means: drop/skip 'this'
            aiulog.log(aiulog.DEBUG, "PEC: Keeping: ", self.prev[self.queue_hash(prev["pid"])])
            # since we can't compute delta, we return nothing
            return None

        # check if self.skip_events is set
        if self.skip_events is True:

            # skip this event if both contain "Cmpt Exec" in the cat
            if ("Cmpt Exec" in prev['cat']) and ("Cmpt Exec" in this['cat']):
                aiulog.log(aiulog.TRACE, "dumped event: ", this['cat'])
                return None

            # skip this event if prev contains " DmaO" and this contains " DmaI"
            if (" DmaO" in prev['cat']) and (" DmaI" in this['cat']):
                aiulog.log(aiulog.TRACE, "dumped event: ", this['cat'])
                return None

            # skip this event if prev contains "Cmpt Exec" and this contains " DmaI"
            if ("Cmpt Exec" in prev['cat']) and (" DmaI" in this['cat']):
                aiulog.log(aiulog.TRACE, "dumped event: ", this['cat'])
                return None

        # delta charge
        new_val = pb - pa if pa < pb else (2**32) + pb - pa

        # _TS_CYCLE_KEY is already in MHz cycles
        if self.power_ts == 3:
            # convert to power:
            # accumulated charge divided number of cycles (MHz)
            # number of cycles = delta time (in usec)*freq (frequency in MHz)
            new_val = round((new_val / (this[TS_CYCLE_KEY] - prev[TS_CYCLE_KEY])), 3)

        elif self.power_ts == 4:
            dt_in_us = (this[TS_CYCLE_KEY] - prev[TS_CYCLE_KEY])  # micro-second
            # Least significant bit for the Analog-2-Digital Converter (LSB=1/512 from snt_dpm_dcr: Register-ro)
            LSB_on_ADC = 1 / 512
            voltage = 12            # volt/s

            # dcharge is sum(current), has to be in nano-Ampere to get Power in Watts
            new_val = voltage * new_val * LSB_on_ADC / dt_in_us
            if new_val > 100:
                aiulog.log(aiulog.DEBUG, "POWER: BAD event a,b: ", new_val, prev, this)
                self.bad_events += 1
                new_val = 0

        # TODO: remove these statements
        # check if power value is huge
        if self.skip_events is True:
            #
            aiulog.log(aiulog.TRACE, "compute-power(): prev['cat'] ", prev['cat'])
            aiulog.log(aiulog.TRACE,
                       f"compute-power(): power: {new_val}, delta-t:"
                       f" {this[TS_CYCLE_KEY] - prev[TS_CYCLE_KEY]}")

        return new_val

    def name_from_category(self, prev: TraceEvent) -> TraceEvent:
        #
        if self.power_ts == 4:
            prev["name"] = "Power"
            return prev

        cat = prev['cat']   # keep name in the category to keep all power readings in one viz track

        # possible categories: DmaI, DmaO, AllGather, Exec
        if "DmaI" in cat:
            cat_type = " DmaI"
        elif "DmaO" in cat:
            cat_type = " DmaO"
        elif "AllGather" in cat:
            cat_type = " AllGather"
        elif "AllReduce" in cat:
            cat_type = " AllReduce"
        elif "Cmpt Exec" in cat:
            cat_type = " Cmpt Exec"
        else:
            cat_type = " Other"

        # overwrite name field
        prev["name"] += cat_type
        return prev

    def create_zero_event(self, event: TraceEvent, ts) -> Union[TraceEvent, None]:
        if self.power_ts == 4:
            return None
        #
        zevent = copy.deepcopy(event)
        zevent['args'][self.unit] = 0.0
        zevent['ts'] = ts

        return zevent

    def compute_power(self, event: TraceEvent) -> list[TraceEvent]:
        # just eat all events and wait for drain to sort and emit
        aiulog.log(aiulog.TRACE, "PEC: processing:", event)

        self.ecount_in += 1
        # get the currently stored prev event matching the event's track/pid
        prev = self.get_prev(event)

        # if there's no prev event, this is the first encounter and we just update prev
        if not prev:
            self.update_prev(event)
            return []

        # else, we need to compute power/delta
        power = self.compute_delta(prev, event)
        # if there are computation issues, then return nothing
        if power is None:
            return []

        if power < 0.0:
            aiulog.log(aiulog.ERROR, "PEC computed negative power", prev, event)
            raise OverflowError("Negative power value detected/computed.")

        # update the older event power value
        prev["args"][self.unit] = power
        prev = self.name_from_category(prev)

        # remove intermediate field
        prev.pop(TS_CYCLE_KEY)

        # remove 'cat' field
        prev['cat'] = "Power"+str(self.power_ts)

        # make a copy of the prev event pulling the counter to 0.0 at the next events TS-1ns
        zero_event = self.create_zero_event(prev, (event['ts'] - 0.001))

        # make the current event the new prev
        self.update_prev(event)
        self.ecount_out += 1
        if zero_event:
            return [prev, zero_event]
        else:
            return [prev]

    def _base_counter(self, pid, cat, counter_value) -> TraceEvent:
        return {
            "ph": "C",
            "name": "Power",
            "pid": pid,
            "cat": cat,
            "args": {
                self.unit: counter_value,
            }
        }

    def build_input_events(self, event: TraceEvent) -> list[TraceEvent]:
        # orig ts4 power routine drops any events with dur < 0.1, so lets not bother with those
        if self.power_ts == 4 and event["dur"] <= 0.1:
            return []

        counters = []
        queue_id = self.queue_hash(event["pid"])
        if self.power_ts == 4 and queue_id not in self.processed_first:
            # for TS3-based Power, there's no first event in that same sense
            # The first event for the TS4-based Power can reference the TS3 timestamp
            # Here we create the first counter in the same TS4-fashion so that the
            # computation has no special case to deal with
            first_counter_at_ts3 = self._base_counter(
                pid=event["pid"],
                cat=event["name"],
                counter_value=0.0   # first event gets power=0.0
            )
            first_counter_at_ts3["ts"] = get_cycle_ts_as_clock(3, event["args"]["ts_all"])
            first_counter_at_ts3[TS_CYCLE_KEY] = first_counter_at_ts3["ts"]  # TS4 power is based on ts_all4
            counters.append(first_counter_at_ts3)
            self.processed_first[queue_id] = True  # mark this pid-hash as 'initial TS3-event created'

        # build the preparation event for the Power computation
        counter = self._base_counter(
            pid=event["pid"],
            cat=event["name"],
            counter_value=float(event["args"]["Power"])
        )
        # Align TSx entries with event timestamps in cycles
        # cycle_count_to_wallclock has added ts_all with converted TS1-5, we just need to get the correct TS from there:
        counter["ts"] = get_cycle_ts_as_clock(self.power_ts, event["args"]["ts_all"])
        # build the _TS_CYCLE_KEY that can be indexed by context.power_ts index
        if self.power_ts == 3:
            counter[TS_CYCLE_KEY] = float(event["args"][TS_KEYS_LIST[self.power_ts]])
        else:
            counter[TS_CYCLE_KEY] = counter["ts"]   # ts4 power computation based on ts directly
        counters.append(counter)
        return counters

    def drain(self) -> list[TraceEvent]:
        return []


def extract_power_event(event: TraceEvent, context: AbstractContext) -> list[TraceEvent]:
    '''
    extracts power counter data from X and b events
    '''
    assert isinstance(context, PowerExtractionContext)
    if event["ph"] in ["X", "b"] and not context.filter.search(event["name"]):
        if "args" in event and "Power" in event["args"] and "ts_all" in event["args"]:
            counters = context.build_input_events(event)
            return [event] + counters

    return [event]


def check_power_ts_sequence(event: TraceEvent, context: AbstractContext) -> list[TraceEvent]:
    '''
    sanity check for power counter events: make sure their TS appears in ascending order
    '''
    assert isinstance(context, PowerExtractionContext)
    queue_id = context.queue_hash(event["pid"])
    if queue_id not in context.last_TS:
        context.last_TS[queue_id] = 0
    if event["ph"] == "C":
        if event["ts"] < context.last_TS[queue_id]:
            aiulog.log(aiulog.ERROR, "PEC: Counter event TS broken. last>new",
                       context.last_TS[queue_id], event["ts"], context.ecount_out)
            assert False
        context.last_TS[queue_id] = event["ts"]
    return [event]


def compute_power(event: TraceEvent, context: AbstractContext) -> list[TraceEvent]:

    assert isinstance(context, PowerExtractionContext)

    # only process 'C' events
    if event['ph'] == "C" and event["name"] == "Power":
        return context.compute_power(event)
    else:
        return [event]
