# Copyright 2024-2025 IBM Corporation

import copy

import aiu_trace_analyzer.logger as aiulog
from aiu_trace_analyzer.types import TraceEvent
from aiu_trace_analyzer.pipeline import AbstractContext, EventPairDetectionContext
from aiu_trace_analyzer.pipeline.timesync import get_cycle_ts_as_clock


class DataTransferExtractionContext(EventPairDetectionContext):
    '''
    Bytes counter extraction (experimental):
    Hashes counter events into a queue per process ID and sorts events by their TS
    When emitting (during drain), bandwidth is computed dividing number of bytes dy
    corresponding duration
    '''
    def __init__(self) -> None:
        super().__init__()
        self.last_TS = {}  # used for TS sequence checking
        self.ts_offsets = {}  # used to align cycle-TS with wall clock (experimental)
        self.prev = {}  # holds the previous event (may need if Dma events overlap)
        self.ecount_in = 0
        self.ecount_out = 0

    def __del__(self):
        aiulog.log(aiulog.INFO, "DTC event stats o/i:", self.ecount_out, '/', self.ecount_in)

    # dma (bytes transferred) is per process, so we drop the tid information
    def queue_hash(self, pid, _=0) -> int:
        return hash(pid)

    def get_prev(self, event: TraceEvent) -> TraceEvent:
        queue_id = self.queue_hash(event["pid"])
        if queue_id not in self.prev:
            self.prev[queue_id] = None
            return None
        return self.prev[queue_id]

    def update_prev(self, event: TraceEvent):
        self.prev[self.queue_hash(event["pid"])] = event

    def compute_bandwidth_value(self, prev: TraceEvent, this: TraceEvent) -> float:
        aiulog.log(aiulog.TRACE, "DTC: Bandwidth: ", prev, this)

        # Handle Counter events (DmaI)
        if " DmaI" in prev['cat']:
            # data transfer size
            dts = int(prev["args"]["Bytes"])

            assert (prev['DmaI_end'] > prev['DmaI_start'])

            aiulog.log(aiulog.TRACE, "DmaI duration (usec):  %f" % ((prev['DmaI_end'] - prev['DmaI_start'])))

            new_val = round(dts / (prev['DmaI_end'] - prev['DmaI_start']), 3)

            return new_val

        # Handle Counter events (DmaO)
        elif " DmaO" in prev['cat']:
            # data transfer size
            dts = int(prev["args"]["Bytes"])

            assert (prev['DmaO_end'] > prev['DmaO_start'])

            aiulog.log(aiulog.TRACE, "DmaO duration (usec): %f" % (prev['DmaO_end'] - prev['DmaO_start']))

            new_val = round(dts / (prev['DmaO_end'] - prev['DmaO_start']), 3)

            return new_val

    def name_from_category(self, prev: TraceEvent) -> TraceEvent:
        _cat = prev['cat']  # keep name in the category to keep all power readings in one viz track

        # possible categories: DmaI, DmaO, AllGather, Exec
        if "DmaI" in _cat:
            cat_type = " DmaI"
        elif "DmaO" in _cat:
            cat_type = " DmaO"
        else:
            cat_type = " Other"

        # overwrite name field
        prev["name"] += cat_type

        return prev

    def create_zero_event(self, event: TraceEvent) -> TraceEvent:
        # deepcopy
        zevent = copy.deepcopy(event)

        zevent['args']['Bytes'] = 0.0

        # Get the appropriate timestamp (end of event)
        if " DmaI" in zevent['cat']:
            zevent['ts'] = zevent['DmaI_end']
        elif " DmaO" in zevent['cat']:
            zevent['ts'] = zevent['DmaO_end']

        # remove intermediate fields from zevent
        zevent.pop('DmaI_start')
        zevent.pop('DmaI_end')

        zevent.pop('DmaO_start')
        zevent.pop('DmaO_end')

        # remove 'cat' field
        zevent.pop('cat')

        return zevent

    def compute_bandwidth(self, event: TraceEvent) -> list[TraceEvent]:
        # just eat all events and wait for drain to sort and emit
        aiulog.log(aiulog.TRACE, "DTC: processing:", event)

        self.ecount_in += 1
        # get the currently stored prev event matching the event's track/pid
        prev = self.get_prev(event)

        # if there's no prev event, this is the first encounter and we just update prev
        if not prev:
            self.update_prev(event)
            return []

        # else, we need to compute power/delta
        bw = self.compute_bandwidth_value(prev, event)
        # if there are computation issues, then return nothing
        if not bw:
            return []

        # update the older event bandwidth value
        prev["args"]["Bytes"] = bw
        # may need to fix this function
        prev = self.name_from_category(prev)

        # make a copy of the prev event
        zero_event = self.create_zero_event(prev)

        # remove intermediate fields
        prev.pop('DmaI_start')
        prev.pop('DmaI_end')

        prev.pop('DmaO_start')
        prev.pop('DmaO_end')

        # remove 'cat' field
        prev.pop('cat')

        # make the current event the new prev
        self.update_prev(event)
        self.ecount_out += 1
        return [prev, zero_event]

    def drain(self) -> list[TraceEvent]:
        return []


def extract_data_transfer_event(event: TraceEvent, context: AbstractContext) -> list[TraceEvent]:
    '''
    extracts data transfer counter data from X and b events
    '''
    assert isinstance(context, DataTransferExtractionContext)
    if event["ph"] in ["X", "b"]:
        if "args" in event and "Bytes" in event["args"]:
            # Align TSx entries with event timestamps in cycles
            # cycle_count_to_wallclock has added ts_all with converted TS1-5, we just need to get TS3 from there:

            # If event name contains " DmaI" use TS1
            if " DmaI" in event['name']:
                ts = int(get_cycle_ts_as_clock(1, event["args"]["ts_all"]))
            # If event name contains " DmaO" use TS4
            elif " DmaO" in event['name']:
                ts = int(get_cycle_ts_as_clock(4, event["args"]["ts_all"]))
            else:
                return [event]

            counter = {
                "ph": "C",
                "pid": event["pid"],
                "ts": ts,
                "cat": event['name'],
                "name": "BW",
                "args": {
                    "Bytes": event["args"]["Bytes"]
                }
            }

            return [event, counter]

    return [event]


def compute_bandwidth(event: TraceEvent, context: AbstractContext) -> list[TraceEvent]:

    assert isinstance(context, DataTransferExtractionContext)

    # only process 'C' events (BW)
    if event['ph'] == "C" and event['name'] == "BW":
        return context.compute_bandwidth(event)
    else:
        return [event]
