# Copyright 2024-2025 IBM Corporation

import aiu_trace_analyzer.logger as aiulog
from aiu_trace_analyzer.types import TraceEvent
from aiu_trace_analyzer.pipeline import AbstractContext, TwoPhaseWithBarrierContext


class FirmwareEventsContext(TwoPhaseWithBarrierContext):
    def __init__(self,
                 soc_frequency: float,
                 warnings=None):
        super().__init__(warnings)
        self.soc_frequency = soc_frequency
        self.max_offset = -1e99
        self.min_offset = 1e99
        self.flow_id_seq = 0

    def is_relevant_event(self, event: TraceEvent) -> bool:
        return event["ph"] == "X" and "args" in event and "fw_begin_time" in event["args"] and "TS5" in event["args"]

    def collect_offset(self, ts_diff: float) -> None:
        self.min_offset = min(ts_diff, self.min_offset)
        self.max_offset = max(ts_diff, self.max_offset)

    def get_next_flow_id(self) -> int:
        self.flow_id_seq += 1
        return self.flow_id_seq

    def drain(self) -> list[TraceEvent]:
        if self.phase == self._COLLECTION_PHASE:
            pass
        else:
            pass
        offset_window = abs(self.min_offset - self.max_offset)

        if self.flow_id_seq == 0:
            return []

        if offset_window > 50:
            aiulog.log(aiulog.WARN, f"FW_FLOW: min/max offset window (fw_ts - ev_ts): {offset_window}."
                       " May indicate timing/alignment issues of FW and Regular events.")
        else:
            aiulog.log(aiulog.INFO, f"FW_FLOW: min/max offset window (fw_ts - ev_ts): {offset_window}.")
        return []


def collect_fw_event_data(event: TraceEvent, ctx: AbstractContext) -> list[TraceEvent]:
    assert isinstance(ctx, FirmwareEventsContext)
    if not ctx.is_relevant_event(event):
        return [event]
    return [event]


def create_fw_events(event: TraceEvent, ctx: AbstractContext) -> list[TraceEvent]:
    assert isinstance(ctx, FirmwareEventsContext)
    if not ctx.is_relevant_event(event):
        return [event]

    fw_ts = float(event["args"]["fw_begin_time"]) / ctx.soc_frequency
    fw_dur = float(event["args"]["fw_end_time"]) / ctx.soc_frequency - fw_ts
    fw_orig_ts = fw_ts
    fw_ts = event["ts"] + event["dur"] - fw_dur
    # fw_dur = 0.05   # turn into 'CB queued events'

    fw_flow_id = ctx.get_next_flow_id()
    fw_event = {
        "ph": "X",
        "pid": event["pid"],
        "tid": event["tid"] * 10000,
        "name": f'FW_{event["name"]}',
        "ts": fw_ts,
        "dur": fw_dur,
        "args": event["args"],
        "flow_id": fw_flow_id
    }
    event["flow_id"] = fw_flow_id
    fw_event["args"]["ts_diff"] = fw_orig_ts - fw_ts

    ctx.collect_offset(fw_orig_ts - fw_ts)

    aiulog.log(aiulog.DEBUG, "Creating event:", fw_event)

    return [event, fw_event]


def create_fw_flow_events(event: TraceEvent, _: AbstractContext) -> list[TraceEvent]:
    if "flow_id" not in event:
        return [event]

    is_fw = event["name"].startswith("FW_")

    flow_event = {
        "ph": "s" if is_fw else "f",
        "pid": event["pid"],
        "tid": event["tid"],
        "name": "fw2e",
        "cat": "fw2e",
        "ts": event["ts"],
        "id": event.pop("flow_id")
    }
    if not is_fw:
        flow_event["bp"] = "e"
    return [event, flow_event]
