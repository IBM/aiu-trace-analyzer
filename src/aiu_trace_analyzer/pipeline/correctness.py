# Copyright 2024-2025 IBM Corporation

from aiu_trace_analyzer.pipeline.context import AbstractContext
from aiu_trace_analyzer.types import TraceEvent


def _args_ts_n_sanity_check(event: TraceEvent) -> TraceEvent:
    '''
    Checking the sequence of TS1-5 for monotonic increasing values
    '''
    if "args" in event and "TS1" in event["args"]:
        last = -1e31
        for i, t in enumerate(["TS1", "TS2", "TS3", "TS4", "TS5"]):
            assert last <= float(event["args"][t]), \
                f'TS{i+2} is smaller than TS{i+1}, {event["name"]}, {event["args"]}'
            last = float(event["args"][t])
    return event


def event_sanity_checks(event: TraceEvent, _: AbstractContext) -> list[TraceEvent]:
    event = _args_ts_n_sanity_check(event)
    return [event]
