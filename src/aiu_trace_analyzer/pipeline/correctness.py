# Copyright 2024-2025 IBM Corporation

from aiu_trace_analyzer.pipeline.context import AbstractContext
from aiu_trace_analyzer.types import TraceEvent
from aiu_trace_analyzer.hw_data import has_hw_ts, get_hw_ts_list, hw_ts_label


def _args_ts_n_sanity_check(event: TraceEvent) -> TraceEvent:
    '''
    Checking the sequence of TS1-5 for monotonic increasing values
    '''
    if has_hw_ts(event):
        values = get_hw_ts_list(event)
        for n in range(2, len(values) + 1):
            assert values[n - 2] <= values[n - 1], \
                f'{hw_ts_label(n)} is smaller than {hw_ts_label(n - 1)}, {event["name"]}, {event["args"]}'
    return event


def event_sanity_checks(event: TraceEvent, _: AbstractContext) -> list[TraceEvent]:
    event = _args_ts_n_sanity_check(event)
    return [event]
