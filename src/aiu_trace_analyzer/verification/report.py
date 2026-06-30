# Copyright 2024-2026 IBM Corporation

from aiu_trace_analyzer.types import TraceEvent

VERIFICATION_RESULT_NAME      = "verification_data"
VERIFICATION_TEST_RESULT_NAME = "verification_test_result"
VERIFICATION_EVENT_NAMES      = {VERIFICATION_RESULT_NAME, VERIFICATION_TEST_RESULT_NAME}


def verification_result_filter(event: TraceEvent, ctx=None) -> list[TraceEvent]:
    """Keep only verification result M-events; discard everything else."""
    if event.get("ph") == "M" and event.get("name") in VERIFICATION_EVENT_NAMES:
        return [event]
    return []
