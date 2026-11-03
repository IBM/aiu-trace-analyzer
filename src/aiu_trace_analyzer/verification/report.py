# Copyright 2024-2026 IBM Corporation

from aiu_trace_analyzer.types import (
    TraceEvent,
    TRACE_FINDING_EVENT_NAME,
    VERIFICATION_TEST_RESULT_NAME)

# the events the verification report is built from: the findings of every context plus the per-test
# verdicts. Both names are defined in types.py because the regular pipeline emits findings too.
VERIFICATION_EVENT_NAMES = {TRACE_FINDING_EVENT_NAME, VERIFICATION_TEST_RESULT_NAME}


def verification_result_filter(event: TraceEvent, ctx=None) -> list[TraceEvent]:
    """Keep only verification result M-events; discard everything else."""
    if event.get("ph") == "M" and event.get("name") in VERIFICATION_EVENT_NAMES:
        return [event]
    return []
