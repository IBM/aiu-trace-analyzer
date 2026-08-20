# Copyright 2024-2026 IBM Corporation

from aiu_trace_analyzer.types import (
    TraceEvent,
    TRACE_FINDING_EVENT_NAME,
    VERIFICATION_TEST_RESULT_NAME,
)
from aiu_trace_analyzer.verification.report import verification_result_filter


def _m_event(name):
    return TraceEvent({"ph": "M", "ts": 0, "pid": 0, "name": name, "args": {}})


def _x_event():
    return TraceEvent({"ph": "X", "ts": 0, "dur": 10, "pid": 0, "name": "some_kernel"})


def test_r1_finding_passes_through():
    event = _m_event(TRACE_FINDING_EVENT_NAME)
    result = verification_result_filter(event)
    assert result == [event]


def test_r2_verification_test_result_passes_through():
    event = _m_event(VERIFICATION_TEST_RESULT_NAME)
    result = verification_result_filter(event)
    assert result == [event]


def test_r3_non_m_event_is_dropped():
    event = _x_event()
    assert verification_result_filter(event) == []


def test_r4_m_event_with_unknown_name_is_dropped():
    event = _m_event("some_other_meta")
    assert verification_result_filter(event) == []
