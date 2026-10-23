# Copyright 2026 IBM Corporation

from aiu_trace_analyzer.dialect import InputDialectTORCH
from aiu_trace_analyzer.types import GlobalIngestData, TraceEvent
from aiu_trace_analyzer.verification.duration_timestamp_verify import (
    DurationTimestampVerificationContext,
    verify_duration_timestamp,
)


GlobalIngestData()
_TORCH_JOB = GlobalIngestData.add_job_info(
    source_uri="duration_timestamp_verify.pt.trace.json",
    data_dialect=InputDialectTORCH(),
)


def _event(dur: float, cycles_ts=None) -> TraceEvent:
    args = {"jobhash": _TORCH_JOB}

    if cycles_ts is not None:
        args["cycles_ts"] = cycles_ts

    return TraceEvent({
        "name": "kernel",
        "cat": "kernel",
        "ph": "X",
        "pid": 1,
        "tid": 1,
        "ts": 100.0,
        "dur": dur,
        "args": args,
    })


def test_negative_duration_is_reported():
    context = DurationTimestampVerificationContext()
    result = verify_duration_timestamp(_event(-1.0), context)

    warning = context.warnings["negative_duration"]
    assert warning.args_list["count"] == 1
    assert result == []


def test_zero_duration_is_reported():
    context = DurationTimestampVerificationContext()
    verify_duration_timestamp(_event(0.0), context)

    warning = context.warnings["zero_duration"]
    assert warning.args_list["count"] == 1


def test_zero_duration_with_nonzero_hw_time_is_reported():
    context = DurationTimestampVerificationContext()
    event = _event(0.0, cycles_ts=[10, 10, 10, 10, 20])

    verify_duration_timestamp(event, context)

    assert context.warnings["zero_duration"].args_list["count"] == 1
    assert context.warnings["zero_with_hw_time"].args_list["count"] == 1


def test_zero_duration_with_zero_hw_time_is_not_reported_as_hw_mismatch():
    context = DurationTimestampVerificationContext()
    event = _event(0.0, cycles_ts=[10, 10, 10, 10, 10])

    verify_duration_timestamp(event, context)

    assert context.warnings["zero_duration"].args_list["count"] == 1
    assert context.warnings["zero_with_hw_time"].args_list["count"] == 0


def test_zero_duration_instances_are_capped():
    context = DurationTimestampVerificationContext()

    for _ in range(context._MAX_FINDING_INSTANCES + 1):
        verify_duration_timestamp(_event(0.0), context)

    warning = context.warnings["zero_duration"]
    args = warning.to_verification_event_args()

    assert args["count"] == context._MAX_FINDING_INSTANCES + 1
    assert len(args["instances"]) == context._MAX_FINDING_INSTANCES


def test_positive_duration_has_no_findings():
    context = DurationTimestampVerificationContext()
    verify_duration_timestamp(_event(1.0), context)

    assert context.warnings["negative_duration"].args_list["count"] == 0
    assert context.warnings["zero_duration"].args_list["count"] == 0
    assert context.warnings["zero_with_hw_time"].args_list["count"] == 0


def test_findings_are_emitted_as_verification_data():
    context = DurationTimestampVerificationContext()
    verify_duration_timestamp(_event(-1.0), context)

    events = context.drain()

    findings = [
        event for event in events
        if event["name"] == "verification_data"
    ]
    test_results = [
        event for event in events
        if event["name"] == "verification_test_result"
    ]

    negative = next(
        event for event in findings
        if event["args"]["finding"] == "negative_duration"
    )

    assert negative["args"]["count"] == 1
    assert negative["args"]["is_error"] is False
    assert test_results[0]["args"]["result"] == "warn"
