# Copyright 2024-2026 IBM Corporation

from aiu_trace_analyzer.types import (
    GlobalIngestData,
    InputDialectTORCH,
    TraceEvent,
)
from aiu_trace_analyzer.verification.overlap_verify import (
    OverlapVerificationContext,
    verify_kernel_overlap,
)


def _event(name, cat, ts, dur, stream=None) -> TraceEvent:
    jobhash = GlobalIngestData.add_job_info(
        source_uri="overlap_verify.json",
        data_dialect=InputDialectTORCH(),
    )

    args = {"jobhash": jobhash}
    if stream is not None:
        args["stream"] = stream

    return TraceEvent(
        {
            "name": name,
            "cat": cat,
            "ph": "X",
            "pid": 1,
            "tid": 1,
            "ts": ts,
            "dur": dur,
            "args": args,
        }
    )


def _detect(events: list[TraceEvent]) -> int:
    context = OverlapVerificationContext(strict=True)

    for event in events:
        verify_kernel_overlap(event, context)

    return context.warnings["overlaps"].args_list["count"]


def test_same_stream_kernel_overlap_is_detected():
    events = [
        _event("kernel_1", "kernel", 0.0, 10.0, stream=1),
        _event("kernel_2", "kernel", 5.0, 10.0, stream=1),
    ]
    assert _detect(events) == 1


def test_cross_stream_kernel_overlap_is_allowed():
    events = [
        _event("kernel_1", "kernel", 0.0, 10.0, stream=1),
        _event("kernel_2", "kernel", 5.0, 10.0, stream=2),
    ]
    assert _detect(events) == 0


def test_same_stream_kernel_memory_overlap_is_detected():
    events = [
        _event("kernel", "kernel", 0.0, 10.0, stream=1),
        _event("Memcpy (HtoD)", "gpu_memcpy", 5.0, 10.0, stream=1),
    ]
    assert _detect(events) == 1


def test_cross_stream_kernel_memory_overlap_is_allowed():
    events = [
        _event("kernel", "kernel", 0.0, 10.0, stream=1),
        _event("Memcpy (HtoD)", "gpu_memcpy", 5.0, 10.0, stream=2),
    ]
    assert _detect(events) == 0


def test_same_stream_memory_overlap_is_detected():
    events = [
        _event("Memcpy (HtoD)", "gpu_memcpy", 0.0, 10.0, stream=1),
        _event("Memcpy (DtoH)", "gpu_memcpy", 5.0, 10.0, stream=1),
    ]
    assert _detect(events) == 1


def test_cross_stream_memory_overlap_is_allowed():
    events = [
        _event("Memcpy (HtoD)", "gpu_memcpy", 0.0, 10.0, stream=1),
        _event("Memcpy (DtoH)", "gpu_memcpy", 5.0, 10.0, stream=2),
    ]
    assert _detect(events) == 0


def test_nested_same_stream_memory_overlap_is_detected():
    events = [
        _event("Memcpy (HtoD)", "gpu_memcpy", 0.0, 20.0, stream=1),
        _event("Memcpy (DtoH)", "gpu_memcpy", 5.0, 5.0, stream=1),
    ]
    assert _detect(events) == 1


def test_back_to_back_same_stream_memory_is_allowed():
    events = [
        _event("Memcpy (HtoD)", "gpu_memcpy", 0.0, 10.0, stream=1),
        _event("Memcpy (DtoH)", "gpu_memcpy", 10.0, 10.0, stream=1),
    ]
    assert _detect(events) == 0


def test_missing_stream_events_use_the_same_default_stream():
    events = [
        _event("Memcpy (HtoD)", "gpu_memcpy", 0.0, 10.0),
        _event("Memcpy (DtoH)", "gpu_memcpy", 5.0, 10.0),
    ]
    assert _detect(events) == 1


def test_default_stream_is_removed_after_verification():
    event = _event("Memcpy (HtoD)", "gpu_memcpy", 0.0, 10.0)

    context = OverlapVerificationContext(strict=True)
    verify_kernel_overlap(event, context)

    assert "stream" not in event["args"]
