# Copyright 2026 IBM Corporation

from aiu_trace_analyzer.types import TraceEvent
from aiu_trace_analyzer.verification.kernel_parent_verify import (
    KernelParentVerificationContext,
)


def _event(name, cat, rank, correlation, ts, dur) -> TraceEvent:
    return TraceEvent({
        "name": name,
        "cat": cat,
        "ph": "X",
        "pid": rank,
        "tid": 0,
        "ts": ts,
        "dur": dur,
        "args": {
            "rank": rank,
            "correlation": correlation,
        },
    })


def test_same_correlation_id_is_isolated_between_ranks():
    ctx = KernelParentVerificationContext()

    ctx.collect_parent_and_kernels(
        _event("ScheduleCompute", "cpu_op", 0, 42, 0.0, 10.0)
    )
    ctx.collect_parent_and_kernels(
        _event("kernel", "kernel", 1, 42, 1.0, 1.0)
    )

    ctx.drain()
    ctx.drain()

    assert ctx.warnings["orphan_parents"].args_list["count"] == 1
    assert ctx.warnings["orphan_kernels"].args_list["count"] == 1


def test_kernel_containment_uses_parent_from_same_rank():
    ctx = KernelParentVerificationContext()

    rank0_parent = _event("ScheduleCompute", "cpu_op", 0, 42, 0.0, 100.0)
    rank1_parent = _event("ScheduleCompute", "cpu_op", 1, 42, 200.0, 10.0)
    rank1_kernel = _event("kernel", "kernel", 1, 42, 50.0, 1.0)

    ctx.collect_parent_and_kernels(rank0_parent)
    ctx.collect_parent_and_kernels(rank1_parent)
    ctx.collect_parent_and_kernels(rank1_kernel)

    ctx.verify_kernel_containment(rank1_kernel)

    finding = ctx.warnings["kernel_outside_parent"].to_finding_args()

    assert finding["count"] == 1
    assert finding["instances"][0]["rank"] == 1
