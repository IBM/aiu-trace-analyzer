# Copyright 2026 IBM Corporation

import pytest

from aiu_trace_analyzer.dialect import InputDialectFLEX, InputDialectTORCH
from aiu_trace_analyzer.pipeline.categorize import (
    EventCategorizerContext, EventClass, event_categorizer, event_categorizer_update)
from aiu_trace_analyzer.types import GlobalIngestData, TraceEvent

GlobalIngestData()
_FLEX_JOB = GlobalIngestData.add_job_info("categorize_test_flex.json", InputDialectFLEX())
_TORCH_JOB = GlobalIngestData.add_job_info("categorize_test_torch.pt.trace.json", InputDialectTORCH())

_ALLREDUCE_KERNEL = "/tmp/session/spyre-comms/ops/rank_0/key_wsi_1/bundle_Allreduce_1/sdsc_add_0/bundle.mlir"


def _event(cat: str, name: str, job: int = _TORCH_JOB, rank: int = 0, **args) -> TraceEvent:
    return TraceEvent({
        "ph": "X", "cat": cat, "name": name, "ts": 1.0, "dur": 1.0, "pid": rank,
        "args": {"jobhash": job, "rank": rank, **args}})


@pytest.mark.parametrize("cat, name, expected", [
    ("kernel", "spyre_kernel_v1_fused_mm_slice_view_t4bsh522pavkslox#2", EventClass.COMPUTE_EXEC),
    ("kernel", "null_op", EventClass.COMPUTE_EXEC),
    ("kernel", _ALLREDUCE_KERNEL, EventClass.COLLECTIVE_EXEC),
    ("kernel", "ncclAllreduce", EventClass.COLLECTIVE_EXEC),
    ("cpu_op", _ALLREDUCE_KERNEL, EventClass.OTHER),
    ("gpu_memcpy", "Memcpy (HtoD)", EventClass.DATA_IN),
    ("gpu_memcpy", "Memcpy (DtoH)", EventClass.DATA_OUT),
    ("gpu_memcpy", "Memcpy (DtoD)", EventClass.DATA_OTHER),
    ("gpu_memset", "Memset (Device)", EventClass.MEM_ALLOC_FREE),
    ("privateuse1_driver", "Memory (Release)", EventClass.MEM_ALLOC_FREE),
    ("privateuse1_driver", "Memory (Alloc)", EventClass.MEM_ALLOC_FREE),
    ("privateuse1_driver", "Memory (Map)", EventClass.DATA_OTHER),
    ("privateuse1_runtime", "Memory (Release)", EventClass.OTHER),
    ("privateuse1_runtime", "aiuDataConvert", EventClass.SEN_DATA_CONVERT),
    ("user_annotation", "execute_context_1(84)_generation_0(0)", EventClass.OTHER),
    ("cpu_op", "aten::mm", EventClass.OTHER),
])
@pytest.mark.parametrize("rank", [0, 1])
def test_classify_torch(cat, name, expected, rank):
    ctx = EventCategorizerContext()
    event = _event(cat, name, rank=rank)
    assert ctx.classify_event(event) == expected
    assert ctx.get_event_class(event) == expected


@pytest.mark.parametrize("cat, name", [
    ("kernel", _ALLREDUCE_KERNEL),
    ("gpu_memcpy", "Memcpy (DtoD)"),
    ("gpu_memset", "Memset (Device)"),
    ("privateuse1_driver", "Memory (Release)"),
])
def test_torch_only_rules_do_not_apply_to_flex(cat, name):
    ctx = EventCategorizerContext()
    assert ctx.classify_event(_event(cat, name, job=_FLEX_JOB)) == EventClass.OTHER


@pytest.mark.parametrize("name, expected", [
    ("Cmpt Prep", EventClass.COMPUTE_PREP),
    ("Cmpt Exec", EventClass.COMPUTE_EXEC),
    ("Flex RoundTrip", EventClass.ROUNDTRIP_FLEX),
    ("AIU Roundtrip", EventClass.ROUNDTRIP_AIU),
    ("Core DmaI", EventClass.DATA_IN),
    ("Core DmaO", EventClass.DATA_OUT),
    ("Barrier: x", EventClass.MAIU_BARRIER),
    ("something else", EventClass.OTHER),
])
def test_classify_flex_unchanged(name, expected):
    ctx = EventCategorizerContext()
    assert ctx.classify_event(_event("", name, job=_FLEX_JOB, TS1=1)) == expected


def test_event_without_name_is_other():
    assert EventCategorizerContext().get_event_class(TraceEvent({"ph": "X", "args": {}})) == EventClass.OTHER


def test_two_rank_stage_run_keeps_classes_per_rank():
    ctx = EventCategorizerContext()
    events = [_event("kernel", _ALLREDUCE_KERNEL, rank=r, correlation=7) for r in (0, 1)] + \
             [_event("kernel", "null_op", rank=r, correlation=7) for r in (0, 1)]
    for e in events:
        event_categorizer(e, ctx)
    out = [event_categorizer_update(e, ctx)[0] for e in events]
    assert [e["args"]["class"] for e in out] == [
        str(EventClass.COLLECTIVE_EXEC)] * 2 + [str(EventClass.COMPUTE_EXEC)] * 2
    assert set(ctx.first_ts_per_rank) == {0, 1}
