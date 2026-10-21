# Copyright 2026 IBM Corporation

import pytest

from aiu_trace_analyzer.dialect import InputDialectFLEX, InputDialectTORCH
from aiu_trace_analyzer.pipeline.categorize import (
    CATEGORY_PID, CategoryTrackContext, EventCategorizerContext, EventClass, _PRIORITY_OF_CLASS,
    category_track, event_categorizer, event_categorizer_update, _PriorityTrack)
from aiu_trace_analyzer.trace_view import AbstractEventType
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
    ("cpu_op", _ALLREDUCE_KERNEL, EventClass.CPU_OP),
    ("gpu_memcpy", "Memcpy (HtoD)", EventClass.DATA_IN),
    ("gpu_memcpy", "Memcpy (DtoH)", EventClass.DATA_OUT),
    ("gpu_memcpy", "Memcpy (DtoD)", EventClass.DATA_OTHER),
    ("gpu_memset", "Memset (Device)", EventClass.MEM_ALLOC_FREE),
    ("privateuse1_driver", "Memory (Release)", EventClass.MEM_ALLOC_FREE),
    ("privateuse1_driver", "Memory (Alloc)", EventClass.MEM_ALLOC_FREE),
    ("privateuse1_driver", "Memory (Map)", EventClass.DATA_OTHER),
    ("privateuse1_runtime", "Memory (Release)", EventClass.CPU_OP),
    ("privateuse1_runtime", "aiuDataConvert", EventClass.SEN_DATA_CONVERT),
    ("user_annotation", "execute_context_1(84)_generation_0(0)", EventClass.CPU_OP),
    ("cpu_op", "aten::mm", EventClass.CPU_OP),
    ("Trace", "PyTorch Profiler (0)", EventClass.CPU_OP),
    ("python_function", "forward", EventClass.CPU_OP),
    ("gpu_user_annotation", "device annotation", EventClass.OTHER),
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
    ("cpu_op", "aten::mm"),
    ("user_annotation", "execute_context_1(84)_generation_0(0)"),
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


def _classified(rank: int, cat: str = "kernel", cls: str = "COMPUTE_EXEC",
                ts: float = 1.0, dur: float = 1.0) -> TraceEvent:
    ev = _event(cat, "spyre_kernel_v1_fused_mm", rank=rank)
    ev["ts"], ev["dur"] = ts, dur
    ev["args"]["class"] = cls
    return ev


def _iv(start: float, end: float, cls: str) -> tuple:
    return (start, end, _PRIORITY_OF_CLASS[cls], cls)


def _resolve(intervals: list[tuple]) -> list[tuple]:
    track = _PriorityTrack()
    result = []
    for interval in sorted(intervals, key=lambda i: i[0]):
        result += track.add(*interval)
    return result + track.flush()


@pytest.mark.parametrize("intervals, expected", [
    # single interval
    ([_iv(0, 10, "DATA_IN")], [(0, 10, "DATA_IN")]),
    # disjoint intervals leave the gap empty
    ([_iv(0, 4, "DATA_IN"), _iv(6, 8, "DATA_IN")], [(0, 4, "DATA_IN"), (6, 8, "DATA_IN")]),
    # adjacent intervals of the same class merge
    ([_iv(0, 4, "DATA_IN"), _iv(4, 8, "DATA_IN")], [(0, 8, "DATA_IN")]),
    # higher priority embedded in a lower one splits it
    ([_iv(0, 10, "MEM_ALLOC_FREE"), _iv(3, 5, "COMPUTE_EXEC")],
     [(0, 3, "MEM_ALLOC_FREE"), (3, 5, "COMPUTE_EXEC"), (5, 10, "MEM_ALLOC_FREE")]),
    # lower priority embedded in a higher one is hidden
    ([_iv(0, 10, "COMPUTE_EXEC"), _iv(3, 5, "DATA_IN")], [(0, 10, "COMPUTE_EXEC")]),
    # partial overlap: the higher priority one wins the shared part
    ([_iv(0, 6, "DATA_IN"), _iv(4, 10, "COLLECTIVE_EXEC")], [(0, 4, "DATA_IN"), (4, 10, "COLLECTIVE_EXEC")]),
    ([_iv(0, 6, "COLLECTIVE_EXEC"), _iv(4, 10, "DATA_IN")], [(0, 6, "COLLECTIVE_EXEC"), (6, 10, "DATA_IN")]),
    # several events active at once: the slice falls back to the next in priority when the winner ends
    ([_iv(0, 10, "MEM_ALLOC_FREE"), _iv(1, 8, "DATA_IN"), _iv(2, 4, "COMPUTE_EXEC")],
     [(0, 1, "MEM_ALLOC_FREE"), (1, 2, "DATA_IN"), (2, 4, "COMPUTE_EXEC"), (4, 8, "DATA_IN"),
      (8, 10, "MEM_ALLOC_FREE")]),
    ([_iv(0, 6, "COMPUTE_EXEC"), _iv(4, 10, "COLLECTIVE_EXEC")], [(0, 6, "COMPUTE_EXEC"), (6, 10, "COLLECTIVE_EXEC")]),
    # a covered same-class sandwich keeps one merged slice
    ([_iv(0, 10, "COMPUTE_EXEC"), _iv(2, 4, "COMPUTE_EXEC")], [(0, 10, "COMPUTE_EXEC")]),
    # classes outside the priority list lose against all listed ones
    ([_iv(0, 10, "MAIU_BARRIER"), _iv(2, 4, "MEM_ALLOC_FREE")],
     [(0, 2, "MAIU_BARRIER"), (2, 4, "MEM_ALLOC_FREE"), (4, 10, "MAIU_BARRIER")]),
    ([], []),
], ids=["single", "gap", "adjacent-merge", "split", "hidden", "partial-late-high", "partial-early-high",
        "partial-compute-over-coll", "stacked", "same-class-sandwich", "unlisted-class", "empty"])
def test_priority_track(intervals, expected):
    assert _resolve(intervals) == expected


def test_priority_order_matches_blamed_breakdown():
    order = [str(c) for c in (EventClass.COMPUTE_EXEC, EventClass.COLLECTIVE_EXEC, EventClass.DATA_IN,
                              EventClass.MEM_ALLOC_FREE)]
    prios = [_PRIORITY_OF_CLASS[c] for c in order]
    assert prios == sorted(prios)
    assert _PRIORITY_OF_CLASS[str(EventClass.CPU_OP)] > _PRIORITY_OF_CLASS[str(EventClass.MEM_ALLOC_FREE)]


def test_category_track_passes_events_through_and_emits_on_drain():
    ctx = CategoryTrackContext()
    src = _classified(rank=3, ts=5.0, dur=2.0)
    assert category_track(src, ctx) == [src]   # the slice is not final before more events or the drain

    out = [e for e in ctx.drain() if e["ph"] == "X"]
    assert len(out) == 1
    assert out[0]["name"] == "COMPUTE_EXEC"
    assert (out[0]["ts"], out[0]["dur"]) == (5.0, 2.0)
    assert out[0]["pid"] == CATEGORY_PID
    assert out[0]["tid"] == 3


@pytest.mark.parametrize("event", [
    _event("kernel", "no_class_yet"),                                        # not classified
    _classified(0, cat="user_annotation", cls="CPU_OP"),                     # scope marker
    _classified(0, cat="Trace", cls="CPU_OP"),                               # scope marker
    TraceEvent({"ph": "C", "ts": 1.0, "pid": 0, "name": "ctr", "args": {}}),  # not an X event
])
def test_category_track_ignores_other_events(event):
    ctx = CategoryTrackContext()
    assert category_track(event, ctx) == [event]
    assert ctx.drain() == []


def test_category_track_ignores_zero_duration():
    ctx = CategoryTrackContext()
    category_track(_classified(0, dur=0.0), ctx)
    assert ctx.drain() == []


def test_category_track_resolves_overlap_per_rank():
    ctx = CategoryTrackContext()
    # rank 0: compute inside a transfer; rank 1: only a transfer over the same time span
    category_track(_classified(0, cat="gpu_memcpy", cls="DATA_IN", ts=0.0, dur=10.0), ctx)
    category_track(_classified(0, cls="COMPUTE_EXEC", ts=4.0, dur=2.0), ctx)
    category_track(_classified(1, cat="gpu_memcpy", cls="DATA_IN", ts=0.0, dur=10.0), ctx)

    slices = [(e["tid"], e["name"], e["ts"], e["dur"]) for e in ctx.drain() if e["ph"] == "X"]
    assert sorted(slices) == [
        (0, "COMPUTE_EXEC", 4.0, 2.0),
        (0, "DATA_IN", 0.0, 4.0),
        (0, "DATA_IN", 6.0, 4.0),
        (1, "DATA_IN", 0.0, 10.0),
    ]


def test_category_track_metadata_once():
    ctx = CategoryTrackContext()
    category_track(_classified(0), ctx)
    category_track(_classified(1), ctx)
    category_track(_classified(1, ts=5.0), ctx)

    meta = [e for e in ctx.drain() if e["ph"] == "M"]
    names = [(m["name"], m.get("tid")) for m in meta]
    assert ("process_name", None) in names
    assert ("thread_name", 0) in names and ("thread_name", 1) in names
    assert len(meta) == 4
    assert all(m["pid"] == CATEGORY_PID for m in meta)
    assert ctx.drain() == []


def test_category_track_metadata_survives_event_conversion():
    # rank 0 is falsy: its thread_name must keep the tid through the dict -> TraceView conversion
    ctx = CategoryTrackContext()
    category_track(_classified(0), ctx)
    category_track(_classified(1), ctx)

    converted = {m["args"]["name"]: AbstractEventType.from_dict(m)
                 for m in ctx.drain() if m["name"] == "thread_name"}
    assert converted["Rank 0"].tid == 0
    assert converted["Rank 1"].tid == 1


def test_category_track_emits_slices_while_streaming():
    ctx = CategoryTrackContext()
    emitted = []
    for ev in [_classified(0, cls="DATA_IN", cat="gpu_memcpy", ts=0.0, dur=10.0),
               _classified(0, cls="COMPUTE_EXEC", ts=4.0, dur=2.0),
               _classified(0, cls="COMPUTE_EXEC", ts=7.0, dur=1.0)]:
        emitted += [e for e in category_track(ev, ctx) if e["cat"] == "category"]

    # final slices leave before the drain; the slice still being extended is held back
    assert [(e["name"], e["ts"], e["dur"]) for e in emitted] == [
        ("DATA_IN", 0.0, 4.0), ("COMPUTE_EXEC", 4.0, 2.0)]
    rest = [(e["name"], e["ts"], e["dur"]) for e in ctx.drain() if e["ph"] == "X"]
    assert rest == [("DATA_IN", 6.0, 1.0), ("COMPUTE_EXEC", 7.0, 1.0), ("DATA_IN", 8.0, 2.0)]


def test_category_track_requires_sorted_events():
    ctx = CategoryTrackContext()
    category_track(_classified(0, ts=5.0), ctx)
    with pytest.raises(AssertionError):
        category_track(_classified(0, ts=1.0), ctx)


def test_category_track_rounds_boundaries_without_overlap():
    # large timestamps with sub-ns noise in the boundaries, as in real traces (~1e12 us)
    base = 1216487644535.0
    ctx = CategoryTrackContext()
    category_track(_classified(0, cat="gpu_memcpy", cls="DATA_OUT", ts=base + 0.55, dur=0.173), ctx)
    category_track(_classified(0, cat="gpu_memcpy", cls="DATA_IN", ts=base + 0.5662, dur=3.116), ctx)

    slices = sorted((e for e in ctx.drain() if e["ph"] == "X"), key=lambda e: e["ts"])
    assert [e["name"] for e in slices] == ["DATA_OUT", "DATA_IN"]
    for e in slices:
        assert e["ts"] == round(e["ts"], 3) and e["dur"] == round(e["dur"], 3)
    # at ~1e12 us a double resolves only ~0.2 ns: compare the way a viewer sees it, rounded to ns
    assert round(slices[0]["ts"] + slices[0]["dur"], 3) <= slices[1]["ts"]


def test_category_track_drops_slices_vanishing_by_rounding():
    ctx = CategoryTrackContext()
    category_track(_classified(0, cls="MEM_ALLOC_FREE", ts=0.0, dur=10.0), ctx)
    category_track(_classified(0, cls="COMPUTE_EXEC", ts=5.0001, dur=0.0002), ctx)

    slices = [e for e in ctx.drain() if e["ph"] == "X"]
    assert all(e["dur"] > 0.0 for e in slices)
    assert "COMPUTE_EXEC" not in [e["name"] for e in slices]


def test_category_track_host_events_fill_gaps_only():
    ctx = CategoryTrackContext()
    emitted = []
    # a long host op with a kernel and a copy running inside, then a gap, then OTHER overlapped by a host op
    for ev in [_classified(0, cat="cpu_op", cls="CPU_OP", ts=0.0, dur=10.0),
               _classified(0, cat="kernel", cls="COMPUTE_EXEC", ts=2.0, dur=2.0),
               _classified(0, cat="gpu_memcpy", cls="DATA_IN", ts=3.0, dur=3.0),
               _classified(0, cat="privateuse1_runtime", cls="OTHER", ts=12.0, dur=1.0),
               _classified(0, cat="cpu_op", cls="CPU_OP", ts=12.5, dur=1.0)]:
        emitted += [e for e in category_track(ev, ctx) if e["cat"] == "category"]

    slices = [(e["name"], e["ts"], e["dur"]) for e in emitted + ctx.drain() if e["ph"] == "X"]
    assert slices == [
        ("CPU_OP", 0.0, 2.0),
        ("COMPUTE_EXEC", 2.0, 2.0),
        ("DATA_IN", 4.0, 2.0),
        ("CPU_OP", 6.0, 4.0),      # 10.0 - 12.0 stays empty
        ("OTHER", 12.0, 0.5),
        ("CPU_OP", 12.5, 1.0),
    ]


@pytest.mark.parametrize("higher, lower", [
    ("MEM_ALLOC_FREE", "CPU_OP"),
    ("COMPUTE_EXEC", "CPU_OP"),
    ("MEM_ALLOC_FREE", "SEN_DATA_CONVERT"),
    ("SEN_DATA_CONVERT", "CPU_OP"),
    ("CPU_OP", "OTHER"),
    ("OTHER", "MAIU_BARRIER"),
])
def test_host_and_other_classes_rank_below_device_classes(higher, lower):
    assert _PRIORITY_OF_CLASS[higher] < _PRIORITY_OF_CLASS[lower]


def test_classes_missing_from_priority_table_rank_lowest():
    from aiu_trace_analyzer.pipeline.categorize import _CATEGORY_PRIORITY
    listed = {str(c) for c in _CATEGORY_PRIORITY}
    lowest_listed = max(_PRIORITY_OF_CLASS[c] for c in listed)
    unlisted = [c for c in _PRIORITY_OF_CLASS if c not in listed]
    assert unlisted
    assert all(_PRIORITY_OF_CLASS[c] > lowest_listed for c in unlisted)
