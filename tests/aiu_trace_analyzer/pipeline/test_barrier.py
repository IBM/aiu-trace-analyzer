# Copyright 2024-2025 IBM Corporation

import pytest

from aiu_trace_analyzer.pipeline import pipeline_barrier
from aiu_trace_analyzer.pipeline.barrier import BarrierContext


list_test_cases = [
    (("X", 10), 10)
]


@pytest.mark.parametrize('generate_event_list, explen', list_test_cases, indirect=['generate_event_list'])
def test_pipeline_barrier(generate_event_list, explen):
    barrier_ctx = BarrierContext()
    assert len(generate_event_list) == explen

    for e in generate_event_list:
        assert pipeline_barrier(e, barrier_ctx) == []

    assert len(barrier_ctx.hold) == explen

    held_list = barrier_ctx.drain()

    for a, b in zip(generate_event_list, held_list):
        assert a == b

    assert barrier_ctx.hold == []


def test_independent_barrier_contexts_do_not_share_events():
    ctx1 = BarrierContext()
    ctx2 = BarrierContext()
    e1 = {"ph": "X", "ts": 1, "pid": 0, "tid": 0, "name": "a"}
    e2 = {"ph": "X", "ts": 2, "pid": 0, "tid": 0, "name": "b"}

    pipeline_barrier(e1, ctx1)
    pipeline_barrier(e2, ctx2)

    assert ctx1.drain() == [e1]
    assert ctx2.drain() == [e2]
    assert ctx1.hold == []
    assert ctx2.hold == []


def test_barrier_drain_clears_hold():
    ctx = BarrierContext()
    e = {"ph": "X", "ts": 1, "pid": 0, "tid": 0, "name": "a"}
    pipeline_barrier(e, ctx)
    ctx.drain()
    assert ctx.drain() == []
