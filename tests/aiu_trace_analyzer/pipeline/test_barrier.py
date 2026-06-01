# Copyright 2024-2025 IBM Corporation

import pytest

from aiu_trace_analyzer.pipeline import pipeline_barrier
from aiu_trace_analyzer.pipeline.barrier import _main_barrier_context


list_test_cases = [
    (("X", 10), 10)
]


@pytest.mark.parametrize('generate_event_list, explen', list_test_cases, indirect=['generate_event_list'])
def test_pipeline_barrier(generate_event_list, explen):
    assert len(generate_event_list) == explen

    for e in generate_event_list:
        assert pipeline_barrier(e, None) == []

    assert len(_main_barrier_context.hold) == explen

    held_list = _main_barrier_context.drain()

    for a, b in zip(generate_event_list, held_list):
        assert a == b

    assert _main_barrier_context.hold == []
