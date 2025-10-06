# Copyright 2024-2025 IBM Corporation

import pytest

from aiu_trace_analyzer.pipeline.time_align import TimeAlignmentContext, PTorchToFlexTimeMapping


@pytest.fixture
def ta_ctx(tmp_path):
    return TimeAlignmentContext()


list_filter_priority = [
    # single-priority test
    ([(0.1, 0.1, 0), (0.1, 0.1, 0), (0.2, 0.1, 0), (0.3, 0.1, 0)],
     [(0.1, 0.1, 0), (0.1, 0.1, 0), (0.2, 0.1, 0), (0.3, 0.1, 0)]),
    # multi-priority list tests
    ([(0.1, 0.1, 0), (0.1, 0.1, 1), (0.2, 0.1, 0), (0.3, 0.1, 2)],
     [(0.1, 0.1, 0), (0.2, 0.1, 0)]),
    ([(0.1, 0.1, 2), (0.1, 0.1, 1), (0.2, 0.1, 1), (0.3, 0.1, 2)],
     [(0.1, 0.1, 1), (0.2, 0.1, 1)]),
]


@pytest.mark.parametrize('data, expected', list_filter_priority)
def test_filter_priority(data: tuple[float, float, int], expected: tuple[float, float, int], ta_ctx):
    ta_ctx.queues[0] = PTorchToFlexTimeMapping(0)
    result = ta_ctx.queues[0].filter_priority(data=data)

    assert result == expected


list_compute_offset = [
    # regular test with equal an inequal list length
    ([(0.1, 0.5, 0), (1.0, 0.6, 0), (2.0, 0.5, 0), (3.0, 0.4, 0)],
     [(100.1, 0.4), (101.0, 0.5), (102, 0.45, 0), (103.0, 0.3, 0)],
     -99.95),
    ([(0.1, 0.5, 0), (1.0, 0.6, 0), (2.0, 0.5, 0), (3.0, 0.4, 0)],
     [(100.1, 0.4), (101.0, 0.5)],
     -99.90),

    # testing with priorities and low-prio events appearing first
    ([(2.0, 0.5, 1), (3.0, 0.4, 1), (0.1, 0.5, 0), (1.0, 0.6, 0)],
     [(100.1, 0.4), (101.0, 0.5)],
     -99.90),

    # testing with one list empty
    ([(0.1, 0.5, 0), (1.0, 0.6, 0), (2.0, 0.5, 0), (3.0, 0.4, 0)],
     [],
     0.0),
    ([],
     [(0.1, 0.5), (1.0, 0.6), (2.0, 0.5), (3.0, 0.4)],
     0.0),
]


@pytest.mark.parametrize('torch, flex, expected_offset', list_compute_offset)
def test_compute_offset(
        torch: list[tuple[float, float, int]],
        flex: list[tuple[float, float]],
        expected_offset: float, ta_ctx):
    ta_ctx.queues[0] = PTorchToFlexTimeMapping(0)

    ta_ctx.queues[0].flex_tsdur = flex
    ta_ctx.queues[0].ptorch_tsdur = torch

    ta_ctx.queues[0].compute_offset()
    result = ta_ctx.queues[0].flex_offset

    assert result == expected_offset
