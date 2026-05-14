# Copyright 2024-2025 IBM Corporation

import pytest

from aiu_trace_analyzer.core.processing import PipelineStage
from aiu_trace_analyzer.pipeline.barrier import BarrierContext


def _noop(event, ctx):
    return [event]


def _noop_b(event, ctx):
    return [event]


def _noop_c(event, ctx):
    return [event]


def _noop_d(event, ctx):
    return [event]


class RegistrationCapture:
    '''Minimal stand-in for EventProcessor that records registration calls.'''

    def __init__(self):
        self.names = []
        self.contexts = []

    def register_stage(self, callback, context=None, **kwargs):
        self.names.append(callback.__name__)
        self.contexts.append(context)


@pytest.fixture
def capture():
    return RegistrationCapture()


registration_order_cases = [
    pytest.param(
        [(_noop, None)], None,
        ["_noop"],
        id="pre_only_no_barrier",
    ),
    pytest.param(
        [(_noop, None)], [(_noop_b, None)],
        ["_noop", "pipeline_barrier", "_noop_b"],
        id="pre_and_post_inserts_barrier",
    ),
    pytest.param(
        [(_noop, None)], [],
        ["_noop"],
        id="empty_post_no_barrier",
    ),
    pytest.param(
        [(_noop, None), (_noop_b, None)],
        [(_noop_c, None), (_noop_d, None)],
        ["_noop", "_noop_b", "pipeline_barrier", "_noop_c", "_noop_d"],
        id="multiple_steps_preserve_order",
    ),
]


@pytest.mark.parametrize("pre_steps, post_steps, expected_names", registration_order_cases)
def test_registration_order(capture, pre_steps, post_steps, expected_names):
    PipelineStage(pre_steps=pre_steps, post_steps=post_steps).register(capture)
    assert capture.names == expected_names


def test_context_objects_are_passed_through(capture):
    ctx_a, ctx_b = object(), object()
    PipelineStage(
        pre_steps=[(_noop, ctx_a)],
        post_steps=[(_noop_b, ctx_b)],
    ).register(capture)
    assert capture.contexts[0] is ctx_a
    assert capture.contexts[2] is ctx_b


def test_each_instance_has_independent_barrier():
    cap1 = RegistrationCapture()
    cap2 = RegistrationCapture()
    PipelineStage(pre_steps=[(_noop, None)], post_steps=[(_noop_b, None)]).register(cap1)
    PipelineStage(pre_steps=[(_noop, None)], post_steps=[(_noop_b, None)]).register(cap2)
    assert isinstance(cap1.contexts[1], BarrierContext)
    assert isinstance(cap2.contexts[1], BarrierContext)
    assert cap1.contexts[1] is not cap2.contexts[1]


def test_barrier_passed_to_pipeline_barrier_is_a_BarrierContext(capture):
    PipelineStage(pre_steps=[(_noop, None)], post_steps=[(_noop_b, None)]).register(capture)
    assert isinstance(capture.contexts[1], BarrierContext)
