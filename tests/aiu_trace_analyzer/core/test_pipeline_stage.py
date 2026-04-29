# Copyright 2024-2025 IBM Corporation

from aiu_trace_analyzer.core.processing import PipelineStage
from aiu_trace_analyzer.pipeline.barrier import BarrierContext


class RegistrationCapture:
    def __init__(self):
        self.names = []
        self.contexts = []

    def register_stage(self, callback, context=None, **kwargs):
        self.names.append(callback.__name__)
        self.contexts.append(context)


def _cb_a(event, ctx): return [event]
def _cb_b(event, ctx): return [event]
def _cb_c(event, ctx): return [event]
def _cb_d(event, ctx): return [event]


def test_pre_only_registers_no_barrier():
    cap = RegistrationCapture()
    PipelineStage(pre_steps=[(_cb_a, None)]).register(cap)
    assert cap.names == ["_cb_a"]


def test_pre_and_post_inserts_barrier_between_them():
    cap = RegistrationCapture()
    PipelineStage(
        pre_steps=[(_cb_a, None)],
        post_steps=[(_cb_b, None)],
    ).register(cap)
    assert cap.names == ["_cb_a", "pipeline_barrier", "_cb_b"]


def test_empty_post_list_registers_no_barrier():
    cap = RegistrationCapture()
    PipelineStage(pre_steps=[(_cb_a, None)], post_steps=[]).register(cap)
    assert cap.names == ["_cb_a"]


def test_multiple_pre_and_post_steps_preserve_order():
    cap = RegistrationCapture()
    PipelineStage(
        pre_steps=[(_cb_a, None), (_cb_b, None)],
        post_steps=[(_cb_c, None), (_cb_d, None)],
    ).register(cap)
    assert cap.names == ["_cb_a", "_cb_b", "pipeline_barrier", "_cb_c", "_cb_d"]


def test_context_objects_are_passed_through():
    ctx_a, ctx_b = object(), object()
    cap = RegistrationCapture()
    PipelineStage(
        pre_steps=[(_cb_a, ctx_a)],
        post_steps=[(_cb_b, ctx_b)],
    ).register(cap)
    assert cap.contexts[0] is ctx_a
    assert cap.contexts[2] is ctx_b


def test_each_instance_owns_an_independent_barrier():
    stage1 = PipelineStage(pre_steps=[(_cb_a, None)], post_steps=[(_cb_b, None)])
    stage2 = PipelineStage(pre_steps=[(_cb_a, None)], post_steps=[(_cb_b, None)])
    assert stage1._barrier is not stage2._barrier


def test_barrier_context_passed_to_pipeline_barrier():
    cap = RegistrationCapture()
    stage = PipelineStage(pre_steps=[(_cb_a, None)], post_steps=[(_cb_b, None)])
    stage.register(cap)
    barrier_ctx = cap.contexts[1]
    assert isinstance(barrier_ctx, BarrierContext)
    assert barrier_ctx is stage._barrier
