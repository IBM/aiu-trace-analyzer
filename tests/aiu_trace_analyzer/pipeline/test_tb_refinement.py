# Copyright 2024-2025 IBM Corporation

import pytest

from aiu_trace_analyzer.types import TraceEvent
from aiu_trace_analyzer.export.exporter import JsonFileTraceExporter
from aiu_trace_analyzer.pipeline.hashqueue import AbstractHashQueueContext
from aiu_trace_analyzer.pipeline.tb_refinement import RefinementContext


@pytest.fixture
def tb_ctx(tmp_path):
    exporter = JsonFileTraceExporter(f'{tmp_path}/tb_test_out.json')
    return RefinementContext(exporter)



list_events_test_heavy = [
    # regular, simple case for fn-idx removal
    ({"name":"event_123", "pid": 1, "tid": 1, "args": {}},
     {"name":"event_[N]", "pid": 1, "tid": 1, "args": {"fn_idx": "123"}}),

    # testing for only replacing the first index and keep the rest
    ({"name":"event_123[sync=sgroup_0_s2_321]", "pid": 1, "tid": 1, "args": {}},
     {"name":"event_[N][sync=sgroup_0_s2_321]", "pid": 1, "tid": 1, "args": {"fn_idx": "123"}} ),

    # testing existing fn_idx will be overwritten if it already exists
    ({"name":"event_123", "pid": 1, "tid": 1, "args": {"fn_idx": "321"}},
     {"name":"event_[N]", "pid": 1, "tid": 1, "args": {"fn_idx": "123"}}),

    # testing with event name without idx
    ({"name":"eventname", "pid": 1, "tid": 1, "args": {}},
     {"name":"eventname", "pid": 1, "tid": 1, "args": {}}),

    # testing not adding new items if nothing to do
    ({"name":"eventname", "pid": 1, "tid": 1},
     {"name":"eventname", "pid": 1, "tid": 1}),

    # testing the coll-tid replacement
    ({"name":"event_123", "pid": 1, "tid": "coll1", "args": {}},
     {"name":"event_[N]", "pid": 1, "tid": 10001, "args": {"fn_idx": "123"}}),
]

@pytest.mark.parametrize('event_in, reference', list_events_test_heavy)
def test_tb_refinement_heavy(event_in: TraceEvent, reference: TraceEvent, tb_ctx):
    result = tb_ctx.update_event_data_heavy(event_in)

    assert result["name"] == reference["name"]

    if "args" not in reference:
        assert "args" not in result
    else:
        assert "args" in result
        if "fn_idx" in reference["args"]:
            assert "fn_idx" in result["args"]
            assert result["args"]["fn_idx"] == reference["args"]["fn_idx"]
        else:
            assert "fn_idx" not in result["args"]
