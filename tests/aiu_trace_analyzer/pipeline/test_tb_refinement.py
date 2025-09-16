# Copyright 2024-2025 IBM Corporation

import pytest

from aiu_trace_analyzer.types import TraceEvent, GlobalIngestData, InputDialectFLEX
from aiu_trace_analyzer.export.exporter import JsonFileTraceExporter
from aiu_trace_analyzer.pipeline.hashqueue import AbstractHashQueueContext
from aiu_trace_analyzer.pipeline.tb_refinement import RefinementContext


@pytest.fixture
def tb_ctx(tmp_path):
    exporter = JsonFileTraceExporter(f'{tmp_path}/tb_test_out.json')
    return RefinementContext(exporter)

list_events_test_heavy = [
    # regular, simple case for fn-idx removal
    ({"name":"event_123", "pid": 1, "tid": 1, "cat": "kernel", "args": {"TS1": 12345}},
     {"name":"event", "pid": 1, "tid": 1, "args": {"orig_name": "event_123"}}),

    # testing for only replacing the first index and keep the rest
    ({"name":"event_123[sync=sgroup_0_s2_321]", "pid": 1, "tid": 1, "args": {"TS1": 12345}},
     {"name":"event[sync=sgroup_s]", "pid": 1, "tid": 1, "args": {"orig_name": "event_123[sync=sgroup_0_s2_321]"}} ),

    # testing existing fn_idx will be overwritten if it already exists
    ({"name":"event_123", "pid": 1, "tid": 1, "args": {"TS1": 12345, "orig_name": "event_321"}},
     {"name":"event", "pid": 1, "tid": 1, "args": {"orig_name": "event_123"}}),

    # testing with event name without idx
    ({"name":"eventname", "pid": 1, "tid": 1, "args": {"TS1": 12345}},
     {"name":"eventname", "pid": 1, "tid": 1, "args": {}}),

    # testing not adding new items if nothing to do
    ({"name":"eventname", "pid": 1, "tid": 1, "args": {}},
     {"name":"eventname", "pid": 1, "tid": 1, "args": {}}),

    # testing the coll-tid replacement
    ({"name":"event_123", "pid": 1, "tid": "coll1", "args": {}},
     {"name":"event_123", "pid": 1, "tid": 10001, "args": {}}),
]

### @pytest.mark.parametrize('generate_event_list, explen', list_test_cases, indirect=['generate_event_list'])

@pytest.mark.parametrize('flex_event_with_jobhash, reference', list_events_test_heavy, indirect=['flex_event_with_jobhash'])
def test_tb_refinement_heavy(flex_event_with_jobhash: TraceEvent, reference: TraceEvent, tb_ctx):
    result = tb_ctx.update_event_data_heavy(flex_event_with_jobhash)

    assert result["name"] == reference["name"]

    if "args" not in reference:
        assert "args" not in result
    else:
        assert "args" in result
        if "orig_name" in reference["args"]:
            assert "orig_name" in result["args"]
            assert result["args"]["orig_name"] == reference["args"]["orig_name"]
        else:
            assert "orig_name" not in result["args"]
