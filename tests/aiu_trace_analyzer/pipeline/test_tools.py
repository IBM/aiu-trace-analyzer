# Copyright 2024-2025 IBM Corporation

import pytest

from aiu_trace_analyzer.pipeline.tools import PipelineContextTool
from aiu_trace_analyzer.types import TraceEvent, GlobalIngestData
from aiu_trace_analyzer.dialect import InputDialectFLEX, InputDialectTORCH


@pytest.fixture
def tool_base():
    return PipelineContextTool()


test_cases = [
    (
        "test_file.json", "summary", "test_file_summary.csv"
    ),
    (
        "test_file", "extension", "test_file_extension.csv"
    ),
    pytest.param("", None, None, marks=pytest.mark.xfail)
]


@pytest.mark.parametrize("fname_base, category, result", test_cases)
def test_generate_filename(fname_base, category, result, tool_base):
    assert tool_base.generate_filename(fname_base, category) == result


_flex_job = GlobalIngestData.add_job_info("tools_test_flex.json", InputDialectFLEX())
_torch_job = GlobalIngestData.add_job_info("tools_test_torch.pt.trace.json", InputDialectTORCH())


def _cat_event(name: str, job: int, cat: str = None, **args) -> TraceEvent:
    event = TraceEvent({"ph": "X", "name": name, "ts": 1.0, "dur": 1.0, "args": {"jobhash": job, **args}})
    if cat:
        event["cat"] = cat
    return event


@pytest.mark.parametrize("event, category, result", [
    # '-' entries: the category does not exist in the dialect
    (_cat_event("op Cmpt Prep", _torch_job, cat="kernel"), "acc_compute_prep", False),
    (_cat_event("aiuLaunchControlBlocks", _flex_job), "acc_launch_cb", False),
    (_cat_event("op Cmpt Prep", _flex_job), "acc_compute_prep", True),
    # plain name
    (_cat_event("Execute Graph", _flex_job), "acc_graph_exec", True),
    (_cat_event("Execute Graph 2", _flex_job), "acc_graph_exec", False),
    # 'is' regex
    (_cat_event("op Cmpt Exec", _flex_job), "acc_kernel", True),
    (_cat_event("op Cmpt Exec[sync=s0]", _flex_job), "acc_kernel", False),
    (_cat_event("op", _torch_job, cat="kernel"), "acc_kernel", True),
    (_cat_event("op", _torch_job), "acc_kernel", False),
    # 'has'
    (_cat_event("op Cmpt Exec", _flex_job, TS1="1"), "acc_event_cat", True),
    (_cat_event("op Cmpt Exec", _flex_job), "acc_event_cat", False),
    # no dialect
    (TraceEvent({"ph": "X", "name": "op Cmpt Exec"}), "acc_kernel", False),
])
def test_is_category(event, category, result):
    # PipelineContextTool delegates to aiu_trace_analyzer.dialect
    assert PipelineContextTool.is_category(event, category) is result
