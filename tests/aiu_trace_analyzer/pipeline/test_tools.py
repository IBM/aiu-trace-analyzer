# Copyright 2024-2025 IBM Corporation

import pytest

from aiu_trace_analyzer.pipeline.tools import PipelineContextTool


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
