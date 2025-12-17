# Copyright 2024-2025 IBM Corporation

import json
import pytest

from aiu_trace_analyzer.core.acelyzer import Acelyzer


@pytest.fixture
def default_acelyzer(tmp_path):
    return Acelyzer(in_args=[
        "-i",
        "tests/test_data/basic_event_test_cases.json",
        "-o",
        f"{tmp_path}/default_test.json"])


def test_default_profile_is_everything(tmp_path):
    args_list = ["-i", "api://jsonbuffer", "--disable_file", "--tb", "-o", f"{tmp_path}/only_tables.json"]

    with open("tests/test_data/basic_event_test_cases.json", 'r') as sourcefile:
        json_data = json.load(sourcefile)

    jsonbuffer = json.dumps(json_data).encode()

    ace = Acelyzer(args_list, in_data=jsonbuffer)
    assert ace is not None

    ace.run()

    data = ace.get_output_data()
    assert isinstance(data, str), "Return data is not a dict."

    result = json.loads(data)
    assert "traceEvents" in result, "Return data is missing TraceEvents"


@pytest.mark.parametrize("arg, expected_attr", [
    ("drop",  "OVERLAP_RESOLVE_DROP"),
    ("tid",   "OVERLAP_RESOLVE_TID"),
    ("async", "OVERLAP_RESOLVE_ASYNC"),
    ("warn",  "OVERLAP_RESOLVE_WARN"),
    ("shift", "OVERLAP_RESOLVE_SHIFT"),
])
def test_overlap_option_valid(default_acelyzer: Acelyzer, arg, expected_attr):
    """
    For each valid argument, ensure the static method returns the correct constant value
    and that the type is int.
    """
    import aiu_trace_analyzer.pipeline as event_pipe
    expected = getattr(event_pipe.OverlapDetectionContext, expected_attr)

    result = default_acelyzer._overlap_option_from_arg(arg)
    assert result == expected
    assert isinstance(result, int)


def test_overlap_option_invalid(default_acelyzer: Acelyzer):
    """
    Invalid arguments must raise ValueError with the exact (updated) message.
    """
    with pytest.raises(ValueError) as exc:
        default_acelyzer._overlap_option_from_arg("invalid")
    assert "UNRECOGNIZED Overlap Option" in str(exc.value)
