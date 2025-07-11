# Copyright 2024-2025 IBM Corporation

import pytest
import json

from aiu_trace_analyzer.core.acelyzer import Acelyzer

def test_default_is_everything():
    args_list = ["-i", "api://jsonbuffer", "--disable_file", "--tb"]

    with open("tests/test_data/basic_event_test_cases.json", 'r') as sourcefile:
        json_data = json.load(sourcefile)

    jsonbuffer = json.dumps(json_data).encode()

    ace = Acelyzer(args_list, in_data=jsonbuffer)
    assert ace is not None

    ace.run()

    data = ace.get_output_data()
    assert isinstance(data, str), "Return data is not a dict."

    result = json.loads(data)
    print(result.keys())
    assert "traceEvents" in result, "Return data is missing TraceEvents"
