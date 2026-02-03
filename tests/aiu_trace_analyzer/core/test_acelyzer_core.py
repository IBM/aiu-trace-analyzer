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


class TestParseEventLimitType:
    """Test suite for _parse_event_limit_type method"""

    def test_valid_attributes_list(self, default_acelyzer: Acelyzer):
        """
        Since subsequent tests use attributes from defaults,
        Test that all expected valid attributes are present in defaults
        """
        expected_attrs = {"skip", "count", "ts_start", "ts_end", "no_count_types"}
        actual_attrs = set(default_acelyzer.defaults["event_limits"].keys())
        assert actual_attrs == expected_attrs

    def test_parse_empty_json(self, default_acelyzer: Acelyzer):
        """Test parsing empty JSON object returns defaults"""
        result = default_acelyzer._parse_event_limit_type('{}')
        assert result == default_acelyzer.defaults["event_limits"]

    @pytest.mark.parametrize("json_str,expected_overrides", [
        ('{"skip": 10}', {"skip": 10}),
        ('{"count": 100}', {"count": 100}),
        ('{"ts_start": 1.5}', {"ts_start": 1.5}),
        ('{"ts_end": 100.0}', {"ts_end": 100.0}),
        ('{"no_count_types": "XYZ"}', {"no_count_types": "XYZ"}),
        ('{"skip": 5, "count": 100}', {"skip": 5, "count": 100}),
        ('{"skip": 5, "count": 100, "ts_start": 1.5}', {"skip": 5, "count": 100, "ts_start": 1.5}),
        ('{"skip": 10, "count": 200, "ts_start": 2.0, "ts_end": 100.0, "no_count_types": "XYZ"}',
         {"skip": 10, "count": 200, "ts_start": 2.0, "ts_end": 100.0, "no_count_types": "XYZ"}),
    ])
    def test_parse_valid_attributes(self, default_acelyzer: Acelyzer, json_str, expected_overrides):
        """Test parsing JSON with valid attributes merges with defaults correctly"""
        result = default_acelyzer._parse_event_limit_type(json_str)

        # Check that all expected overrides are present
        for key, value in expected_overrides.items():
            assert result[key] == value

        # Check that non-overridden defaults remain
        for key, default_value in default_acelyzer.defaults["event_limits"].items():
            if key not in expected_overrides:
                assert result[key] == default_value

    @pytest.mark.parametrize("json_str,expected_invalid_attrs", [
        ('{"invalid_attr": 10}', ["invalid_attr"]),
        ('{"invalid1": 10, "invalid2": 20}', ["invalid1", "invalid2"]),
        ('{"skip": 10, "invalid_attr": 20}', ["invalid_attr"]),
        ('{"bad_key": 5, "another_bad": 10, "count": 100}', ["another_bad", "bad_key"]),
    ])
    def test_parse_invalid_attributes(self, default_acelyzer: Acelyzer, json_str, expected_invalid_attrs):
        """Test that invalid attributes raise ValueError with proper error message"""
        with pytest.raises(ValueError) as exc:
            default_acelyzer._parse_event_limit_type(json_str)

        error_msg = str(exc.value)
        assert "Invalid attributes in event_limits" in error_msg
        assert "Valid attributes are" in error_msg

        # Check that all expected invalid attributes are mentioned
        for invalid_attr in expected_invalid_attrs:
            assert invalid_attr in error_msg

    @pytest.mark.parametrize("invalid_input,expected_error,error_substring", [
        ({"skip": 10}, TypeError, "must be a string"),
        (123, TypeError, "must be a string"),
        (['list'], TypeError, "must be a string"),
        ('{"skip": invalid}', ValueError, "Invalid JSON format"),
        ('{invalid json', ValueError, "Invalid JSON format"),
        ('[1, 2, 3]', ValueError, "must be a dictionary"),
        ('"just a string"', ValueError, "must be a dictionary"),
        ('123', ValueError, "must be a dictionary"),
        ('true', ValueError, "must be a dictionary"),
    ])
    def test_parse_invalid_input_types(
            self, default_acelyzer: Acelyzer,
            invalid_input, expected_error, error_substring):
        """Test that invalid input types and formats raise appropriate errors"""
        with pytest.raises(expected_error) as exc:
            default_acelyzer._parse_event_limit_type(invalid_input)
        assert error_substring in str(exc.value)
