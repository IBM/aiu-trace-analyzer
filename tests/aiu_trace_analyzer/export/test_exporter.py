# Copyright 2024-2026 IBM Corporation

import json

import pytest

from aiu_trace_analyzer.types import TRACE_ISSUE_EVENT_NAME
from aiu_trace_analyzer.trace_view import AbstractEventType
from aiu_trace_analyzer.export.exporter import JsonFileTraceExporter


@pytest.fixture
def json_exporter() -> JsonFileTraceExporter:
    return JsonFileTraceExporter(target_uri="unused.json")


def _issue_event(name: str, text: str, is_error: bool = False) -> AbstractEventType:
    return AbstractEventType.from_dict({
        "ph": "M", "ts": 0, "pid": 0,
        "name": TRACE_ISSUE_EVENT_NAME,
        "args": {"finding": name, "text": text, "is_error": is_error},
    })


def _instant_event() -> AbstractEventType:
    return AbstractEventType.from_dict({
        "ph": "i", "ts": 1, "pid": 0, "tid": 0, "s": "g",
        "name": "regular_event", "args": {},
    })


def test_export_captures_issue_events(json_exporter):
    text = "OVC: Detected 3 event(s) with long duration."
    json_exporter.export([_issue_event("long_dur", text)])

    other_data = json.loads(json_exporter.get_data())["otherData"]
    assert other_data["warnings"] == [{"finding": "long_dur", "text": text}]


def test_export_separates_errors_from_warnings(json_exporter):
    json_exporter.export([_issue_event("long_dur", "warn text"),
                          _issue_event("bad_ts", "error text", is_error=True)])

    other_data = json.loads(json_exporter.get_data())["otherData"]
    assert other_data["warnings"] == [{"finding": "long_dur", "text": "warn text"}]
    assert other_data["errors"] == [{"finding": "bad_ts", "text": "error text"}]


def test_export_issue_events_do_not_leak_into_trace(json_exporter):
    json_exporter.export([_issue_event("long_dur", "text"), _instant_event()])

    dumped = json.loads(json_exporter.get_data())
    names = [e["name"] for e in dumped["traceEvents"]]
    assert TRACE_ISSUE_EVENT_NAME not in names
    assert "regular_event" in names


def test_export_no_issue_section_when_absent(json_exporter):
    json_exporter.export([_instant_event()])

    other_data = json.loads(json_exporter.get_data())["otherData"]
    assert "warnings" not in other_data
    assert "errors" not in other_data


def test_export_repeated_finding_collapses_to_one_entry(json_exporter):
    '''
    a context attached to several pipeline stages (e.g. normalize_phase1 + normalize_phase2) is
    drained once per stage and reports the same finding each time. Only the last report, which
    carries the complete counts, may end up in the output.
    '''
    json_exporter.export([_issue_event("long_dur", "Detected 2 event(s)."),
                          _issue_event("long_dur", "Detected 5 event(s).")])

    other_data = json.loads(json_exporter.get_data())["otherData"]
    assert other_data["warnings"] == [{"finding": "long_dur", "text": "Detected 5 event(s)."}]


def test_export_finding_without_optional_args_does_not_crash(json_exporter):
    '''an M-event named like a finding may also arrive from an input trace, without our args'''
    foreign = AbstractEventType.from_dict({"ph": "M", "ts": 0, "pid": 0,
                                           "name": TRACE_ISSUE_EVENT_NAME, "args": {}})
    json_exporter.export([foreign])

    other_data = json.loads(json_exporter.get_data())["otherData"]
    assert other_data["warnings"] == [{"finding": "unknown", "text": ""}]
