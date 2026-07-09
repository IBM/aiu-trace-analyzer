# Copyright 2024-2026 IBM Corporation

import json
import pytest

from aiu_trace_analyzer.trace_view import MetaEvents, CompleteEvents
from aiu_trace_analyzer.export.exporter import VerificationReportExporter
from aiu_trace_analyzer.verification.report import (
    VERIFICATION_RESULT_NAME,
    VERIFICATION_TEST_RESULT_NAME,
)

_NO_FILE = {"save_to_file": False}


def _error_finding(finding="bad_thing", count=1):
    return MetaEvents(
        name=VERIFICATION_RESULT_NAME,
        args={"finding": finding, "is_error": True, "count": count, "instances": []},
        ph="M", ts=0, pid=0,
    )


def _warning_finding(finding="odd_thing", count=1):
    return MetaEvents(
        name=VERIFICATION_RESULT_NAME,
        args={"finding": finding, "is_error": False, "count": count, "instances": []},
        ph="M", ts=0, pid=0,
    )


def _passed_finding(finding="clean_check"):
    return MetaEvents(
        name=VERIFICATION_RESULT_NAME,
        args={"finding": finding, "is_error": False, "count": 0, "instances": []},
        ph="M", ts=0, pid=0,
    )


def _test_result_event(test="My Check", result="pass"):
    return MetaEvents(
        name=VERIFICATION_TEST_RESULT_NAME,
        args={"test": test, "result": result},
        ph="M", ts=0, pid=0,
    )


def _non_m_event():
    return CompleteEvents(name="kernel", cat="compute", ts=100, dur=10, pid=0, tid=0)


@pytest.fixture
def exporter():
    return VerificationReportExporter("report.json", fmt="json", settings=_NO_FILE)


def test_e1_non_m_events_are_ignored(exporter):
    exporter.export([_non_m_event()])
    data = exporter.get_data()
    assert data["errors"] == []
    assert data["warnings"] == []
    assert data["passed"] == []


def test_e2_error_finding_sets_has_errors(exporter):
    exporter.export([_error_finding(count=1)])
    assert exporter.has_errors is True
    data = exporter.get_data()
    assert len(data["errors"]) == 1
    assert data["errors"][0]["finding"] == "bad_thing"


def test_e3_warning_finding_does_not_set_has_errors(exporter):
    exporter.export([_warning_finding(count=1)])
    assert exporter.has_errors is False
    data = exporter.get_data()
    assert len(data["warnings"]) == 1
    assert data["warnings"][0]["finding"] == "odd_thing"


def test_e4_zero_count_finding_goes_to_passed(exporter):
    exporter.export([_passed_finding()])
    assert exporter.has_errors is False
    data = exporter.get_data()
    assert len(data["passed"]) == 1
    assert data["passed"][0]["finding"] == "clean_check"


def test_e5_result_is_fail_when_has_errors(exporter):
    exporter.export([_error_finding(count=2)])
    assert exporter.get_data()["result"] == "FAIL"


def test_e6_result_is_pass_with_no_errors(exporter):
    exporter.export([_warning_finding(count=1), _passed_finding()])
    assert exporter.get_data()["result"] == "PASS"


def test_e7_flush_json_writes_valid_file(tmp_path):
    path = tmp_path / "report.json"
    exp = VerificationReportExporter(str(path), fmt="json", settings={"save_to_file": True})
    exp.export([_error_finding()])
    exp.flush()
    assert path.exists()
    with open(path) as f:
        doc = json.load(f)
    assert "result" in doc
    assert "errors" in doc
    assert "warnings" in doc
    assert "passed" in doc
    assert doc["version"] == "1.0"


def test_e8_flush_text_writes_readable_file(tmp_path):
    path = tmp_path / "report.txt"
    exp = VerificationReportExporter(str(path), fmt="text", settings={"save_to_file": True})
    exp.export([_error_finding(), _warning_finding(), _passed_finding()])
    exp.flush()
    assert path.exists()
    content = path.read_text()
    assert "ERRORS" in content
    assert "WARNINGS" in content
    assert "PASSED" in content
    assert "Result:" in content


def test_e9_export_meta_appears_in_metadata(exporter):
    exporter.export_meta({"input_file": "trace.json", "version": "2"})
    data = exporter.get_data()
    assert data["metadata"]["input_file"] == "trace.json"
    assert data["metadata"]["version"] == "2"


def test_e10_test_result_events_populate_test_results(exporter):
    exporter.export([_test_result_event(test="Kernel-Parent Check", result="fail")])
    data = exporter.get_data()
    assert len(data["test_results"]) == 1
    assert data["test_results"][0]["test"] == "Kernel-Parent Check"
    assert data["test_results"][0]["result"] == "fail"
