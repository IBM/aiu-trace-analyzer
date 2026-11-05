# Copyright 2024-2026 IBM Corporation

import pytest

from aiu_trace_analyzer.pipeline.sanity import SanityCheckContext, sanity_check
from aiu_trace_analyzer.types import TraceEvent


def _valid_event() -> TraceEvent:
    return TraceEvent({
        "ph": "X",
        "ts": 1.0,
        "pid": 0,
        "name": "test_event",
    })


def test_valid_event_passes_unchanged():
    ctx = SanityCheckContext()
    event = _valid_event()

    result = sanity_check(event, ctx)

    assert result == [event]
    assert ctx.warnings["missing_required_key"].to_finding_args()["count"] == 0


@pytest.mark.parametrize("missing_key", ["ph", "ts", "pid", "name"])
def test_missing_required_key_drops_event_and_records_finding(missing_key):
    ctx = SanityCheckContext()
    event = _valid_event()
    del event[missing_key]

    result = sanity_check(event, ctx)

    finding = ctx.warnings["missing_required_key"].to_finding_args()

    assert result == []
    assert finding["count"] == 1
    assert finding["is_error"] is True
    assert finding["instances"][0]["missing_key"] == missing_key


def test_each_malformed_event_counts_once():
    ctx = SanityCheckContext()

    first = _valid_event()
    del first["ts"]

    second = _valid_event()
    del second["pid"]

    sanity_check(first, ctx)
    sanity_check(second, ctx)

    finding = ctx.warnings["missing_required_key"].to_finding_args()

    assert finding["count"] == 2
    assert [instance["missing_key"] for instance in finding["instances"]] == [
        "ts",
        "pid",
    ]


def test_event_with_multiple_missing_keys_counts_once():
    ctx = SanityCheckContext()
    event = _valid_event()
    del event["ph"]
    del event["ts"]

    result = sanity_check(event, ctx)

    finding = ctx.warnings["missing_required_key"].to_finding_args()

    assert result == []
    assert finding["count"] == 1
    assert len(finding["instances"]) == 1


def test_event_processor_has_no_implicit_sanity_stage():
    from aiu_trace_analyzer.core.processing import EventProcessor

    processor = EventProcessor()

    assert processor.stages == []


def _processor_with_sanity_stage():
    from aiu_trace_analyzer.core.processing import EventProcessor
    from aiu_trace_analyzer.core.stage_profile import StageProfile

    profile = StageProfile(
        {"stages": [{"sanity_check": True}]},
        {"stages": [{"sanity_check": True}]},
    )
    processor = EventProcessor(profile=profile)
    processor.register_stage(
        callback=sanity_check,
        context=SanityCheckContext(),
    )
    return processor


def test_missing_required_key_finding_is_emitted_on_drain():
    from aiu_trace_analyzer.types import TRACE_FINDING_EVENT_NAME

    processor = _processor_with_sanity_stage()
    event = _valid_event()
    del event["name"]

    assert processor.process(event) == []

    drained = processor.drain()
    findings = [
        event for event in drained
        if event.name == TRACE_FINDING_EVENT_NAME
    ]

    assert len(findings) == 1
    assert findings[0].args["finding"] == "missing_required_key"
    assert findings[0].args["count"] == 1
    assert findings[0].args["is_error"] is True
    assert findings[0].args["instances"] == [{"missing_key": "name"}]


def test_missing_required_key_fails_verification_report():
    from aiu_trace_analyzer.export.exporter import VerificationReportExporter

    processor = _processor_with_sanity_stage()
    event = _valid_event()
    del event["pid"]

    assert processor.process(event) == []

    exporter = VerificationReportExporter(
        "unused.json",
        settings={"save_to_file": False},
    )
    exporter.export(processor.drain())

    assert exporter.has_errors is True

    report = exporter.get_data()
    assert report["result"] == "FAIL"
    assert [finding["finding"] for finding in report["errors"]] == [
        "missing_required_key",
    ]
