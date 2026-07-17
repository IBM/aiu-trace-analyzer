# Copyright 2024-2026 IBM Corporation

import json

import pytest

from aiu_trace_analyzer.core.processing import EventProcessor
from aiu_trace_analyzer.pipeline.context import AbstractContext
from aiu_trace_analyzer.types import TraceWarning, TRACE_ISSUE_EVENT_NAME
from aiu_trace_analyzer.export.exporter import JsonFileTraceExporter


@pytest.fixture
def warned_context() -> AbstractContext:
    warning = TraceWarning(
        name="long_dur",
        text="OVC: Detected {d[count]} long event(s).",
        data={"count": 0},
        update_fn={"count": int.__add__},
        auto_log=False,
    )
    ctx = AbstractContext(warnings=[warning])
    ctx.enable()
    return ctx


def _processor_with(context: AbstractContext) -> EventProcessor:
    proc = EventProcessor()
    # register the stage directly to avoid pulling in a full StageProfile for the test
    proc.stages.append((lambda event, ctx: [event], context, {}))
    return proc


def test_drain_emits_active_warning_as_meta_event(warned_context):
    warned_context.issue_warning("long_dur", {"count": 3})

    drained = _processor_with(warned_context).drain()

    issue_events = [e for e in drained if e.name == TRACE_ISSUE_EVENT_NAME]
    assert len(issue_events) == 1
    assert issue_events[0].args == {"finding": "long_dur",
                                    "text": "OVC: Detected 3 long event(s)."}


def test_drain_emits_nothing_when_no_warning(warned_context):
    drained = _processor_with(warned_context).drain()

    assert [e for e in drained if e.name == TRACE_ISSUE_EVENT_NAME] == []


def test_warning_reaches_exporter_other_data(warned_context):
    warned_context.issue_warning("long_dur", {"count": 3})

    drained = _processor_with(warned_context).drain()
    exporter = JsonFileTraceExporter(target_uri="unused.json")
    exporter.export(drained)

    output = json.loads(exporter.get_data())
    assert output["otherData"]["issues"] == {"long_dur": "OVC: Detected 3 long event(s)."}
    assert output["traceEvents"] == []


def test_drain_warning_bypasses_remaining_pipeline_stages(warned_context):
    warned_context.issue_warning("long_dur", {"count": 3})
    proc = _processor_with(warned_context)

    def drop_everything(event, ctx):
        return []

    proc.stages.append((drop_everything, None, {}))

    drained = proc.drain()

    assert [e for e in drained if e.name == TRACE_ISSUE_EVENT_NAME]
