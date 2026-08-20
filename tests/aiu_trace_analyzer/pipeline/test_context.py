# Copyright 2024-2025 IBM Corporation

import pytest
import math

from aiu_trace_analyzer.types import (
    TraceWarning,
    TRACE_FINDING_EVENT_NAME,
    VERIFICATION_TEST_RESULT_NAME)
from aiu_trace_analyzer.pipeline import AbstractContext
from aiu_trace_analyzer.pipeline.context import AbstractVerificationContext


@pytest.fixture
def default_warning() -> TraceWarning:
    return TraceWarning(
        name="pytest",
        text="A Warning with 2 args: {d[count]} and {d[max]}",
        data={"count": 0, "max": 0.0},
        update_fn={"count": int.__add__, "max": max},
        auto_log=False
    )


fail_test_cases = [
    (
        ("arg_count_mismatch_data", "Arg {d[a1]} and Arg {d[a2]}", {"a1": 0}, {}),
        ValueError
    ),
    (
        ("arg_count_mismatch_text", "Arg {d[a1]} and no other", {"a1": 0, "a2": 0}, {}),
        ValueError
    ),
    (
        ("miss_data_arg", "Arg {d[a1]} and Arg {d[a2]}", {"a1": 0, "b1": 1}, {}),
        KeyError
    ),
    (
        ("miss_text_arg", "Arg {d[a1]} and Arg {d[b1]}", {"a1": 0, "a2": 0}, {}),
        KeyError
    ),
    (
        ("miss_update_fn", "Arg {d[a1]}", {"a1": 0}, {"b1": int.__add__}),
        KeyError
    )
]


@pytest.mark.parametrize('warning_arg, exception', fail_test_cases)
def test_fail_warning(warning_arg, exception):
    with pytest.raises(exception) as cli_res:
        TraceWarning(
            name=warning_arg[0],
            text=warning_arg[1],
            data=warning_arg[2],
            update_fn=warning_arg[3],
            auto_log=False)
    assert isinstance(cli_res.value, exception)


def test_warning_name(default_warning):
    name = default_warning.get_name()
    assert name == "pytest"


def test_warning_update(default_warning):
    default_warning.update(
        {"count": 1, "max": 10.0}
    )
    assert default_warning.args_list["count"] == 1
    assert math.isclose(default_warning.args_list["max"], 10.0, abs_tol=1e-9)

    # add another warning with new max
    default_warning.update(
        {"count": 1, "max": 11.0}
    )
    assert default_warning.args_list["count"] == 2
    assert math.isclose(default_warning.args_list["max"], 11.0, abs_tol=1e-9)


def test_has_warning(default_warning):
    assert default_warning.has_warning() is False

    default_warning.update(
        {"count": 1, "max": 10.0}
    )

    assert default_warning.has_warning() is True
    assert default_warning.args_list["count"] == 1


def test_output_warning(default_warning):
    output = default_warning.__str__()
    assert output == "A Warning with 2 args: 0 and 0.0"

    default_warning.update(
        {"count": 2, "max": 10.0}
    )

    output = default_warning.__str__()
    assert output == "A Warning with 2 args: 2 and 10.0"


###########################################################
# Abstract-Context class tests

@pytest.fixture
def abstract_context(default_warning) -> AbstractContext:
    return AbstractContext(warnings=[default_warning])


def test_abstract_context(abstract_context, default_warning):
    assert isinstance(abstract_context.warnings, dict)
    assert abstract_context.warnings["pytest"] == default_warning


def test_add_warning(abstract_context):
    new_warning = TraceWarning(
        name="added",
        text="Another Warning with 1 arg: {d[count]}",
        data={"count": 0},
        update_fn={},
        auto_log=False
    )
    abstract_context.add_warning(new_warning)

    assert "added" in abstract_context.warnings
    assert abstract_context.warnings["added"] == new_warning


def test_issue_warning(abstract_context):
    abstract_context.warnings["pytest"].auto_log = False   # disable auto-output for tests
    abstract_context.issue_warning("pytest", {"count": 2, "max": 5.0})

    assert abstract_context.warnings["pytest"].has_warning() is True
    assert abstract_context.warnings["pytest"].args_list["count"] == 2
    assert abstract_context.warnings["pytest"].args_list["max"] == 5.0
    assert abstract_context.warnings["pytest"].__str__() == "A Warning with 2 args: 2 and 5.0"


def test_emit_findings_reports_every_warning(abstract_context):
    # findings are emitted for all warnings, fired or not: a count of 0 is what tells the
    # verification report that a check ran and passed. Filtering is up to the consumer.
    abstract_context.warnings["pytest"].auto_log = False   # disable auto-output for tests

    events = abstract_context._emit_findings()

    assert len(events) == 1
    assert events[0]["args"]["finding"] == "pytest"
    assert events[0]["args"]["count"] == 0


def test_emit_findings_one_event_per_warning():
    warnings = [
        TraceWarning(name="fired", text="fired {d[count]} time(s)",
                     data={"count": 0}, auto_log=False),
        TraceWarning(name="quiet", text="quiet {d[count]} time(s)",
                     data={"count": 0}, auto_log=False),
    ]
    context = AbstractContext(warnings=warnings)
    context.issue_warning("fired", {"count": 1})

    by_name = {e["args"]["finding"]: e["args"] for e in context._emit_findings()}

    assert sorted(by_name) == ["fired", "quiet"]
    assert by_name["fired"]["count"] == 1
    assert by_name["quiet"]["count"] == 0


def test_emit_findings_payload(abstract_context):
    abstract_context.warnings["pytest"].auto_log = False   # disable auto-output for tests
    abstract_context.issue_warning("pytest", {"count": 1, "max": 5.0})

    events = abstract_context._emit_findings()

    assert len(events) == 1
    assert events[0]["ph"] == "M"
    assert events[0]["name"] == TRACE_FINDING_EVENT_NAME
    # the full payload: the rendered text for the trace export, count/instances for the report.
    # No 'test' key - a plain context has no test to attribute its findings to.
    assert events[0]["args"] == {"finding": "pytest",
                                 "text": "A Warning with 2 args: 1 and 5.0",
                                 "is_error": False,
                                 "count": 1,
                                 "instances": []}


def test_emit_findings_of_error_warning():
    error = TraceWarning(
        name="pytest_err",
        text="An Error with {d[count]} occurrence(s)",
        data={"count": 0},
        update_fn={"count": int.__add__},
        auto_log=False,
        is_error=True,
    )
    context = AbstractContext(warnings=[error])
    context.issue_warning("pytest_err", {"count": 1})

    events = context._emit_findings()

    assert len(events) == 1
    assert events[0]["args"]["is_error"] is True
    assert events[0]["args"]["text"] == "An Error with 1 occurrence(s)"


def test_drain_of_disabled_context_is_empty(abstract_context):
    # the fixture context was never enabled: a stage that the profile switched off must not report
    # findings, or the report would list checks that never ran as passed
    assert abstract_context.is_enabled() is False
    assert abstract_context.drain() == []


def test_drain_of_enabled_context_emits_findings(abstract_context):
    abstract_context.warnings["pytest"].auto_log = False
    abstract_context.enable()

    events = abstract_context.drain()

    assert [e["args"]["finding"] for e in events] == ["pytest"]


def test_is_enabled(abstract_context):
    assert abstract_context.is_enabled() is False

    abstract_context.disable()
    assert abstract_context.is_enabled() is False

    abstract_context.enable()
    assert abstract_context.is_enabled() is True

    abstract_context.disable()
    assert abstract_context.is_enabled() is True


###########################################################
# TraceWarning.add_instance / to_finding_args tests


@pytest.fixture
def count_warning() -> TraceWarning:
    return TraceWarning(
        name="test_w",
        text="Found {d[count]} issues",
        data={"count": 0},
        update_fn={"count": int.__add__},
        auto_log=False,
    )


@pytest.fixture
def error_warning() -> TraceWarning:
    return TraceWarning(
        name="test_err",
        text="Found {d[count]} errors",
        data={"count": 0},
        update_fn={"count": int.__add__},
        auto_log=False,
        is_error=True,
    )


def test_w1_to_finding_args_fresh(count_warning):
    args = count_warning.to_finding_args()
    assert args["finding"] == "test_w"
    assert args["is_error"] is False
    assert args["count"] == 0
    assert args["instances"] == []


def test_w2_multiple_instances(count_warning):
    count_warning.add_instance({"key": "a"})
    count_warning.add_instance({"key": "b"})
    count_warning.add_instance({"key": "c"})
    args = count_warning.to_finding_args()
    assert len(args["instances"]) == 3
    assert args["instances"][0] == {"key": "a"}
    assert args["instances"][2] == {"key": "c"}


def test_w3_count_key_takes_precedence_over_instances(count_warning):
    count_warning.update({"count": 5})
    count_warning.add_instance({"x": 1})
    args = count_warning.to_finding_args()
    # args_list["count"] == 5 beats len(instances) == 1
    assert args["count"] == 5


def test_w4_error_level_is_error_true(error_warning):
    args = error_warning.to_finding_args()
    assert args["is_error"] is True


###########################################################
# AbstractVerificationContext tests


class _ConcreteVerificationContext(AbstractVerificationContext):
    test_name = "Unit Test Check"


def _enabled(context):
    context.enable()
    return context


@pytest.fixture
def verif_context_no_warnings() -> _ConcreteVerificationContext:
    return _enabled(_ConcreteVerificationContext(warnings=[]))


@pytest.fixture
def verif_context_warn(count_warning) -> _ConcreteVerificationContext:
    return _enabled(_ConcreteVerificationContext(warnings=[count_warning]))


@pytest.fixture
def verif_context_error(error_warning) -> _ConcreteVerificationContext:
    return _enabled(_ConcreteVerificationContext(warnings=[error_warning]))


def _find_events(events, name):
    return [e for e in events if e["name"] == name]


def _find_finding(events, finding):
    return [e for e in _find_events(events, TRACE_FINDING_EVENT_NAME)
            if e["args"]["finding"] == finding]


def test_v1_drain_no_warnings_produces_pass(verif_context_no_warnings):
    events = verif_context_no_warnings.drain()
    test_results = _find_events(events, VERIFICATION_TEST_RESULT_NAME)
    assert len(test_results) == 1
    assert test_results[0]["args"]["result"] == "pass"
    assert test_results[0]["args"]["test"] == "Unit Test Check"


def test_v2_drain_warn_level_warning_produces_warn(verif_context_warn):
    verif_context_warn.warnings["test_w"].update({"count": 1})
    events = verif_context_warn.drain()

    test_result = _find_events(events, VERIFICATION_TEST_RESULT_NAME)[0]
    assert test_result["args"]["result"] == "warn"

    # one finding event carries everything: rendered text, severity, count and test provenance
    findings = _find_finding(events, "test_w")
    assert len(findings) == 1
    assert findings[0]["args"] == {
        "finding": "test_w",
        "text": "Found 1 issues",
        "is_error": False,
        "count": 1,
        "instances": [],
        "test": "Unit Test Check",
    }


def test_v3_drain_error_level_warning_produces_fail(verif_context_error):
    verif_context_error.warnings["test_err"].update({"count": 1})
    events = verif_context_error.drain()

    test_result = _find_events(events, VERIFICATION_TEST_RESULT_NAME)[0]
    assert test_result["args"]["result"] == "fail"

    findings = _find_finding(events, "test_err")
    assert len(findings) == 1
    assert findings[0]["args"]["is_error"] is True
    assert findings[0]["args"]["count"] == 1


def test_v4_drain_propagates_instances(verif_context_warn):
    verif_context_warn.warnings["test_w"].update({"count": 1})
    verif_context_warn.warnings["test_w"].add_instance({"detail": "bad_event", "ts": 42.0})
    events = verif_context_warn.drain()
    finding = _find_finding(events, "test_w")[0]
    assert finding["args"]["instances"] == [{"detail": "bad_event", "ts": 42.0}]


def test_v5_drain_test_name_from_subclass():
    class _NamedCtx(AbstractVerificationContext):
        test_name = "My Custom Check"

    ctx = _enabled(_NamedCtx(warnings=[]))
    events = ctx.drain()
    test_result = _find_events(events, VERIFICATION_TEST_RESULT_NAME)[0]
    assert test_result["args"]["test"] == "My Custom Check"


def test_v6_drain_emits_only_one_finding_kind(verif_context_warn):
    '''
    the findings and the verification data used to be two separate event kinds for the same
    warning. There is only one now: findings plus exactly one test result.
    '''
    verif_context_warn.warnings["test_w"].update({"count": 1})
    events = verif_context_warn.drain()

    names = sorted(e["name"] for e in events)
    assert names == [TRACE_FINDING_EVENT_NAME, VERIFICATION_TEST_RESULT_NAME]


def test_v7_findings_of_a_plain_context_have_no_test_provenance(abstract_context):
    '''the 'test' key is what separates a verification check from an advisory warning'''
    abstract_context.warnings["pytest"].auto_log = False
    abstract_context.enable()

    assert "test" not in abstract_context.drain()[0]["args"]
