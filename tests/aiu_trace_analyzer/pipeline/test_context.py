# Copyright 2024-2025 IBM Corporation

import pytest
import math

from aiu_trace_analyzer.types import TraceWarning, TRACE_ISSUE_EVENT_NAME
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


def test_emit_issue_events_none_when_inactive(abstract_context):
    abstract_context.warnings["pytest"].auto_log = False   # disable auto-output for tests
    assert abstract_context.emit_issue_events() == []


def test_emit_issue_events(abstract_context):
    abstract_context.warnings["pytest"].auto_log = False   # disable auto-output for tests
    abstract_context.issue_warning("pytest", {"count": 1, "max": 5.0})

    events = abstract_context.emit_issue_events()

    assert len(events) == 1
    assert events[0]["ph"] == "M"
    assert events[0]["name"] == TRACE_ISSUE_EVENT_NAME
    assert events[0]["args"] == {"finding": "pytest",
                                  "text": "A Warning with 2 args: 1 and 5.0",
                                  "is_error": False}


def test_emit_issue_events_of_error_warning():
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

    events = context.emit_issue_events()

    assert len(events) == 1
    assert events[0]["args"] == {"finding": "pytest_err",
                                  "text": "An Error with 1 occurrence(s)",
                                  "is_error": True}


def test_drain(abstract_context):
    assert abstract_context.drain() == []


def test_is_enabled(abstract_context):
    assert abstract_context.is_enabled() is False

    abstract_context.disable()
    assert abstract_context.is_enabled() is False

    abstract_context.enable()
    assert abstract_context.is_enabled() is True

    abstract_context.disable()
    assert abstract_context.is_enabled() is True


###########################################################
# TraceWarning.add_instance / to_verification_event_args tests


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


def test_w1_to_verification_event_args_fresh(count_warning):
    args = count_warning.to_verification_event_args()
    assert args["finding"] == "test_w"
    assert args["is_error"] is False
    assert args["count"] == 0
    assert args["instances"] == []


def test_w2_multiple_instances(count_warning):
    count_warning.add_instance({"key": "a"})
    count_warning.add_instance({"key": "b"})
    count_warning.add_instance({"key": "c"})
    args = count_warning.to_verification_event_args()
    assert len(args["instances"]) == 3
    assert args["instances"][0] == {"key": "a"}
    assert args["instances"][2] == {"key": "c"}


def test_w3_count_key_takes_precedence_over_instances(count_warning):
    count_warning.update({"count": 5})
    count_warning.add_instance({"x": 1})
    args = count_warning.to_verification_event_args()
    # args_list["count"] == 5 beats len(instances) == 1
    assert args["count"] == 5


def test_w4_error_level_is_error_true(error_warning):
    args = error_warning.to_verification_event_args()
    assert args["is_error"] is True


###########################################################
# AbstractVerificationContext tests


class _ConcreteVerificationContext(AbstractVerificationContext):
    test_name = "Unit Test Check"


@pytest.fixture
def verif_context_no_warnings() -> _ConcreteVerificationContext:
    return _ConcreteVerificationContext(warnings=[])


@pytest.fixture
def verif_context_warn(count_warning) -> _ConcreteVerificationContext:
    return _ConcreteVerificationContext(warnings=[count_warning])


@pytest.fixture
def verif_context_error(error_warning) -> _ConcreteVerificationContext:
    return _ConcreteVerificationContext(warnings=[error_warning])


def _find_events(events, name):
    return [e for e in events if e["name"] == name]


def test_v1_drain_no_warnings_produces_pass(verif_context_no_warnings):
    events = verif_context_no_warnings.drain()
    test_results = _find_events(events, "verification_test_result")
    assert len(test_results) == 1
    assert test_results[0]["args"]["result"] == "pass"
    assert test_results[0]["args"]["test"] == "Unit Test Check"


def test_v2_drain_warn_level_warning_produces_warn(verif_context_warn):
    verif_context_warn.warnings["test_w"].update({"count": 1})
    events = verif_context_warn.drain()
    issue_events = _find_events(events, TRACE_ISSUE_EVENT_NAME)
    test_result = _find_events(events, "verification_test_result")[0]
    assert test_result["args"]["result"] == "warn"
    assert len(issue_events) == 1
    assert issue_events[0]["args"] == {
        "finding": "test_w",
        "text": "Found 1 issues",
        "is_error": False,
    }
    data_events = _find_events(events, "verification_data")
    assert len(data_events) == 1
    assert data_events[0]["args"]["is_error"] is False


def test_v3_drain_error_level_warning_produces_fail(verif_context_error):
    verif_context_error.warnings["test_err"].update({"count": 1})
    events = verif_context_error.drain()
    issue_events = _find_events(events, TRACE_ISSUE_EVENT_NAME)
    test_result = _find_events(events, "verification_test_result")[0]
    assert test_result["args"]["result"] == "fail"
    assert len(issue_events) == 1
    assert issue_events[0]["args"] == {
        "finding": "test_err",
        "text": "Found 1 errors",
        "is_error": True,
    }
    data_events = _find_events(events, "verification_data")
    assert data_events[0]["args"]["is_error"] is True


def test_v4_drain_propagates_instances(verif_context_warn):
    verif_context_warn.warnings["test_w"].update({"count": 1})
    verif_context_warn.warnings["test_w"].add_instance({"detail": "bad_event", "ts": 42.0})
    events = verif_context_warn.drain()
    data_event = _find_events(events, "verification_data")[0]
    assert data_event["args"]["instances"] == [{"detail": "bad_event", "ts": 42.0}]


def test_v5_drain_test_name_from_subclass():
    class _NamedCtx(AbstractVerificationContext):
        test_name = "My Custom Check"

    ctx = _NamedCtx(warnings=[])
    events = ctx.drain()
    test_result = _find_events(events, "verification_test_result")[0]
    assert test_result["args"]["test"] == "My Custom Check"
