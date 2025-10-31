# Copyright 2024-2025 IBM Corporation

import pytest
import math

from aiu_trace_analyzer.types import TraceWarning
from aiu_trace_analyzer.pipeline import AbstractContext


@pytest.fixture
def default_warning() -> TraceWarning:
    return TraceWarning(
        name="pytest",
        text="A Warning with 2 args: {d[count]} and {d[max]}",
        data={"count": 0, "max": 0.0},
        update_fn={"count": int.__add__, "max": max}
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
            update_fn=warning_arg[3])
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
        update_fn={}
    )
    abstract_context.add_warning(new_warning)

    assert "added" in abstract_context.warnings
    assert abstract_context.warnings["added"] == new_warning


def test_issue_warning(abstract_context):
    abstract_context.issue_warning("pytest", {"count": 2, "max": 5.0})

    assert abstract_context.warnings["pytest"].has_warning() is True
    assert abstract_context.warnings["pytest"].__str__() == "A Warning with 2 args: 2 and 5.0"


def test_drain(abstract_context):
    assert abstract_context.drain() == []
