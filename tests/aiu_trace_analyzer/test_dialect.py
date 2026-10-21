# Copyright 2026 IBM Corporation

import pytest

import aiu_trace_analyzer.dialect as dialect
from aiu_trace_analyzer.types import TraceEvent, GlobalIngestData
from aiu_trace_analyzer.dialect import EventClassifier, InputDialectFLEX, InputDialectTORCH

GlobalIngestData()
_FLEX_JOB = GlobalIngestData.add_job_info("dialect_test_flex.json", InputDialectFLEX())
_TORCH_JOB = GlobalIngestData.add_job_info("dialect_test_torch.pt.trace.json", InputDialectTORCH())


def _event(name: str = "op", job: int = _FLEX_JOB, args_key: str = "args", **fields) -> TraceEvent:
    return TraceEvent({"ph": "X", "name": name, "ts": 1.0, "dur": 1.0, args_key: {"jobhash": job}, **fields})


@pytest.mark.parametrize("entry, operator, operand", [
    ("Execute Graph", "name", "Execute Graph"),
    ("is.name;Cmpt Exec$", "is", ["name"]),
    ("is.args.Type;a;b", "is", ["args", "Type"]),
    ("has.args.TS1", "has", ["args", "TS1"]),
    ("xyz.name", "unknown", "xyz"),
])
def test_classifier_parsing(entry, operator, operand):
    c = EventClassifier(entry, "test")
    assert (c.operator, c.operand) == (operator, operand)
    assert (c.regex is not None) == (operator == "is")


def test_classifier_regex_keeps_semicolons():
    c = EventClassifier("is.name;a;b", "test")
    assert c.matches(TraceEvent({"name": "xa;by"}))
    assert not c.matches(TraceEvent({"name": "ab"}))


def test_classifier_malformed_is_entry_fails_at_parse():
    with pytest.raises(AssertionError):
        EventClassifier("is.name", "test")


@pytest.mark.parametrize("entry, event, result", [
    ("op", TraceEvent({"name": "op"}), True),
    ("op", TraceEvent({"name": "op2"}), False),
    ("is.name;^op", TraceEvent({"name": "op2"}), True),
    ("is.cat;gpu|kernel", TraceEvent({"name": "k", "cat": "gpu_memcpy"}), True),
    ("is.cat;gpu|kernel", TraceEvent({"name": "k"}), False),
    ("has.args.TS1", TraceEvent({"name": "k", "args": {"TS1": "1"}}), True),
    ("has.args.TS1", TraceEvent({"name": "k", "args": {}}), False),
    ("xyz.name", TraceEvent({"name": "k"}), False),
])
def test_classifier_matches(entry, event, result):
    assert EventClassifier(entry, "test").matches(event) is result


def test_classifier_is_on_non_leaf_attribute():
    with pytest.raises(AssertionError):
        EventClassifier("is.args;x", "test").matches(TraceEvent({"name": "k", "args": {"a": 1}}))


@pytest.mark.parametrize("dialect_cls", [InputDialectFLEX, InputDialectTORCH])
def test_classifiers_parsed_at_registration(dialect_cls):
    d = dialect_cls()
    for category in d.dialect_map[dialect_cls.__name__]:
        c = d.classifier(category)
        if d.get(category) is None:
            assert c is None
        else:
            assert isinstance(c, EventClassifier)
            assert d.classifier(category) is c   # the same parsed object, no re-parsing


def test_dash_entry_has_no_classifier():
    assert InputDialectTORCH().get("acc_compute_prep") is None
    assert InputDialectTORCH().classifier("acc_compute_prep") is None
    assert not dialect.is_category(_event("op Cmpt Prep", _TORCH_JOB), "acc_compute_prep")


def test_old_import_path():
    import aiu_trace_analyzer.types as types
    from aiu_trace_analyzer.types import InputDialect, InputDialectFLEX as OldFLEX, InputDialectTORCH as OldTORCH
    assert InputDialect is dialect.InputDialect
    assert OldFLEX is dialect.InputDialectFLEX and OldTORCH is dialect.InputDialectTORCH
    with pytest.raises(AttributeError):
        types.NoSuchName


@pytest.mark.parametrize("event, expected", [
    (_event(job=_FLEX_JOB), "FLEX"),
    (_event(job=_TORCH_JOB), "TORCH"),
    (_event(job=_FLEX_JOB, args_key="attr"), None),     # get_dialect_of_event only looks into args
    (TraceEvent({"name": "no args"}), None),
])
def test_get_dialect_of_event(event, expected):
    d = dialect.get_dialect_of_event(event)
    assert (d.get("NAME") if d else None) == expected


@pytest.mark.parametrize("event, args_key, expected", [
    (_event(job=_FLEX_JOB), "args", "FLEX"),
    (_event(job=_TORCH_JOB, args_key="attr"), "attr", "TORCH"),
    (_event(job=-1), "args", None),                         # unregistered job
    (TraceEvent({"name": "no jobhash", "args": {}}), "args", None),
    (TraceEvent({"name": "no args"}), "args", None),
])
def test_find_dialect_of_event(event, args_key, expected, capsys):
    d = dialect.find_dialect_of_event(event, args_key)
    assert (d.get("NAME") if d else None) == expected
    assert capsys.readouterr().out == ""                    # silent, unlike get_dialect_of_event


def test_get_context_id():
    assert dialect.get_context_id(_event(job=_FLEX_JOB)) == _FLEX_JOB
    torch_event = _event(job=_TORCH_JOB)
    torch_event["args"]["correlation"] = 42
    assert dialect.get_context_id(torch_event) == 42
    assert dialect.get_context_id(TraceEvent({"name": "no args"})) == 0


@pytest.mark.parametrize("args, result", [
    ({"External id": 1}, False),
    ({"Python id": 1}, False),
    ({"TS1": "1"}, True),
])
def test_is_flex_event(args, result):
    assert dialect.is_flex_event(TraceEvent({"name": "e", "args": args})) is result
