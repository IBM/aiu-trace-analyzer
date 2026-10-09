# Copyright 2026 IBM Corporation

import pytest

import aiu_trace_analyzer.hw_data as hw_data
from aiu_trace_analyzer.types import TraceEvent, GlobalIngestData
from aiu_trace_analyzer.dialect import InputDialectFLEX, InputDialectTORCH
from aiu_trace_analyzer.hw_data import (
    HW_TS_COUNT,
    hw_ts_label,
    has_hw_ts,
    get_hw_ts,
    get_hw_ts_list,
    set_hw_ts_list,
    has_hw_power,
    get_hw_power,
    set_hw_power,
    canonicalize_hw_data,
    HwPhase,
    hw_phase,
    hw_ts_span,
)

GlobalIngestData()
_JOBS = {
    "FLEX": GlobalIngestData.add_job_info("hw_data_test_flex.json", InputDialectFLEX()),
    "TORCH": GlobalIngestData.add_job_info("hw_data_test_torch.pt.trace.json", InputDialectTORCH()),
}
_JOB_WITHOUT_DIALECT = GlobalIngestData.add_job_info("hw_data_test_no_dialect.json")
# jobhash values are hash() % 10000, so a negative one is never registered
_UNREGISTERED_JOB = -1

_FLEX_KEYS = ["TS1", "TS2", "TS3", "TS4", "TS5"]


@pytest.fixture(autouse=True)
def all_dialects_enabled(monkeypatch):
    # the accessor tests cover both layouts; the default dialect gate is tested separately below
    monkeypatch.setattr(hw_data, "HW_DATA_DIALECTS", {"FLEX", "TORCH"})


_VALUES = [2568310617, 2568311551, 2568311560, 2568311560, 2568311566]
_POWER = 2656188424

_ENCODINGS = {
    "int": lambda v: v,
    "dec": str,
    "hex": hex,
}


def _hw_args(dialect: str, encode, values=None, power=_POWER) -> dict:
    values = _VALUES if values is None else values
    if dialect == "FLEX":
        args = {k: encode(v) for k, v in zip(_FLEX_KEYS, values)}
        args["Power"] = encode(power)
    else:
        args = {"cycles_ts": [encode(v) for v in values], "charge": encode(power)}
    return args


def _event(dialect: str, encoding: str = "dec", job: str = "registered", args_key: str = "args",
           args: dict = None) -> TraceEvent:
    args = _hw_args(dialect, _ENCODINGS[encoding]) if args is None else args
    if job == "registered":
        args["jobhash"] = _JOBS[dialect]
    elif job == "unregistered":
        args["jobhash"] = _UNREGISTERED_JOB
    elif job == "no_dialect":
        args["jobhash"] = _JOB_WITHOUT_DIALECT
    return TraceEvent({"ph": "X", "name": "op Cmpt Exec", "ts": 1.0, "dur": 1.0, args_key: args})


@pytest.mark.parametrize("dialect", ["FLEX", "TORCH"])
@pytest.mark.parametrize("encoding", list(_ENCODINGS))
@pytest.mark.parametrize("job", ["registered", "none", "unregistered", "no_dialect"])
@pytest.mark.parametrize("args_key", ["args", "attr"])
def test_get_hw_ts(dialect, encoding, job, args_key):
    event = _event(dialect, encoding, job, args_key)
    assert has_hw_ts(event, args_key)
    assert get_hw_ts_list(event, args_key) == _VALUES
    for n in range(1, HW_TS_COUNT + 1):
        value = get_hw_ts(event, n, args_key)
        assert value == _VALUES[n - 1] and type(value) is int


@pytest.mark.parametrize("dialect", ["FLEX", "TORCH"])
@pytest.mark.parametrize("encoding", list(_ENCODINGS))
def test_set_hw_ts_list_writes_native_format(dialect, encoding):
    event = _event(dialect, encoding)
    set_hw_ts_list(event, get_hw_ts_list(event))
    if dialect == "FLEX":
        assert [event["args"][k] for k in _FLEX_KEYS] == [str(v) for v in _VALUES]
        assert "cycles_ts" not in event["args"]
    else:
        assert event["args"]["cycles_ts"] == _VALUES
        assert all(type(v) is int for v in event["args"]["cycles_ts"])
        assert not any(k in event["args"] for k in _FLEX_KEYS)

    new_values = [v + (1 << 32) for v in _VALUES]
    set_hw_ts_list(event, new_values)
    assert get_hw_ts_list(event) == new_values


@pytest.mark.parametrize("dialect", ["FLEX", "TORCH"])
def test_no_hw_ts(dialect):
    # e.g. a TORCH kernel event without cycles_ts: acc event, but no HW data
    event = _event(dialect, args={"correlation": 1})
    assert not has_hw_ts(event)
    assert not has_hw_power(event)
    with pytest.raises(KeyError):
        get_hw_ts(event, 1)
    with pytest.raises(KeyError):
        get_hw_ts_list(event)
    with pytest.raises(KeyError):
        get_hw_power(event)


@pytest.mark.parametrize("event", [
    TraceEvent({"ph": "X", "name": "no args"}),
    TraceEvent({"ph": "X", "name": "args not a dict", "args": "x"}),
    TraceEvent({"ph": "X", "name": "no hw data, no job", "args": {"other": 1}}),
])
def test_no_hw_ts_without_layout(event):
    assert not has_hw_ts(event)
    assert not has_hw_power(event)
    with pytest.raises(KeyError):
        set_hw_ts_list(event, _VALUES)
    with pytest.raises(KeyError):
        set_hw_power(event, _POWER)


@pytest.mark.parametrize("dialect, foreign", [("FLEX", "TORCH"), ("TORCH", "FLEX")])
def test_registered_dialect_wins_over_keys(dialect, foreign):
    # layout of the registered job's dialect is used, even if the args look like the other dialect
    event = _event(dialect, args=_hw_args(foreign, str))
    assert not has_hw_ts(event)
    assert not has_hw_power(event)


def test_incomplete_flex_ts():
    event = _event("FLEX", args={"TS1": "1", "TS2": "2"})
    assert has_hw_ts(event)
    assert get_hw_ts(event, 2) == 2
    with pytest.raises(KeyError):
        get_hw_ts(event, 3)
    with pytest.raises(AssertionError):
        get_hw_ts_list(event)


def test_torch_ts_list_length():
    event = _event("TORCH", args={"cycles_ts": [1, 2, 3, 4]})
    assert has_hw_ts(event)
    with pytest.raises(AssertionError):
        get_hw_ts_list(event)


@pytest.mark.parametrize("dialect", ["FLEX", "TORCH"])
def test_set_hw_ts_list_length(dialect):
    with pytest.raises(ValueError):
        set_hw_ts_list(_event(dialect), _VALUES[:4])


@pytest.mark.parametrize("n", [0, HW_TS_COUNT + 1])
def test_ts_index_range(n):
    with pytest.raises(IndexError):
        get_hw_ts(_event("FLEX"), n)
    with pytest.raises(IndexError):
        hw_ts_label(n)


def test_hw_ts_label():
    assert [hw_ts_label(n) for n in range(1, HW_TS_COUNT + 1)] == _FLEX_KEYS


@pytest.mark.parametrize("dialect", ["FLEX", "TORCH"])
def test_unsupported_value_type(dialect):
    event = _event(dialect, args=_hw_args(dialect, float))
    with pytest.raises(TypeError):
        get_hw_ts_list(event)


@pytest.mark.parametrize("dialect", ["FLEX", "TORCH"])
@pytest.mark.parametrize("encoding", list(_ENCODINGS))
@pytest.mark.parametrize("job", ["registered", "none"])
def test_get_hw_power(dialect, encoding, job):
    event = _event(dialect, encoding, job)
    assert has_hw_power(event)
    power = get_hw_power(event)
    assert power == float(_POWER) and type(power) is float


def test_get_hw_power_non_integer_str():
    event = _event("FLEX", args={"Power": "12.5"})
    assert get_hw_power(event) == 12.5


@pytest.mark.parametrize("dialect, native_type", [("FLEX", str), ("TORCH", int)])
@pytest.mark.parametrize("encoding", list(_ENCODINGS))
def test_set_hw_power_writes_native_format(dialect, native_type, encoding):
    event = _event(dialect, encoding)
    power_key = "Power" if dialect == "FLEX" else "charge"
    set_hw_power(event, int(get_hw_power(event)))
    assert type(event["args"][power_key]) is native_type
    assert event["args"][power_key] == native_type(_POWER)


def test_set_hw_power_torch_rejects_float():
    with pytest.raises(TypeError):
        set_hw_power(_event("TORCH"), 12.5)


@pytest.mark.parametrize("job", ["registered", "none"])
def test_torch_gated_by_default(monkeypatch, job):
    monkeypatch.undo()   # restore the default HW_DATA_DIALECTS
    assert hw_data.HW_DATA_DIALECTS == {"FLEX"}
    torch_event = _event("TORCH", job=job)
    assert not has_hw_ts(torch_event)
    assert not has_hw_power(torch_event)
    with pytest.raises(KeyError):
        get_hw_ts_list(torch_event)
    with pytest.raises(KeyError):
        set_hw_ts_list(torch_event, _VALUES)
    flex_event = _event("FLEX", job=job)
    assert has_hw_ts(flex_event) and get_hw_ts_list(flex_event) == _VALUES


@pytest.mark.parametrize("dialect", ["FLEX", "TORCH"])
@pytest.mark.parametrize("encoding", ["dec", "hex"])
def test_canonicalize_hw_data(dialect, encoding):
    event = _event(dialect, encoding)
    assert canonicalize_hw_data(event) is event   # updated in place and returned
    if dialect == "FLEX":
        assert [event["args"][k] for k in _FLEX_KEYS] == [str(v) for v in _VALUES]
        assert event["args"]["Power"] == str(_POWER)
    else:
        assert event["args"]["cycles_ts"] == _VALUES
        assert event["args"]["charge"] == _POWER


@pytest.mark.parametrize("args, expected", [
    ({"TS1": "12345"}, {"TS1": "12345"}),                    # decimal str stays
    ({"TS2": "0x12345"}, {"TS2": "74565"}),                  # incomplete TS set
    ({"Power": "12345"}, {"Power": "12345"}),
    ({"Power": "0x10"}, {"Power": "16"}),                    # power only
    ({"TS1": 7, "Power": 12}, {"TS1": 7, "Power": 12}),      # non-str values stay unchanged
    ({"TS1": "abc", "Power": "12.5"}, {"TS1": "abc", "Power": "12.5"}),  # unparsable str stays unchanged
    ({"SOME": "0x10"}, {"SOME": "0x10"}),                    # no HW data
])
def test_canonicalize_hw_data_flex_partial(args, expected):
    event = TraceEvent({"ph": "X", "name": "e", "args": dict(args)})
    canonicalize_hw_data(event)
    assert event["args"] == expected


def test_canonicalize_hw_data_gated(monkeypatch):
    monkeypatch.undo()
    event = _event("TORCH", "hex")
    canonicalize_hw_data(event)
    assert event["args"]["cycles_ts"] == [hex(v) for v in _VALUES]


def _phase_event(dialect: str, name: str, cat: str = None, job: str = "registered", with_ts: bool = True) -> TraceEvent:
    event = _event(dialect, job=job, args=_hw_args(dialect, str) if with_ts else {})
    event["name"] = name
    if cat:
        event["cat"] = cat
    return event


@pytest.mark.parametrize("job", ["registered", "none"])
@pytest.mark.parametrize("name, phase, span", [
    ("op DmaI", HwPhase.DMA_IN, (1, 2)),
    ("op Cmpt Prep", HwPhase.CMPT_PREP, (2, 3)),
    ("op Cmpt Exec", HwPhase.CMPT_EXEC, (3, 4)),
    ("op DmaO", HwPhase.DMA_OUT, (4, 5)),
    ("Flex RoundTrip", None, (1, 5)),
])
def test_hw_phase_flex(name, phase, span, job):
    event = _phase_event("FLEX", name, job=job)
    assert hw_phase(event) == phase
    assert hw_ts_span(event) == span


@pytest.mark.parametrize("name, cat, phase", [
    ("Memcpy (HtoD)", "gpu_memcpy", HwPhase.DMA_IN),
    ("embedding", "kernel", HwPhase.CMPT_EXEC),
    ("Memcpy (DtoH)", "gpu_memcpy", HwPhase.DMA_OUT),
    ("op Cmpt Prep", "kernel", HwPhase.CMPT_EXEC),    # TORCH has no Cmpt Prep category
    ("Memset (Device)", "gpu_memset", None),
])
def test_hw_phase_torch(name, cat, phase):
    assert hw_phase(_phase_event("TORCH", name, cat)) == phase


@pytest.mark.parametrize("dialect, name", [("FLEX", "op Cmpt Exec"), ("TORCH", "embedding")])
def test_hw_phase_requires_hw_ts(dialect, name):
    event = _phase_event(dialect, name, cat="kernel", with_ts=False)
    assert hw_phase(event) is None
    assert hw_ts_span(event) == (1, HW_TS_COUNT)


def test_hw_phase_torch_gated(monkeypatch):
    monkeypatch.undo()
    assert hw_phase(_phase_event("TORCH", "embedding", "kernel")) is None
