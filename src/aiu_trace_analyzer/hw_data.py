# Copyright 2026 IBM Corporation

'''
Dialect-aware access to the HW data of trace events: the 5 device cycle timestamps (TS1..TS5) and the power value.

The storage layout differs between input dialects and is defined by the dialect entries
'hw_ts_key', 'hw_power_key', and 'hw_value_type' (see types.py):
  FLEX:  args["TS1"] ... args["TS5"], args["Power"]; values written back as decimal str
  TORCH: args["cycles_ts"] = [ts1, ..., ts5], args["charge"]; values written back as int

Input values may be int, decimal str, or hex str in either dialect. Getters always return numbers.
Timestamp indices are 1-based to match the TS1..TS5 naming: get_hw_ts(event, 3) is TS3.

The layout is selected via the dialect of the event's job (args.jobhash). If the event has no jobhash or
its job is not registered, the layout is detected from the keys present in the event.
'''

from typing import Optional

from aiu_trace_analyzer.types import TraceEvent
from aiu_trace_analyzer.dialect import InputDialect, InputDialectFLEX, InputDialectTORCH, find_dialect_of_event

HW_TS_COUNT = 5


class _HwLayout:
    def __init__(self, dialect: InputDialect) -> None:
        ts_keys = dialect.get("hw_ts_key").split(",")
        # a single key holds the list of all timestamps; otherwise one key per timestamp
        self.ts_list_key = ts_keys[0] if len(ts_keys) == 1 else None
        self.ts_keys = ts_keys if len(ts_keys) > 1 else None
        assert self.ts_keys is None or len(self.ts_keys) == HW_TS_COUNT, \
            f"hw_ts_key of {dialect.get('NAME')} dialect requires 1 or {HW_TS_COUNT} keys"
        self.power_key = dialect.get("hw_power_key")
        value_type = dialect.get("hw_value_type")
        assert value_type in ("str", "int"), f"unknown hw_value_type of {dialect.get('NAME')} dialect: {value_type}"
        self.write_as_str = (value_type == "str")

    def to_native(self, value):
        # str layout: decimal str for numbers, other str (e.g. non-integer power) unchanged
        if self.write_as_str:
            return value if isinstance(value, str) else str(value)
        return _to_int(value)

    def has_ts(self, args: dict) -> bool:
        if self.ts_list_key:
            return self.ts_list_key in args
        return self.ts_keys[0] in args

    def get_ts_raw(self, args: dict, n: int):
        if self.ts_list_key:
            return args[self.ts_list_key][n - 1]
        return args[self.ts_keys[n - 1]]

    def get_ts_list_raw(self, args: dict) -> list:
        if self.ts_list_key:
            values = args[self.ts_list_key]
            assert len(values) == HW_TS_COUNT, f"{self.ts_list_key} requires {HW_TS_COUNT} entries: {values}"
            return list(values)
        missing = [k for k in self.ts_keys if k not in args]
        assert not missing, f"incomplete HW timestamps, missing {missing}: {args}"
        return [args[k] for k in self.ts_keys]

    def set_ts_list(self, args: dict, values: list[int]) -> None:
        if self.ts_list_key:
            args[self.ts_list_key] = [self.to_native(v) for v in values]
        else:
            for k, v in zip(self.ts_keys, values):
                args[k] = self.to_native(v)


# layouts by dialect name; dialects are singletons with static entries
_layouts: dict[str, _HwLayout] = {}


def _layout_of_dialect(dialect: InputDialect) -> _HwLayout:
    name = dialect.get("NAME")
    if name not in _layouts:
        _layouts[name] = _HwLayout(dialect)
    return _layouts[name]


def _sniff_layout(args: dict) -> Optional[_HwLayout]:
    for dialect in (InputDialectTORCH(), InputDialectFLEX()):
        layout = _layout_of_dialect(dialect)
        if layout.has_ts(args) or layout.power_key in args:
            return layout
    return None


def _args_and_layout(event: TraceEvent, args_key: str) -> tuple[Optional[dict], Optional[_HwLayout]]:
    args = event.get(args_key)
    if not isinstance(args, dict):
        return None, None
    dialect = find_dialect_of_event(event, args_key)
    if dialect is not None:
        return args, _layout_of_dialect(dialect)
    return args, _sniff_layout(args)


def _to_int(value) -> int:
    if isinstance(value, str):
        return int(value, 0)
    if isinstance(value, int):
        return value
    raise TypeError(f"HW data value must be int or str, got {type(value).__name__}: {value!r}")


def _check_index(n: int) -> None:
    if not 1 <= n <= HW_TS_COUNT:
        raise IndexError(f"HW timestamp index must be in 1..{HW_TS_COUNT}, got {n}")


def hw_ts_label(n: int) -> str:
    '''
    name of the n-th timestamp for logs, assertions, and annotations, e.g. "TS3"
    '''
    _check_index(n)
    return f"TS{n}"


def has_hw_ts(event: TraceEvent, args_key: str = "args") -> bool:
    args, layout = _args_and_layout(event, args_key)
    return layout is not None and layout.has_ts(args)


def get_hw_ts(event: TraceEvent, n: int, args_key: str = "args") -> int:
    '''
    returns TS<n> (1-based) as int
    '''
    _check_index(n)
    args, layout = _args_and_layout(event, args_key)
    if layout is None or not layout.has_ts(args):
        raise KeyError(f"event has no HW timestamps: {event.get('name')}")
    return _to_int(layout.get_ts_raw(args, n))


def get_hw_ts_list(event: TraceEvent, args_key: str = "args") -> list[int]:
    '''
    returns [TS1, ..., TS5] as ints
    '''
    args, layout = _args_and_layout(event, args_key)
    if layout is None or not layout.has_ts(args):
        raise KeyError(f"event has no HW timestamps: {event.get('name')}")
    return [_to_int(v) for v in layout.get_ts_list_raw(args)]


def set_hw_ts_list(event: TraceEvent, values: list[int], args_key: str = "args") -> None:
    '''
    writes [TS1, ..., TS5] back in the layout and value type of the event's dialect
    '''
    if len(values) != HW_TS_COUNT:
        raise ValueError(f"{HW_TS_COUNT} HW timestamps required, got {len(values)}")
    args, layout = _args_and_layout(event, args_key)
    if layout is None:
        raise KeyError(f"cannot determine the HW data layout of event: {event.get('name')}")
    layout.set_ts_list(args, values)


def has_hw_power(event: TraceEvent, args_key: str = "args") -> bool:
    args, layout = _args_and_layout(event, args_key)
    return layout is not None and layout.power_key in args


def get_hw_power(event: TraceEvent, args_key: str = "args") -> float:
    args, layout = _args_and_layout(event, args_key)
    if layout is None or layout.power_key not in args:
        raise KeyError(f"event has no HW power data: {event.get('name')}")
    value = args[layout.power_key]
    if isinstance(value, str):
        try:
            return float(int(value, 0))
        except ValueError:
            return float(value)   # non-integer str: same as the float() conversion used by the power stage so far
    return float(value)


def set_hw_power(event: TraceEvent, value, args_key: str = "args") -> None:
    '''
    writes the power value back in the value type of the event's dialect
    '''
    args, layout = _args_and_layout(event, args_key)
    if layout is None:
        raise KeyError(f"cannot determine the HW data layout of event: {event.get('name')}")
    args[layout.power_key] = layout.to_native(value)
