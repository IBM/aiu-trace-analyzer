# Copyright 2026 IBM Corporation

import pytest

from aiu_trace_analyzer.types import TraceEvent
from aiu_trace_analyzer.pipeline.mp_sync_v2 import MpTsCalibV2Context


def _event(**args) -> TraceEvent:
    return TraceEvent({"ph": "X", "name": "op Cmpt Exec", "ts": 1.0, "dur": 1.0,
                       "args": {"TS1": "1000", "TS2": "2000", "TS3": "3000", "TS4": "4000", "TS5": "5600", **args}})


@pytest.mark.parametrize("soc_frequency, expected", [(1000.0, 5.6), (560.0, 10.0)])
def test_ts5_in_us_uses_soc_frequency(soc_frequency, expected):
    context = MpTsCalibV2Context(soc_frequency=soc_frequency)
    assert context._get_ts5_in_us(_event()) == pytest.approx(expected)


def test_ts5_in_us_prefers_ts_dev():
    context = MpTsCalibV2Context(soc_frequency=1000.0)
    assert context._get_ts5_in_us(_event(ts_dev=[1.0, 2.0, 3.0, 4.0, 7.5])) == 7.5
