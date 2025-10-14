# Copyright 2024-2025 IBM Corporation

import pytest

from aiu_trace_analyzer.pipeline.normalize import (
    _attr_to_args,
    _hex_to_int_str,
    NormalizationContext
)


@pytest.mark.parametrize("event,key,result",
                         [
                             ({'ph': 'X', 'args': {'TS1': '12345'}}, 'TS1', '12345'),
                             ({'ph': 'X', 'args': {'Power': '12345'}}, 'Power', '12345'),
                             ({'ph': 'X', 'args': {'TS2': '0x12345'}}, 'TS2', '74565'),
                             ({'ph': 'X', 'args': {'SOME': 'TEXT'}}, 'SOME', 'TEXT'),
                         ])
def test__hex_to_int_str(event, key, result):
    tevent = _hex_to_int_str(event)
    assert key in tevent["args"] and tevent["args"][key] == result


@pytest.mark.parametrize("event,result",
                         [
                             ({'ph': 'X', 'args': {'a': 1, 'b': '2'}}, {'ph': 'X', 'args': {'a': 1, 'b': '2'}}),
                             ({'ph': 'X', 'attr': {'a': 1, 'b': '2'}}, {'ph': 'X', 'args': {'a': 1, 'b': '2'}}),
                         ])
def test__attr_to_args(event, result):
    assert _attr_to_args(event) == result


@pytest.fixture
def normalization_ctx():
    return NormalizationContext(soc_frequency=1000.0,
                                ignore_crit=False)


ts_seq_test_events = [
    ({'name': 'testevent', 'ph': 'X', 'ts': 3.141, 'args': {'TS1': '1', 'TS2': '2', 'TS3': '2', 'TS4': '3', 'TS5': '5', 'jobhash': 0}},
     {'TS1': '1', 'TS2': '2', 'TS3': '2', 'TS4': '3', 'TS5': '5', 'jobhash': 0, 'OVC': 0}),
    ({'name': 'testevent', 'ph': 'X', 'ts': 3.141, 'args': {'TS1': '1', 'TS2': '2', 'TS3': '1', 'TS4': '3', 'TS5': '5', 'jobhash': 0}},
     None),
]


@pytest.mark.parametrize("event,result", ts_seq_test_events)
def test_tsx_32bit_global_correction(event, result, normalization_ctx):
    if result is None:
        with pytest.raises(AssertionError) as exit_info:
            normalization_ctx.tsx_32bit_global_correction(0, event)
        print(exit_info.value)
        assert exit_info.value.args == ('local_correction of TS-sequence incomplete.',)
    else:
        assert normalization_ctx.tsx_32bit_global_correction(0, event) == result


@pytest.fixture
def normalization_ctx_ign_crit():
    return NormalizationContext(soc_frequency=1000.0,
                                ignore_crit=True)


ts_seq_ign_err_test_events = [
    ({'name': 'testevent', 'ph': 'X', 'ts': 3.141, 'args': {'TS1': '1', 'TS2': '2', 'TS3': '2', 'TS4': '3', 'TS5': '5', 'jobhash': 0}},
     {'TS1': '1', 'TS2': '2', 'TS3': '2', 'TS4': '3', 'TS5': '5', 'jobhash': 0, 'OVC': 0}),
    ({'name': 'testevent', 'ph': 'X', 'ts': 3.141, 'args': {'TS1': '1', 'TS2': '2', 'TS3': '1', 'TS4': '3', 'TS5': '5', 'jobhash': 0}},
     {'TS1': '1', 'TS2': '2', 'TS3': '1', 'TS4': '3', 'TS5': '5', 'jobhash': 0, 'OVC': 0}),
]


@pytest.mark.parametrize("event,result", ts_seq_ign_err_test_events)
def test_tsx_32bit_global_correction_ign(event, result, normalization_ctx_ign_crit):
    assert normalization_ctx_ign_crit.tsx_32bit_global_correction(0, event) == result
