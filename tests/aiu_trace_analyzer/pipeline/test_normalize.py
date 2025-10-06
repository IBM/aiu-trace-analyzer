# Copyright 2024-2025 IBM Corporation

import pytest

from aiu_trace_analyzer.pipeline.normalize import _attr_to_args, _hex_to_int_str


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
