# Copyright 2024-2025 IBM Corporation

import pytest

from aiu_trace_analyzer.pipeline.mappings import map_complete_to_duration, remove_ids_from_name


@pytest.mark.parametrize("event,result",
                         [
                             ({'ph': 'B', 'name': 'testevent'}, [{'ph': 'B', 'name': 'testevent'}]),
                             ({'ph': 'E', 'name': 'testevent'}, [{'ph': 'E', 'name': 'testevent'}]),
                             ({'ph': 'X', 'name': 'testevent', 'dur': 5, 'ts': 0},
                              [{'ph': 'B', 'name': 'testevent', 'ts': 0},
                               {'ph': 'E', 'name': 'testevent', 'ts': 5}]),
                         ])
def test_map_complete_to_duration(event, result):
    assert map_complete_to_duration(event, None) == result


@pytest.mark.parametrize("event,result",
                         [
                             ({'ph': 'X', 'name': 'test_123_tset'},
                              [{'ph': 'X', 'name': 'test tset', 'args': {'reqID': '123'}}]),
                             pytest.param({'ph': 'b'}, None, marks=pytest.mark.xfail),
                         ])
def test_file_gen(event, result):
    assert remove_ids_from_name(event, None) == result
