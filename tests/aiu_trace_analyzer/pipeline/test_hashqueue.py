# Copyright 2024-2025 IBM Corporation

import pytest

from aiu_trace_analyzer.pipeline.be_pair import AbstractHashQueueContext, EventPairDetectionContext


# helper list to test input
_event_list = [
        {'name': 'eventA', 'ph': 'B', 'ts': 0,  'pid': 0},
        {'name': 'eventA', 'ph': 'E', 'ts': 5,  'pid': 0},
        {'name': 'eventA', 'ph': 'B', 'ts': 10, 'pid': 0},
        {'name': 'eventA', 'ph': 'E', 'ts': 15, 'pid': 1},
        {'name': 'eventA', 'ph': 'B', 'ts': 20, 'pid': 1},
        {'name': 'eventA', 'ph': 'E', 'ts': 25, 'pid': 1},
    ]


@pytest.mark.parametrize("event,result",
                         [
                             ({'ph': 'B', 'name': 'testevent', 'ts': 1},                      hash(('testevent',))),
                             ({'ph': 'E', 'name': 'testevent', 'ts': 1, 'args': {'some': 0}}, hash(('testevent',))),
                             ({'ph': 'X', 'name': 'testevent', 'dur': 5, 'ts': 0},            hash(('testevent', 5))),
                             pytest.param({'ph': 'b'}, None, marks=pytest.mark.xfail),
                         ])
def test_event_data_hash(event, result):
    context = AbstractHashQueueContext()
    include_keys = ["name", "dur"]
    actual = context.event_data_hash(event, include_keys, ignore_missing=True)
    assert actual == result


@pytest.mark.parametrize("event,mixed,result",
                         [
                             ({'ph': 'B', 'name': 'testevent', 'pid': 0, 'ts': 1},
                              False, hash(('testevent', 0))),
                             ({'ph': 'B', 'name': 'testevent', 'pid': 0, 'ts': 1},
                              True,  hash(('testevent',))),
                             ({'ph': 'E', 'name': 'testevent', 'pid': 0, 'ts': 1, 'args': {'some': 0}},
                              False, hash(('testevent', 0))),
                             ({'ph': 'E', 'name': 'testevent', 'ts': 1, 'args': {'some': 0}},
                              True,  hash(('testevent',))),
                             ({'ph': 'X', 'name': 'testevent', 'pid': 0, 'dur': 5, 'ts': 0},
                              False, hash(('testevent', 0))),
                             ({'ph': 'X', 'name': 'testevent', 'dur': 5, 'ts': 0},
                              True,  hash(('testevent',))),
                             pytest.param({'ph': 'b'}, False, None, marks=pytest.mark.xfail),
                             pytest.param({'ph': 'b'}, True,  None, marks=pytest.mark.xfail),
                         ])
def test_create_reference_data(event, mixed, result):
    context = EventPairDetectionContext()
    ref, include = context.create_reference_data(event, mixed_queue=mixed)
    assert ref == result
    if not mixed:
        assert "pid" in include
        assert "tid" in include
    assert (not mixed and len(include) == 3) or (mixed and len(include) == 1)
