# Copyright 2024-2025 IBM Corporation

import pytest

from aiu_trace_analyzer.types import TraceEvent
from aiu_trace_analyzer.pipeline.mp_calc_bw_v2 import MpCalcBwV2Context


# setup the context class instance with a table as a fixture
@pytest.fixture
def mpc_ctx():
    mpc = MpCalcBwV2Context()

    # build up a context with some state to play with
    mpc.prev_group = (10.0, 20.0, set(["PreviousCollGroup"]))
    mpc.coll_groups["CurrentCollGroup"] = {
                    'T_bw_beg': 25.0,
                    'T_bw_end': 35.0,
                    'Coll_data_size': 104506,
                    'Procs': 4,
                }
    mpc.NP = 4

    return mpc


def test_create_counter(mpc_ctx):
    counter = mpc_ctx._create_counter(0.5, 10.2)

    assert isinstance(counter, dict)
    assert counter["ts"] == 0.5
    assert counter["args"]["GBps"] == 10.2


def test_gen_bw_counter_events(mpc_ctx):
    cg_start = 25.0
    cg_end = 35.0
    event = {"name": "Allreduce", "ph": "X", "ts": 34.2, "dur": 0.8, "args": {"CollGroup": "CurrentCollGroup"}}

    revents = mpc_ctx._gen_bw_counter_events(event, cg_start, cg_end, 104506, "CurrentCollGroup")

    # checking results:
    assert len(revents) == 3, "even, prev_zero, and current value need to be emitted"
    assert revents[0] == event
    assert revents[1]["ph"] == "C", "counter events type must be C"
    assert revents[1]["ts"] == 20.0, "zero event needs to have ts of prev collgroup end"
    assert revents[2]["ts"] == cg_start, "current counter ts to match beginning of new coll group"

    # checking mpc_ctx state changes after event generation:
    assert "CurrentCollGroup" not in mpc_ctx.coll_groups
    assert mpc_ctx.prev_group[0] == cg_start
    assert mpc_ctx.prev_group[1] == cg_end
    assert "CurrentCollGroup" in mpc_ctx.prev_group[2]
    assert "PreviousCollGroup" not in mpc_ctx.prev_group[2]
    assert mpc_ctx.NP == 0, "expecting NP to be reset after zero event"


def test_gen_bw_counter_events_overlap(mpc_ctx):
    cg_start = 18.0
    cg_end = 35.0
    event = {"name": "Allreduce", "ph": "X", "ts": 34.2, "dur": 0.8, "args": {"CollGroup": "CurrentCollGroup"}}

    revents = mpc_ctx._gen_bw_counter_events(event, cg_start, cg_end, 104506, "CurrentCollGroup")

    # checking results:
    assert len(revents) == 2, "no zero event when overlap detected"
    assert revents[0] == event
    assert revents[1]["ph"] == "C", "counter events type must be C"
    assert revents[1]["ts"] == cg_start, "current counter ts to match beginning of new coll group"

    # checking mpc_ctx state changes after event generation:
    assert "CurrentCollGroup" not in mpc_ctx.coll_groups
    assert mpc_ctx.prev_group[0] == 10.0, "previous start cannot be updated when overlapping"
    assert mpc_ctx.prev_group[1] == cg_end
    assert "CurrentCollGroup" in mpc_ctx.prev_group[2]
    assert "PreviousCollGroup" in mpc_ctx.prev_group[2]
    assert mpc_ctx.NP == 4, "expecting NP to be reset after zero event"


def test_insert():
    mpc_ctx = MpCalcBwV2Context()
    np = 4
    ts_list = [10, 12, 9, 11,   # arrivals of ranks of group1
               30, 31, 38, 35,  # arrivals of ranks of group2
               65, 60, 61, 62,  # arrivals of ranks of repeated group1
               ]
    name_list = ["Group1", "Group2", "Group1"]

    def create_event(i: int, name: str, ts: float) -> TraceEvent:
        return {
            "name": "CollectiveEvent",
            "ph": "X",
            "dur": 20,
            "ts": ts,
            "pid": i % np,
            "args": {
                "CollGroup": name,
                "peers": "{1,2,3,4}",
                "Coll_data_size": 1024
            }
        }

    cgroup_list = [x for x in name_list for _ in range(np)]

    inputlist = [create_event(i, name, ts) for i, (name, ts) in enumerate(zip(cgroup_list, ts_list))]

    result = []
    for event in inputlist:
        result += mpc_ctx.insert(event=event)

    # 12 events, 1 directly adjacent group would remove the zero-event for first group1
    #   and the final zero event requires drain to get called
    assert len(result) == 12 + 4
    assert len(mpc_ctx.coll_groups) == 0
    assert mpc_ctx.prev_group == (60.0, 85.0, {"Group1"})

    result += mpc_ctx.drain()
    assert len(result) == 12 + 5  # drain should add the final zero event
