# Copyright 2024-2025 IBM Corporation

import pytest

from aiu_trace_analyzer.pipeline.overlap import OverlapDetectionContext, recombine_cpu_events
from aiu_trace_analyzer.types import TraceEvent


list_of_cpu_combine_tests = [
    # wrong name -> do not touch
    ({"name": "AIU Roundtrip", "ph": "X", "tid": 2000,
      "args": {"some": "random"}},
     {"name": "AIU Roundtrip", "ph": "X", "tid": 2000,
      "args": {"some": "random"}}),
    # wrong name (no args) -> do not touch
    ({"name": "AIU Roundtrip", "ph": "X", "tid": 2000},
     {"name": "AIU Roundtrip", "ph": "X", "tid": 2000}),
    # regular CPU -> update
    ({"name": "cpu_event1", "ph": "X", "tid": 2000,
      "args": {"some": "random"}},
     {"name": "cpu_event1", "ph": "X", "tid": 1000,
      "args": {"some": "random"}}),
    # aiu event -> do not touch
    ({"name": "aiu_event1", "ph": "X", "tid": 3000,
      "args": {"TS1": 123456}},
     {"name": "aiu_event1", "ph": "X", "tid": 3000,
      "args": {"TS1": 123456}}),
    # cpu without args -> update
    ({"name": "cpu_event2", "ph": "X", "tid": 3000},
     {"name": "cpu_event2", "ph": "X", "tid": 1000}),
    # not X type -> do not touch
    ({"name": "cpu_event3", "ph": "b", "tid": 3000},
     {"name": "cpu_event3", "ph": "b", "tid": 3000}),
]


# flex_event_with_jobhash fixture is adding a flex-dialect jobhash info
# to allow dialect detection to work in the called functions
@pytest.mark.parametrize(
    "flex_event_with_jobhash,expected",
    list_of_cpu_combine_tests,
    indirect=['flex_event_with_jobhash'])
def test_get_cycles(flex_event_with_jobhash: TraceEvent, expected: TraceEvent):

    modified = recombine_cpu_events(flex_event_with_jobhash, context=None, config={"cpu_stream_tid": 1000})

    # revert the 'addition' of the jobhash, before comparison
    assert "args" in modified[0]
    modified[0]["args"].pop("jobhash")
    if len(modified[0]["args"]) == 0:
        modified[0].pop("args")

    # actual result check
    assert modified[0] == expected


###########################################################
# overlap detection: strict vs. non-strict mode

def _x_event(ts, dur, pid=1, tid=1, name="kernel") -> TraceEvent:
    return TraceEvent({"ph": "X", "pid": pid, "tid": tid, "ts": ts, "dur": dur, "name": name, "args": {}})


def _detect(events: list[TraceEvent], strict: bool) -> int:
    '''run the events through a warn-only detection and return the number of detected overlaps'''
    context = OverlapDetectionContext(
        overlap_resolve=OverlapDetectionContext.OVERLAP_RESOLVE_WARN, strict=strict)
    for event in events:
        context.overlap_detection(event)
    return context.warnings["overlaps"].args_list["count"]


# (description, events, expected non-strict count, expected strict count)
list_of_overlap_mode_tests = [
    ("no overlap at all",
     [_x_event(0.0, 10.0), _x_event(20.0, 10.0)], 0, 0),
    ("back-to-back: previous event ends when the next one starts",
     [_x_event(0.0, 10.0), _x_event(10.0, 10.0)], 0, 0),
    ("partial overlap is flagged in both modes",
     [_x_event(0.0, 10.0), _x_event(5.0, 10.0)], 1, 1),
    ("fully embedded event is a legitimate nesting only in non-strict mode",
     [_x_event(0.0, 100.0), _x_event(10.0, 10.0)], 0, 1),
    ("embedded event sharing the start ts",
     [_x_event(0.0, 100.0), _x_event(0.0, 10.0)], 0, 1),
    ("several independent events on one stream stay unflagged",
     [_x_event(0.0, 5.0), _x_event(10.0, 5.0), _x_event(20.0, 5.0), _x_event(30.0, 5.0)], 0, 0),
    ("separate streams never overlap each other",
     [_x_event(0.0, 10.0, tid=1), _x_event(5.0, 10.0, tid=2)], 0, 0),
]


@pytest.mark.parametrize(
    "description,events,expected_default,expected_strict",
    list_of_overlap_mode_tests)
def test_overlap_modes(description, events, expected_default, expected_strict):
    assert _detect(events, strict=False) == expected_default, f"non-strict: {description}"
    assert _detect(events, strict=True) == expected_strict, f"strict: {description}"


def test_strict_does_not_flag_events_after_the_queue_ran_empty():
    # regression guard: the blocked status of a queue has to be re-evaluated for the ts of the
    # incoming event. Every event leaves its own end-ts behind, so a status that is only updated
    # after the overlap check would keep the queue blocked and flag every subsequent event.
    events = [_x_event(float(i) * 10.0, 5.0) for i in range(10)]
    assert _detect(events, strict=True) == 0


def test_strict_flags_every_event_of_an_embedded_chain():
    # one long event with three events nested inside it: all three are overlaps in strict mode
    events = [_x_event(0.0, 100.0), _x_event(10.0, 5.0), _x_event(20.0, 5.0), _x_event(30.0, 5.0)]
    assert _detect(events, strict=True) == 3
    assert _detect(events, strict=False) == 0
