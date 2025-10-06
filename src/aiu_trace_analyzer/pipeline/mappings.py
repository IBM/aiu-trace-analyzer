# Copyright 2024-2025 IBM Corporation

import copy
import re

import aiu_trace_analyzer.logger as aiulog
from aiu_trace_analyzer.pipeline.context import AbstractContext
from aiu_trace_analyzer.types import TraceEvent


# turn complete (X) events into a pair of duration events (B+E)
def map_complete_to_duration(event: TraceEvent, _: AbstractContext) -> list[TraceEvent]:
    if event["ph"] == "X":
        # extract the duration from the event
        duration = event.pop("dur", 0)
        assert (duration != 0)
        # create a B

        event["ph"] = "B"
        b_event = copy.deepcopy(event)

        # create a E
        event["ph"] = "E"
        event["ts"] += duration

        return [b_event, event]

    return [event]


def remove_ids_from_name(event: TraceEvent, _: AbstractContext) -> list[TraceEvent]:
    '''
    Removes requestIDs from the event names and stores them as an args k/v entry
    This allows async-events to occupy the same viz-stream
    One exception is to be made for Recv_ events: there the id is the peer
    '''
    name_converter = re.compile(r"_\d+[_ -]")
    matchid = name_converter.search(event["name"])
    if matchid and "Recv_" not in event["name"]:
        aiulog.log(aiulog.DEBUG, "REPLACING:", event["name"])
        if "args" not in event:
            event["args"] = {}
        event["args"]["reqID"] = event["name"][matchid.start() + 1:matchid.end() - 1]
        event["name"] = name_converter.sub(" ", event["name"])
    return [event]
