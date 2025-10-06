# Copyright 2024-2025 IBM Corporation

from aiu_trace_analyzer.types import TraceEvent
from aiu_trace_analyzer.pipeline import AbstractContext


def drop_global_events(event: TraceEvent, _: AbstractContext) -> list[TraceEvent]:
    '''
    drop the space taker: Execute graph, SenFusedDeviceNode, AIU Roundtrip, Flex RoundTrip
    '''

    glb_names = ["Execute graph", "SenFusedDeviceNode", "AIU Roundtrip", "Flex RoundTrip",
                 "PostKeys", "FetchKeys", "Callback", "HostPrep", "AllocateFrame of", "Update CBs"]

    def is_global_event(event_name) -> bool:
        for name_part in glb_names:
            if name_part in event_name:
                return True
        return False

    if is_global_event(event["name"]):
        return []

    return [event]
