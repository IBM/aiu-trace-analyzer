# Copyright 2024-2025 IBM Corporation

from aiu_trace_analyzer.types import TraceEvent


class AbstractContext:

    OPENING_EVENTS = ["B", "b", "("]
    CLOSING_EVENTS = ["E", "e", ")"]
    DEFAULT_WINDOWSIZE = 20

    '''
    Abstract Context

    Contexts are passed to processing functions to allow keeping track of any global state
    while events are being streamed through the pipeline without external state.
    E.g. if there's a need to keep track of things like event counts, latest timestamp, mapping tables,
    or if there's a need to hold back any event until a different event appears in the stream: contexts are your friend

    Contexts are attached to processing functions at the time of registration.
    So they're specific to a processing function as of now.

    TODO: Might have to be extended should the need arise to assign contexts to events or other components.
    '''
    def __init__(self) -> None:
        # intentionnaly left blank
        pass

    '''
    If the context has any form of buffer, the processing loop drains those buffers using this function call.
    drain() needs to do any necessary processing of the buffered events and return anything of value as
    a list of events.
    Events are drained following the sequence of registered processing functions.
    '''
    def drain(self) -> list[TraceEvent]:
        return []
