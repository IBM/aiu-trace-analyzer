# Copyright 2024-2025 IBM Corporation

import aiu_trace_analyzer.logger as aiulog
from aiu_trace_analyzer.types import TraceEvent, TraceWarning


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
    def __init__(self, warnings: list[TraceWarning] = None) -> None:
        self.warnings: dict[str, TraceWarning] = {}

        if warnings is not None:
            for w in warnings:
                self.add_warning(w)

    def __del__(self) -> None:
        self.print_warnings()

    def print_warnings(self) -> None:
        for _, w in self.warnings.items():
            if w.has_warning():
                aiulog.log(aiulog.WARN, w)

    def add_warning(self, warning: TraceWarning):
        self.warnings[warning.get_name()] = warning

    def issue_warning(self, w_name: str, data: dict[str, any] = {}) -> int:
        '''
        this will use the default update_fn (int.__add__) for issued warnings
        child classes need to reimplement their own if that's insufficient
        '''
        if len(data) == 0:
            return self.warnings[w_name].update(data={"count": 1})
        else:
            return self.warnings[w_name].update(data)

    def drain(self) -> list[TraceEvent]:
        '''
        If the context has any form of buffer, the processing loop drains those buffers using this function call.
        drain() needs to do any necessary processing of the buffered events and return anything of value as
        a list of events.
        Events are drained following the sequence of registered processing functions.
        '''
        return []
