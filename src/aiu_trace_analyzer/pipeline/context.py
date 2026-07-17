# Copyright 2024-2025 IBM Corporation

import aiu_trace_analyzer.logger as aiulog
from aiu_trace_analyzer.types import DiagnosticEvent, TraceEvent, TraceWarning, TRACE_ISSUE_EVENT_NAME


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
    '''
    def __init__(self, warnings: list[TraceWarning] = None) -> None:
        self.warnings: dict[str, TraceWarning] = {}
        self.enabled = False

        if warnings is not None:
            for w in warnings:
                self.add_warning(w)

    def enable(self) -> bool:
        self.enabled = True
        return self.enabled

    def disable(self) -> bool:
        # use OR to make sure any previous activation cannot be overwritten
        self.enabled |= False
        if not self.enabled:
            self._disable_warnings()
        return self.enabled

    def is_enabled(self) -> bool:
        return self.enabled

    def _disable_warnings(self) -> None:
        for _, w in self.warnings.items():
            w.occurred = False

    def print_warnings(self) -> None:
        for _, w in self.warnings.items():
            if w.has_warning():
                aiulog.log(aiulog.WARN, w)

    def emit_issue_events(self) -> list[TraceEvent]:
        '''
        emit each active warning as a meta-event so the exporter can fold it into the output json.
        this mirrors the verification-event mechanism (see _emit_verification_events) but targets the
        regular trace output: the warnings ride through the pipeline as events instead of being tracked
        on the side, so no context needs to be kept alive past drain().
        '''
        return [
            DiagnosticEvent({"ph": "M", "ts": 0, "pid": 0,
                             "name": TRACE_ISSUE_EVENT_NAME,
                             "args": {"finding": name, "text": str(w)}})
            for name, w in self.warnings.items() if w.has_warning()
        ]

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
        return self.emit_issue_events()

    def _emit_verification_events(self) -> list[TraceEvent]:
        return [
            DiagnosticEvent({"ph": "M", "ts": 0, "pid": 0,
                             "name": "verification_data",
                             "args": w.to_verification_event_args()})
            for w in self.warnings.values()
        ]

    def _get_test_result_status(self) -> str:
        if any(w.warn_level == aiulog.ERROR and w.has_warning()
               for w in self.warnings.values()):
            return "fail"
        if any(w.has_warning() for w in self.warnings.values()):
            return "warn"
        return "pass"

    def _emit_test_result_event(self, test_name: str) -> TraceEvent:
        return DiagnosticEvent({"ph": "M", "ts": 0, "pid": 0,
                                "name": "verification_test_result",
                                "args": {"test": test_name,
                                         "result": self._get_test_result_status()}})


class AbstractVerificationContext(AbstractContext):
    test_name: str = ""

    def drain(self) -> list[TraceEvent]:
        events = self._emit_verification_events()
        events.append(self._emit_test_result_event(self.test_name))
        return events
