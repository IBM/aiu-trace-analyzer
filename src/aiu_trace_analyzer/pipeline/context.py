# Copyright 2024-2025 IBM Corporation

import aiu_trace_analyzer.logger as aiulog
from aiu_trace_analyzer.types import (
    DiagnosticEvent,
    TraceEvent,
    TraceWarning,
    TRACE_FINDING_EVENT_NAME,
    VERIFICATION_TEST_RESULT_NAME)


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

    def _emit_findings(self) -> list[TraceEvent]:
        '''
        Emit every warning of this context as a finding meta-event, so an exporter can turn it into
        whatever its output format requires.

        All warnings are emitted, including the ones that never fired: a finding with count == 0 is
        what tells the verification report that a check ran and passed. Filtering is up to the
        consumer - the trace exporter only keeps the ones that actually fired.
        '''
        return [
            DiagnosticEvent({"ph": "M", "ts": 0, "pid": 0,
                             "name": TRACE_FINDING_EVENT_NAME,
                             "args": {**w.to_finding_args(), **self._finding_extra_args()}})
            for w in self.warnings.values()
        ]

    def _finding_extra_args(self) -> dict:
        '''
        additional args to attach to every finding of this context. The base context has no extra
        provenance to add; AbstractVerificationContext uses this to mark its findings as belonging
        to a named test.
        '''
        return {}

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

        CONTRACT: this base implementation emits the accumulated warnings of the context.
        Any override MUST chain to super().drain() (e.g. `return revents + super().drain()`),
        otherwise the warnings of that context are silently dropped instead of being exported.
        '''
        if not self.is_enabled():
            # a stage that was disabled by the profile has no findings to report: claiming its
            # checks passed would be wrong, and its counters were never maintained either.
            return []
        return self._emit_findings()


class AbstractVerificationContext(AbstractContext):
    '''
    Context of a verification stage.

    Adds test identity on top of the plain warning accumulation: its findings are marked with the
    name of the test they belong to, which is what distinguishes an actual check from an advisory
    warning of a regular processing stage. On top of the findings it emits one test result event
    per drain, the only event that reports whether the check ran at all.

    Derived classes MUST set test_name: without it the report cannot attribute the findings, and a
    check that reported nothing is indistinguishable from an advisory warning that stayed quiet.
    '''
    test_name: str = ""

    def drain(self) -> list[TraceEvent]:
        if not self.test_name:
            aiulog.log(aiulog.ERROR,
                       f"{type(self).__name__} is a verification context without a test_name."
                       " Its findings cannot be attributed to a test and will not be reported"
                       " as verified. Set the test_name class attribute.")
        events = super().drain()
        events.append(self._emit_test_result_event(self.test_name))
        return events

    def _finding_extra_args(self) -> dict:
        # only a named test can claim its findings: an empty name would make them look verified
        # while leaving the report unable to say by what
        return {"test": self.test_name} if self.test_name else {}

    def _get_test_result_status(self) -> str:
        if any(w.is_error and w.has_warning()
               for w in self.warnings.values()):
            return "fail"
        if any(w.has_warning() for w in self.warnings.values()):
            return "warn"
        return "pass"

    def _emit_test_result_event(self, test_name: str) -> TraceEvent:
        return DiagnosticEvent({"ph": "M", "ts": 0, "pid": 0,
                                "name": VERIFICATION_TEST_RESULT_NAME,
                                "args": {"test": test_name,
                                         "result": self._get_test_result_status()}})
