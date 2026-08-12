# Copyright 2024-2026 IBM Corporation

"""
Overlap verification for accelerator compute events.

Compute events of the same stream cannot overlap in time: the stream processes them one after
another. Any detected overlap therefore indicates a data problem (e.g. inaccurate timestamps)
and is reported as an error-level finding of the verification report.

This module reuses the overlap detection of the regular processing pipeline
(OverlapDetectionContext) and only replaces the parts that differ in verification mode:
  * overlaps are reported, never resolved
  * the finding is error-level and records each offending event as an instance
  * the streams are identified per input dialect instead of by pid+tid
"""

from aiu_trace_analyzer.types import TraceEvent, TraceWarning
from aiu_trace_analyzer.pipeline.context import AbstractContext, AbstractVerificationContext
from aiu_trace_analyzer.pipeline.overlap import OverlapDetectionContext
from aiu_trace_analyzer.pipeline.tools import PipelineContextTool


class OverlapVerificationContext(OverlapDetectionContext, AbstractVerificationContext):
    '''
    Verification-mode variant of the overlap detection: it only reports overlapping compute
    events, it never resolves them (always OVERLAP_RESOLVE_WARN). Every detected overlap is
    recorded as an instance of an error-level finding and the accumulated findings are emitted
    as verification meta-data events by AbstractVerificationContext.drain() (reached via the MRO,
    the parent drain chains up to it; no two-phase barrier required for this context).

    Compute streams are identified per dialect: FLEX events have a single stream per pid, while
    TORCH events separate the streams within a pid by their 'args.stream' entry.
    '''
    test_name = "Compute Overlap Check"

    _OVERLAP_WARNING = "overlaps"
    _STREAM_KEY = "stream"
    _STREAM_ARG = "args." + _STREAM_KEY
    _DEFAULT_STREAM = 0  # convention: actual stream numbers start at 1, 0 means 'no stream entry'

    def __init__(self, strict=False) -> None:
        super().__init__(self.OVERLAP_RESOLVE_WARN, strict=strict)
        # replace the resolution-oriented warning of the parent: in verification mode nothing is
        # resolved, a detected overlap is a finding that has to fail the test
        self.add_warning(
            TraceWarning(
                name=self._OVERLAP_WARNING,
                text="Overlapping accelerator events detected: {d[count]}",
                data={"count": 0},
                is_error=True,
            )
        )

    def _select_queue_id_keys(self, event: TraceEvent) -> list[str]:
        dialect = PipelineContextTool.get_dialect_of_event(event)
        assert dialect is not None, \
            "OVL: cannot determine the dialect of the first event." \
            " Register this stage before any stage that removes the jobhash."
        if dialect.get("NAME") == "TORCH":
            return ["pid", self._STREAM_ARG]
        return ["pid"]

    def _record_overlap(self, oevent: TraceEvent) -> None:
        super()._record_overlap(oevent)
        self.warnings[self._OVERLAP_WARNING].add_instance({
            "name": oevent["name"],
            "pid": oevent["pid"],
            "tid": oevent["tid"],
            "stream": oevent["args"].get(self._STREAM_KEY),
            "ts": oevent["ts"],
            "dur": oevent["dur"],
        })

    def add_default_stream(self, event: TraceEvent) -> bool:
        '''
        the stream-based separation of queues requires the stream entry to exist. Events without
        one are all attributed to the same default stream. Returns whether a default was added.
        '''
        if self._STREAM_KEY in event["args"]:
            return False
        event["args"][self._STREAM_KEY] = self._DEFAULT_STREAM
        return True

    def remove_default_stream(self, event: TraceEvent) -> None:
        del event["args"][self._STREAM_KEY]


def verify_kernel_overlap(event: TraceEvent, context: AbstractContext) -> list[TraceEvent]:
    assert isinstance(context, OverlapVerificationContext)

    # only compute events are checked; anything else just passes through
    if event["ph"] not in "X" or not PipelineContextTool.is_acc_event(event):
        return [event]

    # the default is only required to determine the queue/stream of this event: drop it again to
    # keep the event unchanged for any downstream stage
    default_added = context.add_default_stream(event)
    revents = context.overlap_detection(event)
    if default_added:
        context.remove_default_stream(event)
    return revents
