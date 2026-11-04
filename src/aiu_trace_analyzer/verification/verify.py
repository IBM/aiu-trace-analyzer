# Copyright 2024-2026 IBM Corporation

"""
Verification module for AIU trace events - EXAMPLE/TEMPLATE.

This module serves as an educational example and template for implementing
trace event verification in the AIU trace analyzer pipeline. It demonstrates:
- How to create a custom context class extending AbstractVerificationContext
- How to implement event verification logic
- How to track warnings and collect statistics
- How to create pipeline-compatible functions

For additional details how to integrate into the pipeline, see '../pipeline/template.py'

IMPORTANT: Verification stages require a Context, because everything the report is built
from is emitted at drain time by the context, as two kinds of metadata event:

  * one 'trace_finding' per TraceWarning of the context, emitted by AbstractContext.
    Every warning is reported, including the ones that never fired: a finding with
    count 0 is what lets the report list a check as passed. Findings of a verification
    context carry the test_name, which is what marks them as an actual check rather
    than an advisory warning of a regular processing stage.
  * one 'verification_test_result' per stage, emitted by AbstractVerificationContext,
    carrying the PASS/WARN/FAIL status derived from the warnings of the context.

Without a context, the report will not contain info about this stage.

Two consequences worth knowing when writing a stage:
  * a drain() override MUST chain to super().drain() (see the example below), or none of
    those events are emitted and the stage silently disappears from the report.
  * the is_error flag of a warning decides both the section it lands in (errors vs
    warnings) and the outcome: an error-level warning that fired makes the report FAIL
    and the acelyzer exit code non-zero. It never aborts the run - the report is always
    written first.

The verification logic here is intentionally simple (checking event phase types)
to serve as a learning reference. Real verification implementations should be
adapted based on specific trace analysis requirements.
"""

import aiu_trace_analyzer.logger as aiulog

from aiu_trace_analyzer.types import TraceEvent
from aiu_trace_analyzer.pipeline.context import AbstractContext, AbstractVerificationContext
from aiu_trace_analyzer.types import TraceWarning


class VerificationContext(AbstractVerificationContext):
    """
    EXAMPLE: Context class for managing trace event verification state.

    This serves as a template demonstrating how to create a custom context
    for trace analysis. It shows how to:
    - Extend AbstractContext, TwoPhaseWithBarrierContext or other abstract
      support classes for pipeline integration
    - Initialize and track warnings
    - Collect statistics during event processing

    This example tracks verification failures and collects event types
    encountered during trace processing.

    Attributes:
        _FWARN_KEY (str): Key for accessing the failed verification warning.
        _UWARN_KEY (str): Key for accessing the unchecked-phase warning.
        type_collect (set): Set of unique event phase types encountered.
    """

    # the name of the test as it appears in the report. Required for every verification context:
    # findings are attributed to it, and without it they are not reported as verified
    test_name = "Phase Type Verification"
    _FWARN_KEY = "failed_verification"
    _UWARN_KEY = "unchecked_phase"

    # phase types this example accepts, and ones it tolerates without actually checking them
    _VALID_PHASES = "XBEbe"
    _TOLERATED_PHASES = "iCM"

    def __init__(self, warnings: list[TraceWarning] = None) -> None:
        """
        Initialize the verification context - EXAMPLE IMPLEMENTATION.

        This demonstrates how to set up a context with custom warnings.
        The warnings are configured with a template message that uses data
        placeholders for dynamic values.

        Two warnings on purpose, to show what the is_error flag decides:
        - an error-level one lands in the 'errors' section of the report and, once it
          fires, turns the result into FAIL and the exit code non-zero
        - a warning-level one lands in 'warnings' and only degrades the result to WARN
        A warning that never fires is reported as a passed check instead.

        Args:
            warnings: Optional list of additional warnings to track.
        """
        super().__init__(warnings=[
            TraceWarning(
                name=self._FWARN_KEY,
                text="VRF: Encountered {d[count]} Events that failed verification",
                data={"count": 0},
                is_error=True,
            ),
            TraceWarning(
                name=self._UWARN_KEY,
                text="VRF: Encountered {d[count]} Events of a phase type that is not checked",
                data={"count": 0},
            )]
        )
        # Initialize set to collect unique event phase types encountered while processing
        self.type_collect = set([])

    def event_verification(self, event: TraceEvent) -> bool:
        """
        EXAMPLE: Verify a trace event meets expected criteria.

        This is a simple example verification that checks event phase types.
        Real implementations should include more sophisticated validation
        based on specific trace requirements (e.g., timestamp ordering,
        required fields, value ranges, etc.).

        This example collects the event's phase type and checks if it's one
        of the valid phase types (X, B, E, b, e). Events with invalid phase
        types are counted as verification failures. The valid list is
        intentionally made incomplete to demonstrate how verification can fail.

        It also shows the difference between the two severities: an unchecked
        phase type is only worth a warning, an unknown one is an error.

        Args:
            event: The trace event to verify.

        Returns:
            bool: True if the event passes verification, False otherwise.
                  Return values are not required depending on your use case.
        """
        # Collect the event phase type for analysis
        self.type_collect.add(event["ph"])

        # Verify event phase type is one of the expected types:
        # X: Complete event, B: Begin event, E: End event, b: Nested begin, e: Nested end
        if event["ph"] in self._VALID_PHASES:
            return True

        if event["ph"] in self._TOLERATED_PHASES:
            # warning level: nothing is wrong with the event, this check just says nothing about it.
            # Counted only - use issue_warning() when the individual events are not of interest.
            self.issue_warning(self._UWARN_KEY)
            return True

        # error level: this one fails the test. add_instance() records the offending event so the
        # report can list the details, not just the count. Instances go into the verification
        # report only, never into a trace file, where they could grow without bound.
        self.issue_warning(self._FWARN_KEY)
        self.warnings[self._FWARN_KEY].add_instance({"phase": event["ph"]})
        return False

    def drain(self) -> list[TraceEvent]:
        """
        EXAMPLE: Finalize verification and log collected event types.

        Note the super().drain() call: it is what emits the trace_finding events for the
        warnings of this context plus the verification_test_result of the test. An override
        that forgets to chain drops all of them, and the stage vanishes from the report
        without any error - so always return super().drain() alongside your own events.

        Returns:
            list[TraceEvent]: one trace_finding M-event per warning of this context, plus
                              one verification_test_result M-event for the test itself.
        """
        aiulog.log(aiulog.INFO, "VRF: found types:", self.type_collect)
        return super().drain()


def verify(event: TraceEvent, ctx: AbstractContext, _cfg: dict = None) -> list[TraceEvent]:
    """
    EXAMPLE: Pipeline function to verify a trace event.

    This demonstrates the standard signature for pipeline functions:
    - Takes an event, context, and optional config
    - Returns a list of events (allows filtering, splitting, or passing through)
    - Uses assertions to validate context type

    This example delegates verification to the VerificationContext and passes
    the event through unchanged. Real implementations might filter out invalid
    events or modify them based on verification results.

    Args:
        event: The trace event to verify.
        ctx: The context object, must be a VerificationContext instance.
        _cfg: Optional configuration dictionary (unused in this example).

    Returns:
        list[TraceEvent]: Single-element list containing the input event.

    Raises:
        AssertionError: If ctx is not a VerificationContext instance.
    """
    assert isinstance(ctx, VerificationContext), "context must be of type VerificationContext"
    ctx.event_verification(event)
    return [event]


def verify_cleanup(event: TraceEvent, _ctx: AbstractContext = None, _cfg: dict = None) -> list[TraceEvent]:
    """
    EXAMPLE: Clean up trace event by removing ingestion artifacts.

    This demonstrates a simple cleanup/transformation pipeline function.
    It shows how to:
    - Modify event data in-place
    - Handle optional fields safely
    - Work with different field name conventions

    This example removes the 'jobhash' field that may have been added during
    trace ingestion. Real cleanup functions might normalize timestamps,
    remove debug fields, or standardize event formats.

    Args:
        event: The trace event to clean up.
        _ctx: Optional context object (unused in this example).
        _cfg: Optional configuration dictionary (unused in this example).

    Returns:
        list[TraceEvent]: Single-element list containing the cleaned event.
    """
    # Determine which field name is used for event arguments
    # Some events use 'args', others use 'attr' (used by FLEX trace files)
    the_args = "args" if "attr" not in event else "attr"

    # Remove jobhash if it exists in the event's arguments
    if the_args in event and "jobhash" in event[the_args]:
        del event[the_args]["jobhash"]
    return [event]
