# Copyright 2024-2026 IBM Corporation

"""
Verification module for AIU trace events - EXAMPLE/TEMPLATE.

This module serves as an educational example and template for implementing
trace event verification in the AIU trace analyzer pipeline. It demonstrates:
- How to create a custom context class extending AbstractContext
- How to implement event verification logic
- How to track warnings and collect statistics
- How to create pipeline-compatible functions

For additional details how to integrate into the pipeline, see '../pipeline/template.py'

The verification logic here is intentionally simple (checking event phase types)
to serve as a learning reference. Real verification implementations should be
adapted based on specific trace analysis requirements.
"""

import aiu_trace_analyzer.logger as aiulog

from aiu_trace_analyzer.types import TraceEvent
from aiu_trace_analyzer.pipeline.context import AbstractContext
from aiu_trace_analyzer.types import TraceWarning


class VerificationContext(AbstractContext):
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
        type_collect (set): Set of unique event phase types encountered.
    """

    _FWARN_KEY = "failed_verification"

    def __init__(self, warnings: list[TraceWarning] = None) -> None:
        """
        Initialize the verification context - EXAMPLE IMPLEMENTATION.

        This demonstrates how to set up a context with custom warnings.
        The warning is configured with a template message that uses data
        placeholders for dynamic values.

        Args:
            warnings: Optional list of additional warnings to track.
        """
        super().__init__(warnings=[
            TraceWarning(
                name=self._FWARN_KEY,
                text="VRF: Encountered {d[count]} Events that failed verification",
                data={"count": 0},
                is_error=True,
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
        types are counted as verification failures. The list is intentionally
        made incomplete to demonstrate how verification can fail.

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
        if event["ph"] not in "XBEbe":
            self.warnings[self._FWARN_KEY].update()
            return False
        return True

    def drain(self) -> list[TraceEvent]:
        """
        EXAMPLE: Finalize verification and log collected event types.

        This demonstrates how to implement cleanup/finalization logic in a
        context. The drain() method is called at the end of trace processing
        and is useful for logging summaries, finalizing statistics, or
        performing any end-of-processing tasks.

        Returns:
            list[TraceEvent]: Empty list from parent drain() method. May also return
                              a list of events that have been held back during processing
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
