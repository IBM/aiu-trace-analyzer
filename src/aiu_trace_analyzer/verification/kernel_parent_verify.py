# Copyright 2024-2026 IBM Corporation

"""
Kernel-Parent Timestamp Verification Module

This module implements verification logic to ensure that kernel events are properly
contained within their parent 'ScheduleCompute' event timeframes. This verification
is critical for validating trace data integrity and detecting timing anomalies.

The verification uses a two-phase processing approach:
1. Collection Phase: Gather parent (ScheduleCompute) timing information and track kernel counts
2. Verification Phase: Check each kernel event against its parent's timeframe

Key Features:
- Verifies kernel events fit within parent ScheduleCompute event bounds
- Detects orphan kernels (kernels without parent events)
- Detects orphan parents (parent events without any kernels)
- Reports violations as errors that fail verification mode
- Works with both Torch and Flex trace dialects

Data Structure:
    For each correlation ID, tracks:
    - parent_start: Minimum start timestamp across all parent events (inf if not seen)
    - parent_end: Maximum end timestamp across all parent events (-inf if not seen)
    - kernel_count: Number of kernel events seen for this correlation ID

Usage:
    This module is designed to be integrated into the verification pipeline profile.
    It requires a pipeline barrier between collection and verification phases.

    Example integration:
        ctx = KernelParentVerificationContext()
        processor.register_stage(callback=kernel_parent_collect, context=ctx)
        processor.register_stage(callback=pipeline_barrier, context=barrier_ctx)
        processor.register_stage(callback=kernel_parent_verify, context=ctx)

Warnings Tracked:
    - kernel_outside_parent: Kernels detected outside parent timeframe (ERROR)
    - orphan_kernels: Kernels without a parent ScheduleCompute event
    - orphan_parents: Parent ScheduleCompute events without any kernels
"""

import math

import aiu_trace_analyzer.logger as aiulog
from aiu_trace_analyzer.types import TraceEvent, TraceWarning
from aiu_trace_analyzer.pipeline.barrier import TwoPhaseWithBarrierContext
from aiu_trace_analyzer.pipeline.context import AbstractContext, AbstractVerificationContext


class KernelParentVerificationContext(AbstractVerificationContext, TwoPhaseWithBarrierContext):
    """
    Context for managing kernel-parent timestamp verification state.

    This context extends TwoPhaseWithBarrierContext to implement a two-phase
    verification process:
    1. Collection Phase: Store parent event timing and count kernel events
    2. Verification Phase: Validate kernel containment within parent bounds

    The context tracks correlation IDs and their associated parent timing
    information, enabling real-time verification of kernel events during
    the verification phase.

    Attributes:
        queues (dict): Inherited from AbstractHashQueueContext. Maps correlation IDs
                      to dictionaries containing:
                      - parent_start (float): Minimum parent event start timestamp (inf if not seen)
                      - parent_end (float): Maximum parent event end timestamp (-inf if not seen)
                      - kernel_count (int): Number of kernels seen for this correlation ID
        warnings (dict): Inherited from AbstractContext. Tracks verification warnings:
                        - kernel_outside_parent: Kernels outside parent bounds (ERROR)
                        - orphan_kernels: Kernels without parent events
                        - orphan_parents: Parents without kernel events
    """

    test_name = "Kernel-Parent Timestamp Check"

    def __init__(self, warnings=None):
        """
        Initialize the kernel-parent verification context.

        Sets up warning trackers for three types of verification issues:
        1. Kernels outside parent timeframe (marked as error)
        2. Orphan kernels (kernels without parent events)
        3. Orphan parents (parent events without kernels)

        Args:
            warnings (list[TraceWarning], optional): Additional warnings to track.
                                                     Defaults to None.
        """
        if warnings is None:
            warnings = []
        warnings.extend([
            TraceWarning(
                name="kernel_outside_parent",
                text="Kernel-Parent Verification: Found {d[count]} kernels outside parent timeframe",
                data={"count": 0},
                is_error=True  # This is a critical error that should fail verification
            ),
            TraceWarning(
                name="orphan_kernels",
                text="Kernel-Parent Verification: Found {d[count]} kernels without parent events",
                data={"count": 0}
            ),
            TraceWarning(
                name="orphan_parents",
                text="Kernel-Parent Verification: Found {d[count]} parent events without kernels",
                data={"count": 0},
            )
        ])
        super().__init__(warnings)

    def collect_parent_and_kernels(self, event: TraceEvent) -> None:
        """
        Collect parent timing information and track kernel events during collection phase.

        This method processes events during the collection phase to build a mapping
        of correlation IDs to their parent timing information and kernel counts.

        For ScheduleCompute events (parents):
            - Stores the start and end timestamps by correlation ID
            - Works with both Torch ("aiuLaunchScheduleCompute") and Flex ("ScheduleCompute")

        For kernel events:
            - Increments the kernel count for the correlation ID
            - Registers the correlation ID if not already tracked

        Correlation ID 0 is ignored as it typically represents invalid or placeholder events.

        Args:
            event (TraceEvent): The trace event to process. Must be a dictionary with
                               standard trace event fields (ph, name, cat, args, ts, dur).
        """
        # Only process events with correlation IDs
        if "args" not in event or "correlation" not in event["args"]:
            return

        correlation_id = event["args"]["correlation"]

        # Skip correlation ID 0 (typically invalid/placeholder)
        if correlation_id == 0:
            return

        # Check if this is a parent ScheduleCompute or ScheduleWait event
        # Pattern matches both Torch ("aiuLaunchScheduleCompute") and Flex ("ScheduleCompute", "ScheduleWait")
        if "ScheduleCompute" in event["name"] or "ScheduleWait" in event["name"]:
            # Get or create entry for this correlation ID
            qid = self.get_or_create(
                correlation_id,
                {"parent_start": float('inf'), "parent_end": float('-inf'), "kernel_count": 0}
            )
            # Store parent timing information, tracking min start and max end
            self.queues[qid]["parent_start"] = min(self.queues[qid]["parent_start"], event["ts"])
            self.queues[qid]["parent_end"] = max(self.queues[qid]["parent_end"], event["ts"] + event["dur"])

        # Check if this is a kernel event
        elif "cat" in event and event["cat"] == "kernel":
            # Get or create entry for this correlation ID
            qid = self.get_or_create(
                correlation_id,
                {"parent_start": float('inf'), "parent_end": float('-inf'), "kernel_count": 0}
            )
            # Increment kernel count
            self.queues[qid]["kernel_count"] += 1

    def verify_kernel_containment(self, event: TraceEvent) -> None:
        """
        Verify that a kernel event is contained within its parent's timeframe.

        This method is called during the verification phase for each kernel event.
        It checks whether the kernel's start and end timestamps fall within the
        bounds of its parent ScheduleCompute event.

        A kernel is considered valid if:
            kernel_start >= parent_start AND kernel_end <= parent_end

        If the parent timing information is not available (orphan kernel), no
        verification is performed here; orphans are detected in drain().

        Args:
            event (TraceEvent): The kernel event to verify. Must have:
                               - cat == "kernel"
                               - args.correlation: correlation ID
                               - ts: start timestamp
                               - dur: duration
        """
        # Only verify kernel events
        if "cat" not in event or event["cat"] != "kernel":
            return

        # Must have correlation ID
        if "args" not in event or "correlation" not in event["args"]:
            return

        correlation_id = event["args"]["correlation"]

        # Skip correlation ID 0
        if correlation_id == 0:
            return

        # Check if we have parent timing information for this correlation ID
        if correlation_id not in self.queues:
            aiulog.log(aiulog.ERROR, "BUG: verify_kernel_containment called for unknown correlation ID", correlation_id)
            return

        parent_data = self.queues[correlation_id]

        # If parent timing is not available, this is an orphan kernel — counted in drain()
        if math.isinf(parent_data["parent_start"]) or math.isinf(parent_data["parent_end"]):
            return

        # Calculate kernel timing
        kernel_start = event["ts"]
        kernel_end = event["ts"] + event["dur"]
        parent_start = parent_data["parent_start"]
        parent_end = parent_data["parent_end"]

        # Verify kernel is fully contained within parent bounds
        if kernel_start < parent_start or kernel_end > parent_end:
            # Log detailed event information
            aiulog.log(
                aiulog.DEBUG,
                f"Kernel-Parent Verification: Kernel outside parent timeframe - "
                f"Correlation ID: {correlation_id}, "
                f"Kernel: '{event['name']}', "
                f"Kernel time: [{kernel_start:.3f}, {kernel_end:.3f}], "
                f"Parent time: [{parent_start:.3f}, {parent_end:.3f}]"
            )
            # Update warning counter and record instance detail for the report
            self.warnings["kernel_outside_parent"].update({"count": 1})
            self.warnings["kernel_outside_parent"].add_instance({
                "corr_id": hex(correlation_id),
                "kernel_start": kernel_start,
                "kernel_end": kernel_end,
                "parent_start": parent_start,
                "parent_end": parent_end,
            })

    def drain(self) -> list[TraceEvent]:
        """
        Finalize verification and detect orphan correlation IDs.

        This method is called at the end of each phase. During the collection phase,
        it switches to the verification phase and releases barrier-held events into
        the downstream stages (kernel_parent_verify, etc.). During the verification
        phase, it performs orphan detection and emits verification M-events via
        AbstractVerificationContext.drain().

        Orphan Detection:
            - Orphan Parents: Parent events with kernel_count == 0
            - Orphan Kernels: Kernel events where parent_start or parent_end is infinity

        Returns:
            list[TraceEvent]: Verification M-events during the application phase; events
                              released from the barrier during the collection phase.
        """
        # Collection phase: hand off to TwoPhaseWithBarrierContext to switch the phase
        # and release any barrier-held events into the downstream stages.
        if self.collection_phase():
            return TwoPhaseWithBarrierContext.drain(self)

        for correlation_id, data in self.queues.items():
            # Check for orphan parents (parent without kernels)
            if data["kernel_count"] == 0 and not math.isinf(data["parent_start"]):
                self.warnings["orphan_parents"].update({"count": 1})
                self.warnings["orphan_parents"].add_instance({"corr_id": correlation_id})
                aiulog.log(
                    aiulog.DEBUG,
                    "Kernel-Parent Verification: Orphan parent - "
                    f"Correlation ID {correlation_id} has parent but no kernels"
                )

            # Check for orphan kernels (kernels without parent)
            if data["kernel_count"] > 0 and math.isinf(data["parent_start"]):
                number_of_kernels = data["kernel_count"]
                self.warnings["orphan_kernels"].update({"count": number_of_kernels})
                self.warnings["orphan_kernels"].add_instance(
                    {"corr_id": correlation_id, "count": number_of_kernels}
                )
                aiulog.log(
                    aiulog.WARN,
                    "Kernel-Parent Verification: Orphan kernels - "
                    f"Correlation ID {correlation_id} has {data['kernel_count']} kernel(s) but no parent"
                )

        # Application phase: MRO resolves super() to AbstractVerificationContext.drain(),
        # which appends verification_data and verification_test_result M-events.
        return super().drain()


def kernel_parent_collect(event: TraceEvent, ctx: AbstractContext) -> list[TraceEvent]:
    """
    Collection phase: gather parent timing and track kernel events.

    This pipeline function is called during the collection phase for each event.
    It delegates to the context's collect_parent_and_kernels method to store
    parent timing information and count kernel events by correlation ID.

    All events are passed through unchanged.

    Args:
        event (TraceEvent): The trace event to process.
        ctx (AbstractContext): Must be a KernelParentVerificationContext instance.

    Returns:
        list[TraceEvent]: Single-element list containing the input event (pass-through).

    Raises:
        AssertionError: If ctx is not a KernelParentVerificationContext instance.
    """
    assert isinstance(ctx, KernelParentVerificationContext), \
        "Context must be KernelParentVerificationContext"

    if event["ph"] != "X":
        return [event]
    ctx.collect_parent_and_kernels(event)
    return [event]


def kernel_parent_verify(event: TraceEvent, ctx: AbstractContext) -> list[TraceEvent]:
    """
    Verification phase: check kernel containment within parent bounds.

    This pipeline function is called during the verification phase (after the
    pipeline barrier) for each event. For kernel events, it verifies that the
    kernel's timestamps fall within its parent ScheduleCompute event's timeframe.

    All events are passed through unchanged.

    Args:
        event (TraceEvent): The trace event to verify.
        ctx (AbstractContext): Must be a KernelParentVerificationContext instance.

    Returns:
        list[TraceEvent]: Single-element list containing the input event (pass-through).

    Raises:
        AssertionError: If ctx is not a KernelParentVerificationContext instance.
    """
    assert isinstance(ctx, KernelParentVerificationContext), \
        "Context must be KernelParentVerificationContext"

    if event["ph"] != "X":
        return [event]
    ctx.verify_kernel_containment(event)
    return [event]
