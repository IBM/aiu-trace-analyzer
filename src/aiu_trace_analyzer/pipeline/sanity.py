# Copyright 2024-2026 IBM Corporation

import aiu_trace_analyzer.logger as aiulog

from aiu_trace_analyzer.pipeline.context import (
    AbstractContext,
    AbstractVerificationContext,
)
from aiu_trace_analyzer.types import TraceEvent, TraceWarning


_REQUIRED_KEYS = ["ph", "ts", "pid", "name"]


class SanityCheckBase:
    def __init__(self) -> None:
        super().__init__(warnings=[
            TraceWarning(
                name="missing_required_key",
                text="SAN: Detected {d[count]} event(s) missing a required key.",
                data={"count": 0},
                is_error=True,
            )
        ])


class PipelineSanityCheckContext(SanityCheckBase, AbstractContext):
    pass


class VerificationSanityCheckContext(
        SanityCheckBase,
        AbstractVerificationContext,
):
    test_name = "Event Sanity Check"


def sanity_check(event: TraceEvent, context: SanityCheckBase) -> list[TraceEvent]:
    for key in _REQUIRED_KEYS:
        if key not in event:
            aiulog.log(
                aiulog.ERROR,
                "Event failed sanityCheck: ",
                key,
                "is not in",
                event,
            )
            context.warnings["missing_required_key"].record({
                "missing_key": key,
            })
            return []

    return [event]
