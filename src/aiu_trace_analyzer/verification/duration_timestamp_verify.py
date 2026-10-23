# Copyright 2026 IBM Corporation

import math

from aiu_trace_analyzer.hw_data import get_hw_ts, has_hw_ts
from aiu_trace_analyzer.pipeline.context import AbstractContext, AbstractVerificationContext
from aiu_trace_analyzer.types import TraceEvent, TraceWarning


class DurationTimestampVerificationContext(AbstractVerificationContext):
    test_name = "Duration and Timestamp Sanity Check"
    _MAX_FINDING_INSTANCES = 100

    def __init__(self) -> None:
        super().__init__(warnings=[
            TraceWarning(
                name="negative_duration",
                text="Duration verification: found {d[count]} event(s) with negative duration",
                data={"count": 0},
            ),
            TraceWarning(
                name="zero_duration",
                text="Duration verification: found {d[count]} complete event(s) with zero duration",
                data={"count": 0},
            ),
            TraceWarning(
                name="zero_with_hw_time",
                text=(
                    "Timestamp verification: found {d[count]} event(s) with zero wallclock "
                    "duration but nonzero HW time"
                ),
                data={"count": 0},
            ),
        ])

    def verify_event(self, event: TraceEvent) -> None:
        if event.get("ph") != "X" or "dur" not in event:
            return

        duration = event["dur"]

        if duration < 0.0:
            self.warnings["negative_duration"].update()
            if self.warnings["negative_duration"].args_list["count"] <= self._MAX_FINDING_INSTANCES:
                self.warnings["negative_duration"].add_instance({
                    "name": event.get("name"),
                    "pid": event.get("pid"),
                    "tid": event.get("tid"),
                    "ts": event.get("ts"),
                    "dur": duration,
                })
            return

        if not math.isclose(duration, 0.0, abs_tol=1e-9):
            return

        self.warnings["zero_duration"].update()
        if self.warnings["zero_duration"].args_list["count"] <= self._MAX_FINDING_INSTANCES:
            self.warnings["zero_duration"].add_instance({
                "name": event.get("name"),
                "pid": event.get("pid"),
                "tid": event.get("tid"),
                "ts": event.get("ts"),
                "dur": duration,
            })

        if has_hw_ts(event) and get_hw_ts(event, 1) != get_hw_ts(event, 5):
            self.warnings["zero_with_hw_time"].update()
            if self.warnings["zero_with_hw_time"].args_list["count"] <= self._MAX_FINDING_INSTANCES:
                self.warnings["zero_with_hw_time"].add_instance({
                    "name": event.get("name"),
                    "pid": event.get("pid"),
                    "tid": event.get("tid"),
                    "ts": event.get("ts"),
                    "dur": duration,
                })


def verify_duration_timestamp(
        event: TraceEvent,
        context: AbstractContext) -> list[TraceEvent]:
    assert isinstance(context, DurationTimestampVerificationContext)

    context.verify_event(event)

    if event.get("ph") == "X" and event.get("dur", 0.0) < 0.0:
        return []

    return [event]
