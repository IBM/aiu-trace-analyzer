# Copyright 2024-2025 IBM Corporation

from typing import Optional

from aiu_trace_analyzer.types import GlobalIngestData, TraceEvent
import aiu_trace_analyzer.dialect as dialect
from aiu_trace_analyzer.dialect import InputDialect


def event_location(event: TraceEvent) -> dict:
    """Return common location details for a trace finding instance."""
    the_args = "attr" if "attr" in event else "args"
    args = event.get(the_args, {})

    location = {
        "rank": args.get("rank"),
        "pid": event.get("pid"),
        "tid": event.get("tid"),
        "stream": args.get("stream"),
        "name": event.get("name"),
        "ts": event.get("ts"),
        "dur": event.get("dur"),
    }

    if "jobhash" in args:
        location["source"] = GlobalIngestData.get_job(args["jobhash"])

    return location


class PipelineContextTool:
    def __init__(self) -> None:
        super().__init__()

    def generate_filename(self, fname, purpose="summary", extension="csv") -> str:
        # build a filename from the provided output file
        # remove ".pt.trace" if present as it is only needed for the output json for tensorboard use
        fname = fname.replace(".pt.trace", "")

        # insert _summary before the last '.' and replace the .nnn with .csv
        fcomponents = fname.split('.')
        assert len(fcomponents) >= 1, "Filename cannot be empty."
        if len(fcomponents) == 1:
            fcomponents.append(extension)

        fcomponents[-2] += "_" + purpose
        fcomponents[-1] = extension

        return '.'.join(fcomponents)

    # dialect-based classification lives in aiu_trace_analyzer.dialect; kept here for existing callers
    @staticmethod
    def get_dialect_of_event(event: TraceEvent) -> Optional[InputDialect]:
        return dialect.get_dialect_of_event(event)

    @staticmethod
    def get_context_id(event: TraceEvent) -> int:
        return dialect.get_context_id(event)

    @staticmethod
    def is_flex_event(event: TraceEvent) -> bool:
        return dialect.is_flex_event(event)

    @staticmethod
    def is_acc_event(event: TraceEvent) -> bool:
        return dialect.is_acc_event(event)

    @staticmethod
    def is_acc_kernel(event: TraceEvent) -> bool:
        return dialect.is_acc_kernel(event)

    @staticmethod
    def is_category(event: TraceEvent, category: str) -> bool:
        return dialect.is_category(event, category)
