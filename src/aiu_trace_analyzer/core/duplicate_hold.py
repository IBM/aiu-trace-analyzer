# Copyright 2024-2025 IBM Corporation

'''
This file is intentionnally located in the core subdirectory because it implements
the special case of exporting intermediate state in the form of json events.
Until there's any need for common use of this functionality, it should stay here.
'''
import copy

import aiu_trace_analyzer.logger as aiulog
from aiu_trace_analyzer.types import TraceEvent
from aiu_trace_analyzer.pipeline import AbstractContext

from aiu_trace_analyzer.export.exporter import JsonFileTraceExporter


class IntermediateDuplicateAndHoldContext(AbstractContext):
    '''
    Special context for export of intermediate state in form of json.
    The central global state here is just a JsonFileTraceExporter for
    the callback function to access export_raw()
    The drain function is somewhat 'misused' to drive the exporters flush()
    '''
    def __init__(self, exporter: JsonFileTraceExporter) -> None:
        super().__init__()
        self.exporter = exporter

    # don't emit the hold queue, we'll need to keep the list for export use
    def drain(self) -> list[TraceEvent]:
        aiulog.log(aiulog.TRACE, "DAH: Exporting stage:", self.exporter.target_uri, self)
        self.exporter.flush()
        return []


def duplicate_and_hold(event: TraceEvent, context: AbstractContext) -> list[TraceEvent]:
    '''
    The function is dedicated to export the event as a raw json dictionary.
    For that, the JsonFileExporter is extended with an export_raw() function to skip
    the restricted conversion to proper TraceView events.
    '''
    assert isinstance(context, IntermediateDuplicateAndHoldContext)

    aiulog.log(aiulog.TRACE,
               "DAH: Holding:",
               context.exporter.target_uri, event, id(context.exporter.traceview.trace_events))
    context.exporter.export_raw(copy.deepcopy(event))

    return [event]
