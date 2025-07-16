# Copyright 2024-2025 IBM Corporation


import aiu_trace_analyzer.logger as aiulog
import aiu_trace_analyzer.trace_view as aiuev
import aiu_trace_analyzer.pipeline.context as procCTX

from aiu_trace_analyzer.types import TraceEvent
from aiu_trace_analyzer.core.duplicate_hold import IntermediateDuplicateAndHoldContext, duplicate_and_hold
from aiu_trace_analyzer.export.exporter import JsonFileTraceExporter
from aiu_trace_analyzer.core.stage_profile import StageProfile, StageProfileChecker

_MINREQKEYS = ["ph", "ts", "pid", "name"]


class EventProcessor:

    '''
    Main event processor:

    High-level 3 stage:
      1. pass event(s) through registered pre-processing functions
      2. convert from python dict no AbstractEventType object
    '''
    def __init__(self, profile: StageProfile = None, intermediate: str = None) -> None:
        self.stages = []
        self.stages.append((EventProcessor.sanity_check, None, {}))
        self.profile = profile
        self.event_count = 0
        self.intermediate = intermediate
        self.stage_count = 0
        self.stage_check = StageProfileChecker(self.profile)

    def __del__(self) -> None:
        aiulog.log(aiulog.INFO, "Exported events: ", self.event_count)

    '''
    Pre-Process functions required to take
       * a single event (as a python dictionary) to process and
       * a context object for any necessary global state (see context.py)
       * a dictionary for k/v config arguments
    '''
    def register_stage(self, callback, context: procCTX.AbstractContext = None, **kwargs):
        if not self.stage_check.fwd_find_stage(callback.__name__):
            aiulog.log(aiulog.DEBUG, "DAH: Skipping registration of", callback.__name__, ": disabled in profile.")
            return
        else:
            aiulog.log(aiulog.DEBUG, "DAH: registering: ", callback.__name__)

        self.stages.append((callback, context, kwargs))

        # if intermediate results are requested, register an additional special function+context
        if self.intermediate:
            next_intermediate = IntermediateDuplicateAndHoldContext(
                JsonFileTraceExporter(target_uri=f'{self.intermediate}_{callback.__name__}_{self.stage_count}')
            )
            aiulog.log(aiulog.TRACE, "DAH: registering preprocessing stage export:", next_intermediate.exporter.target_uri)
            self.stages.append((duplicate_and_hold, next_intermediate, None))
            self.stage_count += 1

    '''
    Basic sanity check of events to make sure they contain the minimum required keys to be valid
    Further checking to be added.
    Returns empty list if input is invalid (which drops the event and ends processing of this event)
    '''
    @staticmethod
    def sanity_check(event: TraceEvent, _: procCTX.AbstractContext) -> list[TraceEvent]:
        for check in _MINREQKEYS:
            if check not in event:
                aiulog.log(aiulog.ERROR, "Event failed sanityCheck: ", check, "is not in", event)
                return []    # TODO: should be exception
        return [event]

    def process(self, event: TraceEvent) -> list[aiuev.AbstractEventType]:
        # turn into a list, pre/post have do be able to expand single events into lists
        aiulog.log(aiulog.DEBUG, "Processing event:", event)

        event_list = self.pre_process(event)

        output_event_list = self.convert_events(event_list)

        # tracking number of exported events
        self.event_count += len(output_event_list)
        return output_event_list

    # walk through the registered pre-processing hooks for the event
    # split any returned list of events into single events for each next stage pre-processor
    def pre_process(self, event: TraceEvent) -> list[TraceEvent]:
        event_list = [event]
        for pre_process, context, keyword_dictionary in self.stages:
            next_event_list = []
            for event in event_list:
                aiulog.log(aiulog.TRACE, "Prehook: ", pre_process, "for: ", event)
                if keyword_dictionary:
                    next_event_list += pre_process(event, context, keyword_dictionary)
                else:  # NOTE: remove once all functions inclue keyword_dictionary attribute
                    next_event_list += pre_process(event, context)

            event_list = next_event_list
            aiulog.log(aiulog.TRACE, "PreProcessed Events", event_list)
            # also stop if no event left to process
            if event_list == []:
                break
        return event_list

    # walk the accumulated list of events and convert:
    def convert_events(self, event_list: list[TraceEvent]) -> list[aiuev.AbstractEventType]:
        aiulog.log(aiulog.DEBUG, "Converting events:", event_list)
        output_event_list = []
        for event in event_list:
            # make sure all events have an 'args' entry
            if "args" not in event:
                event["args"] = {}
            # any key that's not listed is to be moved into args to be preserved
            for key, val in event.items():
                if key not in ["ph", "ts", "pid", "tid", "name", "cat", "args", "id", "bp", "dur"]:
                    event["args"][key] = val

            new_event = aiuev.AbstractEventType.from_dict(event)

            output_event_list.append(new_event)
        return output_event_list

    def drain(self) -> list[aiuev.AbstractEventType]:
        # walk through the registered pre-processing hooks for the event
        # split any returned list of events into single events for each next stage pre-processor
        next_event_list = []

        while len(self.stages) > 0:
            # remove the first hook from the chain to drain and process any remaining/buffered events
            _, drain_context, _ = self.stages.pop(0)

            if drain_context:
                aiulog.log(aiulog.DEBUG, "Draining pre-processing context:", drain_context)
            pending = [] if not drain_context else drain_context.drain()

            # then process the events that came back using the remaining pre-processing hooks + pipeline
            for event in pending:
                next_event_list += self.process(event)
        return next_event_list
