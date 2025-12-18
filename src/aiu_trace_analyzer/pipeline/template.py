# Copyright 2024-2025 IBM Corporation

# import logger for logging output (if needed)
import aiu_trace_analyzer.logger as aiulog

# we certainly need that TraceEvent
from aiu_trace_analyzer.types import TraceEvent

# .. and the AbstractContext (or any of it's derived classes)
from aiu_trace_analyzer.pipeline import AbstractContext

############
# !!!!!!!!!!!!!!!! DON'T FORGET !!!!!!!!!!!!!!!!!
#############
# 0) once you implemented your function (and the optional context)
# 1) add context(s) and function(s) to aiu_trace_analyzer/pipeline/__init__.py to enable easy importing
# 2) create context instance and register the function in acelyzer.py main
# (see also description at aiu_trace_analyzer/pipeline/__init__.py)


##########################################################################
# this is optional and only needed if your function needs any global state
class MyStructsAndFunctionsForCrossEventContext(AbstractContext):
    def __init__(self, __put_any_initializing_args_here__) -> None:
        super().__init__()
        self.event_counts = {}

    # drain function is called after the last event has been ingested
    # and is needed to make sure there are no events stuck in any potential queues that hold events back
    # if you don't have anything to drain, then the top-level class already has an implementation ...
    # that returns an empty list here, we just print the event counts since we haven't stored any events
    def drain(self) -> list[TraceEvent]:
        for evtype, counts in self.event_counts.items():
            aiulog.log(aiulog.INFO, "EVENTCOUNTS:", evtype, counts)
        return []

    # example optional function: here just counting different event types using a dictionary
    def count(self, eventtype: str):
        if eventtype not in self.event_counts:
            self.event_counts[eventtype] = 0

        self.event_counts[eventtype] += 1


############################################################################
# processing function (this is your entry point)

# only neede here because the example uses deepcopy
# any imports should be at the top, but we place it here to not confuse the template head with unnecessary things ;) )
import copy  # noqa: E402


# this is the event processing function, yours needs to match the input and output definition to work
def myprocessing(event: TraceEvent, context: AbstractContext = None, config: dict = None) -> list[TraceEvent]:

    # if your processing needs global state, use your context at will
    # e.g. counting the number of events based on their type
    assert isinstance(context, MyStructsAndFunctionsForCrossEventContext)
    context.count(event["ph"])

    # if your processing needs any static parameters/configuration/settings,
    # the settings can be retrieved from the config dictionary
    event_filter = config.get("filter", None)

    # we just do a deep copy of the incoming event if its type matches any of listed filter events
    if not event_filter or event["ph"] in event_filter:
        second_ev = copy.deepcopy(event)
        # and return those as a list (this shows why the output is forced to be a list of events)
        return [event, second_ev]

    # any other event needs to be passed through as a list of a single event, otherwise it gets lost/dropped
    return [event]
