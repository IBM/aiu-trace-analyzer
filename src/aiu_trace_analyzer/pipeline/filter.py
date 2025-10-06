# Copyright 2024-2025 IBM Corporation

# import logger for logging output (if needed)
import aiu_trace_analyzer.logger as aiulog

# we certainly need that TraceEvent
from aiu_trace_analyzer.types import TraceEvent

# .. and the AbstractContext (or any of it's derived classes)
from aiu_trace_analyzer.pipeline import AbstractContext


##########################################################################
# this is optional and only needed if your function needs any global state
class FilterPatternEventContext(AbstractContext):
    def __init__(self, filter_pattern) -> None:
        super().__init__()
        self.filter_pattern = filter_pattern

    # drain function is called after the last event has been ingested
    # and is needed to make sure there are no events stuck in any potential queues that hold events back
    # if you don't have anything to drain,
    #     then the top-level class already has an implementation that returns an empty list
    # here, we just print the event counts since we haven't stored any events
    def drain(self) -> list[TraceEvent]:
        return []


############################################################################
# processing function (this is your entry point)

# only neede here because the example uses deepcopy
# any imports should be at the top, but we place it here to not confuse the template head with unnecessary things ;) )


# this is the event processing function, yours needs to match the input and output definition to work
def processing_filter(event: TraceEvent, context: AbstractContext, dictionary: dict) -> list[TraceEvent]:

    # we want to keep the events that are in the list
    filter_pattern = dictionary.get("filter_pattern", None)

    if isinstance(filter_pattern, str):
        if event["ph"] in filter_pattern:
            # and return those as a list (required b/c there can be functions that create new events)
            aiulog.log(aiulog.TRACE, "FLT: Unfiltered event:", event['name'])
            return [event]
    return []
