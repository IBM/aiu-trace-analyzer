# Copyright 2024-2025 IBM Corporation

import aiu_trace_analyzer.logger as aiulog
from aiu_trace_analyzer.types import TraceEvent
from aiu_trace_analyzer.pipeline import AbstractContext
from aiu_trace_analyzer.types import GlobalIngestData

class TIDMappingContext(AbstractContext):
    '''
    keep track of existing TID mappings in original and remap lists
    to enable mapping TIDs to more eye-friendly numbers
    '''
    def __init__(self, remap_size, remap_start, remap_step) -> None:
        super().__init__()
        self.tid_original = []
        self.tid_remap = []

        # TODO this could be made more flexible without a pre-defined/limited list of available mappings
        # Let's initialize the tid_remap list
        for rid in range(remap_size) :
            # remapped tid = remap_start + rid*remap_step
            self.tid_remap.append(remap_start + (rid*remap_step))



# maps TIDs to a pre-configured range based on the TIDMappingContext
def map_tid_to_range(event: TraceEvent, context: AbstractContext) -> list[TraceEvent]:
    assert( isinstance( context, TIDMappingContext) )

    # ignore anything that has no tid
    if event["ph"] != "X" or "tid" not in event:
        return [event]

    try:
        if GlobalIngestData.get_dialect(event["args"]["jobhash"]).get("NAME") != "FLEX":
            return [event]
    except KeyError:
        return [event]

    tid = event['tid']
    aiulog.log(aiulog.TRACE, f"tid: {tid}")

    tid_new = 0

    # check if the tid is the tid_original list
    if tid not in context.tid_original:
        # append tid to the tid_orginal list
        context.tid_original.append(tid)

    aiulog.log(aiulog.TRACE, "tid_original[]", len(context.tid_original), ":", context.tid_original)

    # find out the index in the tid_original array
    for index, otid in enumerate(context.tid_original):
        # check if tid matches
        if tid == otid:
            # remaps the tid to new value
            tid_new = context.tid_remap[index]

            aiulog.log(aiulog.TRACE, "Remap tid to new value: ", tid_new)
            break

    # change the value of 'tid' to 'new-tid' in the dictionary
    event['tid'] = tid_new

    return [ event ]
