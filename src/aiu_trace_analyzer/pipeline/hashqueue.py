# Copyright 2024-2025 IBM Corporation

from typing import Optional

import aiu_trace_analyzer.logger as aiulog
from aiu_trace_analyzer.types import TraceEvent
from aiu_trace_analyzer.pipeline import AbstractContext, TraceWarning


class AbstractHashQueueContext(AbstractContext):
    '''
    Provides a dictionary for e.g. queues to group events for processing
    '''
    def __init__(self, warnings: Optional[list[TraceWarning]] = None) -> None:
        super().__init__(warnings=warnings)
        self.queues = {}

    def drain(self) -> list[TraceEvent]:
        '''
        Default drain function returns all events that are still queued in a non-guaranteed order
        by appending one queue after the other to the returned list of events.
        If this is undesired behavior, please override in your derived class
        '''
        revents = []
        while len(self.queues):
            item = self.queues.popitem()
            if isinstance(item, TraceEvent):
                revents += item
        return revents

    def insert(self, event: TraceEvent, queue_id=None) -> int:
        '''
        insertion of the event into a queue and returning the queueID.
        If the queueID is not provided, the function should generate a queueID before insertion.
        '''
        raise NotImplementedError("Abstract base class doesn't implement insert()")

    def event_data_hash(self, a: TraceEvent, include_list: list[str], ignore_missing: bool = False) -> int:
        '''
        create a hash of event data selecting only the entries from the include_list
        The entries of the input list support hierarchical access using '.' (e.g. args.CollGroup)
        '''
        aiulog.log(aiulog.TRACE, "QUEUE IN:", include_list, a)
        hash_tuple = ()
        for key_str in include_list:
            keys = key_str.split('.')  # split to cover hierarchical entries
            base_dict = a
            for idx, key in enumerate(keys):
                if key not in base_dict:
                    if ignore_missing:
                        continue
                    else:
                        raise KeyError(f'Requested key {key} is not in event {a}')
                else:
                    if idx < len(keys)-1:
                        base_dict = a[key]
                    else:
                        hash_tuple += (base_dict[key],)

        aiulog.log(aiulog.TRACE, "QUEUE IN:", hash_tuple)
        assert len(hash_tuple) > 0
        return hash(hash_tuple)
