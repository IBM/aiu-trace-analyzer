# Copyright 2024-2025 IBM Corporation

import math

import aiu_trace_analyzer.logger as aiulog
from aiu_trace_analyzer.types import TraceEvent
from aiu_trace_analyzer.pipeline import AbstractContext, AbstractHashQueueContext


class PTorchToFlexTimeMapping(object):
    '''
    torch-to-flex mapping tracker:
     * collect a list of (ts,dur) pairs
     * sorted by ts, will be used to align events from ptorch and flex for offset computation
    '''
    def __init__(self, rank: int) -> None:
        self.rank = rank
        self.flex_offset = 0.0
        self.ptorch_tsdur = []
        self.flex_tsdur = []

    def compute_offset(self) -> None:
        # filter torch match data for the highest priority event name
        self.ptorch_tsdur = self.filter_priority(self.ptorch_tsdur)
        # offset computation can only sanely happen if the flex and ptorch event trackings have the same length
        if len(self.flex_tsdur) == 0 or len(self.ptorch_tsdur) == 0:
            aiulog.log(aiulog.DEBUG, "TALN",
                       'Flex Time Offset is 0.0 because either Flex or Torch data was not detected.')
            self.flex_offset = 0.0
            return

        if len(self.flex_tsdur) != len(self.ptorch_tsdur):
            aiulog.log(aiulog.WARN, "TALN",
                       'Matching Flex and Torch events will be based on different number of entries.'
                       ' This could result in mis-alignment. Make sure the correct traces are ingested.')

        map_len = min(len(self.flex_tsdur), len(self.ptorch_tsdur))
        assert map_len > 0

        self.ptorch_tsdur = self.ptorch_tsdur[:map_len]
        self.flex_tsdur = self.flex_tsdur[:map_len]

        self.ptorch_tsdur.sort(key=lambda x: x[0])
        self.flex_tsdur.sort(key=lambda x: x[0])

        # make sure the torch duration is at least as long as the flex duration,
        # otherwise, the 2 events don't fit together
        # for x,y in zip(self.ptorch_tsdur, self.flex_tsdur):
        #     assert x[1] >= y[1]

        self.flex_offset = min([(x[0] + x[1]) - (y[0]+y[1]) for x, y in zip(self.ptorch_tsdur, self.flex_tsdur)])
        aiulog.log(aiulog.INFO, "TALN", f'Flex Time Offset for {self.rank}: {self.flex_offset}')

    def filter_priority(self, data: list[tuple[float, float, int]]) -> list[tuple[float, float, int]]:
        min_p = 10000
        for _, _, p in data:
            min_p = min(min_p, p)

        filtered = list(filter(lambda x: x[2] == min_p, data))
        return filtered


class TimeAlignmentContext(AbstractHashQueueContext):
    _FLEX_REFERENCE_EVENT_NAME = "Flex RoundTrip"
    _TORCH_REFERENCE_EVENT_NAME_LIST = ["built-in method Predict of PyCapsule object", "Torch-Compiled Region"]

    _COLLECTION_PHASE = 0
    _ACTION_PHASE = 1

    def __init__(self) -> None:
        super().__init__()
        self.phase = self._COLLECTION_PHASE
        self.is_multi_rank = set()

    def collect_rank_pairs(self, event: TraceEvent) -> None:
        queue_id = hash(event["pid"])
        if queue_id not in self.queues:
            q_data = PTorchToFlexTimeMapping(event["pid"])
        else:
            q_data: PTorchToFlexTimeMapping = self.queues[queue_id]

        if self._FLEX_REFERENCE_EVENT_NAME in event["name"]:
            aiulog.log(aiulog.TRACE, "TALN",
                       f'flex reference event: {queue_id} {event["ts"]}, {event["dur"]}, {event["name"]}')
            q_data.flex_tsdur.append((event["ts"], event["dur"]))
            self.is_multi_rank.add(event["pid"])

        for prio, candidate in enumerate(self._TORCH_REFERENCE_EVENT_NAME_LIST):
            if candidate in event["name"]:
                aiulog.log(aiulog.TRACE, "TALN",
                           f'ptorch reference event: {queue_id} {event["ts"]}, {event["dur"]}, {event["name"]}')
                q_data.ptorch_tsdur.append((event["ts"], event["dur"], prio))
                self.is_multi_rank.add(event["pid"])
                break

        self.queues[queue_id] = q_data

    def is_torch_event(self, event: TraceEvent) -> bool:
        is_torch = "args" in event and ("External id" in event["args"] or "Python id" in event["args"])
        return is_torch

    def time_align_apply(self, event: TraceEvent) -> TraceEvent:
        flex_offset = self.queues[hash(event["pid"])].flex_offset
        if not math.isclose(flex_offset, 0.0, abs_tol=1e-9) and not self.is_torch_event(event):
            event["ts"] += flex_offset
            aiulog.log(aiulog.TRACE, "TALN", f'updated to ts={event["ts"]} for {event["name"]}')
        else:
            pass

        return event

    def check_and_merge_single_rank(self):
        # single rank either:
        #  * works just fine - if pid of flex and torch are the same
        #  * or causes 2 separate collections which need to be merged here:
        if len(self.is_multi_rank) == 2:
            key1 = list(self.queues.keys())[0]
            key2 = list(self.queues.keys())[1]
            data_one: PTorchToFlexTimeMapping = self.queues[key1]
            data_two: PTorchToFlexTimeMapping = self.queues[key2]
            if len(data_one.flex_tsdur) > 0 and len(data_two.flex_tsdur) > 0:
                return

            aiulog.log(aiulog.INFO, "TALN",
                       'found 2 pids that appear belong to a single rank.'
                       ' Assuming single-process run and spreading data to both pids.')
            if len(data_one.flex_tsdur) > 0:
                flex_data = data_one.flex_tsdur
                torch_data = data_two.ptorch_tsdur
            else:
                flex_data = data_two.flex_tsdur
                torch_data = data_one.ptorch_tsdur

            for _, pf_mapping in self.queues.items():
                pf_mapping.flex_tsdur = flex_data
                pf_mapping.ptorch_tsdur = torch_data

    def drain(self) -> list[TraceEvent]:
        if self.phase == self._COLLECTION_PHASE:
            self.check_and_merge_single_rank()
            for _, queue in self.queues.items():
                queue.compute_offset()
            self.phase = self._ACTION_PHASE
        else:
            pass
        return []


def time_align_collect(event: TraceEvent, context: AbstractContext) -> list[TraceEvent]:
    assert isinstance(context, TimeAlignmentContext)

    context.collect_rank_pairs(event)
    # returning event here, next stage will happen after barrier
    return [event]


def time_align_apply(event: TraceEvent, context: AbstractContext) -> list[TraceEvent]:
    assert isinstance(context, TimeAlignmentContext)

    return [context.time_align_apply(event)]
