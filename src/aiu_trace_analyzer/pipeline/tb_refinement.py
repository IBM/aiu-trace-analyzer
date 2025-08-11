# Copyright 2024-2025 IBM Corporation

import re

from aiu_trace_analyzer.types import InputDialectTORCH, GlobalIngestData
import aiu_trace_analyzer.logger as aiulog
from aiu_trace_analyzer.pipeline.context import AbstractContext
from aiu_trace_analyzer.pipeline.hashqueue import AbstractHashQueueContext
from aiu_trace_analyzer.pipeline.tools import PipelineContextTool
from aiu_trace_analyzer.types import TraceEvent
import aiu_trace_analyzer.export.exporter as output

DmaI = "DmaI"
DmaO = "DmaO"
RDMA = "Rdma"

Coll_data_size = "Coll_data_size"
AllReduce = "AllReduce_all_reduce"


class RefinementContext(AbstractHashQueueContext):
    name_converter = re.compile(r"[_-]\d+")

    def __init__(self, exporter: output.AbstractTraceExporter) -> None:
        super().__init__()
        self.exporter = exporter
        self.meta_exported = False
        self.has_coll_bw = False
        self.dialect = InputDialectTORCH()

    def drain(self) -> list[TraceEvent]:
        # make metadata event export idempotent
        if self.meta_exported:
            return []

        revents = []
        for p, (s, i, l, t) in self.queues.items():
            revents.append({
                "ph": "M", "name": "process_name",
                "pid": p,
                "ts": t,
                "args": {"name": s}
            })
            revents.append({
                "ph": "M", "name": "process_label",
                "pid": p,
                "ts": t,
                "args": {"name": l}
            })
            revents.append({
                "ph": "M", "name": "process_sort_index",
                "pid": p,
                "ts": t,
                "args": {"sort_index": i+10}
            })

        if self.has_coll_bw:
            revents.append({
                "ph": "M", "name": "process_name",
                "pid": -1,
                "ts": 0,
                "args": {"name": "CollectiveBW"}
            })
            revents.append({
                "ph": "M", "name": "process_sort_index",
                "pid": -1,
                "ts": 0,
                "args": {"sort_index": 0}
            })

        self.meta_exported = True
        return revents

    def update_event_data_heavy(self, event) -> TraceEvent:
        pid = event["pid"]

        # to group function calls by name without index...
        # ...extract a function index (if any) from the event name
        match = self.name_converter.search(event["name"])
        if match and "args" in event:
            event["args"]["fn_idx"] = event["name"][match.start()+1:match.end()]
        # ...and replace it with _[N]
            event["name"] = re.sub(self.name_converter, "_[N]", event["name"], count=1)

        if "coll" in str(event["tid"]):
            event["tid"] = 10000+int(str(event["tid"])[4])

        # ensure events with different PIDs have different TIDs
        event["tid"] = pid*100000 + event["tid"]
        return event

    def _queue_add_device(self, pid, ts, is_acc: bool = True):
        if pid not in self.queues:
            if is_acc:
                self.queues[pid] = ("AIU Device"+str(pid), pid*2+1, "AIU", ts)
                self.exporter.add_device(
                    pid,
                    {"type": "AIU",
                     "name": "AIU",
                     "core": "PT Array"})
            else:
                self.queues[pid] = ("Host"+str(pid), pid*2, "cpu", ts)

    def is_acc_event(self, event: TraceEvent) -> bool:
        dialect = GlobalIngestData.get_dialect(event["args"]["jobhash"])
        classifier = dialect.get("acc_event_cat").split('.')
        if classifier[0] == "is":
            assert len(classifier) > 2, "Not enough parameters in 'acc_event_cat' classifier. 'is' requires at least 2"
            attribute = event
            for c in classifier[1:-1]:
                if c in attribute:
                    attribute = attribute[c]
                else:
                    return False
            return attribute == classifier[-1]

        if classifier[0] == "has":
            assert len(classifier) > 1, "Not enough parameters in 'acc_event_cat' classifier. 'has' requires at least 1"
            attribute = event
            for c in classifier[1:]:
                if c in attribute:
                    attribute = attribute[c]
                else:
                    return False
            return True
        return False

    def update_event_data_light(self, event) -> TraceEvent:

        def _cat_for_regular_event(event: TraceEvent) -> str:
            # Use DmaI and DmaO with Sen to check memcpy event
            if DmaI in event["name"] or DmaO in event["name"]:

                # if no RDMA, a memory copy event
                # else RDMA send and recv event
                if RDMA not in event["name"]:
                    return "gpu_memcpy"
                else:
                    return "user_annotation"

            else:
                if AllReduce in event["name"] and 'Cmpt Exec' in event["name"]:
                    return "user_annotation"
                else:
                    return self.dialect.get("acc_category_kernel")

        def _update_collective_event(event: TraceEvent) -> TraceEvent:
            # if collective call block, change 'cat'
            # and 'name' for communication tb calculation
            if "args" in event and Coll_data_size in event["args"] and AllReduce in event["name"]:
                event["cat"] = event["cat"] if "cat" in event else "user_annotation"
                event["name"] = "gloo:all_reduce"
                event["external id"] = event["args"].pop("fn_idx")
            else:
                event["cat"] = event["cat"] if "cat" in event else "cpu_op"
            return event

        def _resolve_string_pids(pid) -> int:
            if isinstance(pid, str):
                aiulog.log(aiulog.WARN, f'TBR: input pid is string: {pid}')
                try:
                    return int(pid)
                except (ValueError, TypeError):
                    return hash(pid) % 10000 + 10000
            return pid

        if "args" in event and "jobhash" in event["args"]:
            self.dialect = GlobalIngestData.get_dialect(event["args"]["jobhash"])

        pid = _resolve_string_pids(event["pid"])

        if self.is_acc_event(event):
            event["cat"] = _cat_for_regular_event(event)

            event["args"]["device"] = pid
            self._queue_add_device(pid, event["ts"], is_acc=True)

        else:
            event = _update_collective_event(event)
            event["pid"] = pid + 1000

            self._queue_add_device(pid, event["ts"], is_acc=False)

            # events that are not from flex should appear at the top and thus we shrink the TID
            if not PipelineContextTool.is_FLEX_event(event):
                event["tid"] = int(event["tid"]/10) + (event["tid"] % 10)

        return event


# more significant changes to events that are needed for TB to work
# this function can be disabled with cmd-line flag
def tb_refinement_intrusive(event: TraceEvent, context: AbstractContext) -> list[TraceEvent]:
    assert (isinstance(context, RefinementContext))

    if event["ph"] in "X":
        event = context.update_event_data_heavy(event)

    return [event]


# more lightweight changes to event that are useful for not just TB (e.g. stats)
# this function CANNOT be disable with cmd-line flag
def tb_refinement_lightweight(event: TraceEvent, context: AbstractContext) -> list[TraceEvent]:
    assert (isinstance(context, RefinementContext))

    if event["ph"] in "X":
        event = context.update_event_data_light(event)

    # signal to the context that there's a collective bw counter to create meta events with name and sort index
    if event["ph"] == "C" and not context.has_coll_bw:
        context.has_coll_bw |= (event["name"] == "BW allreduce")

    return [event]
