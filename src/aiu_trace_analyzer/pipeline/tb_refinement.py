# Copyright 2024-2025 IBM Corporation

import re
import copy

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
    name_converter = re.compile(r"([\D\[\]-]+)(\d+)")

    def __init__(self, exporter: output.AbstractTraceExporter, keep_names: bool = False) -> None:
        super().__init__()
        self.exporter = exporter
        self.meta_exported = False
        self.has_coll_bw = False
        self.keep_names = keep_names
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

    def _update_event_names(self, ev_name: str) -> tuple[str, bool]:
        if self.keep_names:
            return ev_name, False

        new_name = ""
        last_idx = 0
        for match in self.name_converter.finditer(ev_name):
            new_name += re.sub("_$", "", match.group(1))
            if match.group(1).endswith("V"):
                new_name += match.group(2)
            last_idx = match.end()

        new_name += ev_name[last_idx:]
        return new_name, last_idx > 0

    def _update_for_collective(self, event: TraceEvent) -> TraceEvent:
        if "coll" in str(event["tid"]):
            event["tid"] = 10000+int(str(event["tid"])[4])

        # ensure events with different PIDs have different TIDs
        event["tid"] = event["pid"]*100000 + event["tid"]
        return event

    def update_event_data_heavy(self, event: TraceEvent) -> TraceEvent:
        if not PipelineContextTool.is_acc_event(event):
            return event
        cur_name = event["name"]
        (new_name, changed) = self._update_event_names(cur_name)
        if changed:
            event["args"]["orig_name"] = cur_name
        assert new_name != "", f"Event Name treatment created an empty name from event:'{cur_name}'."
        event["name"] = new_name

        event = self._update_for_collective(event)
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
                event["args"]["orig_name"] = event["name"]
                event["name"] = "gloo:all_reduce"
                event["external id"] = re.search(r"_(\d+)", event["args"]["orig_name"]).group(1)
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

        self.dialect = GlobalIngestData.get_dialect(event["args"]["jobhash"])

        if self.dialect.get("NAME") == "TORCH":
            if "opid" in event["args"]:
                event["pid"] = event["args"]["opid"]
                event["args"].pop("opid")
            if "otid" in event["args"]:
                event["tid"] = event["args"]["otid"]
                event["args"].pop("otid")
            return event

        pid = _resolve_string_pids(event["pid"])

        if PipelineContextTool.is_acc_event(event):
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
