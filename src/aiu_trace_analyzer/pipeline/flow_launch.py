# Copyright 2024-2025 IBM Corporation

import re

import aiu_trace_analyzer.logger as aiulog
from aiu_trace_analyzer.types import TraceEvent
from aiu_trace_analyzer.pipeline import AbstractContext, TwoPhaseWithBarrierContext


class LaunchFLowContext(TwoPhaseWithBarrierContext):
    launch_pattern = re.compile(r'Launch.*ControlBlock')

    def __init__(self, warnings=None):
        super().__init__(warnings)
        self.flow_id_seq = 0

    def collect_launchflow_ids(self, event: TraceEvent) -> None:
        if event["ph"] == "s":
            id = self.get_or_create(event["id"], {"last_ts": event["ts"], "last_pid_tid": (event["pid"], event["tid"])})
            self.queues[id]["src"] = event
            return

        if event["ph"] != "X" or "args" not in event or "correlation" not in event["args"]:
            return

        if self.launch_pattern.search(event["name"]):
            id = self.get_or_create(
                event["args"]["correlation"],
                {"last_ts": event["ts"], "last_pid_tid": (event["pid"], event["tid"])})
            if id == 0:
                return
            self.max_flow_id_detection(id)
            self.queues[id]["launch"] = event

        elif "ScheduleWait" in event["name"]:
            id = self.get_or_create(
                event["args"]["correlation"],
                {"last_ts": event["ts"], "last_pid_tid": (event["pid"], event["tid"])})
            if id == 0:
                return
            self.max_flow_id_detection(id)
            self.queues[id]["schedwait"] = event

        else:
            # avoid creating entries for id=0 or non-kernel events
            id = event["args"]["correlation"]
            if id == 0 or event["cat"] != "kernel":
                return

            id = self.get_or_create(
                id,
                {"last_ts": event["ts"], "last_pid_tid": (event["pid"], event["tid"])})
            self.max_flow_id_detection(id)

            self.update_last_ts(id, event)

    def update_last_ts(self, qid: int, event: TraceEvent) -> None:
        last_ts = event["ts"] + event["dur"] - 0.001
        sched_wait_end = self.queues[qid]["schedwait"]["ts"] + self.queues[qid]["schedwait"]["dur"] if "schedwait" in self.queues[qid] else last_ts
        if last_ts <= sched_wait_end and last_ts > self.queues[qid]["last_ts"]:
            self.queues[qid]["last_ts"] = last_ts
            self.queues[qid]["last_pid_tid"] = (event["pid"], event["tid"])
            self.queues[qid]["last_event"] = event
        else:
            aiulog.log(aiulog.WARN, "FLOWS: Ignoring event with ts after schedule wait", event)
        assert self.queues[qid]["last_ts"] <= sched_wait_end, f"{qid}: {event}, {self.queues[qid]}"

    def max_flow_id_detection(self, observed_id: int) -> None:
        self.flow_id_seq = max(self.flow_id_seq, observed_id)

    def get_new_flow_id(self) -> int:
        self.flow_id_seq += 1
        return self.flow_id_seq

    def has_required_data(self, event: TraceEvent) -> bool:
        return (event["ph"] == "X" and
                "cat" in event and
                event["cat"] == "kernel" and
                "args" in event and
                "correlation" in event["args"])

    def create_missing(self, event: TraceEvent) -> list[TraceEvent]:
        '''
        Creates missing flow events between launchCB and kernels.
        Duplicating the launch flow event for that purpose
        and creating 2 new flow events with new ids to prevent collision with existing flows

        :param event: any trace event, content is checked for relevance inside the function
        :type event: TraceEvent
        :return: list of 2 flow events connecting the launchCB and the kernel events
        :rtype: list[TraceEvent]
        '''
        if not self.has_required_data(event):
            return []

        id = event["args"]["correlation"]
        if id not in self.queues or "src" not in self.queues[id]:
            return []

        launcher = self.queues[id]["src"]
        new_flow_id = self.get_new_flow_id()
        flow_events = [
            {
                "ph": "s",
                "pid": launcher["pid"],
                "tid": launcher["tid"],
                "name": launcher["name"],
                "cat": launcher["cat"],
                "ts": launcher["ts"],
                "id": new_flow_id,
            },
            {
                "ph": "f",
                "pid": event["pid"],
                "tid": event["tid"],
                "name": self.queues[id]["src"]["name"],
                "cat": self.queues[id]["src"]["cat"],
                "ts": event["ts"],
                "id": new_flow_id,
                "bp": "e"
            }
        ]
        return flow_events

    def drain(self) -> list[TraceEvent]:
        if self.collection_phase():
            return super().drain()

        return_flows = super().drain()
        for id, qdata in self.queues.items():
            if "src" not in qdata or "schedwait" not in qdata:
                continue
            launcher = qdata["src"]
            waiter = qdata["schedwait"]
            new_flow_id = self.get_new_flow_id()
            return_flows.append(
                {
                    "ph": "s",
                    "pid": qdata["last_pid_tid"][0],
                    "tid": qdata["last_pid_tid"][1],
                    "name": launcher["name"],
                    "cat": launcher["cat"],
                    "ts": qdata["last_ts"],
                    "id": new_flow_id
                }
            )
            return_flows.append(
                {
                    "ph": "f",
                    "pid": waiter["pid"],
                    "tid": waiter["tid"],
                    "name": launcher["name"],
                    "cat": launcher["cat"],
                    "ts": waiter["ts"] + waiter["dur"],
                    "id": new_flow_id,
                    "bp": "e"
                }
            )
        return return_flows


def launch_flow_collect(event: TraceEvent, ctx: AbstractContext) -> list[TraceEvent]:
    assert isinstance(ctx, LaunchFLowContext)

    ctx.collect_launchflow_ids(event)
    return [event]


def launch_flow_create_missing(event: TraceEvent, ctx: AbstractContext) -> list[TraceEvent]:
    assert isinstance(ctx, LaunchFLowContext)

    added_flows = ctx.create_missing(event)

    return [event] + added_flows
