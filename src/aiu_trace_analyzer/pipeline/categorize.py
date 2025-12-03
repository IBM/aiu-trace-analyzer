# Copyright 2024-2025 IBM Corporation

from enum import Enum, auto

import aiu_trace_analyzer.logger as aiulog
from aiu_trace_analyzer.types import TraceEvent
from aiu_trace_analyzer.pipeline.context import AbstractContext
from aiu_trace_analyzer.pipeline.tools import PipelineContextTool
from aiu_trace_analyzer.pipeline.barrier import TwoPhaseWithBarrierContext
from aiu_trace_analyzer.types import GlobalIngestData, InputDialect


#################################################
# source from Josh
class EventClass(Enum):
    OTHER = auto()
    COMPUTE_PREP = auto()
    COMPUTE_EXEC = auto()
    DATA_IN = auto()
    DATA_OUT = auto()
    SEN_DATA_CONVERT = auto()
    MAIU_BARRIER = auto()
    MAIU_WIREUP = auto()
    ROUNDTRIP_FLEX = auto()
    ROUNDTRIP_AIU = auto()
    # Local serial setup (e.g., data structure updates)
    MAIU_PROTOCOL_SERIAL = auto()
    # Host DMA: Wait for 'DATA' signal
    MAIU_HDMA_PROTOCOL_WAIT_DATA = auto()
    # Host DMA: Wait for 'ACK' signal
    MAIU_HDMA_PROTOCOL_WAIT_ACK = auto()
    # Host DMA: Send 'Data' signal
    MAIU_HDMA_PROTOCOL_SIGNAL_DATA = auto()
    # Host DMA: Send 'ACK' signal
    MAIU_HDMA_PROTOCOL_SIGNAL_ACK = auto()
    # Host DMA: Waiting for the monitor to acknowledge they issued the delivery of the notice
    MAIU_HDMA_PROTOCOL_MONITOR_NOTICE = auto()
    # Host DMA: Data Send
    MAIU_HDMA_PROTOCOL_SEND_DATA = auto()
    # Host DMA: Data RECV
    MAIU_HDMA_PROTOCOL_RECV_DATA = auto()
    # P2P (R)DMA: Data Send
    MAIU_P2PRDMA_PROTOCOL_SEND_DATA = auto()
    # P2P (R)DMA: Data RECV
    MAIU_P2PRDMA_PROTOCOL_RECV_DATA = auto()

    # def __format__(self, spec):
    #     return f'{self.name:spec}'
    def __str__(self,):
        return self.name

    def toJson(self):
        return self.name
# end source from Josh
########################################


class EventCategorizerContext(TwoPhaseWithBarrierContext, PipelineContextTool):
    def __init__(self, with_zero_align: bool = False):
        super().__init__()
        self.do_zero_align = 1.0 if with_zero_align else 0.0
        self.first_ts = 1e99

    def is_collective_event(self, event: TraceEvent) -> bool:
        return "CollGroup" in event["args"]

    def classify_flex(self, event: TraceEvent) -> EventClass:
        event_class = EventClass.OTHER
        if "Cmpt Prep" in event["name"]:
            # TS1 TS2 --- --- ---
            event_class = EventClass.COMPUTE_PREP
        elif "Cmpt Exec" in event["name"]:
            # --- --- TS3 TS4 TS5
            event_class = EventClass.COMPUTE_EXEC
        elif "DmaI" in event["name"]:
            if "Cleanup Host DMA Wait for ACK" in event["name"]:
                event_class = EventClass.MAIU_HDMA_PROTOCOL_WAIT_ACK
            else:
                event_class = EventClass.DATA_IN
        elif "DmaO" in event["name"]:
            event_class = EventClass.DATA_OUT

        if "Compute of" in event["name"] and "SenFusedDeviceNode" not in event["name"]:
            event_class = EventClass.SEN_DATA_CONVERT
        if "PrepareAndSyncRdma" in event["name"]:
            event_class = EventClass.MAIU_WIREUP
        if "Barrier:" in event["name"]:
            event_class = EventClass.MAIU_BARRIER
        if "Flex RoundTrip" in event["name"]:
            event_class = EventClass.ROUNDTRIP_FLEX
        if "AIU Roundtrip" in event["name"]:
            event_class = EventClass.ROUNDTRIP_AIU

        if self.is_collective_event(event):
            if "Host DMA" in event["name"] or "HCOLL" in event["name"]:
                # PF mode
                if "Wdone DmaI" in event["name"] or "Wait for Data Avail Notice" in event["name"] or "Wait for Notice (gather notifications)" in event["name"] or "R5 Wait DATA" in event["name"]:
                    event_class = EventClass.MAIU_HDMA_PROTOCOL_WAIT_DATA
                elif "Wait for ACK" in event["name"] or "R5 Wait ACK" in event["name"]:
                    event_class = EventClass.MAIU_HDMA_PROTOCOL_WAIT_ACK
                elif "Send ACK Instruction" in event["name"] or "R5 Send ACK" in event["name"]:
                    event_class = EventClass.MAIU_HDMA_PROTOCOL_SIGNAL_ACK
                elif "Send Instruction" in event["name"] or "HCOLL Signal" in event["name"] or "R5 Send DATA" in event["name"]:
                    event_class = EventClass.MAIU_HDMA_PROTOCOL_SIGNAL_DATA
                elif "Wait for Notice" in event["name"] or "Wait for Delivery Notice" in event["name"]:
                    event_class = EventClass.MAIU_HDMA_PROTOCOL_MONITOR_NOTICE
                elif EventClass.DATA_OUT == event_class:
                    event_class = EventClass.MAIU_HDMA_PROTOCOL_SEND_DATA
                elif EventClass.DATA_IN == event_class:
                    event_class = EventClass.MAIU_HDMA_PROTOCOL_RECV_DATA
            # DLM Wait might not have the 'Host DMA' prefix
            # Assume it is waiting on data
            elif "DLM Wait" in event["name"]:
                self.event_class = EventClass.MAIU_HDMA_PROTOCOL_WAIT_DATA
            else:
                if "Set BcList" in event["name"] or "Xseg to rank" in event["name"]:
                    event_class = EventClass.MAIU_PROTOCOL_SERIAL
                elif EventClass.DATA_OUT == event_class:
                    event_class = EventClass.MAIU_P2PRDMA_PROTOCOL_SEND_DATA
                elif EventClass.DATA_IN == event_class:
                    event_class = EventClass.MAIU_P2PRDMA_PROTOCOL_RECV_DATA
        return event_class

    def classify_event(self, event: TraceEvent) -> EventClass:
        event_class = EventClass.OTHER
        if PipelineContextTool.is_category(event, "acc_compute_prep"):
            event_class = EventClass.COMPUTE_PREP
        elif PipelineContextTool.is_category(event, "acc_kernel"):
            event_class = EventClass.COMPUTE_EXEC
        elif PipelineContextTool.is_category(event, "acc_datatransfer_HtoD"):
            # todo: further sub-cat for DMA WAIT FOR ACK
            event_class = EventClass.DATA_IN
        elif PipelineContextTool.is_category(event, "acc_datatransfer_DtoH"):
            event_class = EventClass.DATA_OUT

        if "Compute of" in event["name"] and "SenFusedDeviceNode" not in event["name"]:
            event_class = EventClass.SEN_DATA_CONVERT
        if PipelineContextTool.is_category(event, "acc_rdma_prep_sync"):
            event_class = EventClass.MAIU_WIREUP
        if PipelineContextTool.is_category(event, "acc_barrier"):
            event_class = EventClass.MAIU_BARRIER
        if PipelineContextTool.is_category(event, "acc_supernode_exec") or \
           PipelineContextTool.is_category(event, "acc_supernode_launch"):
            event_class = EventClass.ROUNDTRIP_FLEX
        # this has only a meaning in FLEX traces
        if "AIU Roundtrip" in event["name"]:
            event_class = EventClass.ROUNDTRIP_AIU

        if not PipelineContextTool.is_category(event, "acc_collective"):
            return event_class

        if "Host DMA" in event["name"] or "HCOLL" in event["name"]:
            # PF mode
            if "Wdone DmaI" in event["name"] or "Wait for Data Avail Notice" in event["name"] or "Wait for Notice (gather notifications)" in event["name"] or "R5 Wait DATA" in event["name"]:
                event_class = EventClass.MAIU_HDMA_PROTOCOL_WAIT_DATA
            elif "Wait for ACK" in event["name"] or "R5 Wait ACK" in event["name"]:
                event_class = EventClass.MAIU_HDMA_PROTOCOL_WAIT_ACK
            elif "Send ACK Instruction" in event["name"] or "R5 Send ACK" in event["name"]:
                event_class = EventClass.MAIU_HDMA_PROTOCOL_SIGNAL_ACK
            elif "Send Instruction" in event["name"] or "HCOLL Signal" in event["name"] or "R5 Send DATA" in event["name"]:
                event_class = EventClass.MAIU_HDMA_PROTOCOL_SIGNAL_DATA
            elif "Wait for Notice" in event["name"] or "Wait for Delivery Notice" in event["name"]:
                event_class = EventClass.MAIU_HDMA_PROTOCOL_MONITOR_NOTICE
            elif EventClass.DATA_OUT == event_class:
                event_class = EventClass.MAIU_HDMA_PROTOCOL_SEND_DATA
            elif EventClass.DATA_IN == event_class:
                event_class = EventClass.MAIU_HDMA_PROTOCOL_RECV_DATA
        # DLM Wait might not have the 'Host DMA' prefix
        # Assume it is waiting on data
        elif "DLM Wait" in event["name"]:
            self.event_class = EventClass.MAIU_HDMA_PROTOCOL_WAIT_DATA
        else:
            if "Set BcList" in event["name"] or "Xseg to rank" in event["name"]:
                event_class = EventClass.MAIU_PROTOCOL_SERIAL
            elif EventClass.DATA_OUT == event_class:
                event_class = EventClass.MAIU_P2PRDMA_PROTOCOL_SEND_DATA
            elif EventClass.DATA_IN == event_class:
                event_class = EventClass.MAIU_P2PRDMA_PROTOCOL_RECV_DATA

        return event_class

    def get_event_class(self, event: TraceEvent) -> EventClass:
        if not event["name"]:
            return EventClass.OTHER

        fevent_class = self.classify_flex(event)
        event_class = self.classify_event(event)
        assert fevent_class == event_class, f"Flex and generic classificaton diff: {fevent_class.name} != {event_class.name} in {event}"

        return event_class


    def collect_stats(self, event: TraceEvent) -> None:
        self.first_ts = min(self.first_ts, event["ts"])

    def apply_stats(self, event: TraceEvent) -> TraceEvent:
        if self.first_ts <= event["ts"]:
            event["ts"] -= self.first_ts * self.do_zero_align
        else:
            aiulog.log(aiulog.ERROR, "CAT: ACELYZER-BUG: Event ts smaller than min of collected event ts.")

        if "class" in event["args"]:
            event["args"]["class"] = str(event["args"]["class"])
        return event

    def drain(self) -> list[TraceEvent]:
        return super().drain()


def event_categorizer(event: TraceEvent, context: AbstractContext) -> list[TraceEvent]:
    if event["ph"] != "X":
        return [event]
    assert isinstance(context, EventCategorizerContext)

    context.collect_stats(event)
    event["args"]["class"] = context.get_event_class(event)

    return [event]


def event_categorizer_update(event: TraceEvent, context: AbstractContext) -> list[TraceEvent]:
    if event["ph"] != "X":
        return [event]
    assert isinstance(context, EventCategorizerContext)

    event = context.apply_stats(event)

    return [event]
