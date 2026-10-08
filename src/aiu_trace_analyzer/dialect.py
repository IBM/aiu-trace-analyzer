# Copyright 2024-2026 IBM Corporation

'''
Input dialects (FLEX, TORCH) and dialect-based classification of trace events.

Each dialect maps a category to an entry. Entries used as classifiers have one of these forms:
  "<name>"              the event name equals <name>
  "is.<path>;<regex>"   the attribute at the '.'-separated <path> exists and matches <regex>
  "has.<path>"          the attribute at <path> exists
  "-"                   the category does not exist in this dialect (never matches)
Classifier entries are parsed once when the dialect registers them.
'''

import re
from typing import Optional

import aiu_trace_analyzer.logger as aiulog
from aiu_trace_analyzer.types import TraceEvent, GlobalIngestData


class EventClassifier:
    '''
    parsed form of a classifier entry of a dialect (see module doc for the entry format)
    '''
    def __init__(self, entry: str, label: str) -> None:
        self.label = label
        self.regex = None

        deconstruct = entry.split(';')
        classifier = deconstruct[0].split('.')

        if len(classifier) == 1:
            self.operator, self.operand = "name", classifier[0]
        elif classifier[0] == "is":
            compare_str = ';'.join(deconstruct[1:])  # recombined remaining parts of the string
            assert len(compare_str) > 0, f"Incorrect format of {label}."
            self.operator, self.operand, self.regex = "is", classifier[1:], re.compile(compare_str)
        elif classifier[0] == "has":
            self.operator, self.operand = "has", classifier[1:]
        else:
            self.operator, self.operand = "unknown", classifier[0]

    def matches(self, event: TraceEvent) -> bool:
        if self.operator == "name":
            return event["name"] == self.operand

        if self.operator in ("is", "has"):
            attribute = event
            for c in self.operand:
                if c in attribute:
                    attribute = attribute[c]
                else:
                    return False
            if self.operator == "has":
                return True
            assert isinstance(attribute, dict) is False, \
                f"Attribute '{attribute}' is not a leaf node in {self.label}."
            return (self.regex.search(str(attribute)) is not None)

        aiulog.log(aiulog.WARN, f"Dialect entry for {self.label} has unknown operator:", self.operand)
        return False


class InputDialect:
    categories = set()
    dialect_map = {}
    classifier_map = {}

    @classmethod
    def register(cls, category: str, entry: str) -> bool:
        if category not in cls.categories:
            raise KeyError(f"ERROR: Category {category} is not part of this dialect.")

        if cls.__name__ not in cls.dialect_map:
            cls.dialect_map[cls.__name__] = {}

        if entry == "-":
            entry = None
        cls.dialect_map[cls.__name__][category] = entry
        if cls.__name__ not in cls.classifier_map:
            cls.classifier_map[cls.__name__] = {}
        # parse once at registration; entries that are no classifiers (e.g. NAME) parse as name classifiers unused
        cls.classifier_map[cls.__name__][category] = \
            EventClassifier(entry, f"'{category}' classifier of {cls.__name__}") if entry is not None else None
        return True

    @classmethod
    def add_category(cls, category: str) -> bool:
        if category in cls.categories:
            return False
        cls.categories.add(category)
        return True

    @classmethod
    def get(cls, category: str) -> str:
        return cls.dialect_map[cls.__name__][category]

    @classmethod
    def classifier(cls, category: str) -> Optional["EventClassifier"]:
        '''
        returns the parsed classifier of the category, None if the category does not exist in the dialect ('-')
        '''
        return cls.classifier_map[cls.__name__][category]


class InputDialectFLEX(InputDialect):
    _FLEX_DIALECT = {
        "NAME": "FLEX",
        "acc_launch_cb": "-",
        "acc_graph_init": "-",
        "acc_graph_exec": "Execute Graph",
        "acc_malloc": "FixupAllocations",
        "acc_resize_tensor_alloc": "AllocateFrame of graph",
        "acc_supernode_launch": "Flex RoundTrip",
        "acc_supernode_exec": "Flex RoundTrip",
        "acc_node_compute": "Compute of $NodeName",
        "acc_data_convert": "is.name;Compute of (?!.*SenFusedDeviceNode).*$",
        "acc_scheduler_init": "SchedulerConstruct",
        "acc_virtaddr_create": "CreatePipoIovas",
        "acc_launch_schedule_compute": "ScheduleCompute",
        "acc_schedule_wait": "WaitForCompletionAndReturnStatus",
        "acc_dma_prep": "PrepareDmas",
        "acc_rdma_prep_sync": "PrepareAndSyncRdma",
        "acc_cache_clear": "LaunchClearScratchpad",
        "acc_cache_preload": "LaunchPreloadScratchpad",
        "acc_launch_compute_stream": "LaunchComputeStream",
        "acc_rdma_barrier1": "Barrier1",
        "acc_rdma_post_keys": "PostKeys",
        "acc_rdma_barrier2": "Barrier2",
        "acc_rdma_fetch_keys": "FetchKeys",
        "acc_rdma_update_cb": "Update CBs",
        "acc_rdma_barrier3": "Barrier3",
        "acc_rdma_check_deadlock": "Deadlock Check",
        "acc_barrier": "is.name;[Bb]arrier:",
        "acc_filetransfer_DtoF": "is.name; DtoF",
        "acc_filetransfer_MtoF": "-",
        "acc_filetransfer_FtoD": "-",
        "acc_filetransfer_FtoM": "-",
        "acc_datatransfer_DtoH": "is.name; DmaO",
        "acc_datatransfer_HtoD": "is.name; DmaI",
        "acc_clock_calibration": "-",
        "acc_compile_graph": "-",
        "acc_category_kernel": "kernel",
        "acc_category_runtime": "cuda_runtime",
        "acc_compute_prep": "is.name;Cmpt Prep$",
        "acc_kernel": "is.name;Cmpt Exec$",
        "acc_event_cat": "has.args.TS1",
        "acc_collective": "has.args.CollGroup",
    }

    def __new__(cls):
        if not hasattr(cls, '_flex_dialect_instance'):
            cls._flex_dialect_instance = super(InputDialectFLEX, cls).__new__(cls)
            for c, e in cls._FLEX_DIALECT.items():
                cls._flex_dialect_instance.add_category(c)
                cls._flex_dialect_instance.register(c, e)
        return cls._flex_dialect_instance


class InputDialectTORCH(InputDialect):
    _TORCH_DIALECT = {
        "NAME": "TORCH",
        "acc_launch_cb": "aiuLaunchControlBlocks",
        "acc_graph_init": "aiuInitGraph",
        "acc_graph_exec": "aiuGraphExecution",
        "acc_malloc": "aiuMalloc",
        "acc_resize_tensor_alloc": "aiuResizeTensorAllocation",
        "acc_supernode_launch": "aiuLaunchSuperNode",
        "acc_supernode_exec": "aiuSuperNodeExecution",
        "acc_node_compute": "aiuNodeCompute",
        "acc_data_convert": "aiuDataConvert",
        "acc_scheduler_init": "aiuInitScheduler",
        "acc_virtaddr_create": "aiuCreateVirtualAddresses",
        "acc_launch_schedule_compute": "aiuLaunchScheduleCompute",
        "acc_schedule_wait": "aiuScheduleWait",
        "acc_dma_prep": "aiuPrepareDMAs",
        "acc_rdma_prep_sync": "aiuPrepareAndSyncRdma",
        "acc_cache_clear": "aiuClearCache",
        "acc_cache_preload": "aiuPreloadCache",
        "acc_launch_compute_stream": "aiuLaunchComputeStream",
        "acc_rdma_barrier1": "aiuRdmaBarrier1",
        "acc_rdma_post_keys": "aiuPostRdmaKeys",
        "acc_rdma_barrier2": "aiuRdmaBarrier2",
        "acc_rdma_fetch_keys": "aiuFetchRdmaKeys",
        "acc_rdma_update_cb": "aiuUpdateRdmaCBs",
        "acc_rdma_barrier3": "aiuRdmaBarrier3",
        "acc_rdma_check_deadlock": "aiuCheckRdmaDeadlock",
        "acc_barrier": "is.name;[Bb]arrier:",
        "acc_filetransfer_DtoF": "aiuFileTransferDtoF",
        "acc_filetransfer_MtoF": "aiuFileTransferMtoF",
        "acc_filetransfer_FtoD": "aiuFileTransferFtoD",
        "acc_filetransfer_FtoM": "aiuFileTransferFtoM",
        "acc_datatransfer_DtoH": "is.name;[Mm]emcpy \\(DtoH\\)",
        "acc_datatransfer_HtoD": "is.name;[Mm]emcpy \\(HtoD\\)",
        "acc_clock_calibration": "aiuClockCalibration",
        "acc_compile_graph": "aiuCompileGraph",
        "acc_category_kernel": "kernel",
        "acc_category_runtime": "cuda_runtime",
        "acc_compute_prep": "-",
        "acc_kernel": "is.cat;kernel",
        "acc_event_cat": "is.cat;gpu|kernel",
        "acc_collective": "is.name;HCOLL",
    }

    def __new__(cls):
        if not hasattr(cls, '_torch_dialect_instance'):
            cls._torch_dialect_instance = super(InputDialectTORCH, cls).__new__(cls)
            for c, e in cls._TORCH_DIALECT.items():
                cls._torch_dialect_instance.add_category(c)
                cls._torch_dialect_instance.register(c, e)
        return cls._torch_dialect_instance


def get_dialect_of_event(event: TraceEvent) -> Optional[InputDialect]:
    if "args" not in event:
        return None
    if "jobhash" not in event["args"]:
        print("ERROR: no jobhash in event. You hit a bug in the code.")
        return None
    return GlobalIngestData.get_dialect(event["args"]["jobhash"])


def find_dialect_of_event(event: TraceEvent, args_key: str = "args") -> Optional[InputDialect]:
    '''
    like get_dialect_of_event(), but silently returns None if the event has no (registered) job;
    args_key allows lookups in events that still keep their data in 'attr'
    '''
    args = event.get(args_key)
    if not isinstance(args, dict) or "jobhash" not in args:
        return None
    return GlobalIngestData.find_dialect(args["jobhash"])


def get_context_id(event: TraceEvent) -> int:
    dialect = get_dialect_of_event(event)
    if dialect is None:
        return 0
    if dialect.get("NAME") == "FLEX":
        return event["args"]["jobhash"]
    elif dialect.get("NAME") == "TORCH":
        return event["args"]["correlation"]
    else:
        return 0


def is_flex_event(event: TraceEvent) -> bool:
    '''
    Returns True for events that do not contain the information that torch profiler would add
    '''
    is_torch = "args" in event and ("External id" in event["args"] or "Python id" in event["args"])
    return (is_torch is False)


def is_category(event: TraceEvent, category: str) -> bool:
    dialect = get_dialect_of_event(event)
    if not dialect:
        return False
    classifier = dialect.classifier(category)
    if classifier is None:   # registered as '-'
        return False
    return classifier.matches(event)


def is_acc_event(event: TraceEvent) -> bool:
    return is_category(event, "acc_event_cat")


def is_acc_kernel(event: TraceEvent) -> bool:
    return is_category(event, "acc_kernel")
