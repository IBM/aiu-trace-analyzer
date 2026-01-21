# Copyright 2024-2025 IBM Corporation

from pathlib import Path
import re

import aiu_trace_analyzer.logger as aiulog


# define TraceEvent to be a dictionary for consistency
class TraceEvent(dict):
    pass


class InputDialect:
    categories = set()
    dialect_map = {}

    @classmethod
    def register(cls, category: str, entry: str) -> bool:
        if category not in cls.categories:
            raise KeyError(f"ERROR: Category {category} is not part of this dialect.")

        if cls.__name__ not in cls.dialect_map:
            cls.dialect_map[cls.__name__] = {}

        if entry == "-":
            entry = None
        cls.dialect_map[cls.__name__][category] = entry
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
        "acc_compute_prep": "Cmpt Prep$",
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
        "acc_rdma_barrier1": "aiuRDMABarrier1",
        "acc_rdma_post_keys": "aiuPostRDMAKeys",
        "acc_rdma_barrier2": "aiuRDMABarrier2",
        "acc_rdma_fetch_keys": "aiuFetchRDMAKeys",
        "acc_rdma_update_cb": "aiuUpdateRDMACBs",
        "acc_rdma_barrier3": "aiuRDMABarrier3",
        "acc_rdma_check_deadlock": "aiuCheckRDMADeadlock",
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
        "acc_compute_prep": "Cmpt Prep$",
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


class GlobalIngestData(object):
    _jobmap = None

    def __new__(cls):
        if not hasattr(cls, '_instance'):
            cls._instance = super(GlobalIngestData, cls).__new__(cls)
            cls._jobmap = {}
        return cls._instance

    @classmethod
    def add_job_info(cls, source_uri: str, data_dialect: InputDialect = None) -> int:
        jobhash = hash(source_uri) % 10000
        if jobhash not in cls._jobmap:
            cls._jobmap[jobhash] = (Path(source_uri).name, data_dialect)
        return jobhash

    @classmethod
    def get_job(cls, jobhash: int) -> str:
        try:
            return cls._jobmap[jobhash][0]
        except KeyError:
            print(f"no jobmap entry for {jobhash}.")
            return "Not Available"

    @classmethod
    def get_dialect(cls, jobhash: int) -> InputDialect:
        try:
            return cls._jobmap[jobhash][1]
        except KeyError:
            print(f"no jobmap entry for {jobhash}.")
            raise


class TraceWarning:
    """
    Keep track of warnings to allow accumulated warning at the end of a run
    Example:

        w = TraceWarning(
            name="MyWarning",
            text="This stage has detected {d[count]} issues with max {d[max]}.",
            data={"count": 0, "max": 0.0},
            update_fn={"count": int.__add__, "max": max},
            autolog=True,
            is_error=True,
        )

        name:      a key that can be used to manage multiple warnings in e.g. a dictionary
        text:      the warning text with variables (always us 'd' as the dictionary name)
        data:      dictionary with entries that match the text variables
        update_fn: functions to run when the update function is called with data
        autolog:   automatically print the warning at destruction time (default)
        is_error:  print the summary warning as ERROR level and not as WARN (default)

        Whenever a warning should be added:

            w.update({"count": 1, "max": 100.0})

        this calls the preset update_fn for each item to update the values
        if update_fn is e.g. int.__add__, then the new_val entry for count will be increased by 1

        If the warning summary should be issued:
        print(w)
        -> this uses the __str__() method to assemble the text and should print
        "This stage has detected 1 issues with max 100.0"
    """

    def __init__(
            self,
            name: str,
            text: str,
            data: dict[str, any],
            update_fn: dict[str, callable] = {},
            auto_log: bool = True,
            is_error: bool = False):
        self.occurred = False
        self.name = name
        # format-string with {d[key]} placeholders
        self.text: str = text
        self.args_list: dict[str, any] = {k: v for k, v in data.items()}
        self.update_fn: dict[str, callable] = {k: v for k, v in update_fn.items()}
        self.auto_log = auto_log
        self.warn_level = aiulog.WARN if not is_error else aiulog.ERROR

        text_keys = re.findall(r"{d\[([.\w]+)\]}", self.text)
        if len(text_keys) != len(self.args_list):
            raise ValueError(
                "Number of args needs to match placeholders in format string."
                " Make sure to use format {d[<key>]}")

        # check keys of text, args, and update_fn overlap
        self._check_update_fn_keys(text_keys)
        self._check_text_keys(text_keys)
        self._check_data_keys(text_keys)

    def __del__(self) -> None:
        if self.auto_log is True and self.has_warning():
            aiulog.log(self.warn_level, self)

    def _check_update_fn_keys(self, text_keys: list) -> None:
        # check if the keys for the update functions exist within the output text and the data items
        for k in self.update_fn.keys():
            if k not in text_keys:
                raise KeyError(f"Update_fn key {k} not found in text pattern {text_keys}")
            if k not in self.args_list:
                raise KeyError(f"Update_fn key {k} not found in args {self.args_list}")

    def _check_text_keys(self, text_keys: list) -> None:
        # check if the keys in the output text exist in both update functions and data items
        for k in text_keys:
            if k not in self.args_list:
                raise KeyError(f"Text key {k} not found in args {self.args_list}.")
            if k not in self.update_fn:
                aiulog.log(aiulog.DEBUG, f"Text key {k} not in update functions {self.update_fn}. Using default.")
                self.update_fn[k] = int.__add__

    def _check_data_keys(self, text_keys: list) -> None:
        # check if the data item keys exist in both output text and update functions
        for k in self.args_list.keys():
            if k not in text_keys:
                raise KeyError(f"Args key {k} not in text pattern {text_keys}.")
            if k not in self.update_fn:
                aiulog.log(aiulog.DEBUG, f"Args key {k} not in update functions {self.update_fn}. Using default.")
                self.update_fn[k] = int.__add__

    def get_name(self) -> str:
        return self.name

    def update(self,
               data: dict[str, any] = {"count": 1}) -> int:
        items_changed = 0
        for k, v in data.items():
            if k not in self.args_list:
                raise KeyError(f"Requested args key {k} does not exist in exsting args: {self.args_list.keys()}")
            self.args_list[k] = self.update_fn[k](self.args_list[k], v)
            items_changed += 1

        self.occurred |= (items_changed > 0)
        return items_changed

    def has_warning(self) -> bool:
        return self.occurred

    def __str__(self) -> str:
        return self.text.format(d=self.args_list)
