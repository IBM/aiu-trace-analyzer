# Copyright 2024-2025 IBM Corporation

from pathlib import Path


# define TraceEvent to be a dictionary for consistency
class TraceEvent(dict):
    pass


class InputDialect:
    dialect_map = {}
    categories = set()

    @classmethod
    def register(cls, category: str, entry: str) -> bool:
        if category not in cls.categories:
            raise KeyError(f"ERROR: Category {category} is not part of this dialect.")

        if entry == "-":
            entry = None
        cls.dialect_map[category] = entry
        return True

    @classmethod
    def add_category(cls, category: str) -> bool:
        if category in cls.categories:
            return False
        cls.categories.add(category)
        return True


class InputDialectFLEX(InputDialect):
    _FLEX_DIALECT = {
        "NAME": "FLEX",
        "acc_launch_cb": "-",
        "acc_graph_init": "-",
        "acc_graph_exec": "Execute Graph",
        "acc_malloc": "FixupAllocations",
        "acc_resize_tensor_alloc": "AllocateFrame of graph",
        "acc_supernode_launch": "Flex Roundtrip",
        "acc_supernode_exec": "Flex Roundtrip",
        "acc_node_compute": "Compute of $NodeName",
        "acc_data_convert": "Compute of $NodeName-HostPrep",
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
        "acc_filetransfer_DtoF": "-",
        "acc_filetransfer_MtoF": "-",
        "acc_filetransfer_FtoD": "-",
        "acc_filetransfer_FtoM": "-",
        "acc_datatransfer_DtoH": "-",
        "acc_datatransfer_HtoD": "-",
        "acc_clock_calibration": "-",
        "acc_compile_graph": "-",
    }

    def __new__(cls):
        if not hasattr(cls, '_instance'):
            cls._jobmap = {}
            for c, e in cls._FLEX_DIALECT.items():
                cls.register(c, e)
        return super(InputDialectFLEX, cls).__new__(cls)


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
        "acc_rdma_prep_sync": "aiuPrepareAndSyncRDMA",
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
        "acc_filetransfer_DtoF": "aiuFileTransferDtoF",
        "acc_filetransfer_MtoF": "aiuFileTransferMtoF",
        "acc_filetransfer_FtoD": "aiuFileTransferFtoD",
        "acc_filetransfer_FtoM": "aiuFileTransferFtoM",
        "acc_datatransfer_DtoH": "aiuDataTransferDtoH",
        "acc_datatransfer_HtoD": "aiuDataTransferHtoD",
        "acc_clock_calibration": "aiuClockCalibration",
        "acc_compile_graph": "aiuCompileGraph",
    }

    def __new__(cls):
        if not hasattr(cls, '_instance'):
            cls._jobmap = {}
            for c, e in cls._TORCH_DIALECT.items():
                cls.add_category(c)
                cls.register(c, e)
        return super(InputDialectTORCH, cls).__new__(cls)


class GlobalIngestData(object):
    _jobmap = None

    def __new__(cls):
        if not hasattr(cls, '_instance'):
            cls._jobmap = {}
        return super(GlobalIngestData, cls).__new__(cls)

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
            print("jobmap empty")
            return "Not Available"
