# Copyright 2024-2025 IBM Corporation

'''
central imports for contexts and processing functions

Adding new functionality as follows:

  1)  create a file in src/aiu_trace_analyzer/pipeline
  2)  implement your processing function and corresponding context
      (derive from existing contexts or AbstractContext)
  3)  add import lines below for your new context and function(s)
  4)  integrate to acelyzer.py as needed (watch the order of registered functions,
      because they're almost never independent)
  5)  add the new callback function names to any personalities/profiles in matching order

  Simple example to get started could be TIDMappingContext and map_tid_to_range in tid_mapping.py
'''

from aiu_trace_analyzer.types import TraceWarning

# Make available the abstract top-level context class
from aiu_trace_analyzer.pipeline.context import AbstractContext
from aiu_trace_analyzer.pipeline.hashqueue import AbstractHashQueueContext
from aiu_trace_analyzer.pipeline.barrier import TwoPhaseWithBarrierContext

# import extracted subclass context
from aiu_trace_analyzer.pipeline.be_pair import EventPairDetectionContext

# import the separated contexts:
from aiu_trace_analyzer.pipeline.overlap import OverlapDetectionContext, TSSequenceContext
from aiu_trace_analyzer.pipeline.inverse_ts import InversedTSDetectionContext
from aiu_trace_analyzer.pipeline.normalize import NormalizationContext
from aiu_trace_analyzer.pipeline.sort import EventSortingContext
from aiu_trace_analyzer.pipeline.make_slice import SliceCreationContext
from aiu_trace_analyzer.pipeline.power import PowerExtractionContext
from aiu_trace_analyzer.pipeline.tid_mapping import TIDMappingContext
from aiu_trace_analyzer.pipeline.filter import FilterPatternEventContext
from aiu_trace_analyzer.pipeline.dma import DataTransferExtractionContext
from aiu_trace_analyzer.pipeline.stats import StatsExtractionContext
from aiu_trace_analyzer.pipeline.stats_v2 import EventStatsTrackerContext
from aiu_trace_analyzer.pipeline.mp_sync import MpTsCalibContext
from aiu_trace_analyzer.pipeline.mp_sync_v2 import MpTsCalibV2Context
from aiu_trace_analyzer.pipeline.coll_group import CollectiveGroupingContext, CommunicationGroupContext
from aiu_trace_analyzer.pipeline.mp_sync_tight import MpSyncTightContext
from aiu_trace_analyzer.pipeline.mp_calc_bw import MpCalcBwContext
from aiu_trace_analyzer.pipeline.mp_calc_bw_v2 import MpCalcBwV2Context
from aiu_trace_analyzer.pipeline.cmpt_collection import QueueingCounterContext
from aiu_trace_analyzer.pipeline.rcu_utilization import MultiRCUUtilizationContext
from aiu_trace_analyzer.pipeline.tb_refinement import RefinementContext
from aiu_trace_analyzer.pipeline.iteration_detect import IterationDectectContext
from aiu_trace_analyzer.pipeline.time_align import TimeAlignmentContext
from aiu_trace_analyzer.pipeline.flex_job_offset import FlexJobOffsetContext

# for reference of the template, you'd do here:
#       from aiu_trace_analyzer.pipeline.template import MyStructsAndFunctionsForCrossEventContext

# import the separated processing functions
from aiu_trace_analyzer.pipeline.mappings import map_complete_to_duration, remove_ids_from_name
from aiu_trace_analyzer.pipeline.normalize import normalize_phase1, normalize_phase2
from aiu_trace_analyzer.pipeline.correctness import event_sanity_checks
from aiu_trace_analyzer.pipeline.overlap import (
    detect_partial_overlap_events,
    assert_ts_sequence,
    assert_global_ts_sequence,
    recombine_cpu_events)
from aiu_trace_analyzer.pipeline.inverse_ts import drop_timestamp_reversed_events
from aiu_trace_analyzer.pipeline.sort import sort_events
from aiu_trace_analyzer.pipeline.make_slice import create_slice_from_BE
from aiu_trace_analyzer.pipeline.power import extract_power_event, check_power_ts_sequence, compute_power
from aiu_trace_analyzer.pipeline.filter import processing_filter
from aiu_trace_analyzer.pipeline.tid_mapping import map_tid_to_range
from aiu_trace_analyzer.pipeline.tripple_event import tripple_phased_events
from aiu_trace_analyzer.pipeline.timesync import (
    cycle_count_to_wallclock,
    cycle_count_conversion_cleanup,
    realign_dts_to_hts,
    tighten_hts_by_instr_type,
    get_opIds_from_event,
    cleanup_copy_of_device_ts)
from aiu_trace_analyzer.pipeline.dma import extract_data_transfer_event, compute_bandwidth
from aiu_trace_analyzer.pipeline.stats import calculate_stats
from aiu_trace_analyzer.pipeline.stats_v2 import calculate_stats_v2
from aiu_trace_analyzer.pipeline.mp_calc_bw import mp_calc_bw
from aiu_trace_analyzer.pipeline.mp_calc_bw_v2 import mp_calc_bw_v2

from aiu_trace_analyzer.pipeline.mp_sync import mp_ts_calibration
from aiu_trace_analyzer.pipeline.mp_sync_v2 import mp_ts_calibration_v2
from aiu_trace_analyzer.pipeline.mp_sync_tight import mp_sync_tight_v1
from aiu_trace_analyzer.pipeline.drop_global_event import drop_global_events
from aiu_trace_analyzer.pipeline.coll_group import (
    flow_prepare_event_data,
    flow_extraction,
    flow_data_cleanup,
    communication_event_collection,
    communication_event_apply)

from aiu_trace_analyzer.pipeline.cmpt_collection import queueing_counter
from aiu_trace_analyzer.pipeline.rcu_utilization import compute_utilization, compute_utilization_fingerprints
from aiu_trace_analyzer.pipeline.tb_refinement import tb_refinement_intrusive, tb_refinement_lightweight
from aiu_trace_analyzer.pipeline.iteration_detect import collect_iteration_stats
from aiu_trace_analyzer.pipeline.barrier import pipeline_barrier, _main_barrier_context
from aiu_trace_analyzer.pipeline.time_align import time_align_collect, time_align_apply
from aiu_trace_analyzer.pipeline.flex_job_offset import frequency_align_collect, frequency_align_apply

# for reference of the template, you'd do here:
#       from aiu_trace_analyzer.pipeline.template import myprocessing
