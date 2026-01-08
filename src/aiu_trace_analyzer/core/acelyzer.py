# Copyright 2024-2025 IBM Corporation


import sys
import argparse
import math
import os

from aiu_trace_analyzer.constants import TS_CYCLE_KEY
from aiu_trace_analyzer.core.stage_profile import StageProfile
import aiu_trace_analyzer.core.engine as engine
import aiu_trace_analyzer.core.processing as processor
import aiu_trace_analyzer.ingest.ingestion as ingest
import aiu_trace_analyzer.export.exporter as output
import aiu_trace_analyzer.logger as aiulog
import aiu_trace_analyzer.pipeline as event_pipe
from aiu_trace_analyzer import __version__


class AcelyzerArgsFormatter(argparse.RawTextHelpFormatter, argparse.ArgumentDefaultsHelpFormatter):
    """
    Combine argsparse formatting for preserved line breaks and including default values
    """
    pass


class Acelyzer:

    defaults = {
        # general defaults
        "output": {"tb_enabled": "processed_trace.pt.trace.json", "tb_disabled": "processed_trace.json"},
        "format": "json",
        "loglevel": aiulog.INFO,

        "filter": "",
        "intermediate": False,

        # tid-remapping defaults
        "remap_size": 30,
        "remap_start": 1000,
        "remap_step": 100,

        # overlap detection defaults (method and potential 'ts-shift' threshold)
        "overlap": "tid",
        "ts_shift_threshold": 0.005,

        # event manipulation/extraction
        "split_events": False,

        # to reference to the epoch of host-timer or device-timer
        "sync_to_dev": False,

        # skip events power calculation
        "skip_events": False,

        # clock sync for multiple device traces (TP)
        "mp_sync_v2": False,
        # Do not use mpsync (useful if torch profile data is available which then does auto-align)
        "skip_mpsync": False,

        # output file name of stats
        "stats": True,

        # to process different counter events
        "counter": "power_ts4 coll_bw prep_queue rcu_util",

        # whether to enable flow event extraction
        "flow": False,

        # drop global events (anything without TSx)
        "drop_globals": False,

        # default frequency ratio to scale ideal cycles
        "ideal_scale_factor": 0.0,

        # To use Chrome-trace to render timeline, disable TB refinement
        "tb_refinement": True,

        # whether to enable collective event extraction
        "build_coll_event": False,

        # stats_v2 (metrics and stat_class name)
        "stats_v2": {'comp_duration': 'DurationStat', 'comp_active': 'TimeStat', 'comm_active': 'TimeStat'},

        # TensorBoard directory
        "tb": False,

        # configuration file in case this runs from a non-interactive session
        "config_file": "ace.conf",

        # default ideal and SOC Frequency
        "freq": 1000.0,
        "ideal_freq": 1100.0,

        # experimental FLEX per-job timestamp correction
        "flex_ts_fix": False,

        # data file/url that contains kernel category measurements from autopilot=0 runs
        "autopilot_db": "ai_kernel.db",

        # summarize communication sequence events
        "comm_summ": False,

        # displayTimeUnit setting for exported json trace files
        "time_unit": "ns",

        # name of a processing profile
        "stage_profile": os.path.join(os.path.dirname(__file__), "../profiles/default.json")
    }

    # define default event sort key: timestamp + reverse_duration
    _default_sort_ts_and_rev_dur = "ts,dur:r"

    def __init__(self, in_args=None, in_data=None):
        self.args = self.parse_inputs(in_args)

        try:
            if self.args.version is True:
                print(__version__)
                sys.exit(0)

            if not self._args_sanity_check(self.args):
                sys.exit(1)
        except Exception as e:
            print(e)
            sys.exit(1)

        aiulog.loglevel = self.args.loglevel
        aiulog.log(aiulog.INFO, "Starting Test parser")

        self.freq_soc = self.args.freq[0]
        self.freq_core = self.args.freq[1]

        if in_data is not None and "api://" in self.args.input:
            self.direct_data = memoryview(in_data)
        else:
            self.direct_data = None

    def run(self) -> int:
        # setup/configure data ingestion
        try:
            importer = ingest.MultifileIngest(
                source_uri=self.args.input,
                show_warnings=(not self.args.disable_input_warnings),
                direct_data=self.direct_data)
        except FileNotFoundError:
            sys.exit(1)

        # create event processor
        profile = StageProfile.from_json(self.args.profile)
        intermediate_file = self.args.output if self.args.intermediate else None
        process = processor.EventProcessor(profile=profile,
                                           intermediate=intermediate_file)

        # set up output
        if self.args.tb and self.args.tb_refinement:
            # prepare traces for TB distributed view
            self.exporter = output.TensorBoardFileTraceExporter(target_uri=self.args.output,
                                                                timescale=self.args.time_unit,
                                                                settings=vars(self.args))
        else:
            if self.args.format == "json":
                self.exporter = output.JsonFileTraceExporter(target_uri=self.args.output,
                                                             timescale=self.args.time_unit,
                                                             settings=vars(self.args))
            elif self.args.format == "pddf":
                self.exporter = output.DataframeExporter(target_uri=self.args.output,
                                                         timescale=self.args.time_unit,
                                                         settings=vars(self.args))
            elif self.args.format == "proto":
                self.exporter = output.ProtobufTraceExporter(target_uri=self.args.output,
                                                             settings=vars(self.args))
            else:
                aiulog.log(
                    aiulog.ERROR,
                    "Unrecognized export format. Available options: json or pddf (and unsupported: proto)")
                return -1
        self.exporter.export_meta(importer.get_passthrough_meta())

        self.register_processing_functions(process, self.args, self.exporter)

        # create main engine and run
        dr = engine.Engine(importer, process, self.exporter)
        rc = dr.run()

        aiulog.log(aiulog.INFO, "Finishing Test parser. Return code=", rc)
        return rc

    def parse_inputs(self, args=None):
        # to include default value in --help output
        parser = argparse.ArgumentParser(prog="acelyzer", formatter_class=AcelyzerArgsFormatter)
        required_group = parser.add_mutually_exclusive_group(required=True)
        parser.add_argument("-C", "--counter", type=str, nargs='*', default=self.defaults["counter"],
                            choices=["power_ts4", "power_ts3", "coll_bw", "bandwidth", "prep_queue", "rcu_util"],
                            help="Space-separated list of counters to extract/display."
                            " Note: power_ts4 and power_ts3 are mutually exclusive.")

        parser.add_argument("-c", "--compiler_log", type=str, default=None,
                            help="(Comma-separated list of per-rank) Path/Filename of compiler log to ingest"
                            " compile-time data references. Required e.g. for rcu_util counters."
                            " Multi-AIU rank outputs need to be sorted by rank.")

        parser.add_argument("-D", "--loglevel", type=int, default=self.defaults["loglevel"],
                            choices=range(0, 5), help="Logging level 0(ERROR)..4(TRACE)")

        parser.add_argument("--event_filter", type=str, default="",
                            help="optional event filters based on attribute and regex. "
                            "Comma-separated list of <attribute>:<regex>. "
                            "Events matching any of the entries are dropped from the stream.\n"
                            "Example:\n   acelyzer ... --event_filter='name:XYZ$','args.Type:^XYZ$'...\n"
                            "drops events if event[name] ends in XYZ or event[args][Type]=='XYZ'",
                            )

        parser.add_argument("-F", "--filter", type=str, default=self.defaults["filter"],
                            help="List of event types to keep. E.g. 'C' to just keep counters.")

        parser.add_argument("-f", "--format", type=str, default=self.defaults["format"],
                            choices=["json", "pddf", "protobuf"],
                            help="Type of output format")

        parser.add_argument("--freq", type=str, default=':'.join([str(self.defaults["freq"]),
                                                                  str(self.defaults["ideal_freq"])]),
                            help="Frequency spec for <SoC_freq>[:<core_freq>] in MHz")

        parser.add_argument("--flow", dest="flow", action="store_true", default=self.defaults["flow"],
                            help="Enable flow detection/visualization")

        parser.add_argument("--freq_scaling", type=float, default=self.defaults["ideal_scale_factor"],
                            help="(oblolete by --freq)")

        required_group.add_argument("-i", "--input", type=str,
                                    help="Comma-separated list of input files. Or file pattern (requires quotes)")

        parser.add_argument("-I", "--intermediate", dest='intermediate', action='store_true',
                            default=self.defaults["intermediate"],
                            help="Enable export of intermediate results after each processing step.")

        parser.add_argument("-k", "--skip_events", dest="skip_events", action='store_true',
                            default=self.defaults["skip_events"],
                            help="skip certain events when calculating the power")

        parser.add_argument("--drop_globals", dest="drop_globals", action='store_true',
                            default=self.defaults["drop_globals"],
                            help="drop throw-away events like Prep, etc.")

        parser.add_argument("-M", "--no_mp_sync", dest="skip_mpsync", action='store_true',
                            default=self.defaults["skip_mpsync"],
                            help="Do not attempt to sync multi-AIU streams based on AIU timestamps,"
                            " e.g. if torch profile input aligns those already.")

        parser.add_argument("-O", "--overlap", type=str, default=self.defaults["overlap"],
                            choices=["drop", "tid", "async", "warn", "shift"],
                            help="How to resolve overlapping/non-displayable events )")

        parser.add_argument("-P", "--profile", type=str, default="not_set",
                            help="Name of a processing profile json that lists"
                            " the active processing stages to run")

        parser.add_argument("-o", "--output", type=str, default=None, help="Output file name.")
        parser.add_argument("-R", "--build_coll_event", dest="build_coll_event", action="store_true",
                            default=self.defaults["build_coll_event"],
                            help="Enable collective event detection/visualization."
                            " Note: The --flow option must be enabled first for this feature to work.")

        parser.add_argument("-S", "--use_mp_sync_v2", dest='mp_sync_v2', action='store_true',
                            default=self.defaults["mp_sync_v2"],
                            help="Use the newer version of multi-AIU time alignment (v2).")

        parser.add_argument("-s", "--split_events", dest='split_events', action='store_const',
                            const=True, default=self.defaults["split_events"],
                            help="(Obsolete) When set, split events into DmaI, Cmpt, DmaO based on TS1-5")
        parser.add_argument("-t", "--no_stats", dest="stats", action='store_false',
                            default=self.defaults["stats"],
                            help="When set, disable export of statistics to <output_file.csv>")

        parser.add_argument("-T", "--sync_to_dev", dest='sync_to_dev', action='store_const', const=True,
                            default=self.defaults["sync_to_dev"],
                            help="When set, use epoch from device timers")

        parser.add_argument("--autopilot_data", type=str, default=self.defaults["autopilot_db"],
                            help="(Not yet implemented) Where to look for kernel category data"
                            " from runs with 'autopilot=0'.")

        parser.add_argument("--keep_prep", dest="keep_prep", action="store_true", default=False,
                            help="Prep-events are counted and the dropped. Use this option to keep them.")
        parser.add_argument("--keep_names", dest="keep_names", action="store_true", default=False,
                            help="Keep original event names when using the --tb option."
                            " By default most numbers are removed from name for aggregation purposes.")

        parser.add_argument("--disable_tb", dest="tb_refinement", action="store_false",
                            default=self.defaults["tb_refinement"],
                            help="To use Chrome-trace to render timeline, disable TB refinement")

        parser.add_argument("--tb", dest="tb", action="store_true", default=self.defaults["tb"],
                            help="Enable output files for tensorboard. "
                            "IMPORTANT NOTE: Switches to 'torch_minimal' profile (use -P to override)!")

        parser.add_argument("--disable_file", dest="save_to_file", action="store_false", default=True,
                            help="Disable output to file (primarily for TensorBoard integration)."
                            " Prevents output file creation for integrated mode.")

        parser.add_argument("--flex_ts_fix", dest="flex_ts_fix", action="store_true",
                            default=self.defaults["flex_ts_fix"],
                            help="Enable an experimental per-job time-stamp adjustment.")

        parser.add_argument("--disable_input_warnings", action="store_true", default=True,
                            help="Disable warnings encountered while ingesting data.")

        parser.add_argument("--comm_summarize_seq", action="store_true",
                            default=self.defaults["comm_summ"],
                            help="Combine each sequence of communication events into a single send/recv.")

        parser.add_argument("--time_unit", default=self.defaults["time_unit"], choices=["ms", "ns"],
                            help="Display Time Unit of the resulting json.")

        parser.add_argument("--ignore_crit", action="store_true",
                            default=False,
                            help="Attempt to force through errors without breaking and just print error msgs instead.")

        required_group.add_argument("--version", action="store_true",
                                    default=False,
                                    help="Print version and exit.")

        parsed_args = parser.parse_args(args)
        if parsed_args.output is None:
            if parsed_args.tb_refinement:
                parsed_args.output = self.defaults["output"]["tb_enabled"]
            else:
                parsed_args.output = self.defaults["output"]["tb_disabled"]

        if ":" in parsed_args.freq:
            freq_split = parsed_args.freq.split(':')
            try:
                parsed_args.freq = [float(freq_split[0]), float(freq_split[1])]
            except ValueError:
                print('ERROR: Frequency setting requires float values.')
                sys.exit(1)
        else:
            print(f'HINT: --freq only specified SoC freqency {parsed_args.freq}.'
                  f' Using default core freq {self.defaults["ideal_freq"]}.'
                  ' You may specify both by using: --freq=<soc>:<core>')
            try:
                parsed_args.freq = [float(parsed_args.freq), self.defaults["ideal_freq"]]
            except ValueError:
                print('ERROR: Frequency setting requires float values.')
                sys.exit(1)

        if parsed_args.profile == "not_set":
            # update the default profile depending on --tb argument
            if parsed_args.tb is True:
                parsed_args.profile = os.path.join(os.path.dirname(__file__), "../profiles/torch_minimal.json")
            else:
                parsed_args.profile = self.defaults["stage_profile"]
        return parsed_args

    def get_output_data(self):
        return self.exporter.get_data()

    def _args_sanity_check(self, args) -> bool:
        assert math.isclose(args.freq_scaling, 0.0, abs_tol=1e-9), \
            "ERROR: Use of obsolete cmdline '--freq_scaling'." \
            " Use '--freq=<soc>:<core>' instead!"
        assert args.freq[0] > 0.0 and args.freq[1] > 0.0, \
            "Frequency settings are required to be a positive non-zero float value."
        if "power_ts3" in args.counter and "power_ts4" in args.counter:
            aiulog.log(aiulog.ERROR, "power_ts3 and power_ts4 are mutually exclusive")
            return False

        if "rcu_util" in args.counter and args.compiler_log is None:
            aiulog.log(aiulog.WARN, "rcu_util counter requested but no compiler log provided."
                       " No utilization will be plotted.")

        return True

    def _overlap_option_from_arg(self, inarg: str) -> int:
        if inarg == "drop":
            return event_pipe.OverlapDetectionContext.OVERLAP_RESOLVE_DROP
        elif inarg == "tid":
            return event_pipe.OverlapDetectionContext.OVERLAP_RESOLVE_TID
        elif inarg == "async":
            return event_pipe.OverlapDetectionContext.OVERLAP_RESOLVE_ASYNC
        elif inarg == "warn":
            return event_pipe.OverlapDetectionContext.OVERLAP_RESOLVE_WARN
        elif inarg == "shift":
            return event_pipe.OverlapDetectionContext.OVERLAP_RESOLVE_SHIFT
        else:
            raise ValueError("UNRECOGNIZED Overlap Option (drop, tid, async).")

    def register_processing_functions(self,
                                      process: processor.EventProcessor,
                                      args,
                                      exporter: output.AbstractTraceExporter):

        ##############################################################
        # Event preparation, cleanup, and sanitization
        # register pre-processing: filtern events with broken time stamps (E < B)
        process.register_stage(
            callback=event_pipe.drop_timestamp_reversed_events,
            context=event_pipe.InversedTSDetectionContext())
        # register pre-processing: turn B/E into X-slices
        process.register_stage(
            callback=event_pipe.create_slice_from_BE,
            context=event_pipe.SliceCreationContext(make_complete=True))

        ##############################################################
        # frequency detection and per-job offset correction
        # and event manipulation/normalization in 2 phases
        normalize_ctx = event_pipe.NormalizationContext(soc_frequency=args.freq[0],
                                                        ignore_crit=args.ignore_crit,
                                                        filterstr=args.event_filter)
        frequency_align_ctx = event_pipe.FlexJobOffsetContext(soc_frequency=args.freq[0])
        process.register_stage(callback=event_pipe.normalize_phase1, context=normalize_ctx)
        if args.flex_ts_fix:
            process.register_stage(callback=event_pipe.frequency_align_collect, context=frequency_align_ctx)
        process.register_stage(callback=event_pipe.pipeline_barrier, context=event_pipe._main_barrier_context)
        if args.flex_ts_fix:
            process.register_stage(callback=event_pipe.frequency_align_apply, context=frequency_align_ctx)
        process.register_stage(callback=event_pipe.normalize_phase2, context=normalize_ctx)

        # make sure the data in the events have no unexpected values
        process.register_stage(callback=event_pipe.event_sanity_checks)

        ##############################################################
        # Torch/Flex time alignment
        time_align_ctx = event_pipe.TimeAlignmentContext()
        process.register_stage(callback=event_pipe.time_align_collect, context=time_align_ctx)
        process.register_stage(callback=event_pipe.pipeline_barrier, context=event_pipe._main_barrier_context)
        process.register_stage(callback=event_pipe.time_align_apply, context=time_align_ctx)

        ##############################################################
        # Event manipulation: making changes to args or other event parameters beyond cleanup
        # move request IDs from the event name into args
        if self._overlap_option_from_arg(args.overlap) == event_pipe.OverlapDetectionContext.OVERLAP_RESOLVE_ASYNC:
            process.register_stage(callback=event_pipe.remove_ids_from_name)

        # register pre-processing: mapping TID to human eye-friendly
        cid_mapping_ctx = event_pipe.TIDMappingContext(
            self.defaults["remap_size"],
            self.defaults["remap_start"],
            self.defaults["remap_step"])
        process.register_stage(callback=event_pipe.map_tid_to_range, context=cid_mapping_ctx)

        # add a args.ts_all list for TS1-5 cycle timestamps if available
        process.register_stage(callback=event_pipe.cycle_count_to_wallclock, soc_frequency=args.freq[0])
        process.register_stage(callback=event_pipe.tighten_hts_by_instr_type, soc_frequency=args.freq[0])

        if args.split_events:
            # split each X-event into DmaI, Cmpt, DmaO based on TS1-5
            process.register_stage(callback=event_pipe.tripple_phased_events, soc_frequency=args.freq[0])

        if not args.skip_mpsync:
            if not args.mp_sync_v2:
                process.register_stage(callback=event_pipe.mp_sync_tight_v1,
                                       context=event_pipe.MpSyncTightContext())
            else:
                process.register_stage(callback=event_pipe.mp_ts_calibration_v2,
                                       context=event_pipe.MpTsCalibV2Context())

        # process.registerPreProcess(callback=event_pipe.cycle_count_conversion_cleanup)
        # process.registerPreProcess(callback=event_pipe.cycle_count_to_wallclock)

        # dealing with prep-queue needs to be done before dropping prep events might happen
        if any(args.counter) and "prep_queue" in args.counter:
            process.register_stage(
                callback=event_pipe.queueing_counter,
                context=event_pipe.QueueingCounterContext(),
                keep_prep=args.keep_prep)

        # optionally dropping events without TSx or are Prep events
        if args.drop_globals:
            process.register_stage(callback=event_pipe.drop_global_events, context=None)

        # Merge CPU events into a single TID-stream (except AIU Roundtrip)
        process.register_stage(callback=event_pipe.recombine_cpu_events, context=None, cpu_stream_tid=1000)

        ##############################################################
        # modifying/detecting things across groups of events (e.g. overlapping, sorting)
        # register pre-processing: resolve overlap conflicts caused by partially overlapping slices
        ts_sorting_ctx = event_pipe.EventSortingContext(
            event_types=None,
            sortkey=self._default_sort_ts_and_rev_dur)
        process.register_stage(callback=event_pipe.sort_events, context=ts_sorting_ctx)

        # check whether the inflow into overlap detection has monotonic increasing ts (per pid/tid stream)
        monotonic_ts_ctx_a = event_pipe.TSSequenceContext(ts3check=True)
        process.register_stage(callback=event_pipe.assert_ts_sequence, context=monotonic_ts_ctx_a)

        # register pre-processing: resolve overlap conflicts caused by partially overlapping slices
        overlap_arg = self._overlap_option_from_arg(args.overlap)
        overlap_ctx = event_pipe.OverlapDetectionContext(overlap_resolve=overlap_arg,
                                                         ts_shift_threshold=self.defaults["ts_shift_threshold"])
        if overlap_arg == event_pipe.OverlapDetectionContext.OVERLAP_RESOLVE_TID:
            process.register_stage(callback=event_pipe.detect_partial_overlap_tids, context=overlap_ctx)
            process.register_stage(callback=event_pipe.pipeline_barrier, context=event_pipe._main_barrier_context)
        process.register_stage(callback=event_pipe.detect_partial_overlap_events, context=overlap_ctx)

        # validate that the overlap has not messed up the event stream ordering
        monotonic_ts_ctx_b = event_pipe.TSSequenceContext(ts3check=True)
        process.register_stage(callback=event_pipe.assert_ts_sequence, context=monotonic_ts_ctx_b)

        process.register_stage(callback=event_pipe.collect_iteration_stats,
                               context=event_pipe.IterationDectectContext())

        ##############################################################
        # dealing with power counter data
        if any(args.counter) and ("power_ts3" in args.counter or "power_ts4" in args.counter):
            use_ts4 = "power_ts4" in args.counter
            power_data_ctx = event_pipe.PowerExtractionContext(
                filter_pattern=" Prep",
                use_ts4=use_ts4)
            process.register_stage(callback=event_pipe.extract_power_event, context=power_data_ctx)

            # register callback to to the power counter sorting
            # create global context for sorting power counter events (using TS3 timestamp)
            sorting_counter_ctx = event_pipe.EventSortingContext(event_types=["C"], sortkey=TS_CYCLE_KEY)
            process.register_stage(callback=event_pipe.sort_events, context=sorting_counter_ctx)

            # calculate power
            power_compute_ctx = event_pipe.PowerExtractionContext(
                skip_events_flag=args.skip_events,
                filter_pattern=" Prep",
                use_ts4=use_ts4)
            process.register_stage(callback=event_pipe.compute_power, context=power_compute_ctx)

        # dealing with bandwidth counter data
        if any(args.counter) and "bandwidth" in args.counter:
            # dealing with bytes (data transfer) counter data
            data_transfer_data_ctx = event_pipe.DataTransferExtractionContext()
            process.register_stage(callback=event_pipe.extract_data_transfer_event, context=data_transfer_data_ctx)

            # calculate bandwidth
            data_transfer_compute_ctx = event_pipe.DataTransferExtractionContext()
            process.register_stage(callback=event_pipe.compute_bandwidth, context=data_transfer_compute_ctx)

        if any(args.counter) and "rcu_util" in args.counter and args.compiler_log:
            rcu_util_ctx = event_pipe.MultiRCUUtilizationContext(
                compiler_log=args.compiler_log,
                csv_fname=args.output,
                soc_freq=self.freq_soc,
                core_freq=self.freq_core)
            process.register_stage(callback=event_pipe.compute_utilization_fingerprints, context=rcu_util_ctx)

        ##############################################################
        # dealing with collective call flows
        monotonic_ts_ctx_c = event_pipe.TSSequenceContext(ts3check=True)
        process.register_stage(callback=event_pipe.assert_ts_sequence, context=monotonic_ts_ctx_c)

        if args.comm_summarize_seq:
            communication_event_ctx = event_pipe.CommunicationGroupContext()
            process.register_stage(callback=event_pipe.communication_event_collection, context=communication_event_ctx)
        process.register_stage(callback=event_pipe.pipeline_barrier, context=event_pipe._main_barrier_context)

        if args.comm_summarize_seq:
            process.register_stage(callback=event_pipe.communication_event_apply, context=communication_event_ctx)

        if any(args.counter) and "rcu_util" in args.counter and args.compiler_log:
            process.register_stage(callback=event_pipe.compute_utilization, context=rcu_util_ctx)

        # register callback to to the power counter sorting
        # create an event sorter for X-events with a global order across ranks required for flow detection
        sorting_flow_ctx = event_pipe.EventSortingContext(
            event_types=None,
            sortkey=self._default_sort_ts_and_rev_dur,
            global_sort=True)
        process.register_stage(callback=event_pipe.sort_events, context=sorting_flow_ctx)

        if args.flow:
            monotonic_ts_ctx_c = event_pipe.TSSequenceContext()
            process.register_stage(callback=event_pipe.assert_global_ts_sequence, context=monotonic_ts_ctx_c)

            process.register_stage(callback=event_pipe.flow_prepare_event_data)

            flow_ctx = event_pipe.CollectiveGroupingContext(build_coll_event=args.build_coll_event)
            process.register_stage(callback=event_pipe.flow_extraction, context=flow_ctx)

            # process.registerPreProcess(callback=event_pipe.flow_data_cleanup)

        # dealing with collective call bandwidth
        if any(args.counter) and "coll_bw" in args.counter:
            if args.build_coll_event:
                process.register_stage(callback=event_pipe.mp_calc_bw_v2, context=event_pipe.MpCalcBwV2Context())
            else:
                process.register_stage(callback=event_pipe.mp_calc_bw, context=event_pipe.MpCalcBwContext())

        # calculate statistics
        if args.stats:
            # pass stats filename
            data_stats_compute_ctx = event_pipe.StatsExtractionContext(stats_filename=args.output)
            process.register_stage(callback=event_pipe.calculate_stats, context=data_stats_compute_ctx)

        ##############################################################
        # This is for json file debugging or evaluation of new features
        # special optional filter out all events, except for the ones in the event pattern
        if args.filter != "":
            process.register_stage(
                callback=event_pipe.processing_filter,
                context=None,
                filter_pattern=args.filter,
            )

        ##############################################################
        # event cleanup for cases where processing functions had added temporary data
        # remove the ts_all from args that got added by cycle_count_to_wallclock
        process.register_stage(callback=event_pipe.flow_data_cleanup)
        process.register_stage(callback=event_pipe.cleanup_copy_of_device_ts)

        tb_refinement_ctx = event_pipe.RefinementContext(exporter, keep_names=args.keep_names)
        if args.tb_refinement:
            process.register_stage(callback=event_pipe.tb_refinement_intrusive, context=tb_refinement_ctx)

        # lightweight tb refinement changes cannot be disabled
        process.register_stage(callback=event_pipe.tb_refinement_lightweight, context=tb_refinement_ctx)
        process.register_stage(callback=event_pipe.cycle_count_conversion_cleanup)

        # calculate V2 statistics: relies on some lightweight tb-refinements
        if args.stats:
            if args.build_coll_event:
                data_stats_compute_ctx = event_pipe.EventStatsTrackerContext(
                    stats_filename=args.output,
                    stat_metrics=self.defaults["stats_v2"])
                process.register_stage(callback=event_pipe.calculate_stats_v2, context=data_stats_compute_ctx)

        final_sort_ctx = event_pipe.EventSortingContext(
            event_types=None,
            sortkey=self._default_sort_ts_and_rev_dur,
            global_sort=True)
        process.register_stage(callback=event_pipe.sort_events, context=final_sort_ctx)

        # <<< END Event processing functions registration
