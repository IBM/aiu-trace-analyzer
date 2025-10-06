# Copyright 2024-2025 IBM Corporation

import re
import statistics
import math

import aiu_trace_analyzer.logger as aiulog
from aiu_trace_analyzer.types import TraceEvent
from aiu_trace_analyzer.pipeline import AbstractContext, EventPairDetectionContext

from aiu_trace_analyzer.pipeline.rcu_utilization import RCU_pt_util_counter_name, RCU_pt_util_counter_unit
from aiu_trace_analyzer.pipeline.tools import PipelineContextTool


class StatsExtractionContext(EventPairDetectionContext, PipelineContextTool):
    '''
    Stats extraction (experimental):
    Hashes counter events into a queue per process ID and sorts events by their TS
    When emitting (during drain), bandwidth is computed dividing number of bytes dy
    corresponding duration
    '''
    def __init__(self, stats_filename) -> None:
        super().__init__()
        self.stats_filename = stats_filename
        self.total_util = {}

        self.min_ts_map = {}

        self.max_ts_map = {}

    def __del__(self) -> None:
        for pid, data in self.total_util.items():
            mean_pt_util, tot_matmul, tot_other = data
            total = tot_matmul + tot_other
            total_pt_util = mean_pt_util*(tot_matmul/(tot_matmul+tot_other))
            aiulog.log(aiulog.INFO, f'UTL: pid={pid}: avg_mm_util={round(mean_pt_util, 3)},'
                       f' avg_util={round(total_pt_util,3)}, t_mm={tot_matmul}, t_sf={tot_other},'
                       f' ratio_mm/sf={round(tot_matmul/total*100.0, 2)}:{round(tot_other/total*100.0, 2)}')

    def update_min_ts(self, pid, ts):
        #
        if pid not in self.min_ts_map:
            # set to max float value
            self.min_ts_map[pid] = 1e30

        # update min
        self.min_ts_map[pid] = min(self.min_ts_map[pid], ts)

    def update_max_ts(self, pid, ts):
        #
        if pid not in self.max_ts_map:
            # set to minumum
            self.max_ts_map[pid] = 0.0

        # update max
        self.max_ts_map[pid] = max(self.max_ts_map[pid], ts)

    def calculate_stats_using_event_duration(self, name: str, durations: list[float]) -> list[float]:
        # just eat all events and wait for drain to sort and emit

        # Time (%) Total Time  Num Calls  Avg   Med   Min  Max  StDev   Name

        # process the durations (using statistics package)
        time = 0.0

        total_time = sum(durations)

        num_calls = len(durations)

        min_val = min(durations)
        max_val = max(durations)

        mean = round(statistics.mean(durations), 3)
        median = round(statistics.median(durations), 3)

        # standard deviation needs at least 2 data points
        if num_calls > 1:
            stdev = round(statistics.stdev(durations), 3)
        else:
            stdev = 0.0

        return [time, total_time, num_calls, mean, median, min_val, max_val, stdev]

    def collect_util(self, event: TraceEvent) -> None:
        util = event["args"][RCU_pt_util_counter_unit]
        mean_util, tot_matmul, tot_other = self.total_util.setdefault(event["pid"], (0.0, 0.0, 0.0))

        if util > 0.0:
            tot_matmul += event["dur"]
            mean_util += (util - mean_util) * (event["dur"] / tot_matmul)   # cumulative weighted average computation
        else:
            tot_other += event["dur"]

        self.total_util[event["pid"]] = (mean_util, tot_matmul, tot_other)

    def drain(self) -> list[TraceEvent]:

        stats_list = {}    # create empty dict

        # total running time for all kernels
        total_compute_time = 0.0

        # create stats file
        filename_summary = self.generate_filename(self.stats_filename, "summary")

        # check if name is not ""
        if filename_summary != "":
            # create file
            fp_stats = open(filename_summary, 'w')
            aiulog.log(aiulog.INFO, "SEC statistics summary file created: ", filename_summary)

        # create active analysis file
        filename_active = self.generate_filename(self.stats_filename, "active")

        # check if name is not ""
        if filename_active != "":
            # create file
            fp_active = open(filename_active, 'w')
            aiulog.log(aiulog.INFO, "SEC statistics active file created: ", filename_active)

        # create TS1 timestamp analysis file
        filename_ts_analysis = self.generate_filename(self.stats_filename, "ts_analysis")

        # check if name is not ""
        if filename_ts_analysis != "":
            # create file
            fp_ts_analysis = open(filename_ts_analysis, 'w')
            aiulog.log(aiulog.INFO, "SEC statistics timestamp analysis file created: ", filename_ts_analysis)

        # check if summary file was created and write the header
        if fp_stats != 0:
            #            Time Tot Time  calls   mean    med     min     max    stdev    pid   name
            fp_stats.write("Time  \t Total Time\t Calls\t    Mean  \t  Median  \t    Min    \t   Max   \t   StDev  \t  pid\t Name \n")  # noqa: E501

        # check if active file was created and write the header
        if fp_active != 0:
            #     Total Kernel time Elapsed Time  Start Time   End Time    Active percentage   pid
            fp_active.write("Total Kernel Time  \t  Elapsed Time\t  Start Time\t         End Time  \t Active percentage  \t   pid \n")  # noqa: E501

        # check if active file was created and write the header
        if fp_ts_analysis != 0:
            #     Total Kernel time Elapsed Time  Start Time   End Time    Active percentage   pid
            fp_ts_analysis.write("Calls  \t TS2-1_Mean   \tTS2-1_Median  \tTS2-1_Min    \tTS2-1_Max   \tTS2-1_StDev  \tTS3-2_Mean   \tTS3-2_Median  \tTS3-2_Min    \tTS3-2_Max   \tTS3-2_StDev  \tTS4-3_Mean   \tTS4-3_Median  \tTS4-3_Min    \tTS4-3_Max   \tTS4-3_StDev  \tTS5-4_Mean   \tTS5-4_Median  \tTS5-4_Min    \tTS5-4_Max   \tTS5-4_StDev  \tpid\t Name \n")  # noqa: E501

        # create empty dict
        total_times = {}

        # go over all the queues
        for _, queue in self.queues.items():
            #
            name, event_pid, durations, ts1ts2, ts2ts3, ts3ts4, ts4ts5 = queue

            if event_pid not in total_times:
                total_times[event_pid] = 0.0

            # calculate stats using event duration
            stats = self.calculate_stats_using_event_duration(name, durations)

            # calculate stats using event ts1ts2
            stats_ts1ts2 = self.calculate_stats_using_event_duration(name, ts1ts2)

            # calculate stats using event ts2ts3
            stats_ts2ts3 = self.calculate_stats_using_event_duration(name, ts2ts3)

            # calculate stats using event ts3ts4
            stats_ts3ts4 = self.calculate_stats_using_event_duration(name, ts3ts4)

            # calculate stats using event ts4ts5
            stats_ts4ts5 = self.calculate_stats_using_event_duration(name, ts4ts5)

            # update total time for each pid
            total_times[event_pid] += stats[1]

            if event_pid not in stats_list:
                #
                stats_list[event_pid] = []

            # add stats to the queue
            # stats_list[event_pid].append( (stats, name) )
            stats_list[event_pid].append((stats, name, stats_ts1ts2, stats_ts2ts3, stats_ts3ts4, stats_ts4ts5))

        # sort stats_list using first field (pid)
        for pid in sorted(stats_list):

            total_compute_time = 0.0

            for (stats,
                 name,
                 stats_ts1ts2,
                 stats_ts2ts3,
                 stats_ts3ts4,
                 stats_ts4ts5) in sorted(stats_list[pid], key=lambda x: x[0][1], reverse=True):
                # calculate Time in percentage
                stats[0] = round((stats[1] / total_times[pid]) * (100.0), 2)

                # write the statistics into the stats file
                # stats[0] = Time, stats[1] = total_time, stats[2] = num_calls, stats[3] = mean, ....
                if fp_stats != 0:
                    #             Time  Tot Time  calls   mean  med   min     max    stdev     name
                    fp_stats.write("%5.2f\t  %9.3f\t %5d\t %8.3f\t %8.3f\t %8.3f\t %8.3f\t %8.3f\t %4d\t %s\n" % (stats[0], stats[1], stats[2], stats[3], stats[4], stats[5], stats[6], stats[7], pid, name))  # noqa: E501

                # compute the total time for all pids
                total_compute_time += stats[1]

                if fp_ts_analysis != 0:
                    # mean  med   min     max    stdev X4,  pid    name
                    fp_ts_analysis.write(" %5d\t %8.3f\t %8.3f\t %8.3f\t %8.3f\t %8.3f\t %8.3f\t %8.3f\t %8.3f\t %8.3f\t %8.3f\t %8.3f\t %8.3f\t %8.3f\t %8.3f\t %8.3f\t %8.3f\t %8.3f\t %8.3f\t %8.3f\t %8.3f\t %4d\t %s\n" % (stats_ts1ts2[2], stats_ts1ts2[3], stats_ts1ts2[4], stats_ts1ts2[5], stats_ts1ts2[6], stats_ts1ts2[7], stats_ts2ts3[3], stats_ts2ts3[4], stats_ts2ts3[5], stats_ts2ts3[6], stats_ts2ts3[7], stats_ts3ts4[3], stats_ts3ts4[4], stats_ts3ts4[5], stats_ts3ts4[6], stats_ts3ts4[7], stats_ts4ts5[3], stats_ts4ts5[4], stats_ts4ts5[5], stats_ts4ts5[6], stats_ts4ts5[7], pid, name))  # noqa: 501

            elapsed_time = (self.max_ts_map[pid] - self.min_ts_map[pid])

            # percentage of time that the device is doing computation
            active_percentage = (total_compute_time/(elapsed_time))*100.0

            if fp_active != 0:
                #     Total Kernel time Elapsed Time  Start Time   End Time    Active percentage   pid
                fp_active.write("%9.3f\t  %9.3f\t  %9.3f\t %9.3f\t %5.2f\t %4d\n" % (total_compute_time, elapsed_time, self.min_ts_map[pid], self.max_ts_map[pid], active_percentage, pid))  # noqa: E501

        # close stats files
        fp_stats.close()

        fp_active.close()

        fp_ts_analysis.close()

        return []


def calculate_stats(event: TraceEvent, context: AbstractContext) -> list[TraceEvent]:
    '''
    parses X events that contains "Cmpt Exec" string in the name
    '''
    assert isinstance(context, StatsExtractionContext)

    if event["ph"] in ["X"] and "Cmpt Exec" in event['name']:
        # remove id from name:
        # "name": "addmm_2_MatMul Cmpt Exec",
        # becames "addmm_N_Matmult Cmpt Exec"
        name_converter = re.compile(r"[_-]\d+")

        stripped_name = name_converter.sub("_[N]", event["name"])

        event_start = event["ts"]
        event_dur = event["dur"]
        ts1ts2 = float(event['args']['TS2']) - float(event['args']['TS1'])
        ts2ts3 = float(event['args']['TS3']) - float(event['args']['TS2'])
        ts3ts4 = float(event['args']['TS4']) - float(event['args']['TS3'])
        ts4ts5 = float(event['args']['TS5']) - float(event['args']['TS4'])
        event_pid = event["pid"]

        # event duration must be non negative
        if event_dur < 0.0:
            #
            print(f"event_name: {event['name']}, event_start: {event_start}, event_dur: {event_dur}")

        assert event_dur > 0.0

        # hash the stripped_name and event_pid
        qid = context.queue_hash(stripped_name, event_pid)

        # create a new queue if does exist yet
        if qid not in context.queues:
            # create queue
            context.queues[qid] = (stripped_name, event_pid, [], [], [], [], [])

        # context.queues[qid][2]: event duration
        # context.queues[qid][3]: TS2 - TS1 time
        # context.queues[qid][4]: TS3 - TS2 time
        # context.queues[qid][5]: TS4 - TS3 time
        # context.queues[qid][6]: TS5 - TS4 time

        context.queues[qid][2].append(event_dur)
        context.queues[qid][3].append(ts1ts2)
        context.queues[qid][4].append(ts2ts3)
        context.queues[qid][5].append(ts3ts4)
        context.queues[qid][6].append(ts4ts5)

        # update min
        context.update_min_ts(event_pid, event_start)

        # update max
        context.update_max_ts(event_pid, (event_start + event_dur))

    if event["ph"] == "C" and event["name"] == RCU_pt_util_counter_name:
        if "dur" not in event:
            return [event]  # ignore events that don't have the artificial duration entry
        context.collect_util(event)
        if "dur" in event and math.isclose(event["args"][RCU_pt_util_counter_unit], 0.0, abs_tol=1e-9):
            return []
        event.pop("dur")

    return [event]
