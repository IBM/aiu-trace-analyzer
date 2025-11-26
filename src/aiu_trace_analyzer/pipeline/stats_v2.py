# Copyright 2024-2025 IBM Corporation

from abc import abstractmethod
import pandas as pd

import aiu_trace_analyzer.logger as aiulog
from aiu_trace_analyzer.types import TraceEvent
from aiu_trace_analyzer.pipeline import AbstractContext
from aiu_trace_analyzer.pipeline.tools import PipelineContextTool


_PID_COL = "pid"
_DURATION_COL = "elapsed_time"
_TIMEACCUM_COL = "_atime"


class Stat():
    @abstractmethod
    def add_event(self, *args, **kwargs):
        pass

    @abstractmethod
    def get_stat(self,  *args, **kwargs):
        pass

    @abstractmethod
    def calculate_stat(self,  *args, **kwargs):
        pass


class DurationStat(Stat):
    def __init__(self) -> None:
        self.earliest_start_time = {}
        self.latest_end_time = {}

    def add_event(self, event: TraceEvent) -> None:
        pid = event['pid']

        # update the earliest start time
        if pid not in self.earliest_start_time or self.earliest_start_time[pid] is None:
            self.earliest_start_time[pid] = event['ts']
        else:
            self.earliest_start_time[pid] = min(self.earliest_start_time[pid], event['ts'])

        # update the latest end time
        if pid not in self.latest_end_time or self.latest_end_time[pid] is None:
            self.latest_end_time[pid] = event['ts'] + event['dur']
        else:
            self.latest_end_time[pid] = max(self.latest_end_time[pid], event['ts'] + event['dur'])

        aiulog.log(aiulog.TRACE, f"STATS_V2: Add DurationStat {pid} for ts {event['ts']} w/ dur {event['dur']}")

    def get_stat(self, name) -> pd.DataFrame:
        dur_dict = {}
        for pid in self.earliest_start_time:
            elapsed_time = self.latest_end_time[pid] - self.earliest_start_time[pid]
            dur_dict[pid] = elapsed_time

        # convert dict to dataframe | pid | elapsed_time|
        df = pd.DataFrame(list(dur_dict.items()), columns=[_PID_COL, _DURATION_COL])

        aiulog.log(aiulog.TRACE, f"STATS_V2: Get DurationStat {df}")
        return df


class TimeAccumStat(Stat):
    def __init__(self) -> None:
        self.accum_time = {}

    def add_event(self, event: TraceEvent) -> None:
        pid = event['pid']

        # accumulate time
        if pid not in self.accum_time or self.accum_time[pid] is None:
            self.accum_time[pid] = event['dur']
        else:
            self.accum_time[pid] += event['dur']

        aiulog.log(aiulog.TRACE, f"STATS_V2: Add TimeAccumStat {pid} for ts {event['ts']} w/ dur {event['dur']}")

    def get_stat(self, name) -> pd.DataFrame:
        # convert dict to dataframe | pid | accum_active_time|
        df = pd.DataFrame(list(self.accum_time.items()), columns=[_PID_COL, name+_TIMEACCUM_COL])

        aiulog.log(aiulog.TRACE, f"STATS_V2: Get TimeAccumStat {df}")
        return df


class StatRegistry:
    def __init__(self, *args) -> None:
        self.stats = {}

    def register_stat(self, name, stat_obj) -> None:
        # Assume every metric has a unique name
        if name in self.stats:
            raise KeyError(f"STATS_V2: {name} already registered, consider changing to a different name")

        self.stats[name] = stat_obj
        aiulog.log(aiulog.TRACE, f"STATS_V2: {name} registered with {self.stats}")

    def add_event(self, event: TraceEvent, event_type: str) -> None:
        for name in self.stats:
            if event_type in name:
                self.stats[name].add_event(event)

    def get_stat(self, name, **kwargs) -> pd.DataFrame:
        if name in self.stats:
            # Currently, all returned stats in df
            stat_df = self.stats[name].get_stat(name, **kwargs)
            return stat_df

        return None


_COMPUTE_EVENT_KEY = "Cmpt Exec"
_COLLECTIVE_EVENT_KEY = "AllReduce_all_reduce"

_SUPPORTED_STAT_CLASS = {
    "DurationStat": DurationStat,
    "TimeStat": TimeAccumStat,
}

_TYPE_COMP = "comp"
_TYPE_COMM = "comm"


class EventStatsTrackerContext(AbstractContext, PipelineContextTool):
    def __init__(self, stats_filename: str, stat_metrics: dict) -> None:
        super().__init__()
        self.stats_filename = stats_filename
        self.registry = StatRegistry()

        aiulog.log(aiulog.TRACE, f"STATS_V2_INIT: Added metrics {stat_metrics}")

        # Stats registration format in [name (str), stat_type(str)]
        # Use stat_type name to locate stat Class
        for name, stat_type_name in stat_metrics.items():

            # check if pair is in reqested stats registration format
            if isinstance(name, str) and isinstance(stat_type_name, str):
                stat_type = _SUPPORTED_STAT_CLASS.get(stat_type_name)
                self.add_stat(name, stat_type())
            else:
                aiulog.log(aiulog.ERROR,
                           f"STATS_V2_INIT: {name} {stat_type_name} registeration failed,"
                           f" please check stats name and type")

        aiulog.log(aiulog.TRACE, f"STATS_V2_INIT: Registered {self.registry.stats.keys()}")

    def add_stat(self, stat_name, stat_obj) -> None:
        self.registry.register_stat(stat_name, stat_obj)

    def add_event(self, event: TraceEvent, event_type: str) -> None:
        # Assum stat_name contains the event type (comp and comm for now)
        # E.g., stat_name="comp_active" is for compute
        self.registry.add_event(event, event_type)

    def get_stat(self, stat_name, **kwargs) -> pd.DataFrame:
        return self.registry.get_stat(stat_name, **kwargs)

    def drain(self) -> list[TraceEvent]:
        # Output df, assume every return df contains a column named 'pid'
        tracker_df = pd.DataFrame(columns=[_PID_COL])

        for name in self.registry.stats:
            stat_df = self.get_stat(name)
            tracker_df = pd.merge(tracker_df, stat_df, on=_PID_COL, how="outer", validate='one_to_one')

        aiulog.log(aiulog.TRACE, f"STATS_V2: Drain {tracker_df} ")

        # Check if contains for total elapase time
        # Currently, only assume 1 _DURATION_COL column and n _TIMEACCUM_COL columns
        elapse_time_name = [col for col in tracker_df.columns if _DURATION_COL in col]
        time_accum_names = [col for col in tracker_df.columns if _TIMEACCUM_COL in col]

        # Calculate the time precentage of accumuated active time
        if len(elapse_time_name) == 1 and len(time_accum_names) > 0:
            for taccum_name in time_accum_names:
                col_name = taccum_name + '_perc'
                aiulog.log(aiulog.TRACE, f"STATS_V2: Drain {col_name} at {elapse_time_name}")
                tracker_df[col_name] = tracker_df[taccum_name] / tracker_df[elapse_time_name[0]]

        stats_filename = self.generate_filename(self.stats_filename, "stats_summary")
        tracker_df.to_csv(stats_filename, index=False)
        return []


def calculate_stats_v2(event: TraceEvent, context: AbstractContext) -> list[TraceEvent]:
    '''
    parses X events that contains "Cmpt Exec" string in the name
    '''
    assert isinstance(context, EventStatsTrackerContext)

    if event["ph"] in ["X"] and _COMPUTE_EVENT_KEY in event['name']:
        aiulog.log(aiulog.TRACE, f"STATS_V2: Capture compute on {event['name']}")
        context.add_event(event, _TYPE_COMP)

    if event["ph"] in ["X"] and _COLLECTIVE_EVENT_KEY == event['name']:
        aiulog.log(aiulog.TRACE, f"STATS_V2: Capture collective on {event['name']}")
        context.add_event(event, _TYPE_COMM)

    return [event]
