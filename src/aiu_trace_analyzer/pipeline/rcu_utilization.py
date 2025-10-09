# Copyright 2024-2025 IBM Corporation

import re
import copy
from math import isclose
import pathlib

import aiu_trace_analyzer.logger as aiulog
from aiu_trace_analyzer.pipeline.context import AbstractContext
from aiu_trace_analyzer.pipeline.tools import PipelineContextTool
from aiu_trace_analyzer.pipeline.barrier import TwoPhaseWithBarrierContext
from aiu_trace_analyzer.types import TraceEvent
from aiu_trace_analyzer.pipeline.tools import KernelDetailsDB, AutopilotDetail

import pandas as pd
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)

RCU_pt_util_counter_name = "PT Active"
RCU_pt_util_counter_unit = "Percent"

_default_fprint_len = 25
_kernel_db_feature_implemented = False


class RCUTableFingerprint():
    def __init__(self, datalimit: int = -1, initial_data: str = ''):
        self.initial_data = initial_data
        self.datalimit = datalimit if datalimit > 0 else 1 << 31   # yes, it's not really unlimited...
        self.reset()

    def get(self) -> int:
        return hash(self.fprint_data)

    def add(self, data: str) -> None:
        if self.datalimit > self.dataitems:
            aiulog.log(aiulog.DEBUG, f"adding to FP: {data}, Hash: {hash(data)}")
            self.fprint_data += data
            self.dataitems += 1

    def reset(self) -> None:
        self.fprint_data = self.initial_data
        self.dataitems = 0


class RCUUtilizationContext(AbstractContext, PipelineContextTool):

    _start_pattern = re.compile(r' Ideal/Total Cycles ')
    _end_pattern = re.compile(r'====== Perf Summary End ======')
    _clock_scaling = re.compile(r'Ideal Clock Scaling:')
    _data_pattern = re.compile(r'^[_\-a-zA-Z\d]+ +\d+ *$')
    _ignore_pattern = re.compile(r'(Precompute|-LxPreload)')
    _category_splitter = re.compile(r'(\-opCat|\-NA$)')
    _autopilot_pattern = re.compile(r'DSM-AutoPilot BEGIN')

    _print_to_log = False

    def __init__(
            self,
            compiler_log: str,
            csv_fname: str,
            soc_freq: float,
            core_freq: float,
            kernel_db_url: str = "ai_kernel.db") -> None:
        super().__init__()

        self.warn_util_100 = 0   # count the number of events with >100% utilization to print warning at the end
        self.other_warning = 0   # count the number of kernels that had to be accounted as 'other'
        self.autopilot = False
        self.csv_fname = self.generate_filename(csv_fname, "categories")
        self.tab_fname = self.generate_filename(csv_fname, "categories", "txt")
        self.kernel_db_url = kernel_db_url
        self._use_core_freq = True
        self.multi_table = -1  # assume no multitable case

        # if scale factor is unknown, set to -1.0 to later identify cycles that need subsequent rescaling
        self.scale_factor = soc_freq/core_freq
        self.cycle_to_clock_factor = 1.0/core_freq
        self.unscaled = False
        aiulog.log(aiulog.DEBUG, "UTL: Input Ideal Cycle To Clock factor", self.cycle_to_clock_factor)

        self.initialize_tables()
        try:
            subdir, fpat = '/'.join(compiler_log.split('/')[:-1]), compiler_log.split('/')[-1]
            compiler_log_name = list(pathlib.Path(subdir).rglob(fpat))[0]
            self.extract_tables(compiler_log=compiler_log_name)
        except Exception as e:
            aiulog.log(aiulog.WARN, "UTL: Unable to open or parse log file.", compiler_log, e)

        self.scale_cycles()

        for n, t in self.kernel_cycles.items():
            self.autopilot_detail = AutopilotDetail(t)
            self.table_hash = self.autopilot_detail.table_hash()

    def __del__(self) -> None:
        if self.multi_table > 0:  # used as index, so 'n-1'
            aiulog.log(aiulog.WARN, "UTL:", self.multi_table+1,
                       "tables with ideal cycles have been detected."
                       " Utilization results should be inspected carefully!!!!")
        if self.warn_util_100:
            aiulog.log(aiulog.WARN, "UTL: Encountered", self.warn_util_100, "Events with >100% utilization")
        if self.other_warning:
            aiulog.log(aiulog.WARN, "UTL: Found", self.other_warning,
                       "Events without a matching kernel category and accounted for 'other'")
        if self.unscaled:
            aiulog.log(aiulog.WARN, "UTL: No ideal/real frequency unscaled (factor 1.0). "
                       "Utilization might be based on undefined data.")

        # dealing with the kernel_db only makes sense if we detected any table at all
        if _kernel_db_feature_implemented and len(self.kernel_cycles):
            self.kernel_db = KernelDetailsDB(self.kernel_db_url, self.autopilot)

            if self.autopilot:
                aiulog.log(aiulog.WARN, "UTL: Detected autopilot=1. "
                           "PT-activity/categories data will be attempted to get from previous runs with AP=0")
                self.categories = self.kernel_db.retrieve(self.table_hash)
            else:
                self.kernel_db.insert(self.table_hash, self.categories)

        if len(self.categories.keys()) > 0:
            self.print_table_as_pd(self.categories)

    def initialize_tables(self) -> None:
        self.kernel_cycles = {}  # tables indexed by fingerprint
        self.categories = {}
        self.kernel_cat_map = {"other": "other"}

    def extract_tables(self, compiler_log: str):
        parse_mode = False
        self.multi_table = -1  # track if there might be multiple tables in the log (force to use the first one only)
        current_table = {}
        fprint = RCUTableFingerprint(_default_fprint_len)  # use the first N kernels as a fingerprint
        with open(compiler_log, 'r') as cl:
            for line in cl:
                # drop out if autopilot=1 is detected
                if self._autopilot_pattern.search(line):
                    self.autopilot = True
                    return

                if self._clock_scaling.search(line):
                    aiulog.log(aiulog.WARN,
                               "UTL: Found obsolete 'Ideal Cycle Scaling' setting in logfile."
                               " This setting is ignored. Use '--freq=<soc>:<core>'.")
                    continue

                if self._start_pattern.search(line):
                    self.multi_table += 1
                    current_table = {}
                    fprint.reset()
                    parse_mode = True
                    aiulog.log(aiulog.DEBUG,
                               "UTL: Start of Ideal Cycle Count section detected. Parse mode:",
                               parse_mode, self.multi_table)
                    continue

                # don't bother checking for the end_pattern if we're not even in parse mode
                if not parse_mode:
                    continue

                if self._end_pattern.search(line):
                    aiulog.log(aiulog.DEBUG, "UTL: End of Ideal Cycle Count section detected. Stopping parse mode.")
                    parse_mode = False
                    if fprint.get() in self.kernel_cycles:
                        aiulog.log(aiulog.ERROR, "UTL: Fingerprint of current table already exists in previous table.")
                        raise NotImplementedError("UTL: Fingerprint of current table already exists in previous table."
                                                  " Support for same-sequence utilisation is not yet implemented."
                                                  " You may want to send us this sample to help enabling this feature.")
                    else:
                        aiulog.log(aiulog.INFO, f"UTL: Adding ideal cycles table with fingerprint: {fprint.get()}")
                        aiulog.log(aiulog.DEBUG, f"UTL:    TablefprintStr: {fprint.fprint_data}")
                        self.kernel_cycles[fprint.get()] = copy.deepcopy(current_table)
                    continue

                # This will needs to be the last regex check because it skips everything else
                if not self._data_pattern.search(line) or self._ignore_pattern.search(line):
                    continue

                ldata = re.split(" +", line)
                if len(ldata) < 2 or len(ldata) > 3:  # strange format includes newline as a 3rd column
                    aiulog.log(aiulog.WARN,
                               "UTL: found data pattern line with more than 2 columns. Check patterns.",
                               ldata)
                    continue

                cycles = int(ldata[1])
                kernel_and_cat = self._category_splitter.split(ldata[0])
                if len(kernel_and_cat) > 1:
                    if kernel_and_cat[1] == "-opCat":
                        category = kernel_and_cat[-1]
                    else:
                        category = "NotAvailable"
                else:
                    category = "Total"

                # The last row of the ideal table, starting with "Total", is not a kernel name.
                if kernel_and_cat[0] == "Total": 
                    continue

                kernel = kernel_and_cat[0]+" Cmpt Exec"
                fprint.add(kernel)

                if kernel not in current_table:
                    aiulog.log(aiulog.TRACE, "UTL: Kernel:", kernel)
                    if cycles != 0:
                        current_table[kernel] = cycles
                elif cycles != current_table[kernel]:
                    aiulog.log(aiulog.WARN,
                               "UTL: Kernel already has an entry with different cycle count:",
                               kernel, cycles, current_table[kernel])
                else:
                    pass  # found the same kernel name associated with the same ideal cycles: still consistent to go

                if kernel not in self.kernel_cat_map:
                    self.kernel_cat_map[kernel] = category
                elif category != self.kernel_cat_map[kernel]:
                    aiulog.log(aiulog.WARN,
                               "UTL: Kernel->Category map already has an entry with different category:",
                               kernel, category, self.kernel_cat_map[kernel])
                else:
                    pass  # found the same kernel name associated with the same category: still consistent to go

    def print_table_as_pd(self, cat_tab):
        """
        Generate time breakdown along kernel categories

        Table columns
        ---------------------------
        .  Kernel Time: observed runtime of kernels
        .  Frac Time: ratio of the accumulated observed time of kernels in a category and
                      the total observed time of all kernels.
        .  Calls: number of kernels observed in a category
        .  Ideal Time: ideal time converted using core-clock frequency.
        .  Ideal Cycles: ideal cycle, as read from table.
        .  Frac Ideal: ratio of the accumulated ideal-time of kernels in a category and
                       the total ideal-time of all kernels.
        .  PT Util: ratio of the accumulated ideal-time of kernels in a category and
                    the accumulated observed-time of kernels in the same category.
        """

        title_row = ["Pid", "Category", "Kernel_Time", "Frac_Time", "Calls",
                     "Ideal_Time", "Ideal_Cyc", "Frac_Ideal", "PT_Util"]
        aiulog.log(aiulog.DEBUG, "UTL: category title_row: ", title_row)

        list_of_list = []
        for p, data in cat_tab.items():
            if len(data) == 0:
                return

            total = data["Total"][0]
            ideal_total = data["Total"][1]

            for k, (dur, ideal, calls) in data.items():
                ideal_cyc = int(ideal / abs(self.cycle_to_clock_factor))

                # prevent div-by-zero exception
                dur_frac = 0 if isclose(total, 0.0, abs_tol=1e-9) else round(dur / total, 4)
                ideal_frac = 0 if isclose(ideal_total, 0.0, abs_tol=1e-9) else round(ideal / ideal_total, 4)
                pt_util = 0 if isclose(dur, 0.0, abs_tol=1e-9) else round(ideal / dur, 4)

                # note: to sync the columns of value_row with title_row
                value_row = [p, k, dur, dur_frac, calls, round(ideal, 4), ideal_cyc, ideal_frac, pt_util]
                list_of_list.append(value_row)

                aiulog.log(aiulog.DEBUG, "UTL: category value_row: ", value_row)

        # the sorting places the "Total" row to the last of each section (section per pid) of the table.
        df = pd.DataFrame(list_of_list, columns=title_row)
        sorted_df = df.sort_values([title_row[0], title_row[2]], kind='stable', inplace=False, ignore_index=True)

        # for traces with a single category, the Total row may not appear to the last: extract, drop, concat
        total_row_idx = sorted_df.index[ sorted_df['Category'] == "Total" ].tolist()[0]
        total_row     = sorted_df.loc[[ total_row_idx ]]
        df_drop_total = sorted_df.drop( total_row_idx )
        sorted_df = pd.concat( [df_drop_total, total_row] )

        sorted_df.to_csv(self.csv_fname, index=False, header=True)                   # dump to CSV file
        print(sorted_df.to_string(index=False), file=open(self.tab_fname, 'w'))    # dump to TXT file

        aiulog.log(aiulog.INFO, "UTL: category table(s) created as CSV:", self.csv_fname)
        aiulog.log(aiulog.INFO, "UTL: category table(s) created as TXT:", self.tab_fname)

    # if there's no category table for the pid, create a new one from the known category keys
    def set_categories_for_pid(self, pid) -> None:
        if pid in self.categories:
            return
        else:
            aiulog.log(aiulog.DEBUG, "UTL: Creating new categories table for", pid)
            # always have the StcdpHbm category
            self.categories[pid] = {"Total": (0.0, 0.0, 0), "StcdpHbm": (0.0, 0.0, 0)}

        for cat in self.kernel_cat_map.values():
            self.categories[pid][cat] = (0.0, 0.0, 0)

    def get_cycles(self, kernel: str, fprint: int) -> int:
        if len(self.kernel_cycles):
            # For the TTFT jobs in long-context granite traces, the ideal-table contains up to 80K entries, 
            #   the existing approach offingerprint.get, hash of a concatenated string from a list of 
            #   kernel names in the order as listed in the ideal table, should need remedy. 
            # Meanwhile, to allow the RCU-UTL processing with single job file and tailered ideal-table text, 
            #   the update here bypass the fingerprint matching when one ideal-table was ingested.
            key = fprint
            if ( len( self.kernel_cycles.keys() ) == 1 ): 
                key = list(self.kernel_cycles.keys())[0]
            rval = self.kernel_cycles[key].get(kernel, 0)
            return rval
        else:
            return 0

    def scale_cycles(self):
        # update: using wallclock based on core freq, no scaling needed any more

        # self.unscaled = math.isclose(self.scale_factor, 1.0)
        # if self.unscaled:
        #     return

        # for n in self.kernel_cycles.keys():
        #     for k,v in self.kernel_cycles[n].items():
        #         self.kernel_cycles[n][k] = int(self.kernel_cycles[n][k] * (self.scale_factor))
        #         aiulog.log(aiulog.TRACE, f"UTL: Updating cycles of table {n}: {k}, {v} -> {self.kernel_cycles[n][k]}")

        return

    def accumulate_categories(self, pid, kernel, ideal_dur, duration):
        if kernel not in self.kernel_cat_map:
            self.other_warning += 1
            aiulog.log(aiulog.DEBUG, "UTL:", pid, "Unexpected kernel name: ", kernel, "Accounting for as 'other'.")
            kernel = "other"

        cat = self.kernel_cat_map[kernel]
        self.set_categories_for_pid(pid)
        aiulog.log(aiulog.TRACE, "UTL: ", kernel, cat, duration, ideal_dur, self.categories[pid][cat])

        dur, i_dur, cnt = self.categories[pid][cat]
        self.categories[pid][cat] = (dur+duration, i_dur+ideal_dur, cnt+1)

        dur, i_dur, cnt = self.categories[pid]["Total"]
        self.categories[pid]["Total"] = (dur+duration, i_dur+ideal_dur, cnt+1)
        return cat


class MultiRCUUtilizationContext(TwoPhaseWithBarrierContext, PipelineContextTool):
    name_converter = re.compile(r"\[N\]")

    def __init__(
            self,
            compiler_log: str,
            csv_fname: str,
            soc_freq: float,
            core_freq: float) -> None:
        super().__init__()
        log_list = compiler_log.split(",")
        self.multi_log = (len(log_list) > 1)
        self.warn_util_100 = 0
        self.warn_nomatch = 0  # count the number of events where no corresponding table/fingerprint was found
        self.fingerprints = {}   # fingerprints per job-file
        if self.multi_log:
            aiulog.log(aiulog.INFO, "UTL: Multi-AIU logs provided. Entries:", len(log_list))

        # event rank will be multiplied by this factor to make the key for the correct rcuctx
        # in single-log case: will turn everything into zero, otherwise use event rank
        self.rank_factor = 1 if self.multi_log else 0

        self.rcuctx = {}
        for rank, log in enumerate(log_list):
            aiulog.log(aiulog.DEBUG, "UTL: Building kernel table for", rank)
            if self.multi_log:
                csv_basename = csv_fname + str(rank)
            else:
                csv_basename = csv_fname
            self.rcuctx[rank] = RCUUtilizationContext(log,
                                                      csv_fname=csv_basename,
                                                      soc_freq=soc_freq,
                                                      core_freq=core_freq)

    def __del__(self) -> None:
        if self.warn_util_100:
            aiulog.log(aiulog.WARN, "UTL: Encountered", self.warn_util_100, "Events with >100% utilization")
        if self.warn_nomatch:
            aiulog.log(aiulog.WARN, "UTL: No matching Ideal Cycles table found for", self.warn_nomatch,
                       "Events. This might indicate a wrong frequency setting.")

    def extract_kernel_from_event_name(self, event: TraceEvent) -> str:
        rname = event["name"]

        # if a fn_idx was removed from the event name, we have to bring it back in to match the ideal cycles table entry
        if "[N]" in rname and "args" in event and "fn_idx" in event["args"]:
            rname = self.name_converter.sub(str(event["args"]["fn_idx"]), event["name"], count=1)

        if not rname.endswith("Cmpt Exec"):
            rname += " Cmpt Exec"

        return rname

    def get_ideal_dur(self, kernel: str, pid: int, fingerprint: int) -> float:
        rank = pid * self.rank_factor
        return self.rcuctx[rank].get_cycles(kernel, fingerprint) * self.rcuctx[rank].cycle_to_clock_factor

    def accumulate_categories(self, pid, kernel, ideal_dur, duration):
        rank = pid * self.rank_factor
        return self.rcuctx[rank].accumulate_categories(pid, kernel, ideal_dur, duration)

    def fingerprint_add(self, job: int, kernel: str) -> None:
        if job not in self.fingerprints:
            self.fingerprints[job] = RCUTableFingerprint(_default_fprint_len)

        self.fingerprints[job].add(kernel)

    def fingerprint_get(self, job: int) -> int:
        return self.fingerprints[job].get()

    # build a counter and a zero event
    def make_utilization_event(self, event: TraceEvent, utilization: float) -> list[TraceEvent]:
        revents = [{
                "ph": "C",
                "ts": event["ts"],
                "pid": event["pid"],
                "name": RCU_pt_util_counter_name,
                "args": {RCU_pt_util_counter_unit: utilization},
                "dur": event["dur"]  # temporary duration in cycles- remove before viz
            }]
        if utilization > 0.0:   # add a reset-to-zero event only if util is non-zero
            revents.append({
                "ph": "C",
                "ts": event["ts"]+event["dur"],
                "pid": event["pid"],
                "name": RCU_pt_util_counter_name,
                "args": {RCU_pt_util_counter_unit: 0.0}
            })
        return revents

    def drain(self) -> list[TraceEvent]:
        # if self.phase == self._COLLECTION_PHASE:
        #     for n,t in self.fingerprints.items():
        #         print(f"Job: {n} -> {t.get()}, {t.fprint_data[:50]}")
        return super().drain()


def compute_utilization_fingerprints(event: TraceEvent, context: AbstractContext) -> list[TraceEvent]:
    if event["ph"] != "X":
        return [event]

    assert isinstance(context, MultiRCUUtilizationContext)

    if PipelineContextTool.is_acc_event(event) and PipelineContextTool.is_acc_kernel(event):
        kernel_name = context.extract_kernel_from_event_name(event)

        context.fingerprint_add(event["args"]["jobhash"], kernel_name)
    return [event]


def compute_utilization(event: TraceEvent, context: AbstractContext) -> list[TraceEvent]:
    if event["ph"] != "X":
        return [event]

    assert isinstance(context, MultiRCUUtilizationContext)

    if PipelineContextTool.is_acc_event(event) and PipelineContextTool.is_acc_kernel(event):
        pid = event["pid"]
        kernel_name = context.extract_kernel_from_event_name(event)

        try:
            job_fingerprint = context.fingerprint_get(event["args"]["jobhash"])
        except KeyError:
            aiulog.log(aiulog.WARN, f"UTL: No matching fingerprint for job {event['args']['jobname']}."
                       " Unable to find a matching Ideal-cycles table.")
            return [event]

        try:
            ideal_dur = float(context.get_ideal_dur(kernel_name, pid, job_fingerprint))
        except KeyError:
            aiulog.log(aiulog.DEBUG, f"UTL: No kernel table matching fingerprint {job_fingerprint}:"
                       f" {context.fingerprints.keys()}/{context.fingerprints[event['args']['jobhash']].fprint_data} ")
            context.warn_nomatch += 1
            ideal_dur = 0.0

        # cmpt_dur = int(event["args"]["TS4"]) - int(event["args"]["TS3"])
        cmpt_dur = float(event["dur"])
        utilization = abs(ideal_dur/cmpt_dur)

        if utilization > 0.0:
            event["args"]["core used"] = True

        if utilization > 1.0:   # warning about >100% utilization
            aiulog.log(aiulog.WARN, "UTL: Event with +100% utilization. "
                       "This could indicate a problem with table fingerprinting: "
                       "(pid, ideal, observed, event)", pid, ideal_dur, cmpt_dur, event)
            context.warn_util_100 += 1
            utilization = 1.0

        if "cat" in event:
            event["args"]["user_cat"] = context.accumulate_categories(pid, kernel_name, ideal_dur, cmpt_dur)
        else:
            event["cat"] = context.accumulate_categories(pid, kernel_name, ideal_dur, cmpt_dur)
        util_counter = context.make_utilization_event(event, utilization*100.0)
        return [event] + util_counter

    return [event]
