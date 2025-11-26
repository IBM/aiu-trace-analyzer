# Copyright 2024-2025 IBM Corporation

import re
import copy
from math import isclose
import pathlib
from enum import Flag, auto

import aiu_trace_analyzer.logger as aiulog
from aiu_trace_analyzer.types import TraceEvent, TraceWarning
from aiu_trace_analyzer.pipeline.context import AbstractContext
from aiu_trace_analyzer.pipeline.tools import PipelineContextTool
from aiu_trace_analyzer.pipeline.barrier import TwoPhaseWithBarrierContext
from aiu_trace_analyzer.pipeline.tools import KernelDetailsDB, AutopilotDetail

import pandas as pd
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)

RCU_pt_util_counter_name = "PT Active"
RCU_pt_util_counter_unit = "Percent"

# for table fingerprint: include only event names that match this regex
_fprint_event_filter = r'^.*'

# default number of events for fingerprint of observed event stream
_default_fprint_len = 30

# default number of kernels for fingerprint of ideal cycles table
_table_data_limit = 500

# keep disabled until db-lookup feature is implemented
_kernel_db_feature_implemented = False


class RCUTableFingerprint():
    _separator = '_'

    def __init__(
            self,
            datalimit: int = -1,
            event_filter: str = r'',
            table_mode: str = "UNKN"):
        self.datalimit = datalimit if datalimit > 0 else 1 << 31   # yes, it's not really unlimited...
        self.event_filter = re.compile(event_filter)
        self.table_mode = table_mode
        self.sim_weights = {
            "sequence": 0.5,
            "tab_len": 0.5,
            "total_time": 0.5
            }
        self.reset()

    def get(self) -> int:
        return self.hash

    def get_table_mode(self) -> int:
        return self.table_mode

    def add(self, data: str, time: float) -> None:
        if self.dataitems < self.datalimit and self.event_filter.search(data) is not None:
            aiulog.log(aiulog.DEBUG, f"adding to FP: {data}, Hash: {hash(data)}")
            converted = self._data_conversion(data)
            self.fprint_data += f"{converted}{self._separator}"
            self.data_hashes.append(converted)

        # item count and accumulated time needed for non-hash similarity checks
        self.dataitems += 1
        self.totaltime += time
        self._update_hash()

    def _data_conversion(self, data: str):
        # enough variety to build a rough 'alphabet' of kernel names
        # no big deal if some collisions occur
        return hash(data) % 65535

    def _update_hash(self) -> None:
        # hash based on data and totaltime (distinguish same sequence for different input sizes)
        self.hash = hash(self.fprint_data + str(self.totaltime))

    def reset(self) -> None:
        self.fprint_data: str = ""
        self.data_hashes: list[int] = []
        self.dataitems: int = 0
        self.totaltime = 0.0
        self._update_hash()

    def similarity(self, other) -> float:
        """
        similarity check happens under assumption:
         * self  == event stream fingerprint
         * other == ideal table fingerprint
        -> exclude criteria:
           * impossible: other.dataitems < self.dataitems
           * impossible: other.totaltime > self.totaltime
        """
        aiulog.log(aiulog.DEBUG, "FPRINT SIMILARITY: -----------------")
        # kernel sequence similarity
        sim_val = self.sim_weights["sequence"] * (1.0 if other.fprint_data.find(self.fprint_data) != -1 else 0.5)
        matched = "sub-sequence not found" if sim_val < self.sim_weights["sequence"] else "sub-sequence found"
        aiulog.log(
            aiulog.DEBUG, "   SIMVAL(sequence):",
            f"{sim_val}  <- ({matched}, {other.fprint_data.find(self.fprint_data)})")

        # table-length similarity
        if self.dataitems > other.dataitems:
            aiulog.log(
                aiulog.DEBUG, "   SIMVAL(tablelen):",
                f"{0.0}  <- (events={self.dataitems} > table={other.dataitems})")
            return 0.0

        sim_val += self.sim_weights["tab_len"] * (self.dataitems / other.dataitems)
        aiulog.log(
            aiulog.DEBUG, "   SIMVAL(tablelen):",
            f"{sim_val}  <- ({self.dataitems} / {other.dataitems} = {self.dataitems / other.dataitems})")

        # exclude table canditate if
        if other.totaltime > self.totaltime:
            aiulog.log(
                aiulog.DEBUG, "   SIMVAL(totaltim):",
                f"0.0  <- ({other.totaltime} / {self.totaltime} = {other.totaltime / self.totaltime})")
            return 0.0

        sim_val += self.sim_weights["total_time"] * (other.totaltime / self.totaltime)
        aiulog.log(
            aiulog.DEBUG, "   SIMVAL(totaltim):",
            f"{sim_val}  <- ({other.totaltime} / {self.totaltime} = {other.totaltime / self.totaltime})")
        return sim_val


class RCUKernelCategoryMap():
    def __init__(self):
        self.kernel_cat_map: dict[str, str] = {"other": "other"}

    def __getitem__(self, key: str) -> str:
        return self.kernel_cat_map[key]

    def __contains__(self, key: str) -> bool:
        return key in self.kernel_cat_map

    def add(self, key: str, value: str) -> str:
        if key not in self.kernel_cat_map:
            self.kernel_cat_map[key] = value
        elif value != self.kernel_cat_map[key]:
            aiulog.log(
                aiulog.WARN,
                "UTL: Kernel->Category map already has an entry with different category:",
                key, value, self.kernel_cat_map[key])
        return self.kernel_cat_map[key]

    def values(self):
        return self.kernel_cat_map.values()


class RCUTableParseMode(Flag):
    ACTIVE_TABLE = auto()
    UNKNOWN = auto()
    PREFILL = auto()

    def get_phase(self):
        if self.UNKNOWN not in self:
            if self.PREFILL in self:
                return "TTFT"
            else:
                return "ITL"
        else:
            return "UNKN"

    def update(self, phase_str: str):
        if phase_str == "PREFILL":
            self |= RCUTableParseMode.PREFILL
            self &= ~RCUTableParseMode.UNKNOWN
        elif phase_str == "DECODING":
            self &= ~RCUTableParseMode.PREFILL
            self &= ~RCUTableParseMode.UNKNOWN
        else:
            self &= ~RCUTableParseMode.PREFILL
            self |= RCUTableParseMode.UNKNOWN
        return self


class RCUUtilizationContext(AbstractContext, PipelineContextTool):

    _start_pattern = re.compile(r' Ideal/Total Cycles ')
    _end_pattern = re.compile(r'====== Perf Summary End ======')
    _clock_scaling = re.compile(r'Ideal Clock Scaling:')
    _data_pattern = re.compile(r'^[_\-a-zA-Z\d]+ +\d+ *$')
    _ignore_pattern = re.compile(r'(Precompute|-LxPreload)')
    _category_splitter = re.compile(r'(\-opCat|\-NA$)')
    _autopilot_pattern = re.compile(r'DSM-AutoPilot BEGIN')
    _iteration_mode_pattern = re.compile(r'^\s+(DECODING|PREFILL)\s+$')
    _non_kernel_names = ["Total"]

    _print_to_log = False

    def __init__(
            self,
            compiler_log: str,
            csv_fname: str,
            soc_freq: float,
            core_freq: float,
            kernel_db_url: str = "ai_kernel.db") -> None:

        super().__init__(warnings=[
            TraceWarning(
                name="util_100",
                text="UTL: Encountered {d[count]} Events with >100% utilization",
                data={"count": 0}
            ),
            TraceWarning(
                name="kernel_other",
                text="UTL: Found {d[count]} Events without a matching kernel category and accounted for 'other'",
                data={"count": 0}
            )
        ])

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
            aiulog.log(aiulog.ERROR, "UTL: Unable to open/parse log file.", compiler_log, e)

        for _, t in self.kernel_cycles.items():
            self.autopilot_detail = AutopilotDetail(t)
            self.table_hash = self.autopilot_detail.table_hash()

    def __del__(self) -> None:
        if self.multi_table > 0:  # used as index, so 'n-1'
            aiulog.log(aiulog.WARN, f"UTL: {len(self.fingerprints)}/{self.multi_table+1} unique",
                       "tables with ideal cycles have been detected."
                       " Utilization results should be inspected carefully!!!!")
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
        self.kernel_cycles: dict[int, dict[str, int]] = {}  # tables indexed by fingerprint
        self.categories = {}
        self.kernel_cat_map: dict[int, RCUKernelCategoryMap] = {}
        self.fingerprints: dict[int, RCUTableFingerprint] = {}
        self.hash_to_pid: dict[int, int] = {}

    def _start_init_table(self, table_mode: str) -> tuple[dict, RCUTableFingerprint]:
        self.multi_table += 1
        current_table = {}
        fprint = RCUTableFingerprint(
            datalimit=_table_data_limit,
            event_filter=_fprint_event_filter,
            table_mode=table_mode)  # use the first N (filtered) kernels as a fingerprint
        self.kernel_cat_map[0] = RCUKernelCategoryMap()  # new kernel-cat-map at temporary fprint key
        return current_table, fprint

    def _finish_add_table(self, fprint: RCUTableFingerprint, table: dict[str, int]) -> None:
        fprint_key = fprint.get()
        if fprint_key in self.kernel_cycles:
            aiulog.log(
                aiulog.WARN,
                "UTL: Fingerprint of current table already exists for previous table. "
                "Duplicated tables? Keeping the previous table only")
        else:
            aiulog.log(
                aiulog.INFO,
                f"UTL: Adding ideal cycles table ({fprint.get_table_mode()}) with fingerprint: {fprint_key}")
            aiulog.log(aiulog.DEBUG, f"UTL:    TablefprintStr: {fprint.fprint_data}")
        self.fingerprints[fprint_key] = fprint
        self.kernel_cycles[fprint_key] = copy.deepcopy(table)
        # re-assign temp kernel-cat-map to actual fingerprint
        self.kernel_cat_map[fprint_key] = self.kernel_cat_map.pop(0)
        fprint = None

    def _handle_category(self, kernel_and_cat) -> str:
        if len(kernel_and_cat) > 1:
            if kernel_and_cat[1] == "-opCat":
                return kernel_and_cat[-1]
            else:
                return "NotAvailable"
        else:
            return "Total"

    def _process_table_line(self,
                            line: str,
                            fprint: RCUTableFingerprint,
                            parse_mode: RCUTableParseMode,
                            current_table: dict[str, int]) -> tuple[
                                bool,
                                bool,
                                dict[str, int],
                                RCUTableFingerprint]:

        # drop out if autopilot=1 is detected
        if self._autopilot_pattern.search(line):
            self.autopilot = True
            return False, parse_mode, current_table, fprint

        if self._clock_scaling.search(line):
            aiulog.log(aiulog.WARN,
                       "UTL: Found obsolete 'Ideal Cycle Scaling' setting in logfile."
                       " This setting is ignored. Use '--freq=<soc>:<core>'.")
            return True, parse_mode, current_table, fprint

        _iter_mode = self._iteration_mode_pattern.search(line)
        if _iter_mode is not None:
            iteration_phase = _iter_mode.group(1)
            parse_mode = parse_mode.update(iteration_phase)
            aiulog.log(aiulog.DEBUG, "DETECTED:", iteration_phase, "table to be next. Parsemode:", parse_mode)
            return True, parse_mode, current_table, fprint

        if self._start_pattern.search(line):
            parse_mode |= RCUTableParseMode.ACTIVE_TABLE
            current_table, fprint = self._start_init_table(parse_mode.get_phase())
            aiulog.log(aiulog.DEBUG,
                       "UTL: Start of Ideal Cycle Count section detected. Parse mode:",
                       parse_mode, self.multi_table)
            return True, parse_mode, current_table, fprint

        # don't bother checking for the end_pattern if we're not even in parse mode
        if RCUTableParseMode.ACTIVE_TABLE not in parse_mode:
            return True, parse_mode, current_table, fprint

        if self._end_pattern.search(line):
            aiulog.log(aiulog.DEBUG, "UTL: End of Ideal Cycle Count section detected. Stopping parse mode.")
            parse_mode &= ~RCUTableParseMode.ACTIVE_TABLE  # reset to scanning/no table
            self._finish_add_table(fprint, current_table)
            return True, parse_mode, current_table, fprint

        # This will need to be the last regex check because it skips everything else
        if not self._data_pattern.search(line) or self._ignore_pattern.search(line):
            return True, parse_mode, current_table, fprint

        ldata = re.split(" +", line)
        if len(ldata) < 2 or len(ldata) > 3:  # strange format includes newline as a 3rd column
            aiulog.log(
                aiulog.WARN,
                "UTL: found data pattern line with more than 2 columns. Check patterns.",
                ldata)
            return True, parse_mode, current_table, fprint

        cycles = int(ldata[1])
        kernel_and_cat = self._category_splitter.split(ldata[0])
        category = self._handle_category(kernel_and_cat)

        # Skip anything that's not a kernel name
        if kernel_and_cat[0] in self._non_kernel_names:
            return True, parse_mode, current_table, fprint

        kernel = kernel_and_cat[0]+" Cmpt Exec"
        fprint.add(kernel, cycles * self.cycle_to_clock_factor)

        if kernel not in current_table:
            aiulog.log(aiulog.TRACE, "UTL: Kernel:", kernel)
            if cycles != 0:
                current_table[kernel] = cycles
        elif cycles != current_table[kernel]:
            aiulog.log(aiulog.WARN,
                       "UTL: Kernel already has an entry with different cycle count:",
                       kernel, cycles, current_table[kernel])

        if kernel not in self.kernel_cat_map[0]:
            self.kernel_cat_map[0].add(kernel, category)
        elif category != self.kernel_cat_map[0][kernel]:
            aiulog.log(aiulog.WARN,
                       "UTL: Kernel->Category map already has an entry with different category:",
                       kernel, category, self.kernel_cat_map[0][kernel])
        return True, parse_mode, current_table, fprint

    def extract_tables(self, compiler_log: str):

        parse_mode = RCUTableParseMode(RCUTableParseMode.UNKNOWN)
        self.multi_table = -1  # track if there might be multiple tables in the log
        current_table = {}
        fprint = None  # fingerprints created when a new table is detected
        with open(compiler_log, 'r') as cl:
            for line in cl:
                (
                    keep_parsing,
                    parse_mode,
                    current_table,
                    fprint
                ) = self._process_table_line(line, fprint, parse_mode, current_table)
                if not keep_parsing:
                    break

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

        title_row = ["Pid", "Phase", "Category", "Kernel_Time", "Frac_Time", "Calls",
                     "Ideal_Time", "Ideal_Cyc", "Frac_Ideal", "PT_Util"]
        aiulog.log(aiulog.DEBUG, "UTL: category title_row: ", title_row)

        list_of_list = []
        for p, data in cat_tab.items():
            if len(data) == 0:
                return

            pid, fp_key = self.hash_to_pid[p]
            try:
                phase = self.fingerprints[fp_key].get_table_mode()
            except KeyError:
                aiulog.log(aiulog.WARN, "UTL: Unexpected entry in category tables", fp_key)
                phase = "UNKN"
            total = data["Total"][0]
            ideal_total = data["Total"][1]

            for k, (dur, ideal, calls) in data.items():
                ideal_cyc = int(ideal / abs(self.cycle_to_clock_factor))

                # prevent div-by-zero exception
                dur_frac = 0 if isclose(total, 0.0, abs_tol=1e-9) else round(dur / total, 4)
                ideal_frac = 0 if isclose(ideal_total, 0.0, abs_tol=1e-9) else round(ideal / ideal_total, 4)
                pt_util = 0 if isclose(dur, 0.0, abs_tol=1e-9) else round(ideal / dur, 4)

                # note: to sync the columns of value_row with title_row
                value_row = [pid, phase, k, dur, dur_frac, calls, round(ideal, 4), ideal_cyc, ideal_frac, pt_util]
                list_of_list.append(value_row)

                aiulog.log(aiulog.DEBUG, "UTL: category value_row: ", value_row)

        # the sorting places the "Total" row to the last of each section (section per pid) of the table.
        df = pd.DataFrame(list_of_list, columns=title_row)
        sorted_df = df.sort_values([title_row[0], title_row[1], title_row[3]],
                                   kind='stable', inplace=False, ignore_index=True)

        sorted_df.to_csv(self.csv_fname, index=False, header=True)                   # dump to CSV file
        print(sorted_df.to_string(index=False), file=open(self.tab_fname, 'w'))    # dump to TXT file

        aiulog.log(aiulog.INFO, "UTL: category table(s) created as CSV:", self.csv_fname)
        aiulog.log(aiulog.INFO, "UTL: category table(s) created as TXT:", self.tab_fname)

    # if there's no category table for the pid, create a new one from the known category keys
    def set_categories_for_pid(self, pid, fprint) -> None:
        cat_hash = hash(fprint+pid)
        if cat_hash in self.categories:
            return
        else:
            aiulog.log(aiulog.DEBUG, "UTL: Creating new categories table for", cat_hash)
            # always have the StcdpHbm category
            self.categories[cat_hash] = {"Total": (0.0, 0.0, 0), "StcdpHbm": (0.0, 0.0, 0)}
            self.hash_to_pid[cat_hash] = (pid, fprint)

        for cat in self.kernel_cat_map[fprint].values():
            self.categories[cat_hash][cat] = (0.0, 0.0, 0)

    def get_cycles(self, kernel: str, fprint: int) -> int:
        if len(self.kernel_cycles):
            rval = self.kernel_cycles[fprint].get(kernel, 0)
            return rval
        else:
            return 0

    def accumulate_categories(self, pid, kernel, ideal_dur, duration, fprint):
        if kernel not in self.kernel_cat_map[fprint]:
            self.issue_warning("kernel_other")
            kernel = "other"

        cat_hash = hash(fprint+pid)
        cat = self.kernel_cat_map[fprint][kernel]
        self.set_categories_for_pid(pid, fprint)
        aiulog.log(aiulog.TRACE, "UTL: ", kernel, cat, duration, ideal_dur, self.categories[cat_hash][cat])

        dur, i_dur, cnt = self.categories[cat_hash][cat]
        self.categories[cat_hash][cat] = (dur+duration, i_dur+ideal_dur, cnt+1)

        dur, i_dur, cnt = self.categories[cat_hash]["Total"]
        self.categories[cat_hash]["Total"] = (dur+duration, i_dur+ideal_dur, cnt+1)
        return cat


class MultiRCUUtilizationContext(TwoPhaseWithBarrierContext, PipelineContextTool):
    _name_converter = re.compile(r"\[N\]")

    """
    Create a warning about uncertain table matching
    if similarity of a job fingerprint and 2 tables differs by less than this tolerance
    """
    _similarity_tolerance = 0.2

    def __init__(
            self,
            compiler_log: str,
            csv_fname: str,
            soc_freq: float,
            core_freq: float) -> None:

        super().__init__(warnings=[
            # count the number of events with >100% utilization (indication of table mismatch)
            TraceWarning(
                name="util_100",
                text="UTL: Encountered {d[count]} Events with >100% utilization",
                data={"count": 0}
            ),
            # count the number of events where no corresponding table/fingerprint was found
            TraceWarning(
                name="kernel_nomatch",
                text="UTL: No matching Ideal Cycles table found for {d[count]} "
                     "Events. This might indicate a wrong frequency setting.",
                data={"count": 0}
            ),
            # count jobs with uncertain table match
            TraceWarning(
                name="uncertain_match",
                text="UTL: Detected uncertain Ideal Cycles table match for {d[count]} "
                     "jobs: {d[joblist]}",
                data={"count": 0, "joblist": set()},
                update_fn={"count": int.__add__, "joblist": set.union}
            )
        ])

        log_list = compiler_log.split(",")
        self.multi_log = (len(log_list) > 1)
        self.fingerprints: dict[int, RCUTableFingerprint] = {}   # fingerprints per job/file
        if self.multi_log:
            aiulog.log(aiulog.INFO, "UTL: Multi-AIU logs provided. Entries:", len(log_list))

        # event rank will be multiplied by this factor to make the key for the correct rcuctx
        # in single-log case: will turn everything into zero, otherwise use event rank
        self.rank_factor = 1 if self.multi_log else 0

        self.rcuctx: dict[int, RCUUtilizationContext] = {}
        for rank, log in enumerate(log_list):
            aiulog.log(aiulog.DEBUG, "UTL: Building kernel table for", rank)
            if self.multi_log:
                csv_basename = csv_fname + str(rank)
            else:
                csv_basename = csv_fname
            self.rcuctx[rank] = RCUUtilizationContext(
                log,
                csv_fname=csv_basename,
                soc_freq=soc_freq,
                core_freq=core_freq)

    def extract_kernel_from_event_name(self, event: TraceEvent) -> str:
        rname = event["name"]

        # if a fn_idx was removed from the event name, we have to bring it back in to match the ideal cycles table entry
        if "[N]" in rname and "args" in event and "fn_idx" in event["args"]:
            rname = self._name_converter.sub(str(event["args"]["fn_idx"]), event["name"], count=1)

        if not rname.endswith("Cmpt Exec"):
            rname += " Cmpt Exec"

        return rname

    def get_ideal_dur(self, kernel: str, pid: int, fingerprint: int) -> float:
        rank = pid * self.rank_factor
        return self.rcuctx[rank].get_cycles(kernel, fingerprint) * self.rcuctx[rank].cycle_to_clock_factor

    def accumulate_categories(self, pid, kernel, ideal_dur, duration, fprint):
        rank = pid * self.rank_factor
        return self.rcuctx[rank].accumulate_categories(pid, kernel, ideal_dur, duration, fprint)

    def fingerprint_add(self, job: int, kernel: str, time: float) -> None:
        if job not in self.fingerprints:
            self.fingerprints[job] = RCUTableFingerprint(_default_fprint_len, event_filter=_fprint_event_filter)

        self.fingerprints[job].add(kernel, time)

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

    def update_fprint_matches(self):
        for job, event_fprint in self.fingerprints.items():
            matching_fprints = []
            for table in self.rcuctx.values():
                for fprint in table.fingerprints.values():
                    matching_fprints.append((fprint, event_fprint.similarity(fprint)))

            if len(matching_fprints) == 0:
                continue

            matching_fprints.sort(key=(lambda x: x[1]), reverse=True)
            job_fprint, similar = matching_fprints[0]
            if similar < 0.8:
                # warn about low similarity value
                self.warnings["uncertain_match"].update({"count": 1, "joblist": [f"{job}:{similar:.2}"]})
            elif len(matching_fprints) > 1 and isclose(similar,
                                                       matching_fprints[1][1],
                                                       abs_tol=self._similarity_tolerance):
                # at least 2 tables with the same best similarity value (spit a warning)
                self.warnings["uncertain_match"].update({"count": 1, "joblist": [f"{job}:mm={len(matching_fprints)}"]})
            else:
                pass

            aiulog.log(aiulog.DEBUG, "UTL: (New) table-fprint for job", job, "to", job_fprint.get())
            self.fingerprints[job] = job_fprint

    def drain(self) -> list[TraceEvent]:
        # run fingerprint-similarity check for all jobs
        self.update_fprint_matches()
        return super().drain()

    def generate_fprint_jobhash(self, event: TraceEvent) -> int:
        jobhash = event["args"]["jobhash"]
        if "correlation" in event["args"]:
            jobhash += event["args"]["correlation"]
        return jobhash


def compute_utilization_fingerprints(event: TraceEvent, context: AbstractContext) -> list[TraceEvent]:
    if event["ph"] != "X":
        return [event]

    assert isinstance(context, MultiRCUUtilizationContext)

    if PipelineContextTool.is_acc_event(event) and PipelineContextTool.is_acc_kernel(event):
        kernel_name = context.extract_kernel_from_event_name(event)

        context.fingerprint_add(context.generate_fprint_jobhash(event), kernel_name, event["dur"])
    return [event]


def compute_utilization(event: TraceEvent, context: AbstractContext) -> list[TraceEvent]:
    if event["ph"] != "X":
        return [event]

    assert isinstance(context, MultiRCUUtilizationContext)

    if PipelineContextTool.is_acc_event(event) and PipelineContextTool.is_acc_kernel(event):
        pid = event["pid"]
        kernel_name = context.extract_kernel_from_event_name(event)

        try:
            jobhash = context.generate_fprint_jobhash(event)
            job_fingerprint = context.fingerprint_get(jobhash)
        except KeyError:
            aiulog.log(aiulog.WARN, f"UTL: No matching fingerprint for job {event['args']['jobname']}."
                       " Unable to find a matching Ideal-cycles table.")
            return [event]

        try:
            ideal_dur = float(context.get_ideal_dur(kernel_name, pid, job_fingerprint))
        except KeyError:
            aiulog.log(aiulog.DEBUG, f"UTL: No kernel table matching fingerprint {job_fingerprint}:"
                       f" {context.fingerprints.keys()}/{context.fingerprints[jobhash].fprint_data} ")
            context.issue_warning("kernel_nomatch")
            ideal_dur = 0.0

        # cmpt_dur = int(event["args"]["TS4"]) - int(event["args"]["TS3"])
        cmpt_dur = float(event["dur"])
        utilization = abs(ideal_dur/cmpt_dur) if not isclose(cmpt_dur, 0.0, abs_tol=1e-9) else 0.0

        if utilization > 0.0:
            event["args"]["core used"] = True

        if utilization > 1.0:   # warning about >100% utilization
            aiulog.log(aiulog.WARN, "UTL: Event with +100% utilization. "
                       "This could indicate a problem with table fingerprinting: "
                       "(pid, ideal, observed, event)", pid, ideal_dur, cmpt_dur, event)
            context.issue_warning("util_100")
            utilization = 1.0

        if "cat" in event:
            event["args"]["user_cat"] = context.accumulate_categories(
                pid,
                kernel_name,
                ideal_dur,
                cmpt_dur,
                job_fingerprint)
        else:
            event["cat"] = context.accumulate_categories(
                pid,
                kernel_name,
                ideal_dur,
                cmpt_dur,
                job_fingerprint)
        util_counter = context.make_utilization_event(event, utilization*100.0)
        if utilization > 0.0:
            event["args"]["pt_active"] = utilization
        return [event] + util_counter

    return [event]
