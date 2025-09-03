# Copyright 2024-2025 IBM Corporation

import json
import re
import pathlib  # for multifile patterns
import math

import aiu_trace_analyzer.logger as aiulog
from aiu_trace_analyzer.types import TraceEvent, GlobalIngestData, InputDialect, InputDialectFLEX, InputDialectTORCH


class AbstractTraceIngest:
    FTYPE_LOG = 0
    FTYPE_JSON = 1
    FTYPE_PFTRACE = 2
    FTYPE_API = 3

    WARN_MSG_MAP = {
        "zero_duration":    "detected 'CompleteEvent' type (ph=X) of zero duration. "
                            "This should be an 'InstantEvent' type (ph=i). Events skipped:",
        "negative_duration": "Ingestion: detected negative duration event(s). Events ignored:"
    }

    '''
    Abstract ingestion class
    Ingestion is implemented as an iterator.
    Each emitted event must be a python dictionary object
    It's required to emit one event at a time until the source is exhausted.
    '''
    def __init__(
            self,
            source_uri,
            jobdata: GlobalIngestData = GlobalIngestData(),
            data_dialect: InputDialect = InputDialectTORCH(),
            scale: float = 1.0,
            show_warnings: bool = True) -> None:
        self.source_uri = source_uri
        self.scale = scale
        self.ts_offset = None
        self.rank_pid = -1
        self.show_warnings = show_warnings
        self.warnings = {}
        self._initialized = False
        self.jobhash = jobdata.add_job_info(source_uri, data_dialect)
        assert scale > 0.0, 'Scale parameter needs to be >0.0'

    def __del__(self):
        if hasattr(self, "show_warnings") and self.show_warnings:
            for warnclass, warning in self.warnings.items():
                aiulog.log(aiulog.WARN, self.source_uri, warnclass, self.WARN_MSG_MAP[warnclass], warning)

    def set_ts_offset(self, offset):
        self.ts_offset = offset

    def is_torch_profile(self, data: dict) -> bool:
        return ("deviceProperties" in data)

    def __iter__(self):
        raise NotImplementedError("Class %s doesn't implement __iter__" % (self.__class__.__name__))

    def __next__(self) -> TraceEvent:
        raise NotImplementedError("Class %s doesn't implement __next__" % (self.__class__.__name__))

    def detect_ftype(self, fname: str) -> bool:
        # TODO: implement a more thorough way to distinguish log and json, for now just check for '.json'
        if ".json" in fname:
            return self.FTYPE_JSON
        elif ".pftrace" in fname:
            return self.FTYPE_PFTRACE
        elif ".log" in fname:
            return self.FTYPE_LOG
        elif fname.startswith("api://"):
            return self.FTYPE_API

        with open(fname, 'r') as f:
            json_re = re.compile(r"[{}\[\]]")
            log_re = re.compile(r"[0-2]\d:0")
            for line in f:
                # skip empty lines for just a little more robustness
                if len(line.strip()) == 0:
                    continue
                if json_re.match(line):
                    return self.FTYPE_JSON
                elif log_re.match(line):
                    return self.FTYPE_LOG
                else:
                    return self.FTYPE_PFTRACE

    def ftype_to_str(self, ftype):
        prl = ["FTYPE_LOG", "FTYPE_JSON", "FTYPE_PFTRACE", "FTYPE_API"]
        return prl[ftype]

    # update event data with things like ts-offset or ts-scaling
    def updated_event(self, event: TraceEvent) -> TraceEvent:
        if "ts" in event:
            event["ts"] *= self.scale
        if "dur" in event:
            event["dur"] = float(event["dur"] * self.scale)
        if self.rank_pid >= 0:
            event["pid"] = self.rank_pid

        # make sure the pid/tid entries are numbers
        try:
            event["pid"] = int(event["pid"])
        except ValueError:
            event["pid"] = hash(event["pid"])
        except KeyError:
            aiulog.log(aiulog.WARN, "Imported event has no PID!!! Setting -1", event)
            raise

        if "tid" in event:
            try:
                event["tid"] = int(event["tid"])
            except ValueError:
                event["tid"] = hash(event["tid"])

        if event["ph"] not in ["F", "f", "s", "t", "C"]:
            if "args" in event:
                event["args"]["jobhash"] = self.jobhash
            elif "attr" in event:
                event["attr"]["jobhash"] = self.jobhash
            else:
                event["args"] = {"jobhash": self.jobhash}
        return event


class JsonEventTraceIngest(AbstractTraceIngest):
    '''
    JSON trace ingestion
    Reading events from a json data and emitting the containing events one by one.
    The input can either just contain a list of json events
    or fully formatted trace data where only the 'traceEvents' section is iterated.
    '''
    # time-stamp gaps may come out of order, give the sequence checking some slack...
    ts_tolerance = 1000000.0

    def __init__(
            self,
            source_uri,
            jobdata: GlobalIngestData,
            data_dialect: InputDialect,
            scale: float = 1.0,
            keep_processed: bool = False,
            show_warnings: bool = True) -> None:
        super().__init__(source_uri, jobdata=jobdata, data_dialect=data_dialect, scale=scale, show_warnings=show_warnings)

        self.data = {}
        self.last_ts = 0.0
        self.pending_close = False
        self.keep_processed = keep_processed

    def _initialize_data(self, data_stream) -> None:
        self.data = data_stream
        # removed: display-unit is only for the visualization not the input data
        # if "displayTimeUnit" in self.data:
        #     if self.data["displayTimeUnit"] == "ms":
        #         self.scale = 1000
        #     elif self.data["displayTimeUnit"] == "ns":
        #         self.scale = 1
        #     else:
        #         raise NotImplementedError(f'Time Scale detection for {self.data["displayTimeUnit"]} not implemented.')
        if "distributedInfo" in self.data and "rank" in self.data["distributedInfo"]:
            self.rank_pid = self.data["distributedInfo"]["rank"]
            aiulog.log(aiulog.DEBUG, "INGEST: Detected distributedInfo Rank", self.rank_pid)

        if "otherData" in self.data and \
            "Application" in self.data["otherData"] and \
                "Acelyzer" in self.data["otherData"]["Application"]:
            if self.keep_processed is False:
                aiulog.log(
                    aiulog.WARN,
                    "INGEST: Attempting to import a json file that had been processed by acelyzer already." +
                    "Dropping ALL Events from file",
                    self.source_uri)
                self.data["traceEvents"] = []
            else:
                self.data = self.data["traceEvents"]
        if "traceEvents" in self.data:
            self.data = self.data["traceEvents"]

        self._len = len(self.data)
        self._index = 0
        self._initialized = True

    def __iter__(self):
        assert self._initialized is True, "ERROR: Data not initialized."
        return self

    def get_next_event(self) -> TraceEvent:
        if self._index < self._len:
            item = self.data[self._index]
            self._index += 1
            if "ts" in item:
                self.last_ts = item["ts"]
            return self.updated_event(item)
        else:
            return None

    def build_complete_event(self) -> TraceEvent:
        def _torch_prof_or_none(name, evtype) -> TraceEvent:
            if evtype == "M":
                return None
            elif "PyTorch Profiler" not in name:
                return event
            else:
                return None

        event = self.get_next_event()
        if not event:
            raise StopIteration

        # ignore anything that's not B/E
        if event["ph"] not in "BE":
            return _torch_prof_or_none(event["name"], event["ph"])

        open_event = None
        if not self.pending_close and event["ph"] == "B":
            self.pending_close = True
            open_event = event

            event = self.get_next_event()
            if not event:
                raise StopIteration

        assert open_event, f'Expected to find B-event in {self.source_uri}. Found {event["ph"]}'
        assert open_event["name"] == event["name"], \
            f'Subsequent B/E events with different names: {open_event["name"]} vs. {event["name"]}'
        if self.pending_close and event["ph"] == "E":
            open_event["ph"] = "X"
            open_event["dur"] = event["ts"] - open_event["ts"]
            self.pending_close = False

            if open_event["dur"] < 0.0:
                self.count_warning('negative_duration')
                return None

            if math.isclose(open_event["dur"], 0.0, abs_tol=1e-9):
                self.count_warning('zero_duration')
                return None

            # TODO commented out until time stamp issue is clarified
            # assert  self.last_ts-self.ts_tolerance <= open_event["ts"], f"TimeStamp Sequence problem in {self.sourceURI}"
            return open_event
        else:
            assert False, f'Expected to find E-event in {self.source_uri}. Found {event["ph"]}'

    def count_warning(self, warn_class: str) -> None:
        if warn_class not in self.warnings:
            self.warnings[warn_class] = 0
        self.warnings[warn_class] += 1

    def __next__(self):
        event = self.build_complete_event()
        while not event:
            event = self.build_complete_event()
        return event


class MemoryJsonTraceIngest(JsonEventTraceIngest):
    _mname_filter = re.compile(r'api://([a-zA-Z]\w+)', re.ASCII)
    '''
    JSON trace ingestion from data stream (memory)
    '''
    def __init__(
            self,
            source_uri,
            jobdata: GlobalIngestData = GlobalIngestData(),
            scale: float = 1.0,
            show_warnings: bool = True,
            direct_data: memoryview = None):
        keep_processed = True
        data = json.loads(direct_data.tobytes())

        data_dialect = InputDialectTORCH() if self.is_torch_profile(data) else InputDialectFLEX()
        super().__init__(source_uri, jobdata, data_dialect, scale, keep_processed, show_warnings)

        self._initialize_data(data)


class JsonFileEventTraceIngest(JsonEventTraceIngest):
    '''
    JSON trace ingestion from file
    '''
    def __init__(
            self,
            source_uri,
            jobdata: GlobalIngestData = GlobalIngestData(),
            scale: float = 1.0,
            keep_processed: bool = False,
            show_warnings: bool = True):

        with open(source_uri, 'r') as sourcefile:
            data = json.load(sourcefile)

        data_dialect = InputDialectTORCH() if self.is_torch_profile(data) else InputDialectFLEX()
        super().__init__(source_uri, jobdata, data_dialect, scale, keep_processed, show_warnings)

        self._initialize_data(data)


# optional perfetto trace import
try:
    from perfetto.trace_processor import TraceProcessor

    class ProtobufIngest(AbstractTraceIngest):
        def __init__(self, sourceURI, show_warnings: bool = True) -> None:
            super().__init__(sourceURI, show_warnings=show_warnings)

            self._index = 0
            self.tp = TraceProcessor(trace=sourceURI)
            # self.data = self.tp.query('SELECT * FROM slice')
            #
            slice_fields = "ts, dur, cat, slice.name as slice_name, slice.id as slice_id, slice.arg_set_id as aid,"
            pt_fields = "utid, thread.name as thread_name, thread.tid as tid, " \
                "process.upid as upid, process.pid as pid, process.name as process_name"
            self.fields = slice_fields+pt_fields

            '''
            data organized in tables, main table for complete events is 'slice'.
            slices have references to process and thread info via their track_id.
            slices have references to args via their arg_set_id.
            process info needs to be extracted via a thread_track.parent_id
             -> because the process is the parent of a thread track.
            the structuring of the args is such that each single arg (k/v) is a single line in 'args' table.
            Args that belong to the same set of args have the same 'arg_set_id'.
            '''
            self.data = self.tp.query("DROP VIEW IF EXISTS slice_with_utid; "
                                      "CREATE VIEW slice_with_utid AS "
                                      f"SELECT {self.fields} FROM slice "
                                      "JOIN thread_track ON thread_track.id = slice.track_id "
                                      "JOIN thread USING (utid) "
                                      "JOIN process_track ON process_track.id = thread_track.parent_id "
                                      "JOIN process USING (upid);"
                                      "SELECT * FROM slice_with_utid;")

            '''
            assemble the arg key-value sets:
            slices reference an arg_set_id which can contain multiple args entries.
            This step collects all args from the same set into a python dictionary
            so that it can be attached to an event by using its arg_set_id as the key to find the set
            '''
            self.argsets = {}
            for arg in self.tp.query("SELECT * FROM args"):

                # determine value type and extract corresponding value
                if "int" in arg.value_type:
                    val = arg.int_value
                elif "str" in arg.value_type:
                    val = arg.string_value
                else:
                    val = arg.real_value

                if arg.arg_set_id not in self.argsets:
                    self.argsets[arg.arg_set_id] = {arg.key: val}
                else:
                    self.argsets[arg.arg_set_id][arg.key] = val

        def __iter__(self):
            return self

        def __next__(self) -> TraceEvent:
            item = self.data.__next__()
            aiulog.log(aiulog.TRACE, item)
            event = {
                "ph": "X",
                "ts": item.ts,
                "name": item.slice_name,
                "dur": item.dur,
                "cat": item.cat,
                "tid": f'{item.thread_name} {str(item.tid)}',
                "pid": f'{item.process_name} {str(item.pid)}',
                "id": item.slice_id,
                "args": self.argsets[item.aid] if item.aid in self.argsets else {}
            }
            return self.updated_event(event)

except Exception:
    class ProtobufIngest(AbstractTraceIngest):
        pass


class MultifileIngest(AbstractTraceIngest):

    '''
    Multifile ingestion
    Takes a list of filenames to allow merging of events from multiple files
    Distinguishes between currently implemented json and log file formats based on filename.
    Uses format-specific ingestion types (see above) to iterate individual files
    Performs a round-robin over all files
    and skips any iterators that are exhausted.
    '''
    def __init__(self, source_uri, show_warnings: bool = True, direct_data: memoryview = None) -> None:
        super().__init__("top_level_multifile", show_warnings=show_warnings)

        self.split_pattern = re.compile(r"[,\s]")
        filelist = self.generate_filelist(source_uri)
        self.ingesters = []
        self.event_front = []
        self.direct_data = direct_data

        aiulog.log(aiulog.INFO, "Ingesting", len(filelist), "files.")
        for ingest in filelist:
            self.add_ingester(ingest)

        if len(self.ingesters) == 0:
            aiulog.log(aiulog.ERROR, "No input files found.")
            raise FileNotFoundError()

    def add_ingester(self, ingest: str) -> bool:
        _added_new = True
        self.ftype = self.detect_ftype(ingest)
        if self.ftype == self.FTYPE_JSON:
            self.ingesters.append(JsonFileEventTraceIngest(ingest, show_warnings=self.show_warnings))
        elif self.ftype == self.FTYPE_PFTRACE:
            self.ingesters.append(ProtobufIngest(ingest))
        elif self.ftype == self.FTYPE_API:
            self.ingesters.append(MemoryJsonTraceIngest(ingest, show_warnings=self.show_warnings, direct_data=self.direct_data))
        else:
            aiulog.log(aiulog.ERROR, "Unrecognized file type. file:", ingest)
            _added_new = False

        aiulog.log(aiulog.DEBUG, "FileType:", self.ftype_to_str(self.ftype), " detected for:", ingest)
        self.ingest_map = [1] * len(self.ingesters)
        return _added_new

    def __iter__(self):
        # prefill an eventfront with the first event from each ingester
        for idx, _ in enumerate(self.ingesters):
            try:
                refill = self.ingesters[idx].__next__()
                self.update_event_front(refill, idx)
            except StopIteration:
                self.disable_ingest(idx)
        return self

    def __next__(self) -> TraceEvent:
        while True:
            # get the earliest event or end iterator if event_front is empty
            try:
                event, idx = self.event_front.pop()
            except IndexError:
                raise StopIteration

            # otherwise: refill the event_front from the ingester[idx]
            if self.ingest_map[idx] > 0:
                try:
                    refill = self.ingesters[idx].__next__()
                    self.update_event_front(refill, idx)
                    break
                except StopIteration:
                    self.disable_ingest(idx)
                    break
        # no extra 'updateEvent' here. That's already done in the specific ingesters
        return event

    def update_event_front(self, event, idx):
        # considered using bisect.insort() but that has no key arg before python 3.10
        # also: both are O(NlogN) but bisect.insort is dominated by array insertion O(N)
        # also: bisect has no reverse order, so pop the earliest TS from list is another O(N) op
        tsidx = (event, idx)   # keep idx with event so we immediately know which iterator/file to use to refill
        self.event_front.append(tsidx)
        # sorting reverse so that list.pop() can be used to emit the event with lowest TS
        self.event_front.sort(reverse=True, key=lambda x: x[0]["ts"])
        aiulog.log(aiulog.TRACE, "INGEST:", [e[0]["ts"] for e in self.event_front])

    # return False if no more active ingests available
    def disable_ingest(self, index) -> bool:
        aiulog.log(aiulog.DEBUG, "IngestIterator", index, "exhausted.")
        self.ingest_map[index] = 0
        return (sum(self.ingest_map) != 0)

    def generate_filelist(self, filestring) -> list[str]:
        fpat_list = self.split_pattern.split(filestring)
        flist = []
        for expanded in fpat_list:
            if expanded.startswith("api://"):
                flist.append(expanded)
                continue
            subdir, fpat = '/'.join(expanded.split('/')[:-1]), expanded.split('/')[-1]
            aiulog.log(aiulog.DEBUG, "Opening path:", pathlib.Path(subdir), "Pattern:", fpat)
            flist += [f'{x}' for x in list(pathlib.Path(subdir).rglob(fpat))]
        aiulog.log(aiulog.INFO, "Reading files:", fpat_list)
        return flist
