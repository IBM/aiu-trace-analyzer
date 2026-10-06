# Copyright 2024-2025 IBM Corporation

import json
import sys
import os
from collections import defaultdict
from typing import Optional

import pandas as pd

import aiu_trace_analyzer.logger as aiulog
import aiu_trace_analyzer.trace_view as tv
from aiu_trace_analyzer.types import TRACE_ISSUE_EVENT_NAME
from aiu_trace_analyzer.verification.report import (
    VERIFICATION_RESULT_NAME,
    VERIFICATION_TEST_RESULT_NAME,
)


class AbstractTraceExporter:
    '''
    Abstract exporter class

    Defines required functions:

    export()
    * input list of AbstractEventType for export to whatever format
    * may also just buffer the exported events

    flush()
    * flush any accumulated buffer (if any)
    * if export() is directly writing to target output, this can be a noop
    '''

    def __init__(self, target_uri, settings=None) -> None:
        self.target_uri = target_uri
        self.meta = {}
        self.meta["Application"] = "Acelyzer: Trace Post-Processing Tool"
        self.meta["CmdLine"] = " ".join(sys.argv)
        if settings is None:
            # basic setting of output needed for traceView
            self.meta["Settings"] = {"output": target_uri}
        else:
            self.meta["Settings"] = settings
        self.device_data = []
        self.save_to_file = settings["save_to_file"] if settings is not None and "save_to_file" in settings else True

    def add_device(self, id, data: dict):
        devdata = {"id": id}
        for k, v in data.items():
            devdata[k] = v
        self.device_data.append(devdata)
        assert isinstance(self.device_data, list)

    def export_meta(self, meta_data: dict) -> None:
        raise NotImplementedError("Class %s doesn't implement export()" % (self.__class__.__name__))

    # export (a list) of events to the configured target
    def export(self, _data: list[tv.AbstractEventType]):
        raise NotImplementedError("Class %s doesn't implement export()" % (self.__class__.__name__))

    def flush(self):
        raise NotImplementedError("Class %s doesn't implement flush()" % (self.__class__.__name__))

    def get_data(self):
        raise NotImplementedError("Class %s doesn't implement get_data()" % (self.__class__.__name__))


class JsonFileTraceExporter(AbstractTraceExporter):
    '''
    Export events as json trace events for vizualization in chrome tracing or perfetto
    Accumulates exported events into TraceView object which is then dumped as json on flush()
    '''
    def __init__(self, target_uri, timescale="ms", settings=None) -> None:
        super().__init__(target_uri, settings=settings)
        self.traceview = tv.TraceView(display_time_unit=timescale, other_data=self.meta)

    # take (a list) of events and append to the traceview
    def export(self, data: list[tv.AbstractEventType]):
        for event in data:
            # trace_issue meta-events go into otherData, not into the trace event stream
            if event.ph == "M" and event.name == TRACE_ISSUE_EVENT_NAME:
                severity = "errors" if event.args["is_error"] else "warnings"
                findings = self.traceview.other_data.setdefault(severity, [])
                findings.append({"finding": event.args["finding"],
                                 "text": event.args["text"]})
                continue
            self.traceview.append_trace_event(event.json())

    def export_meta(self, meta_data):
        self.traceview.add_metadata(meta_data)

    # append a raw event (dictionary as is) to the traceview
    def export_raw(self, data: dict):
        self.traceview.append_trace_event(data)

    # return traceview data as a json string
    def get_data(self) -> str:
        return self.traceview.dump(fp=None)

    # write the traceview to file
    def flush(self):
        assert isinstance(self.device_data, list)
        self.traceview.add_device_data(self.device_data)
        if self.save_to_file:
            with open(self.target_uri, 'w') as json_new_pids_file:
                self.traceview.dump(fp=json_new_pids_file)


class ProtobufTraceExporter(AbstractTraceExporter):
    '''
    TBD: Placeholder for potential future export as protobuf format for perfetto
    '''
    def __init__(self, target_uri, settings=None) -> None:
        super().__init__(target_uri, settings)

    def export(self, data: list[tv.AbstractEventType]):
        # not exporting anything yet
        super().export(data)

    def flush(self):
        # nothing to flush for protobuf exporter
        super().flush()


class TensorBoardFileTraceExporter(JsonFileTraceExporter):
    def __init__(self, target_uri, timescale="ms", settings=None) -> None:
        super().__init__(target_uri, timescale=timescale, settings=settings)
        self.timescale = "ms"
        self.default_extension = '.pt.trace.json'
        self.rank_cnt = 0
        self.traceview_by_rank = {}

    # Save events into different files based on ID
    def _parse_events_by_id(self) -> None:
        # for trace events and get rank cnt
        events_by_id = self._parse_by_rank_id('pid', self.traceview.trace_events)
        if len(events_by_id) > 1:
            self.rank_cnt = len(events_by_id) - 1  # Remove key=-1 which is for CollBandwidth
        else:
            self.rank_cnt = len(events_by_id)  # Single AIU case
        self._update_traceview_value_by_rank("trace_events", self.rank_cnt, events_by_id)

        # for display_time_unit
        self._update_traceview_value_by_rank("display_time_unit", self.rank_cnt, self.traceview.display_time_unit)

        # for other data
        self._update_traceview_value_by_rank("other_data", self.rank_cnt, self.traceview.other_data)

        # for device data
        device_data_by_id = self._parse_by_rank_id('id', self.traceview.device_data)
        self._update_traceview_value_by_rank("device_data", self.rank_cnt, device_data_by_id)

    # Parse items by id for each rank
    def _parse_by_rank_id(self, key, data) -> defaultdict[list]:
        events_by_id = defaultdict(list)

        for event in data:
            rank_id = event[key]
            if rank_id is not None and isinstance(rank_id, int):
                if rank_id >= 1000:
                    rank_id -= 1000

                events_by_id[rank_id].append(event)

        return events_by_id

    # Update traceview attr value based on given variable name
    def _update_traceview_value_by_rank(self, var_name, rank_cnt, value) -> None:
        for rid in range(0, rank_cnt):
            if rid not in self.traceview_by_rank:
                self.traceview_by_rank[rid] = tv.TraceView(display_time_unit=self.timescale, other_data=self.meta)

            traceview_by_rank = self.traceview_by_rank[rid]
            if hasattr(traceview_by_rank, var_name):
                if var_name == "display_time_unit" or var_name == "other_data":
                    setattr(self.traceview_by_rank[rid], var_name, value)
                else:
                    setattr(self.traceview_by_rank[rid], var_name, value[rid])
            else:
                aiulog.log(aiulog.WARN,
                           f"TB_EXPORTER:  no attribute '{var_name}'"
                           " for traceview when preparing distributed view for TB")

    def _save_overall_trace(self) -> None:
        # consider support for other file formats not end with .json
        file_name = self.target_uri
        if file_name.endswith('.json') and not file_name.endswith(self.default_extension):
            file_name = file_name.replace('.json', self.default_extension)

        # NO DUMP TO FILE FOR TB. Export serialized json via get_data instead
        if self.save_to_file:
            with open(file_name, 'w') as json_new_pids_file:
                self.traceview.dump(fp=json_new_pids_file)

    def get_tb_data(self, worker) -> str:
        return self.traceview_by_rank[worker].dump(fp=None)

    # Save events to indivudal file by pid
    def _save_events_by_id(self) -> None:
        # consider support for other file formats not end with .json
        file_name = self.target_uri
        if file_name.endswith(self.default_extension):
            fbase = file_name[:-len(self.default_extension)]
        else:
            fbase = os.path.splitext(file_name)[0]

        for rid in range(0, self.rank_cnt):
            output_file = f'{fbase}_worker_{rid}.pt.trace.json'
            with open(output_file, 'w') as f:
                self.traceview_by_rank[rid].dump(fp=f)

        self._save_overall_trace()

    # write the traceview to file
    def flush(self):
        assert isinstance(self.device_data, list)
        self.traceview.add_device_data(self.device_data)

        self._parse_events_by_id()

        if self.rank_cnt == 1:
            self._save_overall_trace()
            aiulog.log(aiulog.WARN, 'TB_EXPORTER: Only 1 AIU is used, no distributed view')
            return

        self._save_events_by_id()


class DataframeExporter(AbstractTraceExporter):
    def __init__(
            self, target_uri, timescale="ms", settings=None,
            data_map: dict = None):
        super().__init__(target_uri=target_uri, settings=settings)
        self.vertical_view = []
        self.df = None

        # mapping from event entry to dataframe column
        # only entries that appear are picked up
        if not data_map:
            self.data_map = {
                "args.rank": ("Rank", 0),
                "ts": ("Timestamp", 0.0),
                "dur": ("Duration", 0.0),
                "cat": ("Category", "other"),
                "name": ("Event Name", "NoName"),
                "args.class": ("Event CLass", "UNKNOWN"),
                "args.jobname": ("Job", "Unknown"),
                "args.bytes": ("Size", 0.0),
                "args.pt_active": ("PT_Active", 0.0),
                }
        else:
            self.data_map = data_map

    def export_meta(self, meta_data: dict) -> None:
        # no metadata for this exporter type
        return

    def _extract_value(self, key_path: str, event: dict) -> str:
        try:
            default = self.data_map[key_path][1]
        except KeyError:
            return "N/A"

        keys = key_path.split('.')
        value = event
        for k in keys:
            if isinstance(value, dict) and k in value:
                value = value[k]
            else:
                return default
        return value

    def _convert_trace_event(self, event_dict: tv.AbstractEventType) -> Optional[tuple]:
        if event_dict.ph != "X":
            return None

        rval = []
        for jpath in self.data_map.keys():
            rval.append(self._extract_value(jpath, event_dict.json()))

        return tuple(rval)

    # export (a list) of events to the configured target
    def export(self, data: list[tv.AbstractEventType]):
        for event in data:
            if not isinstance(event, tv.CompleteEvents):
                continue

            event_line = self._convert_trace_event(event)
            if event_line:
                self.vertical_view.append(event_line)

    def flush(self):
        title_row = [v[0] for v in self.data_map.values()]
        self.df = pd.DataFrame(self.vertical_view, columns=title_row)

        if self.save_to_file:
            with open(self.target_uri, 'w') as f:
                f.write(self.df.to_string(index=False))

    def get_data(self) -> pd.DataFrame:
        return self.df


class VerificationReportExporter(AbstractTraceExporter):
    """
    Exports a structured verification report instead of a full trace.

    Consumes only verification_data and verification_test_result M-events
    (produced by the verification pipeline stages) and writes a JSON or
    human-readable text report.  The `has_errors` property lets the caller
    propagate a non-zero exit code when errors were detected.
    """

    def __init__(self, target_uri, fmt="json", settings=None):
        super().__init__(target_uri, settings=settings)
        self._fmt = fmt
        self._findings = []
        self._test_results = []
        self._has_errors = False
        self._input_meta = {}

    @property
    def has_errors(self) -> bool:
        return self._has_errors

    def export_meta(self, meta_data):
        self._input_meta = meta_data if meta_data is not None else {}

    def export(self, data):
        for event in data:
            if event.ph != "M":
                continue
            if event.name == VERIFICATION_RESULT_NAME:
                self._findings.append(event.args)
                if event.args.get("is_error") and event.args.get("count", 0) > 0:
                    self._has_errors = True
            elif event.name == VERIFICATION_TEST_RESULT_NAME:
                self._test_results.append(event.args)

    def get_data(self) -> dict:
        errors = [f for f in self._findings if f.get("is_error") and f.get("count", 0) > 0]
        warnings = [f for f in self._findings if not f.get("is_error") and f.get("count", 0) > 0]
        passed = [f for f in self._findings if f.get("count", 0) == 0]
        return {
            "version": "1.0",
            "result": "FAIL" if self._has_errors else "PASS",
            "metadata": {**self.meta, **self._input_meta},
            "test_results": self._test_results,
            "errors": errors,
            "warnings": warnings,
            "passed": passed,
        }

    def _write_json(self, path):
        with open(path, 'w') as f:
            json.dump(self.get_data(), f, indent=2, default=str)

    def _write_text(self, path):
        data = self.get_data()
        meta = data.get("metadata", {})
        cmdline = meta.get("CmdLine", "")
        lines = [
            "Acelyzer Verification Report",
            "=" * 29,
            f"Input:   {cmdline}",
            "",
        ]

        def section(title, findings):
            lines.append(f"{title} ({len(findings)})")
            lines.append("-" * len(f"{title} ({len(findings)})"))
            for f in findings:
                lines.append(f"  {f['finding']}: {f.get('count', 0)} occurrence(s)")
                for inst in f.get("instances", []):
                    lines.append("    " + "  ".join(f"{k}={v}" for k, v in inst.items()))
            lines.append("")

        section("ERRORS",   data["errors"])
        section("WARNINGS", data["warnings"])
        section("PASSED",   data["passed"])

        lines.append(f"TESTS APPLIED ({len(data['test_results'])})")
        lines.append("-" * len(f"TESTS APPLIED ({len(data['test_results'])})"))
        for tr in data["test_results"]:
            lines.append(f"  {tr['result'].upper():<5} {tr['test']}")
        lines.append("")
        lines.append(f"Result: {data['result']}" +
                     (" (errors detected)" if data["result"] == "FAIL" else ""))

        with open(path, 'w') as f:
            f.write("\n".join(lines) + "\n")

    def flush(self):
        if self.save_to_file:
            if self._fmt == "text":
                self._write_text(self.target_uri)
            else:
                self._write_json(self.target_uri)
