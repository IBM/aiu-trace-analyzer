import os
import pathlib
import re
import json

import aiu_trace_analyzer.logger as aiulog
from aiu_trace_analyzer.types import TraceEvent
from aiu_trace_analyzer.pipeline.tools import PipelineContextTool
from aiu_trace_analyzer.pipeline import AbstractContext

_inductor_spyre_dir_name = "inductor-spyre"
_inductor_spyre_cache_dir_name = "inductor-spyre-cache"
_sdsc_bundle_mlir_file = "bundle.mlir"
_event_prefix = "spyre_kernel_v1_"
_trailing_seq_pattern = re.compile(r"#\d+$")
_provenance_key_pattern = re.compile(r"_[a-z0-9]{16}$")
_legacy_kernel_event_pattern = re.compile(
    r"^(?:[0-9a-f]+_)?sdsc_(?P<op>.+)_\d+_[a-z0-9]+(?:_[a-z0-9]+)?$"
)


class SDSCBundles:
    inductor_base_dir = None
    cache_inductor_base_dir = None
    sdsc_bundle_dirs = []
    sdsc_bundle_dir_names = []
    cache_sdsc_bundle_dirs = []
    cache_sdsc_bundle_dir_names = []
    sdsc_bundle_event_match = {}

    def get_inductor_base_dir():
        return SDSCBundles.inductor_base_dir

    def get_cache_inductor_base_dir():
        return SDSCBundles.cache_inductor_base_dir

    def get_sdsc_bundle_dirs():
        return SDSCBundles.sdsc_bundle_dirs

    def get_sdsc_bundle_dir_names():
        return SDSCBundles.sdsc_bundle_dir_names

    def get_cache_sdsc_bundle_dirs():
        return SDSCBundles.cache_sdsc_bundle_dirs

    def get_cache_sdsc_bundle_dir_names():
        return SDSCBundles.cache_sdsc_bundle_dir_names

    def get_sdsc_bundle_event_match_dir(event_name: str):
        return SDSCBundles.sdsc_bundle_event_match.get(event_name, "")


class SDSCBundleMatchContext(AbstractContext, PipelineContextTool):
    def __init__(self, compiler_info: str = None) -> None:
        SDSCBundles.inductor_base_dir = (
            pathlib.Path(compiler_info) / _inductor_spyre_dir_name
        )
        SDSCBundles.cache_inductor_base_dir = (
            pathlib.Path(compiler_info) / _inductor_spyre_cache_dir_name
        )
        SDSCBundles.sdsc_bundle_dirs = (
            [d for d in SDSCBundles.inductor_base_dir.iterdir() if d.is_dir()]
            if SDSCBundles.inductor_base_dir.is_dir()
            else []
        )
        SDSCBundles.sdsc_bundle_dir_names = (
            [d.name for d in SDSCBundles.sdsc_bundle_dirs]
            if SDSCBundles.sdsc_bundle_dirs is not None
            else []
        )
        SDSCBundles.cache_sdsc_bundle_dirs = (
            [d for d in SDSCBundles.cache_inductor_base_dir.iterdir() if d.is_dir()]
            if SDSCBundles.cache_inductor_base_dir.is_dir()
            else []
        )
        SDSCBundles.cache_sdsc_bundle_dir_names = (
            [d.name for d in SDSCBundles.cache_sdsc_bundle_dirs]
            if SDSCBundles.cache_sdsc_bundle_dirs is not None
            else []
        )

    def _truncate_event_name(self, event_name: str):
        if not event_name:
            return None
        trunc_event_name = event_name
        if event_name.startswith(_event_prefix):
            trunc_event_name = event_name[len(_event_prefix) :]
            trunc_event_name = _trailing_seq_pattern.sub("", trunc_event_name)
            trunc_event_name = _provenance_key_pattern.sub("", trunc_event_name)
        elif (
            legacy_kernel_event_match := _legacy_kernel_event_pattern.match(event_name)
        ) is not None:
            trunc_event_name = legacy_kernel_event_match.group("op")
        return trunc_event_name

    def _bundle_match_by_sdsc_bundle_dir_prefix(
        self, event: TraceEvent, event_sdsc_bundle_dir_prefix: str
    ):
        sdsc_bundle_dir_names = (
            SDSCBundles.get_cache_sdsc_bundle_dir_names()
            if len(event_sdsc_bundle_dir_prefix) == 16
            else SDSCBundles.get_sdsc_bundle_dir_names()
        )
        match = [
            sb_name
            for sb_name in sdsc_bundle_dir_names
            if event_sdsc_bundle_dir_prefix in sb_name
        ]

        if match:
            event["args"]["orig_name"] = event["name"]
            trunc_event_name = (
                self._truncate_event_name(event["name"])
                if "provenance_key" in (event.get("args") or {})
                else self._truncate_event_name(event["name"].split("/")[0])
            )
            # Remove digits from the truncated event name to avoid orig_name from getting updated again
            event["name"] = (
                re.sub(r"\d+", "", trunc_event_name)
                if trunc_event_name is not None
                else event["name"]
            )
            return match[0], event
        else:
            return None, event

    def _bundle_match_by_legacy_path(self, event: TraceEvent):
        if _inductor_spyre_dir_name + "/" in event["name"]:
            sdsc_bundle_name = (
                event["name"].split(_inductor_spyre_dir_name + "/")[1].split("/")[0]
            )
        elif _sdsc_bundle_mlir_file in event["name"]:
            sdsc_bundle_name = event["name"].split("/")[0]
        match = [
            sb_name
            for sb_name in SDSCBundles.get_sdsc_bundle_dir_names()
            if sb_name.startswith(sdsc_bundle_name)
        ]
        if match:
            event["args"]["orig_name"] = event["name"]
            trunc_event_name = self._truncate_event_name(sdsc_bundle_name)
            # Remove digits from the truncated event name to avoid orig_name from getting updated again
            event["name"] = (
                re.sub(r"\d+", "", trunc_event_name)
                if trunc_event_name is not None
                else event["name"]
            )
            return match[0], event
        else:
            return None, event

    def check_bundle_match(self, event: TraceEvent) -> list[TraceEvent]:
        if self.is_acc_kernel(event):
            bundle_match = None
            event_sdsc_bundle_dir_prefix = event.get("args", {}).get(
                "sdsc_bundle_dir_prefix"
            )
            # Bundle Match by sdsc_bundle_dir_prefix
            if event_sdsc_bundle_dir_prefix is not None:
                bundle_match, event = self._bundle_match_by_sdsc_bundle_dir_prefix(
                    event, event_sdsc_bundle_dir_prefix
                )
            # Bundle Match by Legacy Path
            else:
                bundle_match, event = self._bundle_match_by_legacy_path(event)
            SDSCBundles.sdsc_bundle_event_match[event["args"]["orig_name"]] = (
                bundle_match
            )
        return [event]

    def read_ideal_cycles(sdsc_bundle_dir: str, with_cache: bool):
        sdsc_bundle_dir_path = (
            SDSCBundles.get_cache_inductor_base_dir() / sdsc_bundle_dir
            if with_cache
            else SDSCBundles.get_inductor_base_dir() / sdsc_bundle_dir
        )
        json_path = sdsc_bundle_dir_path / "perf" / "ideal_cycles.json"
        if not json_path.exists():
            return 0.00, "NA"
        with open(json_path) as f:
            entries = json.load(f)
            for entry in entries:
                if entry["sdsc_name"] == "TOTAL":
                    return entry["ideal_cycles"], entry["op_category"]


def sdsc_bundle_match(event: TraceEvent, context: AbstractContext) -> list[TraceEvent]:
    """
    Get the complete SDSC bundle name for the kernel event
    """
    assert isinstance(context, SDSCBundleMatchContext)
    return context.check_bundle_match(event)
