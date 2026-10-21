# Copyright 2024-2025 IBM Corporation

from __future__ import annotations

from pathlib import Path
import re
from typing import Optional, TYPE_CHECKING

import aiu_trace_analyzer.logger as aiulog

if TYPE_CHECKING:
    from aiu_trace_analyzer.dialect import InputDialect

# the input dialects moved to aiu_trace_analyzer.dialect; resolved lazily to keep the old import path working
# (a regular import would be circular: dialect.py imports TraceEvent and GlobalIngestData from here)
_MOVED_TO_DIALECT = ("InputDialect", "InputDialectFLEX", "InputDialectTORCH")


def __getattr__(name: str):
    if name in _MOVED_TO_DIALECT:
        import aiu_trace_analyzer.dialect as dialect
        return getattr(dialect, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


# define TraceEvent to be a dictionary for consistency
class TraceEvent(dict):
    pass


class GlobalIngestData(object):
    _jobmap = None

    def __new__(cls):
        if not hasattr(cls, '_instance'):
            cls._instance = super(GlobalIngestData, cls).__new__(cls)
            cls._jobmap = {}
        return cls._instance

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
            print(f"no jobmap entry for {jobhash}.")
            return "Not Available"

    @classmethod
    def get_dialect(cls, jobhash: int) -> InputDialect:
        try:
            return cls._jobmap[jobhash][1]
        except KeyError:
            print(f"no jobmap entry for {jobhash}.")
            raise

    @classmethod
    def find_dialect(cls, jobhash: int) -> Optional[InputDialect]:
        '''
        like get_dialect(), but returns None for unknown jobs instead of raising
        '''
        if not cls._jobmap or jobhash not in cls._jobmap:
            return None
        return cls._jobmap[jobhash][1]


class TraceWarning:
    """
    Keep track of warnings to allow accumulated warning at the end of a run
    Example:

        w = TraceWarning(
            name="MyWarning",
            text="This stage has detected {d[count]} issues with max {d[max]}.",
            data={"count": 0, "max": 0.0},
            update_fn={"count": int.__add__, "max": max},
            autolog=True,
            is_error=True,
        )

        name:      a key that can be used to manage multiple warnings in e.g. a dictionary
        text:      the warning text with variables (always us 'd' as the dictionary name)
        data:      dictionary with entries that match the text variables
        update_fn: functions to run when the update function is called with data
        autolog:   automatically print the warning at destruction time (default)
        is_error:  print the summary warning as ERROR level and not as WARN (default)

        Whenever a warning should be added:

            w.update({"count": 1, "max": 100.0})

        this calls the preset update_fn for each item to update the values
        if update_fn is e.g. int.__add__, then the new_val entry for count will be increased by 1

        If the warning summary should be issued:
        print(w)
        -> this uses the __str__() method to assemble the text and should print
        "This stage has detected 1 issues with max 100.0"
    """

    def __init__(
            self,
            name: str,
            text: str,
            data: dict[str, any],
            update_fn: dict[str, callable] = {},
            auto_log: bool = True,
            is_error: bool = False):
        self.occurred = False
        self.name = name
        # format-string with {d[key]} placeholders
        self.text: str = text
        self.args_list: dict[str, any] = dict(data.items())
        self.update_fn: dict[str, callable] = dict(update_fn.items())
        self.auto_log = auto_log
        self.warn_level = aiulog.WARN if not is_error else aiulog.ERROR
        self._instances: list = []

        text_keys = re.findall(r"{d\[([.\w]+)\]}", self.text)
        if len(text_keys) != len(self.args_list):
            raise ValueError(
                "Number of args needs to match placeholders in format string."
                " Make sure to use format {d[<key>]}")

        # check keys of text, args, and update_fn overlap
        self._check_update_fn_keys(text_keys)
        self._check_text_keys(text_keys)
        self._check_data_keys(text_keys)

    def __del__(self) -> None:
        if self.auto_log is True and self.has_warning():
            aiulog.log(self.warn_level, self)

    def _check_update_fn_keys(self, text_keys: list) -> None:
        # check if the keys for the update functions exist within the output text and the data items
        for k in self.update_fn.keys():
            if k not in text_keys:
                raise KeyError(f"Update_fn key {k} not found in text pattern {text_keys}")
            if k not in self.args_list:
                raise KeyError(f"Update_fn key {k} not found in args {self.args_list}")

    def _check_text_keys(self, text_keys: list) -> None:
        # check if the keys in the output text exist in both update functions and data items
        for k in text_keys:
            if k not in self.args_list:
                raise KeyError(f"Text key {k} not found in args {self.args_list}.")
            if k not in self.update_fn:
                aiulog.log(aiulog.DEBUG, f"Text key {k} not in update functions {self.update_fn}. Using default.")
                self.update_fn[k] = int.__add__

    def _check_data_keys(self, text_keys: list) -> None:
        # check if the data item keys exist in both output text and update functions
        for k in self.args_list.keys():
            if k not in text_keys:
                raise KeyError(f"Args key {k} not in text pattern {text_keys}.")
            if k not in self.update_fn:
                aiulog.log(aiulog.DEBUG, f"Args key {k} not in update functions {self.update_fn}. Using default.")
                self.update_fn[k] = int.__add__

    def get_name(self) -> str:
        return self.name

    def update(self,
               data: dict[str, any] = {"count": 1}) -> int:
        items_changed = 0
        for k, v in data.items():
            if k not in self.args_list:
                raise KeyError(f"Requested args key {k} does not exist in exsting args: {self.args_list.keys()}")
            self.args_list[k] = self.update_fn[k](self.args_list[k], v)
            items_changed += 1

        self.occurred |= (items_changed > 0)
        return items_changed

    def has_warning(self) -> bool:
        return self.occurred

    def add_instance(self, data: dict) -> None:
        self._instances.append(data)

    def to_verification_event_args(self) -> dict:
        return {
            "finding": self.name,
            "is_error": self.warn_level == aiulog.ERROR,
            "count": self.args_list.get("count", len(self._instances)),
            "instances": list(self._instances),
        }

    def __str__(self) -> str:
        return self.text.format(d=self.args_list)
