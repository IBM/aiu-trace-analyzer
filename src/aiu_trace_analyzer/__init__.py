# Copyright 2024-2025 IBM Corporation

try:
    from importlib.metadata import version, PackageNotFoundError
    __version__ = version("aiu_trace_analyzer")
except PackageNotFoundError:
    __version__ = "unknown"

from aiu_trace_analyzer.trace_view import AbstractEventType
from aiu_trace_analyzer import *
from aiu_trace_analyzer.ingest import *
from aiu_trace_analyzer.export import *
from aiu_trace_analyzer.core import *
from aiu_trace_analyzer.pipeline import *
