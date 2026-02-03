# Copyright 2024-2025 IBM Corporation

import random
import glob
import pytest

from aiu_trace_analyzer.types import TraceEvent, GlobalIngestData, InputDialectFLEX


@pytest.fixture(scope="module")
def generate_event(request) -> TraceEvent:
    """Generate a single TraceEvent from parametrized test data.

    Args:
        request: pytest request object with param tuple (ph, name, ts)
            - ph: Phase type of the event
            - name: Name of the event
            - ts: Timestamp of the event

    Returns:
        TraceEvent: A dictionary containing the trace event data
    """
    t, n, ts = request.param
    return TraceEvent({"ph": t, "name": n, "ts": ts})


@pytest.fixture(scope="module")
def generate_event_list(request) -> list[TraceEvent]:
    """Generate a list of TraceEvents from parametrized test data.

    Args:
        request: pytest request object with param tuple (eseq, llen)
            - eseq: Sequence of event types (e.g., ['B', 'E', 'X'])
            - llen: Length/number of iterations to generate events

    Returns:
        list[TraceEvent]: A list of trace event dictionaries with randomized timestamps
    """
    basename = "eventName"
    eseq, llen = request.param
    revents = []
    rts = 1
    for i in range(llen):
        for etype in eseq:
            rts += random.randint(1, 10)
            if etype == "X":
                dur = random.randint(5, 20)
                revents.append({"ph": etype, "name": basename+str(i), "ts": rts, "dur": dur, "pid": 0})
            else:
                revents.append({"ph": etype, "name": basename+str(i), "ts": rts, "pid": 0})
    return revents


def extend_args_with_tmpout(args_list: list[str], tmp_out_base: str):
    """Extend argument list with temporary output file path.

    Args:
        args_list: List of command-line arguments to extend
        tmp_out_base: Base path for temporary output file (without extension)

    Returns:
        Extended argument list with output file specification
    """
    args_list += ["-o", tmp_out_base + ".json"]
    return args_list


@pytest.fixture(scope="session")
def shared_tmp_path(tmp_path_factory):
    """Create a shared temporary directory for test data.

    This fixture creates a session-scoped temporary directory that can be
    shared across multiple tests. The directory is created under the base
    temporary directory provided by pytest.

    Args:
        tmp_path_factory: Pytest fixture factory for creating temporary paths

    Returns:
        Path: Path object pointing to the shared temporary directory
    """
    base_temp = tmp_path_factory.getbasetemp()
    shared_dir_path = base_temp/"shared_test_data"
    shared_dir_path.mkdir(exist_ok=True)

    return shared_dir_path


@pytest.fixture
def get_intermediate_filename(shared_tmp_path):  # noqa: F811
    """Fixture that returns a function to get intermediate filenames matching a pattern.

    Args:
        shared_tmp_path: Path to the shared temporary directory (pytest fixture)

    Returns:
        Callable: A function that takes a glob pattern and returns matching file paths
    """
    def _get_intermediate_filename(pattern):
        return glob.glob(f"{str(shared_tmp_path)}/{pattern}")
    return _get_intermediate_filename


@pytest.fixture(scope="session")
def global_ingest_data():
    """Create global ingest data for FLEX dialect testing.

    This fixture initializes a GlobalIngestData instance with test data using
    the FLEX input dialect. The jobhash is generated from the source URI and
    can be used to retrieve job information and dialect settings in tests.

    Returns:
        int: A jobhash identifier for the registered job data
    """
    jobdata = GlobalIngestData.add_job_info(source_uri="test_frame_flex.json", data_dialect=InputDialectFLEX())
    return jobdata


@pytest.fixture(scope="session")
def flex_event_with_jobhash(request, global_ingest_data) -> TraceEvent:
    """Generate a FLEX TraceEvent with jobhash added to its args.

    This fixture takes a parametrized event and enriches it with a jobhash
    identifier from the global_ingest_data fixture, enabling tests to work
    with FLEX dialect events that have proper job context.

    Args:
        request: pytest request object with param containing a TraceEvent dict
        global_ingest_data: Fixture providing the jobhash identifier

    Returns:
        TraceEvent: The input event with jobhash added to its args field
    """
    event = request.param
    if "args" not in event:
        event["args"] = {}
    event["args"]["jobhash"] = global_ingest_data
    return event
