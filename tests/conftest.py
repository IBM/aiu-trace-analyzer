# Copyright 2024-2025 IBM Corporation

import pytest
import random
import glob

from aiu_trace_analyzer.types import TraceEvent, GlobalIngestData, InputDialect, InputDialectFLEX, InputDialectTORCH

@pytest.fixture(scope="module")
def generate_event(request) -> TraceEvent:
    t,n,ts = request.param
    return {"ph":t,"name":n,"ts":ts}

@pytest.fixture(scope="module")
def generate_event_list(request) -> list[TraceEvent]:
    basename="eventName"
    eseq, llen = request.param
    revents = []
    rts = 1
    for i in range(llen):
        for etype in eseq:
            rts += random.randint(1,10)
            if etype == "X":
                dur = random.randint(5,20)
                revents.append({"ph":etype, "name": basename+str(i), "ts":rts, "dur":dur, "pid":0})
            else:
                revents.append({"ph":etype, "name": basename+str(i), "ts":rts, "pid":0})
    return revents


def extend_args_with_tmpout(args_list:list[str], tmp_out_base:str):
    args_list+=["-o", tmp_out_base+".json"]
    return args_list

@pytest.fixture(scope="session")
def shared_tmp_path(tmp_path_factory):
    # get base directory name and create shared directory path if not exists
    base_temp = tmp_path_factory.getbasetemp()
    shared_dir_path = base_temp/"shared_test_data"
    shared_dir_path.mkdir(exist_ok=True)

    return shared_dir_path

@pytest.fixture
def get_intermediate_filename(shared_tmp_path):
    def _get_intermediate_filename(pattern):
        return glob.glob(str(shared_tmp_path / pattern))
    return _get_intermediate_filename

@pytest.fixture
def global_ingest_data():
    jobdata = GlobalIngestData.add_job_info(source_uri="test_frame_flex.json", data_dialect=InputDialectFLEX())
    return jobdata

@pytest.fixture(scope="session")
def flex_event_with_jobhash(request, global_ingest_data) -> tuple[TraceEvent, TraceEvent]:
    event = request.param
    if "args" not in event:
        event["args"] = {}
    event["args"]["jobhash"] = global_ingest_data
    return event
