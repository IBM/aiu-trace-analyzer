# Copyright 2024-2025 IBM Corporation

import importlib
import pytest

from aiu_trace_analyzer.ingest.ingestion import JsonFileEventTraceIngest
from aiu_trace_analyzer.pipeline import AbstractHashQueueContext
import aiu_trace_analyzer.pipeline.coll_group as cgr


@pytest.fixture
def load_variables():
    source = importlib.import_module('aiu_trace_analyzer.pipeline.coll_group')
    return {k: v for k, v in vars(source).items() if not k.startswith("__")}


def test_filename(shared_tmp_path, get_intermediate_filename, load_variables):
    # Check if file exists
    assert shared_tmp_path.exists()
    matching_files = get_intermediate_filename("*flow_prepare*")
    assert matching_files, "No files matching pattern '*flow_prepare*' found"

    fname = matching_files[0]

    # Load variable value from coll_group.py
    globals().update(load_variables)

    importer = JsonFileEventTraceIngest(str(fname), keep_processed=True)
    context = AbstractHashQueueContext()

    for event in importer:
        group_id = hash(event["name"])

        # Store X events
        if event["ph"] in "X" and "args" in event:
            context.queues[group_id] = event["name"]

        elif event["ph"] in "F":
            # Check if every "F" event has corresponding "X"
            assert group_id in context.queues

            # Check if "args" is included in event F
            assert "args" not in event

            # Check name format
            assert not cgr._unify_recv.search(event["name"])
            assert not cgr._unify_rdma.search(event["name"])

            # Check if has Peers
            assert len(event[cgr._KEY_PEER]) > 0 or event[cgr._KEY_TYPE] == cgr._TYPE_MCAST

            if cgr._FLOW_STEP in event:
                assert event[cgr._FLOW_STEP] == cgr._STEP_DONE
                assert event[cgr._KEY_TYPE] == cgr._TYPE_DONE
            else:
                assert event[cgr._KEY_TYPE] in {cgr._TYPE_SEND, cgr._TYPE_BCLIST, cgr._TYPE_MCAST}

            if cgr._FLOW_IO in event:
                assert event[cgr._FLOW_IO] == cgr._IO_TYPE_DMAO

            assert event[cgr._FLOW_SYNC] in event["name"]
