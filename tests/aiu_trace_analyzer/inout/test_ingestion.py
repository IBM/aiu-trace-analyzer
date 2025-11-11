# Copyright 2024-2025 IBM Corporation

import pytest

from aiu_trace_analyzer.ingest.ingestion import JsonFileEventTraceIngest


@pytest.fixture
def json_input():
    return "tests/test_data/basic_event_test_cases.json"


@pytest.fixture
def json_ingest(json_input) -> JsonFileEventTraceIngest:
    return JsonFileEventTraceIngest(json_input)


def test_nonexist_json_file():
    with pytest.raises(FileNotFoundError) as inres:
        JsonFileEventTraceIngest("Nonexist_file")
    assert inres.value.filename == "Nonexist_file"


def test_invalid_event_ts_scale(json_input):
    with pytest.raises(AssertionError):
        JsonFileEventTraceIngest(json_input, scale=-1.0)


# test the number of generated events from basic_event_test_cases.json is correct
def test_json_input(json_ingest: JsonFileEventTraceIngest):
    count = 0
    for _ in json_ingest.__iter__():
        count += 1

    assert count == 20


input_fail_cases: list[tuple[list, Exception, str]] = [
    (
        [{"ph": "B", "name": "openEvent", "ts": 1, "pid": 0}, {"ph": "E", "name": "closeEvent", "ts": 10, "pid": 0}],
        AssertionError,
        "Subsequent B/E events with different names"
    ),
    (
        [{"ph": "E", "name": "openEvent", "ts": 1, "pid": 0}],
        AssertionError,
        "Expected to find B-event in"
    ),
    (
        [{"ph": "B", "name": "openEvent", "ts": 1, "pid": 0}],
        StopIteration,
        ""
    ),
    (
        [{"ph": "B", "name": "openEvent", "ts": 1, "pid": 0}, {"ph": "X", "name": "openEvent", "ts": 2, "pid": 0}],
        AssertionError,
        "Expected to find E-event in"
    )
]


@pytest.mark.parametrize('input,error,err_msg', input_fail_cases)
def test_json_fail_cases(input, error, err_msg, json_ingest: JsonFileEventTraceIngest):
    # update ingest data and len with input list, as workaround to file input
    json_ingest.data = input
    json_ingest._len = len(input)

    with pytest.raises(error) as ecode:
        json_ingest.build_complete_event()
    assert err_msg in str(ecode.value)


@pytest.mark.parametrize('generate_event', [("X", "testevent", 1.0)], indirect=['generate_event'])
def test_event_gen(generate_event):
    assert generate_event['ph'] == 'X'


list_test_cases = [
    (("BE", 10), 20),
    (("X",  10), 10)
]


@pytest.mark.parametrize('generate_event_list, explen', list_test_cases, indirect=['generate_event_list'])
def test_list_gen(generate_event_list, explen):
    assert len(generate_event_list) == explen
    print(generate_event_list)


list_iteration_cases = [
    (("BE",   100), 100, json_input),
    (("X",    100), 100, json_input),
    (("XCfs", 100), 400, json_input)
]


@pytest.mark.parametrize(
        'generate_event_list, exp_len, json_input',
        list_iteration_cases,
        indirect=['generate_event_list', 'json_input'])
def test_json_iterator(generate_event_list, exp_len, json_input):
    json_importer = JsonFileEventTraceIngest(source_uri=json_input)
    json_importer.data = generate_event_list
    json_importer._len = len(json_importer.data)
    ts = 0.0
    count = 0
    for event in json_importer:
        assert event["ph"] in "XCfsbeM", "Unexpected event type"
        assert event["ph"] not in "BE", "Unexpected B/E event."
        assert event["ts"] >= ts, "Timestamps are not sorted"
        ts = event["ts"]
        count += 1
    assert count == exp_len, "Unexpected number of events"


def test_zero_duration_warning(json_ingest):
    # iterate through events to create issued warnings
    for _ in json_ingest:
        pass

    assert json_ingest.warnings['zero_duration'].has_warning() is True
    assert json_ingest.warnings['zero_duration'].args_list['count'] == 2


def test_neg_duration_warning(json_ingest):
    # iterate through events to create issued warnings
    for _ in json_ingest:
        pass

    assert json_ingest.warnings['negative_duration'].has_warning() is True
    assert json_ingest.warnings['negative_duration'].args_list['count'] == 1
