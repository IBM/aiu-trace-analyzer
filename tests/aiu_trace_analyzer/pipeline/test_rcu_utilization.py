# Copyright 2024-2025 IBM Corporation

import pytest
import pandas as pd

from aiu_trace_analyzer.pipeline.context import AbstractContext
from aiu_trace_analyzer.pipeline.rcu_utilization import (
    RCUUtilizationContext,
    MultiRCUUtilizationContext,
    RCUTableFingerprint,
    compute_utilization
)
from acelyzer.acelyzer import main
from conftest import extend_args_with_tmpout


# setup the data/logfile location as a fixture
@pytest.fixture
def logfile():
    return "tests/test_data/dt_cycles_table.log"


# setup the context class instance with a table as a fixture
@pytest.fixture
def rcu(logfile: str, tmp_path):
    return RCUUtilizationContext(compiler_info=logfile,
                                 csv_fname=f'{tmp_path}/test_output.json',
                                 soc_freq=1000,
                                 core_freq=800)


# setup a context class instance without table as a fixture
@pytest.fixture
def rcu_without_table(tmp_path):
    return RCUUtilizationContext(compiler_info=None,
                                 soc_freq=1000,
                                 core_freq=800,
                                 csv_fname=f'{tmp_path}/test_output.json')


@pytest.fixture
def multircu_single(logfile: str, tmp_path):
    return MultiRCUUtilizationContext(compiler_info=logfile,
                                      csv_fname=f'{tmp_path}/test_output.json',
                                      soc_freq=1000,
                                      core_freq=800)


'''
Testing the values of certain member variables after creating RCUUtilizationContext
The input is the fixture created above using the logfile() fixture.
The test cases specify a var and its expected value after reading the log
'''
expected_vars_and_values = [
    ("cycle_to_clock_factor", 1.0/800.0),
    ("multi_table", 0),
    ("autopilot", False),
    ("unscaled", False),
    ("kernel_cycles",
     {"somevalue":
      {'addmm_MatMul-BMM_1 Cmpt Exec': 27648,
       'addmm_1_MatMul-BMM_1 Cmpt Exec': 27648,
       'bmm-BMM_1 Cmpt Exec': 12288,
       'Total Cmpt Exec': 67584,
       }
      }
     )
]


@pytest.mark.parametrize("variable, expected", expected_vars_and_values)
def test_inititialized_members(variable: str, expected, rcu):
    val = vars(rcu)[variable]
    if isinstance(expected, dict):
        for (a, b) in zip(val.values(), expected.values()):
            assert a == b
    else:
        assert val == expected


def test_no_table_context(rcu_without_table: RCUUtilizationContext):
    assert len(rcu_without_table.kernel_cycles) == 0   # empty kernel table has no entries


list_of_cycles_tests = [
    ("addmm_MatMul-BMM_1 Cmpt Exec", 0, 27648),
    ("", 1, 0),
    ("blabla Cmpt Exec", 0, 0),
    ("Total", 0, 0),     # not a kernel
    ("bmm-BMM_1 Cmpt Exec", 0, 12288)
]


@pytest.mark.parametrize("input,pid,expected", list_of_cycles_tests)
def test_get_cycles(input: str, pid: int, expected: int, rcu: RCUUtilizationContext):
    cycles = -1
    for k in rcu.kernel_cycles.keys():
        cycles = rcu.get_cycles(input, k)
        if cycles == expected:
            break
    assert cycles == expected


def test_compute_utilization_assert():
    with pytest.raises(AssertionError):
        compute_utilization({"name": "Testevent", "ph": "X", "ts": 5.0}, AbstractContext())


def test_compute_utilization_event_passthrough(multircu_single):
    event = compute_utilization({"name": "Testevent", "ph": "X", "ts": 5.0}, multircu_single)
    assert len(event) == 1
    assert event[0] == {"name": "Testevent", "ph": "X", "ts": 5.0}


tests_to_run_for_rcu_util: list[tuple[str, str, int, int]] = [
    (
        "--freq=560:800 -c tests/test_data/sample_comp_log_ideal.txt -i tests/test_data/sample_flex_3062_job_4.json",
        10, 10
    ),
    (
        "--freq 560.0 -c tests/test_data/sample_comp_log_ideal.txt -i tests/test_data/sample_flex_3062_job_4.json",
        10, 10
    ),
]


# test the rcu_utilization category table generation, checking the table dimensions (#rows, #columns)
@pytest.mark.parametrize('test_args, num_rows, num_columns', tests_to_run_for_rcu_util)
def test_rcu_util_category(test_args: str, num_rows: int, num_columns: int, tmp_path):
    output_file_base = f'{tmp_path}/rcu_util_category_test'
    args_list = extend_args_with_tmpout(test_args.split(' '), output_file_base)
    main(args_list)

    csv_dims = pd.read_csv(f"{output_file_base}_categories.csv").shape
    txt_dims = pd.read_fwf(f"{output_file_base}_categories.txt").shape   # fixed width format

    assert csv_dims[0] == num_rows
    assert csv_dims[1] == num_columns
    assert txt_dims[0] == num_rows
    assert txt_dims[1] == num_columns


# Test cases for exception catching during similarity computation
zerodiv_test_cases = [
    # (test_id, event_data, table_data, description)
    ("zero_dataitems", [("kernel1", 100.0)], [],
     "table fingerprint has zero dataitems"),
    ("zero_totaltime", [("kernel1", 0.0)], [("kernel1", 100.0)],
     "event fingerprint has zero totaltime"),
    ("both_zero", [("kernel1", 0.0)], [],
     "both dataitems and totaltime are zero"),
]


@pytest.mark.parametrize("test_id,event_data,table_data,description",
                         zerodiv_test_cases)
def test_update_fprint_matches_catches_zerodivision(
        test_id, event_data, table_data, description,
        multircu_single, rcu_without_table):
    """Test that update_fprint_matches catches ZeroDivisionError during similarity computation"""
    # Create a table fingerprint that will cause division by zero
    bad_fprint = RCUTableFingerprint()
    for kernel, time in table_data:
        bad_fprint.add(kernel, time)

    # Create an event fingerprint
    event_fprint = RCUTableFingerprint()
    for kernel, time in event_data:
        event_fprint.add(kernel, time)

    # Add the fingerprints to the context
    multircu_single.fingerprints["test_job"] = event_fprint

    # Use the existing rcu_without_table fixture and add the bad fingerprint
    rcu_without_table.fingerprints = {"bad_table": bad_fprint}
    multircu_single.rcuctx = {"mock": rcu_without_table}

    # This should not raise an exception; it should catch ZeroDivisionError
    # and issue a warning instead
    multircu_single.update_fprint_matches()

    # Verify that the similarity_error warning was issued
    assert "similarity_error" in multircu_single.warnings, f"Failed for case: {description}"
