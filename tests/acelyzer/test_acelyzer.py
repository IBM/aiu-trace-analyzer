# Copyright 2024-2025 IBM Corporation

import pytest

from acelyzer.acelyzer import main
from conftest import extend_args_with_tmpout


tests_to_run_with_raise: list[tuple[str, int]] = [
    (
        "--help",
        0
    ),
    (
        "-i nonexistent",
        1
    ),
    (
        "--missing_required",
        2
    ),
    (
        "-i somefile --unrecognized",
        2
    ),
    (
        "-i somefile -C unknown_counter",
        2
    ),
    (
        "-i somefile -D 5",  # out of range debug level
        2
    ),
    (
        "-i ''",  # empty/none file pattern
        1
    ),
    (
        "-i tests/test_data/basic_event_test_cases.json --freq abcd",   # provide a non-numeric frequency
        1
    ),
    (
        "-i tests/test_data/basic_event_test_cases.json --freq 560:defg",   # provide a non-numeric SoC frequency
        1
    ),
    (
        "-i tests/test_data/basic_event_test_cases.json --freq -500.0",   # provide a negative frequency
        1
    ),
    (
        "-i tests/test_data/basic_event_test_cases.json --freq 560.0:-800",   # provide a negative SoC frequency
        1
    ),
    (
        "-i tests/test_data/basic_event_test_cases.json --freq 0.0:800",   # provide a zero frequency
        1
    ),
    (
        "-i tests/test_data/basic_event_test_cases.json -C power_ts3,power_ts4",   # request mutually exclusive counters
        2
    )
]


tests_to_run: list[tuple[str]] = [
    (
        "-i tests/test_data/basic_event_test_cases.json --tb"
    ),
    (
        "-i tests/test_data/allreduce_tp4.json --flow"
    ),
    (
        "-i tests/test_data/meta_events_only.json"
    ),
    (
        "-i tests/test_data/allreduce_tp4.json --flow -I -R --tb"
    ),   # save intermediate results
]


# any acelyzer runs that would complete with a system exit code
@pytest.mark.parametrize('test_args, exit_code', tests_to_run_with_raise)
def test_acelyzer_with_raise(test_args: str, exit_code: int, tmp_path):
    output_file_base = f'{tmp_path}/acelyzer_fail_test'
    args_list = extend_args_with_tmpout(test_args.split(' '), output_file_base)
    with pytest.raises(SystemExit) as cli_res:
        main(args_list)
    assert cli_res.value.code == exit_code


# regular tests that should just complete without error
@pytest.mark.parametrize('test_args', tests_to_run)
def test_acelyzer(test_args: str, tmp_path, shared_tmp_path):
    if "-I" in test_args:
        output_file_base = f'{shared_tmp_path}/acelyzer_test'
    else:
        output_file_base = f'{tmp_path}/acelyzer_test'
    args_list = extend_args_with_tmpout(test_args.split(' '), output_file_base)
    main(args_list)
