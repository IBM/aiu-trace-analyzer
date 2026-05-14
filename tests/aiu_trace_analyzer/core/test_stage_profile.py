# Copyright 2024-2025 IBM Corporation

import json
import os
from pathlib import Path

import pytest

from aiu_trace_analyzer.core.stage_profile import StageProfile, StageProfileChecker


@pytest.fixture
def default_profile_config() -> str:
    return os.path.join(os.path.dirname(__file__), "../../../src/aiu_trace_analyzer/profiles/default.json")


@pytest.fixture
def everything_profile() -> str:
    return os.path.join(os.path.dirname(__file__), "../../../src/aiu_trace_analyzer/profiles/everything.json")


@pytest.fixture
def torch_minimal_profile() -> str:
    return os.path.join(os.path.dirname(__file__), "../../../src/aiu_trace_analyzer/profiles/torch_minimal.json")


@pytest.fixture
def default_profile(default_profile_config) -> StageProfile:
    return StageProfile.from_json(default_profile_config)


@pytest.fixture
def default_stage_checker(default_profile) -> StageProfileChecker:
    return StageProfileChecker(default_profile)


def test_default_is_everything(default_profile_config, everything_profile):
    def_stage_profile = StageProfile.from_json(default_profile_config)
    all_stage_profile = StageProfile.from_json(everything_profile)

    for d, a in zip(def_stage_profile.profile, all_stage_profile.profile):
        assert d == a


def test_fwd_find(default_stage_checker):
    test_stages = [('create_slice_from_BE', True, 2), ('do_not_move_index', False, 2), ('pipeline_barrier', True, 5)]

    assert isinstance(default_stage_checker, StageProfileChecker)

    for (stage, found, idx) in test_stages:
        assert default_stage_checker.fwd_find_stage(stage) == found
        assert default_stage_checker.reg_idx == idx


def test_torch_minimal_resolves_repeated_stages_by_position(torch_minimal_profile):
    # torch_minimal sets repeated stages per occurrence
    profile = StageProfile.from_json(torch_minimal_profile)

    barrier_states = [enabled for name, enabled in profile.profile if name == "pipeline_barrier"]
    assert barrier_states == [False, True, True, True]

    assert_states = [enabled for name, enabled in profile.profile if name == "assert_ts_sequence"]
    assert assert_states == [False, True, True]


def write_profile(tmp_path: Path, profile_data: dict) -> str:
    profile_path = tmp_path / "profile.json"
    profile_path.write_text(json.dumps(profile_data))
    return str(profile_path)


def test_sparse_profile_unqualified_recurring_name_enables_all(tmp_path):
    profile_data = {
        "stages": [
            {"normalize_phase1": True},
            {"pipeline_barrier": True},
            {"event_categorizer_update": True},
        ]
    }
    profile = StageProfile.from_json(write_profile(tmp_path, profile_data))
    barrier_states = [enabled for name, enabled in profile.profile if name == "pipeline_barrier"]
    assert len(barrier_states) == 4
    assert all(barrier_states)


def test_sparse_profile_ordinal_targets_single_occurrence(tmp_path):
    profile_data = {
        "stages": [
            {"normalize_phase1": True},
            {"pipeline_barrier#2": True},
            {"detect_partial_overlap_events": True},
        ]
    }
    profile = StageProfile.from_json(write_profile(tmp_path, profile_data))
    enabled_at = [i for i, (name, enabled) in enumerate(profile.profile)
                  if name == "pipeline_barrier" and enabled]

    assert len(enabled_at) == 1
    # confirm it is the second occurrence, not the first
    assert sum(1 for name, _ in profile.profile[:enabled_at[0] + 1] if name == "pipeline_barrier") == 2
