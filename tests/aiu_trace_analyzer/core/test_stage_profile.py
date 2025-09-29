# Copyright 2024-2025 IBM Corporation

import pytest
import os

from aiu_trace_analyzer.core.stage_profile import StageProfile, StageProfileChecker


@pytest.fixture
def default_profile_config() -> str:
    return os.path.join(os.path.dirname(__file__), "../../../src/aiu_trace_analyzer/profiles/default.json")


@pytest.fixture
def everything_profile() -> str:
    return os.path.join(os.path.dirname(__file__), "../../../src/aiu_trace_analyzer/profiles/everything.json")


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
