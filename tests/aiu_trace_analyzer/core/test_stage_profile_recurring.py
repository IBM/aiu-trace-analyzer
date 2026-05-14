# Copyright 2024-2025 IBM Corporation

import json
from pathlib import Path

from aiu_trace_analyzer.core.stage_profile import StageProfile


def write_profile(tmp_path: Path, profile_data: dict) -> str:
    profile_path = tmp_path / "profile.json"
    profile_path.write_text(json.dumps(profile_data))
    return str(profile_path)


def test_sparse_unqualified_recurring_enables_all_occurrences():
    all_stages = {
        "stages": [
            {"setup": True},
            {"recurring_stage": True},
            {"middle": True},
            {"recurring_stage": True},
            {"cleanup": True},
        ]
    }
    profile_data = {
        "stages": [
            {"setup": True},
            {"recurring_stage": True},
        ]
    }
    profile = StageProfile(profile_data, all_stages)
    recurring_enabled = [enabled for name, enabled in profile.profile if name == "recurring_stage"]
    assert len(recurring_enabled) == 2
    assert all(recurring_enabled)


def test_sparse_unqualified_recurring_disabled_disables_all_occurrences(tmp_path):
    profile_data = {
        "stages": [
            {"normalize_phase1": True},
            {"pipeline_barrier": False},
        ]
    }
    profile = StageProfile.from_json(write_profile(tmp_path, profile_data))
    barrier_states = [enabled for name, enabled in profile.profile if name == "pipeline_barrier"]
    assert len(barrier_states) == 4
    assert not any(barrier_states)


def test_sparse_recurring_does_not_consume_later_stages(tmp_path):
    profile_data = {
        "stages": [
            {"normalize_phase1": True},
            {"sort_events": True},
            {"compute_power": True},
        ]
    }
    profile = StageProfile.from_json(write_profile(tmp_path, profile_data))

    sort_states = [enabled for name, enabled in profile.profile if name == "sort_events"]
    assert len(sort_states) == 4
    assert all(sort_states)

    assert [enabled for name, enabled in profile.profile if name == "compute_power"] == [True]


def test_positional_profile_with_missing_tail_stages_resolves_per_occurrence(tmp_path):
    # profiles that list all occurrences of each repeated name are matched
    # positionally even when newer tail stages are absent from the profile
    all_stages = {
        "stages": [
            {"setup": True},
            {"recurring_stage": True},
            {"middle": True},
            {"recurring_stage": True},
            {"new_tail_stage": True},
        ]
    }
    profile_data = {
        "stages": [
            {"setup": True},
            {"recurring_stage": False},
            {"middle": True},
            {"recurring_stage": True},
        ]
    }
    profile = StageProfile(profile_data, all_stages)
    recurring_enabled = [enabled for name, enabled in profile.profile if name == "recurring_stage"]
    assert recurring_enabled == [False, True]
