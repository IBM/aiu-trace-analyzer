# Copyright 2024-2025 IBM Corporation

import json
import os
from pathlib import Path
from copy import deepcopy

import aiu_trace_analyzer.logger as aiulog

class StageProfile:
    _everything_profile = os.path.join(os.path.dirname(__file__), "../profiles/everything.json")

    def __init__(self, profile_data: dict, all_stages: dict):
        self.profile = self._ingest_profile_data(profile_data, all_stages)

    @classmethod
    def from_json(cls, file: Path):
        with open(file, 'r') as config_fd:
            profile_data = json.load(config_fd)
        with open(cls._everything_profile, 'r') as all_fd:
            all_stages = json.load(all_fd)

        # if a profile is empty, then assume all stages to be enabled
        if len(profile_data) == 0:
            profile_data = deepcopy(all_stages)

        profile = StageProfile(profile_data, all_stages)
        return profile

    def _ingest_profile_data(self, profile_data: dict, all_stages: dict) -> list[str]:
        if 'stages' not in profile_data:
            raise KeyError("Profile data is missing 'stages' key.")

        # walk the full list and pick up the enabled/disabled flag from the requested config
        next_stage, next_enabled = profile_data['stages'].pop(0).popitem()
        profile: list[tuple[str, bool]] = []
        for stage_data in all_stages['stages']:
            stage, enabled = stage_data.popitem()
            if not enabled:
                aiulog.log(aiulog.WARN, "STP: all-stages profile has unexpectedly disabled stage: ", stage)
            if next_stage == stage:
                profile.append((stage, next_enabled))
                next_stage, next_enabled = profile_data['stages'].pop(0).popitem() if len(profile_data['stages']) > 0 else ("nothing", False)
            else:
                profile.append((stage, False))
            aiulog.log(aiulog.DEBUG, "PRF:", stage, profile[-1][1])
        return profile


class StageProfileChecker:
    def __init__(self, profile: StageProfile):
        self.stages = profile
        self.reg_idx = 0

    def fwd_find_stage(self, stage: str) -> bool:
        for incr, st in enumerate(self.stages.profile[self.reg_idx:]):
            if stage == st[0]:
                self.reg_idx += incr + 1
                return st[1]
        return False
