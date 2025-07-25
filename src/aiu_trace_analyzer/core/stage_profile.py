# Copyright 2024-2025 IBM Corporation

import json
import os
from pathlib import Path


class StageProfile:
    _everything_profile = os.path.join(os.path.dirname(__file__), "../profiles/everything.json")

    def __init__(self, profile_data: dict):
        self.profile = self._ingest_profile_data(profile_data)

    @classmethod
    def from_json(cls, file: Path):
        with open(file, 'r') as config_fd:
            profile_data = json.load(config_fd)

        # if a profile is empty, then assume all stages to be enabled
        if len(profile_data) == 0:
            with open(StageProfile._everything_profile, 'r') as config_fd:
                profile_data = json.load(config_fd)

        profile = StageProfile(profile_data)
        return profile

    def _ingest_profile_data(self, profile_data: dict) -> list[str]:
        if 'stages' not in profile_data:
            raise KeyError("Profile data is missing 'stages' key.")

        profile = []
        for stage_data in profile_data['stages']:
            stage, enabled = stage_data.popitem()
            if enabled:
                profile.append(stage)
        return profile


class StageProfileChecker:
    def __init__(self, profile: StageProfile):
        self.stages = profile
        self.reg_idx = 0

    def fwd_find_stage(self, stage: str) -> bool:
        for incr, st in enumerate(self.stages.profile[self.reg_idx:]):
            if stage == st:
                self.reg_idx += incr
                return True
        return False
