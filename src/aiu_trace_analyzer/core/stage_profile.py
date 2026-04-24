# Copyright 2024-2025 IBM Corporation

from collections import Counter
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
        if not os.path.isfile(file):
            # try find profile file in default install location
            file = os.path.join(os.path.dirname(__file__), "../profiles/", file)
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

        all_entries = self._annotate_stage_entries(all_stages['stages'])
        requested_entries = [self._parse_stage_entry(stage_data) for stage_data in profile_data['stages']]
        full_profile = len(requested_entries) == len(all_entries)
        requested_idx = 0
        profile: list[tuple[str, bool]] = []

        for all_entry in all_entries:
            stage = all_entry["name"]
            enabled = all_entry["enabled"]
            if not enabled:
                aiulog.log(aiulog.WARN, "STP: all-stages profile has unexpectedly disabled stage: ", stage)

            requested_enabled = False
            if requested_idx < len(requested_entries):
                requested_entry = requested_entries[requested_idx]
                if self._entry_matches(requested_entry, all_entry, full_profile=full_profile):
                    requested_enabled = requested_entry["enabled"]
                    requested_idx += 1
            profile.append((stage, requested_enabled))
            aiulog.log(aiulog.DEBUG, "PRF:", stage, requested_enabled)

        if requested_idx != len(requested_entries):
            unresolved = requested_entries[requested_idx]["raw"]
            raise ValueError(f"Profile references unknown or misplaced stage '{unresolved}'.")
        return profile

    @staticmethod
    def _parse_stage_key(stage_key: str) -> tuple[str, int | None]:
        if "#" not in stage_key:
            return stage_key, None
        stage_name, stage_index = stage_key.rsplit("#", 1)
        if stage_index.isdigit():
            return stage_name, int(stage_index)
        return stage_key, None

    @classmethod
    def _parse_stage_entry(cls, stage_data: dict[str, bool]) -> dict[str, str | bool | int | None]:
        if len(stage_data) != 1:
            raise ValueError("Each stage entry must contain exactly one stage key.")
        stage_key, enabled = next(iter(stage_data.items()))
        stage_name, stage_index = cls._parse_stage_key(stage_key)
        return {
            "raw": stage_key,
            "name": stage_name,
            "index": stage_index,
            "enabled": bool(enabled),
        }

    @classmethod
    def _annotate_stage_entries(cls, stage_list: list[dict[str, bool]]) -> list[dict[str, str | bool | int]]:
        stage_counts = Counter()
        for stage_data in stage_list:
            stage_name, _stage_index = cls._parse_stage_key(next(iter(stage_data)))
            stage_counts[stage_name] += 1

        stage_seen = Counter()
        annotated = []
        for stage_data in stage_list:
            stage_key, enabled = next(iter(stage_data.items()))
            stage_name, explicit_index = cls._parse_stage_key(stage_key)
            stage_seen[stage_name] += 1
            stage_index = explicit_index if explicit_index is not None else stage_seen[stage_name]
            unique_key = stage_name if stage_counts[stage_name] == 1 else f"{stage_name}#{stage_index}"
            annotated.append(
                {
                    "raw": stage_key,
                    "name": stage_name,
                    "index": stage_index,
                    "unique": unique_key,
                    "enabled": bool(enabled),
                    "repeated": stage_counts[stage_name] > 1,
                }
            )
        return annotated

    @staticmethod
    def _entry_matches(
        requested_entry: dict[str, str | bool | int | None],
        all_entry: dict[str, str | bool | int],
        *,
        full_profile: bool,
    ) -> bool:
        if full_profile:
            return requested_entry["raw"] in {all_entry["raw"], all_entry["unique"], all_entry["name"]}

        if requested_entry["name"] != all_entry["name"]:
            return False
        if requested_entry["index"] is not None:
            return requested_entry["index"] == all_entry["index"]
        if all_entry["repeated"]:
            raise ValueError(
                f"Ambiguous repeated stage '{requested_entry['name']}' in sparse profile. "
                f"Use an occurrence-qualified name like '{requested_entry['name']}#{all_entry['index']}'."
            )
        return True


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
