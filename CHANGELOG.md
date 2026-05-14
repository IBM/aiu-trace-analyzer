# Changelog

# v1.2.0

## Enhancements
- Added RCU utilization support for autopilot-enabled traces.
- Extended PT-active signal creation to multi-AIU and multi-table traces.
- Added time-weighted power statistics analysis.
- Added Pandas dataframe export support.
- Improved event classification for more detailed trace categorization.
- Added flow events for kernel launch/completion and improved collective flow handling.
- Added configurable event limiting and maximum TID streams for overlap resolution.
- Improved timestamp refinement based on hardware timing.

## Bug Fixes
- Fixed RCU utilization handling for the torch spyre stack.
- Fixed pre-event parsing for queuing counter creation.
- Fixed rank info, process ordering, and deviceProperties ingestion for multi-process torch traces.
- Relaxed classifier mismatches to accumulated warnings.
- Skip launch-flow event creation when inconsistent timestamps are detected.
- Gracefully handle zero-gap events during frequency estimation.
- Removed the obsolete time_align stage now that torch+flex alignment is no longer needed.

## Contributions to this release from
- lasch
- yuhaohaoyu
- WarningRan
- aishwariyachakraborty
- aryanputta
- tej7499

## Code Quality
- Replaced hardcoded package versioning with git-tag-based versioning.
- Disabled warnings from deactivated stages.
- Added and updated unit tests across core processing paths.
- Added SonarQube configuration and tightened CI formatting checks.
- Applied general maintenance updates and fixes.

# v1.0.2
 - requires Python 3.10 or later (3.9 is out of support)

## Enhancements
- Separated utilization categories per table.
- Added similarity-based fingerprinting for ideal cycles table.
- Introduced event filtering using attribute + regex.
- Implemented frequency detection and statistics per event duration/interval.
- Improved kernel sequence fingerprinting by identifying individual jobs.
- Streamlined summary warnings and error handling across multiple pipeline stages.

## Bug Fixes
- Resolved ZeroDivisionError in RCU utilization.
- Corrected power unit scaling issue.

## Code Quality
- Updated code complexity for RCU utilization.
- Applied general updates and fixes.
- Capture summary warnings in pytest
