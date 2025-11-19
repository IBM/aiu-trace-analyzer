# Changelog

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
