# Copyright 2024-2026 IBM Corporation

import pytest
from sys import float_info

from aiu_trace_analyzer.pipeline.normalize import (
    _attr_to_args,
    _hex_to_int_str,
    EventLimiter,
    NormalizationContext
)


@pytest.mark.parametrize("event,key,result",
                         [
                             ({'ph': 'X', 'args': {'TS1': '12345'}}, 'TS1', '12345'),
                             ({'ph': 'X', 'args': {'Power': '12345'}}, 'Power', '12345'),
                             ({'ph': 'X', 'args': {'TS2': '0x12345'}}, 'TS2', '74565'),
                             ({'ph': 'X', 'args': {'SOME': 'TEXT'}}, 'SOME', 'TEXT'),
                         ])
def test__hex_to_int_str(event, key, result):
    tevent = _hex_to_int_str(event)
    assert key in tevent["args"] and tevent["args"][key] == result


@pytest.mark.parametrize("event,result",
                         [
                             ({'ph': 'X', 'args': {'a': 1, 'b': '2'}}, {'ph': 'X', 'args': {'a': 1, 'b': '2'}}),
                             ({'ph': 'X', 'attr': {'a': 1, 'b': '2'}}, {'ph': 'X', 'args': {'a': 1, 'b': '2'}}),
                         ])
def test__attr_to_args(event, result):
    assert _attr_to_args(event) == result


@pytest.fixture
def normalization_ctx():
    return NormalizationContext(soc_frequency=1000.0,
                                ignore_crit=False)


ts_seq_test_events = [
    ({'name': 'testevent', 'ph': 'X', 'ts': 3.141,
      'args': {'TS1': '1', 'TS2': '2', 'TS3': '2', 'TS4': '3', 'TS5': '5', 'jobhash': 0}},
     {'TS1': '1', 'TS2': '2', 'TS3': '2', 'TS4': '3', 'TS5': '5', 'jobhash': 0, 'OVC': 0}),
    ({'name': 'testevent', 'ph': 'X', 'ts': 3.141,
      'args': {'TS1': '1', 'TS2': '2', 'TS3': '1', 'TS4': '3', 'TS5': '5', 'jobhash': 0}},
     None),
]


@pytest.mark.parametrize("event,result", ts_seq_test_events)
def test_tsx_32bit_global_correction(event, result, normalization_ctx):
    if result is None:
        with pytest.raises(AssertionError) as exit_info:
            normalization_ctx.tsx_32bit_global_correction(0, event)
        assert exit_info.value.args == ('local_correction of TS-sequence incomplete.',)
    else:
        assert normalization_ctx.tsx_32bit_global_correction(0, event) == result


ts_seq_ign_err_test_events = [
    ({'name': 'testevent', 'ph': 'X', 'ts': 3.141,
      'args': {'TS1': '1', 'TS2': '2', 'TS3': '2', 'TS4': '3', 'TS5': '5', 'jobhash': 0}},
     {'TS1': '1', 'TS2': '2', 'TS3': '2', 'TS4': '3', 'TS5': '5', 'jobhash': 0, 'OVC': 0},
     False),
    ({'name': 'testevent', 'ph': 'X', 'ts': 3.141,
      'args': {'TS1': '1', 'TS2': '2', 'TS3': '1', 'TS4': '3', 'TS5': '5', 'jobhash': 0}},
     {'TS1': '1', 'TS2': '2', 'TS3': '1', 'TS4': '3', 'TS5': '5', 'jobhash': 0, 'OVC': 0},
     True),
]


@pytest.mark.parametrize("event,result,expect_warning", ts_seq_ign_err_test_events)
def test_tsx_32bit_global_correction_ign(event, result, expect_warning, capsys):
    normalization_ctx_ign_crit = NormalizationContext(
        soc_frequency=1000.0,
        ignore_crit=True)
    assert normalization_ctx_ign_crit.tsx_32bit_global_correction(0, event) == result

    # check whether an error is being issued when detecting out-of-sequence TS
    if expect_warning:
        assert normalization_ctx_ign_crit.warnings["ts_seq_err"].has_warning() is True
        del normalization_ctx_ign_crit
        output = capsys.readouterr()
        assert "ERROR" in output.out
        assert "OVC: local_correction fix has missed" in output.out


# ============================================================================
# EventLimiter Tests
# ============================================================================

class TestEventLimiter:
    """Test suite for EventLimiter class"""

    @pytest.mark.parametrize("config,expected", [
        # Default config
        (
            {},
            {"event_skip": 0, "event_limit": 1 << 60, "event_earliest": 0.0,
             "event_latest": float_info.max, "no_count_types": "M", "event_count": 0}
        ),
        # Skip only
        (
            {"skip": 10},
            {"event_skip": 10, "event_limit": (1 << 60) + 10, "event_earliest": 0.0,
             "event_latest": float_info.max, "no_count_types": "M", "event_count": 0}
        ),
        # Count only
        (
            {"count": 100},
            {"event_skip": 0, "event_limit": 100, "event_earliest": 0.0,
             "event_latest": float_info.max, "no_count_types": "M", "event_count": 0}
        ),
        # Skip and count
        (
            {"skip": 10, "count": 100},
            {"event_skip": 10, "event_limit": 110, "event_earliest": 0.0,
             "event_latest": float_info.max, "no_count_types": "M", "event_count": 0}
        ),
        # ts_start only
        (
            {"ts_start": 1000.0},
            {"event_skip": 0, "event_limit": 1 << 60, "event_earliest": 1000.0,
             "event_latest": float_info.max, "no_count_types": "M", "event_count": 0}
        ),
        # ts_end only
        (
            {"ts_end": 5000.0},
            {"event_skip": 0, "event_limit": 1 << 60, "event_earliest": 0.0,
             "event_latest": 5000.0, "no_count_types": "M", "event_count": 0}
        ),
        # no_count_types only
        (
            {"no_count_types": "MXI"},
            {"event_skip": 0, "event_limit": 1 << 60, "event_earliest": 0.0,
             "event_latest": float_info.max, "no_count_types": "MXI", "event_count": 0}
        ),
        # All parameters
        (
            {"skip": 5, "count": 50, "ts_start": 100.0,
             "ts_end": 1000.0, "no_count_types": "MX"},
            {"event_skip": 5, "event_limit": 55, "event_earliest": 100.0,
             "event_latest": 1000.0, "no_count_types": "MX", "event_count": 0}
        ),
    ])
    def test_init_configurations(self, config, expected):
        """Test EventLimiter initialization with various configurations"""
        limiter = EventLimiter(config)
        assert limiter.event_skip == expected["event_skip"]
        assert limiter.event_limit == expected["event_limit"]
        assert limiter.event_earliest == expected["event_earliest"]
        assert limiter.event_latest == expected["event_latest"]
        assert limiter.no_count_types == expected["no_count_types"]
        assert limiter.event_count == expected["event_count"]

    def test_is_ignored_type_single_char(self):
        """Test is_ignored_type with single character type"""
        limiter = EventLimiter({"no_count_types": "M"})
        assert limiter.is_ignored_type("M") is True
        assert limiter.is_ignored_type("X") is False

    def test_is_ignored_type_multiple_chars(self):
        """Test is_ignored_type with multiple character types"""
        limiter = EventLimiter({"no_count_types": "MXI"})
        assert limiter.is_ignored_type("M") is True
        assert limiter.is_ignored_type("X") is True
        assert limiter.is_ignored_type("I") is True
        assert limiter.is_ignored_type("B") is False

    def test_is_within_limits_basic_event(self):
        """Test is_within_limits with a basic event"""
        limiter = EventLimiter({})
        event = {"ts": 100.0, "dur": 10.0, "ph": "X"}
        assert limiter.is_within_limits(event) is True

    def test_is_within_limits_with_skip(self):
        """Test is_within_limits respects skip parameter"""
        limiter = EventLimiter({"skip": 2})
        event = {"ts": 100.0, "dur": 10.0, "ph": "X"}

        # Need to increment counter past skip threshold
        # First call: count=0, not > skip(2), fails, no increment
        assert limiter.is_within_limits(event, count_this_call=False) is False

        # Manually increment to simulate skipped events
        limiter.event_count = 3  # Now count > skip

        # Now event should pass
        assert limiter.is_within_limits(event) is True
        assert limiter.event_count == 4

    def test_is_within_limits_with_count_limit(self):
        """Test is_within_limits respects count limit"""
        limiter = EventLimiter({"count": 3})
        event = {"ts": 100.0, "dur": 10.0, "ph": "X"}

        # First three events should pass
        assert limiter.is_within_limits(event) is True
        assert limiter.is_within_limits(event) is True
        assert limiter.is_within_limits(event) is True

        # Fourth event should fail (reached limit)
        assert limiter.is_within_limits(event) is False

    def test_is_within_limits_with_ts_start(self):
        """Test is_within_limits respects ts_start boundary"""
        limiter = EventLimiter({"ts_start": 100.0})

        # Event ending before ts_start should fail
        event_before = {"ts": 50.0, "dur": 40.0, "ph": "X"}  # ends at 90.0
        assert limiter.is_within_limits(event_before, count_this_call=False) is False

        # Event intersecting with ts_start increments counter, then passes (count=1 > skip=0)
        event_intersect = {"ts": 50.0, "dur": 60.0, "ph": "X"}  # ends at 110.0
        assert limiter.is_within_limits(event_intersect) is True
        assert limiter.event_count == 1

        # Event starting after ts_start should pass
        event_after = {"ts": 150.0, "dur": 10.0, "ph": "X"}
        assert limiter.is_within_limits(event_after) is True
        assert limiter.event_count == 2

    def test_is_within_limits_with_ts_end(self):
        """Test is_within_limits respects ts_end boundary"""
        limiter = EventLimiter({"ts_end": 1000.0})

        # Event starting after ts_end should fail
        event_after = {"ts": 1100.0, "dur": 10.0, "ph": "X"}
        assert limiter.is_within_limits(event_after, count_this_call=False) is False

        # Event intersecting with ts_end increments counter, then passes
        event_intersect = {"ts": 990.0, "dur": 20.0, "ph": "X"}  # ends at 1010.0
        assert limiter.is_within_limits(event_intersect) is True
        assert limiter.event_count == 1

        # Event ending before ts_end should pass
        event_before = {"ts": 900.0, "dur": 50.0, "ph": "X"}  # ends at 950.0
        assert limiter.is_within_limits(event_before) is True
        assert limiter.event_count == 2

    def test_is_within_limits_with_time_window(self):
        """Test is_within_limits with both ts_start and ts_end"""
        limiter = EventLimiter({"ts_start": 100.0, "ts_end": 1000.0})

        # Event completely before window
        event_before = {"ts": 50.0, "dur": 40.0, "ph": "X"}
        assert limiter.is_within_limits(event_before, count_this_call=False) is False

        # Event completely after window
        event_after = {"ts": 1100.0, "dur": 10.0, "ph": "X"}
        assert limiter.is_within_limits(event_after, count_this_call=False) is False

        # Event within window increments counter, then passes
        event_within = {"ts": 500.0, "dur": 100.0, "ph": "X"}
        assert limiter.is_within_limits(event_within) is True
        assert limiter.event_count == 1

    def test_is_within_limits_count_this_call_false(self):
        """Test is_within_limits with count_this_call=False"""
        limiter = EventLimiter({"count": 2})
        event = {"ts": 100.0, "dur": 10.0, "ph": "X"}

        # First call with counting to get past skip=0
        assert limiter.is_within_limits(event, count_this_call=True) is True
        assert limiter.event_count == 1

        # Check without counting - should still pass
        assert limiter.is_within_limits(event, count_this_call=False) is True
        assert limiter.event_count == 1  # Should not increment

    def test_is_within_limits_event_without_dur(self):
        """Test is_within_limits with event missing dur field"""
        limiter = EventLimiter({"ts_end": 1000.0})
        event = {"ts": 500.0, "ph": "X"}  # No dur field

        # Should use default dur=0.0, increments counter, then passes
        assert limiter.is_within_limits(event) is True
        assert limiter.event_count == 1

    def test_is_within_limits_complex_scenario(self):
        """Test is_within_limits with skip, count, and time window"""
        limiter = EventLimiter({
            "skip": 2,
            "count": 3,
            "ts_start": 100.0,
            "ts_end": 1000.0
        })

        # Event 1: within time window, increments to 1, but 1 not > skip=2
        event1 = {"ts": 200.0, "dur": 10.0, "ph": "X"}
        assert limiter.is_within_limits(event1) is False
        assert limiter.event_count == 1

        # Event 2: within time window, increments to 2, but 2 not > skip=2
        event2 = {"ts": 300.0, "dur": 10.0, "ph": "X"}
        assert limiter.is_within_limits(event2) is False
        assert limiter.event_count == 2

        # Event 3: within time window, increments to 3, and 3 > skip=2, passes
        event3 = {"ts": 400.0, "dur": 10.0, "ph": "X"}
        assert limiter.is_within_limits(event3) is True
        assert limiter.event_count == 3

        # Event 4: within time window, increments to 4, and 4 > skip=2, passes
        event4 = {"ts": 500.0, "dur": 10.0, "ph": "X"}
        assert limiter.is_within_limits(event4) is True
        assert limiter.event_count == 4

        # Event 5: within time window, increments to 5, and 5 <= limit=5, passes
        event5 = {"ts": 600.0, "dur": 10.0, "ph": "X"}
        assert limiter.is_within_limits(event5) is True
        assert limiter.event_count == 5

        # Event 6: within time window, increments to 6, but 6 not <= limit=5, fails
        event6 = {"ts": 700.0, "dur": 10.0, "ph": "X"}
        assert limiter.is_within_limits(event6) is False
        assert limiter.event_count == 6

    def test_is_within_limits_outside_time_window_before_skip(self):
        """Test that events outside time window don't count"""
        limiter = EventLimiter({
            "skip": 2,
            "ts_start": 100.0,
            "ts_end": 1000.0
        })

        # Event outside time window doesn't increment counter
        event_outside = {"ts": 50.0, "dur": 10.0, "ph": "X"}
        assert limiter.is_within_limits(event_outside) is False
        assert limiter.event_count == 0  # Should not increment

        # Another event outside time window
        assert limiter.is_within_limits(event_outside) is False
        assert limiter.event_count == 0  # Should not increment

        # First event inside time window increments to 1, but 1 not > skip=2
        event_inside = {"ts": 500.0, "dur": 10.0, "ph": "X"}
        assert limiter.is_within_limits(event_inside) is False
        assert limiter.event_count == 1

        # Second event inside increments to 2, but 2 not > skip=2
        assert limiter.is_within_limits(event_inside) is False
        assert limiter.event_count == 2

        # Third event inside increments to 3, and 3 > skip=2, passes
        assert limiter.is_within_limits(event_inside) is True
        assert limiter.event_count == 3
