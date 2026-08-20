# Copyright 2024-2026 IBM Corporation

'''
Guard tests for the drain() contract of AbstractContext.

AbstractContext.drain() emits the accumulated warnings of a context. Any override that does not
chain to super().drain() silently drops those warnings instead of exporting them. These tests pin
that contract for the contexts that actually own warnings, so a non-chaining override shows up as
a failing test rather than as a missing entry in the output file.
'''

import pytest

from aiu_trace_analyzer.types import TRACE_ISSUE_EVENT_NAME
from aiu_trace_analyzer.pipeline.categorize import EventCategorizerContext
from aiu_trace_analyzer.pipeline.normalize import NormalizationContext
from aiu_trace_analyzer.pipeline.overlap import OverlapDetectionContext
from aiu_trace_analyzer.verification.overlap_verify import OverlapVerificationContext
from aiu_trace_analyzer.verification.verify import VerificationContext


# (id, factory, name of a warning owned by that context)
# NOTE: the RCUUtilizationContext variants are left out on purpose: their constructors require a
# kernel_db_url and would pull a database into this test.
WARNING_OWNING_CONTEXTS = [
    ("normalize", lambda: NormalizationContext(soc_frequency=1.0), "long_dur"),
    ("categorize", lambda: EventCategorizerContext(), "flex_mismatch"),
    ("overlap", lambda: OverlapDetectionContext(), "overlaps"),
    ("overlap_verify", lambda: OverlapVerificationContext(), "overlaps"),
    ("verify", lambda: VerificationContext(), "failed_verification"),
]

# the two-phase contexts need one drain call to switch phase and a second one to chain up
_MAX_DRAIN_CALLS = 2


def _findings_of(events: list, warning_name: str) -> list:
    return [e for e in events
            if e["name"] == TRACE_ISSUE_EVENT_NAME and e["args"]["finding"] == warning_name]


def _prepared(factory, warning_name):
    context = factory()
    context.enable()
    context.warnings[warning_name].auto_log = False   # keep the test output clean
    return context


@pytest.mark.parametrize("factory,warning_name",
                         [pytest.param(f, w, id=i) for i, f, w in WARNING_OWNING_CONTEXTS])
def test_dc1_fired_warning_survives_drain(factory, warning_name):
    '''
    the core of the contract: a fired warning has to come back out of the drain chain. A drain()
    override that does not chain to super() returns nothing here.
    '''
    context = _prepared(factory, warning_name)
    context.issue_warning(warning_name)

    events = []
    for _ in range(_MAX_DRAIN_CALLS):
        events += context.drain()

    assert _findings_of(events, warning_name), \
        f"warning '{warning_name}' did not survive drain(): the override likely does not chain super().drain()"


@pytest.mark.parametrize("factory,warning_name",
                         [pytest.param(f, w, id=i) for i, f, w in WARNING_OWNING_CONTEXTS])
def test_dc2_warning_issued_late_still_gets_out(factory, warning_name):
    '''
    two-phase contexts accumulate warnings during the application phase, i.e. after their first
    drain() call. The finding of such a late warning still has to be reported by the final drain.
    '''
    context = _prepared(factory, warning_name)

    context.drain()                       # switches phase for the two-phase contexts
    context.issue_warning(warning_name)

    assert _findings_of(context.drain(), warning_name)


def test_dc3_repeated_drain_reports_the_final_count():
    '''
    a context attached to several pipeline stages is drained once per stage, so the same finding is
    reported repeatedly. Every report has to carry the counts accumulated so far - the exporter is
    what collapses them (see test_exporter.py).
    '''
    context = _prepared(lambda: NormalizationContext(soc_frequency=1.0), "long_dur")

    context.issue_warning("long_dur", {"count": 2})
    first = _findings_of(context.drain(), "long_dur")
    context.issue_warning("long_dur", {"count": 3})
    second = _findings_of(context.drain(), "long_dur")

    assert len(first) == 1 and len(second) == 1
    assert "2 event(s)" in first[0]["args"]["text"]
    assert "5 event(s)" in second[0]["args"]["text"]
