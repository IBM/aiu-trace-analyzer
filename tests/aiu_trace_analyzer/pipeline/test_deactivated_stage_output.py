# Copyright 2024-2026 IBM Corporation

import aiu_trace_analyzer.logger as aiulog

from aiu_trace_analyzer.pipeline.coll_group import CollectiveGroupingContext
from aiu_trace_analyzer.pipeline.dma import DataTransferExtractionContext
from aiu_trace_analyzer.pipeline.inverse_ts import InversedTSDetectionContext
from aiu_trace_analyzer.pipeline.overlap import OverlapDetectionContext
from aiu_trace_analyzer.pipeline.power import PowerExtractionContext
from aiu_trace_analyzer.pipeline.rcu_utilization import RCUUtilizationContext
from aiu_trace_analyzer.pipeline.stats import StatsExtractionContext


def test_deactivated_stage_contexts_do_not_emit_output(monkeypatch, capsys):
    log_calls = []
    monkeypatch.setattr(aiulog, "log", lambda *args: log_calls.append(args))

    contexts = []

    dma_context = DataTransferExtractionContext()
    dma_context.ecount_out = 3
    dma_context.ecount_in = 4
    dma_context.disable()
    contexts.append(dma_context)

    inverse_context = InversedTSDetectionContext()
    inverse_context.bad_count = 2
    inverse_context.disable()
    contexts.append(inverse_context)

    overlap_context = OverlapDetectionContext()
    overlap_context.resolved = 5
    overlap_context.disable()
    contexts.append(overlap_context)

    power_context = PowerExtractionContext()
    power_context.bad_events = 1
    power_context.ecount_out = 7
    power_context.ecount_in = 9
    power_context.disable()
    contexts.append(power_context)

    stats_context = StatsExtractionContext("unused")
    stats_context.total_util[11] = (0.9, 10.0, 1.0)
    stats_context.disable()
    contexts.append(stats_context)

    coll_context = CollectiveGroupingContext()
    coll_context.problem_count = 1
    coll_context.flow_div = {0: (1, 2, 3)}
    coll_context.stale_drop = 2
    coll_context.disable()
    contexts.append(coll_context)

    for context in contexts:
        type(context).__del__(context)

    captured = capsys.readouterr()
    assert captured.out == ""
    assert log_calls == []


def test_deactivated_rcu_context_does_not_write_output(tmp_path):
    compiler_log = tmp_path / "compiler.log"
    compiler_log.write_text("")

    csv_path = tmp_path / "utilization.csv"
    db_path = tmp_path / "kernel.db"
    txt_path = tmp_path / "utilization_categories.txt"

    context = RCUUtilizationContext(
        compiler_log=str(compiler_log),
        csv_fname=str(csv_path),
        soc_freq=1000.0,
        core_freq=1000.0,
        kernel_db_url=str(db_path),
    )
    context.categories = {1: {"Total": (1.0, 1.0, 1)}}
    context.disable()

    type(context).__del__(context)

    assert csv_path.exists() is False
    assert db_path.exists() is False
    assert txt_path.exists() is False
