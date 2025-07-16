# Copyright 2024-2025 IBM Corporation

import aiu_trace_analyzer.ingest.ingestion as ingest
import aiu_trace_analyzer.core.processing as processing
import aiu_trace_analyzer.export.exporter as output


class Engine:
    # main engine that drives the processing
    # high-level:
    #   * drive the ingest iterator(s) to retrieve events (as dictionary)
    #   * call processing pipeline to process
    #   * export any returned list of events from processing
    #   * when iteration is finished: flush any potential exporter buffers

    def __init__(self,
                 importer: ingest.AbstractTraceIngest,
                 processor: processing.EventProcessor,
                 exporter: output.AbstractTraceExporter) -> None:
        self.importer = importer
        self.processor = processor
        self.exporter = exporter

    def run(self) -> int:
        # pull from ingest various sources as iterator
        for next_item in self.importer:
            # convert/process
            events = self.processor.process(next_item)
            # push to export
            self.exporter.export(events)

        # drain the context buffers (if any)
        drain = self.processor.drain()
        # export any events emitted during drain
        self.exporter.export(drain)
        # flush the export buffers
        self.exporter.flush()
        return 0
