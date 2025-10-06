# Copyright 2024-2025 IBM Corporation

import math
import copy

import aiu_trace_analyzer.logger as aiulog
from aiu_trace_analyzer.types import TraceEvent
from aiu_trace_analyzer.pipeline import AbstractContext, AbstractHashQueueContext


class IterationStatus(object):
    def __init__(self,
                 reference_word: str = "",
                 collected_word: str = "",
                 collected_ts: list[float] = [],
                 previous_ts: float = 0.0,
                 avg_time: float = 0.0,
                 iterations: int = 0) -> None:
        self.referernce_word = reference_word
        self.collected_word = collected_word
        self.collected_ts = copy.deepcopy(collected_ts)
        self.previous_ts = previous_ts
        self.avg_time = avg_time
        self.iterations = iterations
        self.restarted = False


class IterationDectectContext(AbstractHashQueueContext):
    min_run_len = 5

    def __init__(self) -> None:
        super().__init__()
        # queues-map stores IterationStatus for each pid

    def detect_iteration(self, event: TraceEvent):
        letter = chr(65 + hash(event["name"]) % 26)
        pid = event["pid"]
        ts = event["ts"]

        if pid not in self.queues:
            self.queues[pid] = IterationStatus()
        i_stat: IterationStatus = self.queues[pid]

        # either way: append to currently collected word
        i_stat.collected_word += letter
        i_stat.collected_ts.append(ts)

        if math.isclose(i_stat.previous_ts, 0.0, abs_tol=1e-9):
            # means: the reference word is still unknown and collected word is accumulating
            i_stat.referernce_word += letter

            pref_len, new_ref_word = self.detect_reference_word(i_stat.referernce_word)
            # if the returned word is shorter, we know we found a repeating sequence
            detected = len(new_ref_word) < len(i_stat.referernce_word)

            aiulog.log(aiulog.TRACE, "ITSTATS: ", detected, " reference sequence:", len(new_ref_word), new_ref_word[-1])
            if detected:
                rword_len = len(new_ref_word)
                aiulog.log(aiulog.DEBUG, "ITSTATS: Found reference iteration sequence:", rword_len, new_ref_word)
                i_stat.referernce_word = new_ref_word
                # prev_ts to be set to where the current collected word starts
                i_stat.previous_ts = i_stat.collected_ts[pref_len+rword_len]
                # first entry: mean == value
                i_stat.avg_time = i_stat.previous_ts - i_stat.collected_ts[pref_len]
                i_stat.collected_word = ""
                i_stat.collected_ts = []
                i_stat.iterations = 1
                aiulog.log(aiulog.DEBUG, "ITSTATS: Found iteration:", i_stat.previous_ts, i_stat.avg_time)

        else:
            if len(i_stat.collected_word) > len(i_stat.referernce_word):
                aiulog.log(aiulog.DEBUG,
                           "ITSTATS: NO_MATCH for iteration."
                           " Kernel sequences interupted by missing/additional event. Will retry with later iteration.")
                aiulog.log(aiulog.TRACE, "ITSTATS: REFERENCE:", i_stat.referernce_word)
                aiulog.log(aiulog.TRACE, "ITSTATS: COLLECTED:", i_stat.collected_word)
                i_stat = self.try_skip_kernel(i_stat)
                i_stat.restarted = True

            if self.match(i_stat.referernce_word, i_stat.collected_word):
                this_interval = (i_stat.collected_ts[0] - i_stat.previous_ts)
                if not i_stat.restarted:
                    i_stat.iterations += 1
                    i_stat.avg_time += (this_interval - i_stat.avg_time)/i_stat.iterations  # accumulated average
                    aiulog.log(aiulog.DEBUG, "ITSTATS: Found iteration:", i_stat.previous_ts, this_interval)
                else:
                    i_stat.restarted = False
                    aiulog.log(aiulog.DEBUG, "ITSTATS: Skipping iteration:", i_stat.previous_ts, this_interval)
                i_stat.previous_ts = i_stat.collected_ts[0]
                # reset collected word and ts
                i_stat.collected_word = ""
                i_stat.collected_ts = []

    def match(self, ref: str, cmp: str) -> bool:
        if len(ref) != len(cmp):
            return False
        return ref == cmp

    def detect_reference_word(self, word: str) -> tuple[int, str]:
        '''
        we can only detect the reference word the moment we find the same sequence twice
        -> reference word appears the moment when the 'end' of the total sequence matches a part in the beginning
        Challenge: the could be prefix/one-off kernels running before the first iteration
        that need to be detected and filtered out
        '''
        wlen = len(word)
        if wlen < 2 * self.min_run_len:
            return 0, word

        # if ref and cmp need to have equal len, then a prefix can have a max len of 1/3 of the total len
        max_prefix = int(math.floor(wlen / 3))
        for pref in range(max_prefix):
            if (wlen - pref) % 2 == 1:   # if the remaining word len is not even, don't bother checking
                continue
            half_point = (wlen - pref) >> 1
            if self.match(word[pref:pref + half_point], word[pref + half_point:]):
                return pref, word[pref:pref + half_point]
        return 0, word

    def try_skip_kernel(self, i_stat: IterationStatus) -> IterationStatus:
        skip_letters = 1
        rem_len = min(len(i_stat.collected_word), len(i_stat.referernce_word))

        while rem_len > 0:
            if i_stat.referernce_word[:rem_len] != i_stat.collected_word[skip_letters:skip_letters+rem_len]:
                skip_letters += 1
                rem_len -= 1

            else:
                i_stat.collected_word = i_stat.collected_word[skip_letters:skip_letters+rem_len]
                i_stat.collected_ts = i_stat.collected_ts[skip_letters:skip_letters+rem_len]
                aiulog.log(aiulog.DEBUG, "ITSTATS:",
                           f'detected new iteration by skipping {skip_letters}, match_len {rem_len}')
                aiulog.log(aiulog.TRACE, "ITSTATS: COLLECTED:", i_stat.collected_word)
                return i_stat

        aiulog.log(aiulog.DEBUG, "ITSTATS: No subset of reference word found. Resetting collected word.")
        i_stat.collected_ts = []
        i_stat.collected_word = ""
        return i_stat

    def drain(self) -> list[TraceEvent]:
        for pid, data in self.queues.items():
            aiulog.log(aiulog.INFO, "ITSTATS:",
                       f'[{pid}] avg_iteration_time={data.avg_time}, iterations={data.iterations}')
        return []


def collect_iteration_stats(event: TraceEvent, context: AbstractContext) -> list[TraceEvent]:
    assert isinstance(context, IterationDectectContext)

    if event["ph"] in "X" and "args" in event and "TS1" in event["args"] and "Cmpt Exec" in event["name"]:
        context.detect_iteration(event)

    return [event]
