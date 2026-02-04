# Copyright 2024-2025 IBM Corporation

import statistics
import aiu_trace_analyzer.logger as aiulog
from aiu_trace_analyzer.types import TraceEvent
from aiu_trace_analyzer.pipeline import AbstractContext


class PowerStatisticsContext(AbstractContext):
    """
    Context for accumulating power counter and kernel event data.
    Computes time-weighted power statistics in drain() after all events processed.
    
    Power periods are split based on kernel event overlaps to provide accurate
    statistics for periods with and without kernel execution.
    """
    
    def __init__(self):
        super().__init__()
        # Collect power counters: [(timestamp, power_value), ...]
        self.power_counters = []
        
        # Collect kernel periods: [(start_ts, end_ts), ...]
        self.kernel_periods = []
    
    def _merge_periods(self, periods):
        """
        Merge overlapping time periods into non-overlapping segments.
        Essential to prevent double-counting duration when kernels overlap.
        """
        if not periods:
            return []
        
        sorted_periods = sorted(periods)
        merged = [sorted_periods[0]]
        
        for start, end in sorted_periods[1:]:
            last_start, last_end = merged[-1]
            if start <= last_end:
                # Overlapping, merge by extending the end time
                merged[-1] = (last_start, max(last_end, end))
            else:
                # Non-overlapping, add as a new distinct segment
                merged.append((start, end))
        
        return merged
    
    def _split_power_period(self, power_start, power_end, power_value, kernel_timeline):
        """
        Slice a single power measurement period into sub-segments based on kernel activity.
        
        Args:
            power_start/end: Timestamps for the current power value.
            power_value: Power in Watts.
            kernel_timeline: List of merged (start, end) kernel periods.
            
        Returns:
            List of (duration, power, has_kernel) tuples.
        """
        segments = []
        current_pos = power_start
        
        for k_start, k_end in kernel_timeline:
            # Skip if kernel period is entirely outside the power period
            if k_end <= power_start or k_start >= power_end:
                continue
            
            # Calculate the specific overlap window
            overlap_start = max(power_start, k_start)
            overlap_end = min(power_end, k_end)
            
            # 1. Segment before kernel starts (No Kernel)
            if current_pos < overlap_start:
                segments.append((overlap_start - current_pos, power_value, False))
            
            # 2. Segment during kernel execution (Has Kernel)
            segments.append((overlap_end - overlap_start, power_value, True))
            
            current_pos = overlap_end
        
        # 3. Final segment after last kernel in this window ends (No Kernel)
        if current_pos < power_end:
            segments.append((power_end - current_pos, power_value, False))
        
        return segments
    
    def _compute_weighted_stats(self, segments):
        """
        Compute time-weighted statistics from power segments.
        Distinguishes between operational power (non-zero) and total energy flow.
        """
        if not segments:
            return None
        
        # Total metrics including zero values (system energy perspective)
        total_duration = sum(dur for dur, _ in segments)
        weighted_sum_total = sum(dur * power for dur, power in segments)
        avg_total = weighted_sum_total / total_duration if total_duration > 0 else 0
        
        # Non-zero metrics (hardware operational intensity perspective)
        non_zero_segments = [(dur, p) for dur, p in segments if p > 0]
        non_zero_duration = sum(dur for dur, p in non_zero_segments)
        
        # mean_non_zero: Average power specifically when the device is "active/powered"
        mean_non_zero = sum(dur * p for dur, p in non_zero_segments) / non_zero_duration if non_zero_duration > 0 else 0
        
        # median_non_zero: The 50th percentile of power values weighted by their duration (excluding 0s)
        median_non_zero = 0
        if non_zero_segments:
            sorted_segments = sorted(non_zero_segments, key=lambda x: x[1])
            half_nz_dur = non_zero_duration / 2
            cumulative_dur = 0
            for dur, power in sorted_segments:
                cumulative_dur += dur
                if cumulative_dur >= half_nz_dur:
                    median_non_zero = power
                    break
        
        # Extract power values for min/max
        all_powers = [p for _, p in segments]
        nz_powers = [p for _, p in non_zero_segments]
        
        return {
            'min_non_zero': min(nz_powers) if nz_powers else 0.0,
            'max': max(all_powers) if all_powers else 0.0,
            'mean_non_zero': mean_non_zero,
            'median_non_zero': median_non_zero,
            'avg_total': avg_total,
            'dur_total': total_duration,
            'dur_non_zero': non_zero_duration
        }
    
    def drain(self):
        """
        Compute and report time-weighted power statistics after all events processed.
        """
        if not self.power_counters or len(self.power_counters) < 2:
            aiulog.log(aiulog.WARN, "Insufficient power data (need at least 2 samples) for statistics")
            return []
        
        # Step 1: Sort and define power intervals
        self.power_counters.sort(key=lambda x: x[0])
        power_periods = []
        for i in range(len(self.power_counters) - 1):
            start_ts, power = self.power_counters[i]
            end_ts = self.power_counters[i + 1][0]
            power_periods.append((start_ts, end_ts, power))
        
        # Step 2: Clean up kernel timeline
        kernel_timeline = self._merge_periods(self.kernel_periods)
        
        # Step 3: Fragment power periods by kernel activity
        all_segments = []
        for start, end, power in power_periods:
            all_segments.extend(self._split_power_period(start, end, power, kernel_timeline))
        
        # Step 4: Group segments into Scenario 1 (With Kernels) and Scenario 2 (Without Kernels)
        with_kernels = [(dur, p) for dur, p, has_k in all_segments if has_k]
        without_kernels = [(dur, p) for dur, p, has_k in all_segments if not has_k]
        
        # If no kernel activity was detected at all, treat the entire trace as Scenario 2
        if not self.kernel_periods and not without_kernels:
            without_kernels = [(dur, p) for dur, p, _ in all_segments]
        
        # Step 5: Compute and Log results
        for label, data in [("Power with kernels", with_kernels), ("Power without kernels", without_kernels)]:
            stats = self._compute_weighted_stats(data)
            if stats:
                line = (f"{label}: "
                        f"min_non_zero={stats['min_non_zero']:.2f}W, "
                        f"max={stats['max']:.2f}W, "
                        f"mean_non_zero={stats['mean_non_zero']:.2f}W, "
                        f"median_non_zero={stats['median_non_zero']:.2f}W, "
                        f"avg_total={stats['avg_total']:.2f}W "
                        f"(time-weighted, dur_total={stats['dur_total']:.2f}ms, "
                        f"dur_non_zero={stats['dur_non_zero']:.2f}ms)")
                aiulog.log(aiulog.INFO, line)
            else:
                aiulog.log(aiulog.INFO, f"{label}: No data")
        
        return []


def analyze_power_statistics(event: TraceEvent, context: AbstractContext) -> list[TraceEvent]:
    """
    Dispatcher to collect raw power and kernel timing data from the trace.
    """
    assert isinstance(context, PowerStatisticsContext)
    
    ts = event.get("ts")
    if not ts:
        return [event]
    
    # Identify Power Counter events
    if event.get("ph") == "C" and event.get("name") == "Power":
        if "args" in event and "Watts" in event["args"]:
            context.power_counters.append((ts, event["args"]["Watts"]))
    
    # Identify Kernel execution periods
    elif event.get("ph") == "X" and "Cmpt Exec" in event.get("name", ""):
        dur = event.get("dur", 0)
        if dur > 0:
            context.kernel_periods.append((ts, ts + dur))
    
    return [event]