# Copyright 2024-2025 IBM Corporation

import numpy as np
import copy
import aiu_trace_analyzer.logger as aiulog
from aiu_trace_analyzer.types import TraceEvent
from aiu_trace_analyzer.pipeline import AbstractContext


PRE_TIGHTENED = True


def get_cycle_ts_as_clock(index: int, ts_list: list[float]) -> float:
    '''
    for consistent access to the TS1-5 timestamps after converting to wallclock
    When referring to e.g. TS3, we can use this fn to get the correct ts from the
    converted list (which is at idx=2)
    '''
    return ts_list[index - 1]


def _create_dev_ts_list_in_us(dev_args, freq: float):
    ret_dict = []
    for key in ["TS1", "TS2", "TS3", "TS4", "TS5"]:
        ret_dict.append(int(dev_args[key]) / freq)
    return ret_dict


def _assign_ts_dur(a: int, b: int, ts_list: list[float], overlap_tolerance=1.0/560.0) -> tuple[float, float]:
    '''
    NOTE: takes the TS[1-5] numbered timestamps a,b and returns/computes the wallclock ts
          and dur from the ts_list using adjusted indices

    overlap_tolerance      since slices would overlap if the end exactly matches the next event begin,
                           the tolerance is subtracted from the duration to prevent this
    '''
    duration = 0.0
    ts_a = get_cycle_ts_as_clock(a, ts_list)
    ts_b = get_cycle_ts_as_clock(b, ts_list)
    if ts_a + overlap_tolerance <= ts_b:
        duration = round((ts_b - ts_a - overlap_tolerance / 2.0), 3)  # cut off at ns granularity
        aiulog.log(aiulog.TRACE, "TS CONV:", ts_a, ts_b, duration)
        assert (duration > 0)
    return ts_a, duration


def _conv_DTS_to_array_in_us(event: TraceEvent, freq: float) -> list[float]:
    converted = [0] * 5
    for i, ts in enumerate(["TS1", "TS2", "TS3", "TS4", "TS5"]):
        dts_i = float(event["args"][ts]) / freq
        converted[i] = dts_i

    event["args"]["ts_dev"] = copy.deepcopy(converted)
    return converted


def _get_DTS_rela_to_TS1_in_us(event: TraceEvent, freq: float) -> list[float]:
    converted = _conv_DTS_to_array_in_us(event, freq)
    converted = [converted[i] - converted[0] for i in range(5)]
    return converted


def _get_DTS_rela_to_TSRef_in_us(event: TraceEvent, freq: float, ref_idx: int) -> list[float]:
    converted = _conv_DTS_to_array_in_us(event, freq)
    converted = [converted[i] - converted[ref_idx] for i in range(5)]
    return converted


def _convert_cycle_timestamps(event: TraceEvent, freq: float) -> list[float]:
    '''
    runs through the list of TS1-5 in event.args and creates a list with converted timestamps in event.args
    conversion is based on wallclock ts+dur of event and aiu clock frequency:
        Let T_ts5 = T_flex_end
        Let T_ts[i] = T_ts5 - (TS5-TS[i])/Freq, for i in 1-4

    THIS FN HAS A NASTY SIDE EFFECT OF UPDATING event["ts"] and event["dur"] !!!
    '''
    ref_idx = 4
    if event["name"].endswith("Cmpt Prep"):
        ref_idx = 2
    if event["name"].endswith(" DmaI"):
        ref_idx = 1
    if event["name"].endswith("Cmpt Exec"):
        ref_idx = 3

    wall_clock_tref = event["ts"] + event["dur"]
    converted = _get_DTS_rela_to_TSRef_in_us(event, freq, ref_idx)
    converted = [wall_clock_tref + converted[i] for i in range(5)]
    orig_ts = (event["ts"], event["dur"])
    event["dur"] = converted[ref_idx] - converted[0]
    event["ts"] = wall_clock_tref - event["dur"]
    if event["ts"] != orig_ts[0] or event["dur"] != orig_ts[1]:
        event["args"]["time_adjust"] = {"ts": event["ts"] - orig_ts[0],
                                        "dur": event["dur"] - orig_ts[1]}

    assert event["ts"] >= 0.0, f"new event ts < 0.0 {event}, {converted}, {wall_clock_tref}"

    aiulog.log(aiulog.TRACE, "TS CONV: _convert_cycle_timestamps", event["ts"], event["dur"], converted)
    return converted


def _align_hts_to_beg(event: TraceEvent, freq: float) -> list[float]:
    hts_beg = event["ts"]
    converted = _get_DTS_rela_to_TS1_in_us(event, freq)
    converted = [hts_beg + converted[i] for i in range(5)]
    event["dur"] = converted[4] - converted[0]

    aiulog.log(aiulog.TRACE, "TS CONV: _align_hts_to_beg: ", event["ts"], event["dur"], converted)
    return converted


def _align_hts_by_type(op_id: int, event: TraceEvent, freq: float) -> list[float]:
    """
    THIS FN HAS A NASTY SIDE EFFECT OF UPDATING event["ts"] and event["dur"] !!!
    """
    converted = _get_DTS_rela_to_TS1_in_us(event, freq)
    dts_intervals = [converted[i+1] - converted[i] for i in range(4)]

    hts_end = event["ts"] + event["dur"]
    event["dur"] = dts_intervals[op_id]
    event["ts"] = hts_end - event["dur"]
    assert event["ts"] >= 0.0, f"new event ts < 0.0 {event}, {hts_end}, {dts_intervals}"

    converted = [(converted[i] - converted[op_id + 1] + hts_end) for i in range(5)]

    aiulog.log(aiulog.TRACE, "TS CONV: _align_hts_by_type: ", op_id, event["ts"], event["dur"], converted)
    return converted


def _align_dts_to_hts(event: TraceEvent) -> list[float]:
    '''
    runs through the list of device-ts generated by the mp_sync_v2 pass
    creates a list device-ts aligned to the sync'd host-ts associated with the event.
        Let ts_all[i] = hts_end - (ts_dev[5] - ts_dev[i]), for i in 1-5; ts_dev are in micro-sec unit
    '''
    wall_clock_t5 = event["ts"] + event["dur"]
    converted = [0] * 5
    dts = event["args"]["ts_dev"]
    dts_last = float(dts[4])
    for i, ts in enumerate(dts):
        converted[i] = wall_clock_t5 - (dts_last - float(dts[i]))
    aiulog.log(aiulog.DEBUG, "TS CONV 2:", converted)
    return converted


def cycle_count_to_wallclock(event: TraceEvent, _: AbstractContext, config: dict) -> list[TraceEvent]:
    '''
    convert the cycle-based TS1-5 values into wallclock based on ts+dur for the given event
    stores the 5 converted values in event.args.ts_all for later use
    '''
    assert ("soc_frequency" in config)

    # we can only do that conversion if the event has all necessary data
    if event["ph"] == "X" and "args" in event and "TS1" in event["args"]:
        event["args"]["ts_all"] = _convert_cycle_timestamps(event, config["soc_frequency"])
        assert event["ts"] >= get_cycle_ts_as_clock(1, event["args"]["ts_all"]), \
            f"TS1 is projected before the event timestamp. Please check the frequency setting. {event}"
        assert event["ts"] + event["dur"] <= get_cycle_ts_as_clock(5, event["args"]["ts_all"]), \
            f"TS5 is projected past the end of the event. Please check the frequency setting. {event}"

        last = event["args"]["ts_all"][0]
        for i, t in enumerate(event["args"]["ts_all"][1:]):
            assert last <= t, f'TS{i + 2} is smaller than TS{i + 1}, {event["name"]}, {event["args"]}'
            last = t

    return [event]


def _match_opIds_from_event(event: TraceEvent):
    name = event["name"]
    op_keywords = [" DmaI", " Cmpt Prep", " Cmpt Exec", " DmaO"]
    op_id_map = [key in name for key in op_keywords]
    np_op_ids = np.array(op_id_map)
    op_ids = np.nonzero(np_op_ids)[0]
    return op_ids


def get_opIds_from_event(event: TraceEvent) -> int:

    op_ids = _match_opIds_from_event(event)

    if len(op_ids) >= 1:
        return op_ids[0]
    else:
        return 0


def tighten_hts_by_instr_type(event: TraceEvent, _: AbstractContext, config: dict) -> list[TraceEvent]:
    '''
    convert the cycle-based TS1-5 values into wallclock based on ts+dur for the given event
    stores the 5 converted values in event.args.ts_all for later use
    '''
    assert ("soc_frequency" in config)

    # we can only do that conversion if the event has all necessary data
    if event["ph"] == "X" and "args" in event and "TS1" in event["args"]:
        op_ids = _match_opIds_from_event(event)

        if len(op_ids) < 1:     # instruction type that we have not analyzed, align TS1 to HTS
            ret = _align_hts_to_beg(event, config["soc_frequency"])
        elif PRE_TIGHTENED:     # align TS[i+2] to HTS+DUR for opIds==i, and let DUR = TS[i+2]-TS[i+1]
            ret = _align_hts_by_type(op_ids[0], event, config["soc_frequency"])
        else:                   # earliest implementation, align TS5 to HTS+DUR, and adjust HTS
            ret = _convert_cycle_timestamps(event, config["soc_frequency"])

        event["args"]["ts_all"] = ret

    return [event]


def realign_dts_to_hts(event: TraceEvent, _: AbstractContext) -> list[TraceEvent]:
    '''
    convert the cycle-based TS1-5 values into wallclock based on ts+dur for the given event
    stores the 5 converted values in event.args.ts_all for later use
    '''
    # we can only do that conversion if the event has all necessary data
    if event["ph"] == "X" and "args" in event and "TS1" in event["args"]:
        event["args"]["ts_all"] = _align_dts_to_hts(event)
    return [event]


def cycle_count_conversion_cleanup(event: TraceEvent, _: AbstractContext) -> list[TraceEvent]:
    '''
    no need to keep the temporary data in the args argument for the final output
    therefore establishing this function to cleanup previously inserted data
    '''
    if "args" in event and "ts_all" in event["args"]:
        event["args"].pop("ts_all")
    if "args" in event and "jobhash" in event["args"]:
        event["args"].pop("jobhash")
    return [event]


def cleanup_copy_of_device_ts(event: TraceEvent, _: AbstractContext) -> list[TraceEvent]:
    '''
    ts_dev records are a copy device TS# adjusted to micro-second
    '''
    if "args" in event and "ts_dev" in event["args"]:
        event["args"].pop("ts_dev")
    return [event]
