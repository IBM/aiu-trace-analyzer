# Copyright 2024-2025 IBM Corporation

import copy
import aiu_trace_analyzer.logger as aiulog

from aiu_trace_analyzer.pipeline.timesync import _assign_ts_dur, _create_dev_ts_list_in_us

# we certainly need that TraceEvent
from aiu_trace_analyzer.types import TraceEvent

# .. and the AbstractContext (or any of it's derived classes)
from aiu_trace_analyzer.pipeline import AbstractContext


def tripple_phased_events(event: TraceEvent, _: AbstractContext, config: dict) -> list[TraceEvent]:
    '''
    split single X event into 3 phases (DmaI, Cmpt, DmaO)
    time stamps are extracted from previously injected 'ts_all' entry which has converted timestamps already
    2nd set of time stamps, in device timezone and epoch are kept in 'ts_dev' dict, unit in micro-second.
    '''

    if event["ph"] == "X":
        if "args" not in event or "TS1" not in event["args"]:
            return [event]

        if " Prep" in event["name"]:
            return []

        # Let T_dmai = T_ts[1], Duration_dmai = T_ts[2] - T_ts[1]
        # Let T_cmpt = T_ts[3], Duration_cmpt = T_ts[4] - T_ts[3]
        # Let T_dmao = T_ts[4], Duration_dmao = T_ts[5] - T_ts[4]

        # Create a new TS step array, with device's epoch
        assert ("soc_frequency" in config)
        # DO: move out of here
        event["args"]["ts_dev"] = _create_dev_ts_list_in_us(event["args"], config["soc_frequency"])

        # Decide to use host epoch or device epoch
        converted = event["args"]["ts_all"]

        aiulog.log(aiulog.TRACE, "tripple_phased_events 3: ", converted)

        revents = []

        # Craete 3 events out of one
        event1 = copy.deepcopy(event)
        event1["name"] += "_DmaI" if "DmaI" not in event1["name"] else ""
        event1["ts"], event1["dur"] = _assign_ts_dur(1, 2, converted)
        event2 = copy.deepcopy(event)
        event2["name"] += "_Cmpt" if "Cmpt" not in event2["name"] else ""
        event2["ts"], event2["dur"] = _assign_ts_dur(3, 4, converted)
        event3 = copy.deepcopy(event)
        event3["name"] += "_DmaO" if "DmaO" not in event3["name"] else ""
        event3["ts"], event3["dur"] = _assign_ts_dur(4, 5, converted)

        if " DmaI" in event["name"]:
            event1["ts"], event1["dur"] = _assign_ts_dur(1, 2, converted)
            _ = event1["ts"] + event1["dur"]
            dev_ts = event1["args"]["ts_dev"]
            dev_ts[2] = dev_ts[3] = dev_ts[4] = dev_ts[1]   # alter TSx for x > 1
            # event1["dur"] = min(0.2, event1["dur"])
            event2["dur"] = 0
            event3["dur"] = 0

        elif " Cmpt Ex" in event["name"]:
            event2["ts"], event2["dur"] = _assign_ts_dur(3, 4, converted)
            dev_ts = event2["args"]["ts_dev"]
            # alter TSx for x not in [2,3]
            dev_ts[0] = dev_ts[1] = dev_ts[2]
            dev_ts[4] = dev_ts[3]
            # event2["dur"] = max(0.2, event2["dur"])
            event1["dur"] = 0
            event3["dur"] = 0

        elif " DmaO" in event["name"]:
            event3["ts"], event3["dur"] = _assign_ts_dur(4, 5, converted)
            dev_ts = event3["args"]["ts_dev"]
            dev_ts[0] = dev_ts[1] = dev_ts[2] = dev_ts[3]   # alter TSx for x < 3
            # event3["dur"] = max(0.2, event3["dur"])
            event1["dur"] = 0
            event2["dur"] = 0

        if event1["dur"] > 0:
            revents.append(event1)

        if event2["dur"] > 0:
            revents.append(event2)

        if event3["dur"] > 0:
            revents.append(event3)

        return revents
    return [event]
