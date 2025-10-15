# Copyright 2024-2025 IBM Corporation

import json


class TraceView(object):
    def __init__(
        self,
        trace_events=[],
        display_time_unit="ms",
        system_trace_events="SystemTraceData",
        other_data={},
        stack_frames={},
        samples=[],
    ):
        self.trace_events = list(trace_events)
        self.display_time_unit = display_time_unit
        self.system_trace_events = system_trace_events
        self.other_data = dict(other_data)
        self.stack_frames = dict(stack_frames)
        self.samples = list(samples)
        self.device_data = []
        self.meta_data = {}

    def append_trace_event(self, trace_event):

        if not isinstance(trace_event, dict):
            print("NOT A DICTIONARY:", type(trace_event), trace_event)
            assert False
        self.trace_events.append(trace_event)

    def add_stack_frame(self, sf, stack_frame):
        """
        sf: id for a stack frame object
        stack_frame: stack frame object itself

          stack_frame consists of the following keys
          - name: function name
          - parent: the id of its parent
          - category: app name or something
        """
        self.stack_frames[sf] = stack_frame

    def add_device_data(self, data: list[dict]):
        for d in data:
            self.device_data.append(d)

    def add_metadata(self, meta: dict):
        self.meta_data.update(meta)

    def dump(self, fp=None) -> str:
        """
        trace event json format:
        {
           "traceEvents": [
             {"name": "Asub", "cat": "PERF", "ph": "B", "pid": 22630, "tid": 22630, "ts": 829},
             {"name": "Asub", "cat": "PERF", "ph": "E", "pid": 22630, "tid": 22630, "ts": 833}
           ],
           "displayTimeUnit": "ms",
           "systemTraceEvents": "SystemTraceData",
           "otherData": {
              "version": "My Application v1.0"
           },
           "stackFrames": {...},
           "samples": [...],
        }
        """
        dic = {
            "deviceProperties": self.device_data,
            "traceName": self.other_data["Settings"]["output"],
            "traceEvents": self.trace_events,
            "displayTimeUnit": self.display_time_unit,
            # "systemTraceEvents": self.system_trace_events,
            "otherData": self.other_data,
            # "stackFrames": self.stack_frames,
            # "samples": self.samples,
        }
        dic.update(self.meta_data)

        if fp:
            json.dump(dic, fp, indent=4)
        else:
            return json.dumps(dic)


class AbstractEventType(object):
    def _del_none(self, dic):
        for k, v in list(dic.items()):
            if v is None:
                del dic[k]

    def json(self):
        return self.__dict__

    @classmethod
    def from_dict(self, event):
        """
        convert an event in the form of a python dictionary into TraceView events
        """
        # create events based on their 'ph'
        etype = event["ph"]
        if etype == "B" or etype == "E":
            new_event = DurationEvents(ph=event["ph"],
                                       ts=event["ts"],
                                       pid=event["pid"],
                                       tid=event["tid"],
                                       name=event["name"],
                                       args=event["args"] if "args" in event else {},
                                       cat=event["cat"] if "cat" in event else "",
                                       )
        elif etype == "X":
            new_event = CompleteEvents(name=event["name"],
                                       cat=event["cat"] if "cat" in event else "",
                                       ts=event["ts"],
                                       dur=event["dur"],
                                       pid=event["pid"],
                                       tid=event["tid"],
                                       args=event["args"] if "args" in event else {})
        elif etype == "C":
            new_event = CounterEvents(name=event["name"],
                                      ts=event["ts"],
                                      pid=event["pid"],
                                      cat=event["cat"] if "cat" in event else "",
                                      args=event["args"],
                                      )
        elif etype in ["b", "e"]:
            # TODO: AsyncEvents needs extension
            new_event = AsyncEvents(ph=event["ph"],
                                    ts=event["ts"],
                                    pid=event["pid"],
                                    tid=event["tid"],
                                    name=event["name"],
                                    id=event["id"],
                                    args=event["args"] if "args" in event else {},
                                    cat=event["cat"] if "cat" in event else "",
                                    )
        elif etype in ["s", "f"]:
            new_event = FlowEvents(ph=event["ph"],
                                   ts=event["ts"],
                                   id=event["id"],
                                   pid=event["pid"],
                                   tid=event["tid"],
                                   name=event["name"],
                                   cat=event["cat"],
                                   bp=event["bp"] if "bp" in event else None
                                   )
        elif etype in ["M"]:
            new_event = MetaEvents(ph=event["ph"],
                                   name=event["name"],
                                   ts=event["ts"],
                                   pid=event["pid"],
                                   args=event["args"],
                                   tid=event["tid"] if "tid" in event else None)
        elif etype in ["i"]:
            new_event = InstantEvents(
                name=event["name"],
                cat=event["cat"] if "cat" in event else None,
                ts=event["ts"],
                pid=event["pid"],
                tid=event["tid"],
                s=event["s"],
                args=event["args"] if "args" in event else None)
        else:
            raise Exception(f"Unimplemented or invalid Event-PH: {etype}")
        return new_event


class DurationEvents(AbstractEventType):
    """
    Duration events provide a way to mark the duration of work on a given thread. The
    duration event is specified by the B and E phase types. The B event must come before
    the corresponding E event. You can nest the B and E events. This allows you to capture
    the function calling behaviour on a thread.
    """

    def __init__(self, ph, ts, pid, tid, name=None, cat=None, args={}):
        """
        name: the name of this event (optional)
        cat: the category of this event (optional)
        ph: phase (B or E, requried)
        tid: thread id (requried)
        ts: timestamp (required)
        args: arguments (optional)
        """
        assert ph in ["B", "E"], "ph should be B or E"
        self.ph = ph
        self.ts = ts
        self.pid = pid
        self.tid = tid
        self.name = name.strip()
        self.cat = cat
        self.args = args

    def is_begin(self):
        return self.ph == "B"

    def is_end(self):
        return self.ph == "E"

    def json(self):
        return self.__dict__


class CompleteEvents(AbstractEventType):
    """
    Each complete event logically combines a pair of duration (B and E) events. The
    complete events are designated by the X phase type. In a trace that most of the events
    are duration events, using complete events to replace the duration events can reduce
    the size of the trace to about half.
    """

    def __init__(self, name, cat, ts, dur, pid, tid, args={}):
        self.name = name
        self.cat = cat
        self.ph = "X"
        self.ts = ts
        self.dur = dur
        self.pid = pid
        self.tid = tid
        self.args = args

    def json(self):
        return self.__dict__


class InstantEvents(AbstractEventType):
    """
    The instant events correspond to something that happens but has no duration associated
    with it. For example, vblank events are considered instant events. The instant events
    are designated by the i phase type.

    Example format:
    {"name": "OutOfMemory", "ph": "i", "ts": 1234523.3, "pid": 2343, "tid": 2347, "s": "g"}
    """

    def __init__(self, name, cat, ts, pid, tid, s="g", args={}):
        """
        name: name of event
        ph: phase
        ts: time stamp
        pid: process id
        s: stack trace
        """
        self.name = name
        self.cat = cat
        self.ph = "i"
        self.ts = ts
        self.pid = pid
        self.tid = tid
        self.s = s
        self.args = args


class CounterEvents(AbstractEventType):
    """
    The counter events can track a value or multiple values as they change over
    time. Counter events are specified with the C phase type. Each counter can be provided
    with multiple series of data to display. When multiple series are provided they will
    be displayed as a stacked area chart in Trace Viewer. When an id field exists, the
    combination of the event name and id is used as the counter name. Please note that
    counters are process-local.

    Counter Event Example:
    {..., "name": "ctr", "ph": "C", "ts":  0, "args": {"cats":  0}},
    {..., "name": "ctr", "ph": "C", "ts": 10, "args": {"cats": 10}},
    {..., "name": "ctr", "ph": "C", "ts": 20, "args": {"cats":  0}}


    In the above example the counter tracks a single series named cats. The cats series
    has a value that goes from 0 to 10 and back to 0 over a 20μs period.

    Multi Series Counter Example:
    {..., "name": "ctr", "ph": "C", "ts":  0, "args": {"cats":  0, "dogs": 7}},
    {..., "name": "ctr", "ph": "C", "ts": 10, "args": {"cats": 10, "dogs": 4}},
    {..., "name": "ctr", "ph": "C", "ts": 20, "args": {"cats":  0, "dogs": 1}}

    In this example we have a single counter named ctr. The counter has two series of
    data, cats and dogs. When drawn, the counter will display in a single track with the
    data shown as a stacked graph.
    """

    def __init__(self, name, ts, pid, args={}, cat=None):
        self.name = name.strip()
        self.ts = ts
        self.ph = "C"
        self.pid = pid
        # self.tid = tid
        self.cat = cat
        self.args = args


class AsyncEvents(AbstractEventType):
    """
    Async events are used to specify asynchronous operations. e.g. frames in a game, or
    network I/O. Async events are specified with the b, n and e event types. These three
    types specify the start, instant and end events respectively. You can emit async
    events from different processes and different threads.  Each async event has an
    additional required parameter id.  We consider the events with the same category and
    id as events from the same event tree. An optional scope string can be specified to
    avoid id conflicts, in which case we consider events with the same category, scope,
    and id as events from the same event tree. For instance, if we have an event, A, and
    its child event, B. Trace viewer will infer the parent-child relationship of A and B
    from the fact that event A and B have the same category and id, and the fact that A’s
    start and end time pair encompasses that of B. When displayed, an entire async event
    chain will be drawn such that the parent will be the top slice, and its children are
    in rows beneath it. The root of the async event tree will be drawn as the top-most
    slice with a dark top border.


    Async Event Example:
    {"cat": "foo", "name": "async_read", "id": 0x100, "ph": "b", "args": {"name" : "~/.bashrc"}},
    {"cat": "foo", "name": "async_read", "id": 0x100, "ph": "e"}
    """

    def __init__(self, ph, ts, pid, tid, name, id, cat=None, args={}):
        self.name = name
        self.ts = ts
        self.pid = pid
        self.tid = tid
        self.cat = cat
        self.id = id
        # ph should be either b or e
        # - b: nested start
        # - n: nested instant
        # - e: nested end
        assert ph in ["b", "n", "e"]
        self.ph = ph
        self.args = args


class FlowEvents(AbstractEventType):
    """
    The flow events are very similar in concept to the Async events, but allow duration  to be
    associated with each other across threads/processes. Visually, think of a flow event
    as an arrow between two duration events. With flow events, each event will be drawn in
    the thread it is emitted from. The events will be linked together in Trace Viewer
    using lines and arrows.
    """

    def __init__(self, name, cat, ph, ts, pid, tid, id, bp=None):
        self.name = name
        self.cat = cat
        # ph should be either s, t, or f
        assert ph in ["s", "t", "f"]
        self.ph = ph
        self.ts = ts
        self.pid = pid
        self.tid = tid
        self.id = id
        if bp:
            self.bp = bp


class MetaEvents(AbstractEventType):
    """
    Metadata events are used to associate extra information with the events in the trace file.
    This informationcan be things like process names, or thread names. Metadata events are
    denoted by the M phase type.The argument list may be empty.
    {"name": "thread_name", "ph": "M", "pid": 2343, "tid": 2347,"args": {"name" : "RendererThread"}}
    """

    def __init__(self, name, args, ph, ts, pid, tid=None):
        self.name = name
        assert ph in ["M"]
        self.ph = ph
        self.ts = ts
        self.pid = pid
        if tid:
            self.tid = tid
        self.args = args


class SampleEvents(AbstractEventType):
    """
    Sample events provide a way of adding sampling-profiler based results in a trace. They get
    shown as a separate track in the specified thread. Sample events are designated by the
    P phase type.

    Sampling Event Example:
    {"name": "sample", "cat": "foo", "ph": "P", "ts": 123, "pid": 234, "tid": 645 }
    """

    def __init__(self, name, cat, ts, pid, tid):
        self.name = name
        self.cat = cat
        self.ts = ts
        self.pid = pid
        self.tid = tid
        self.ph = "P"


class ObjectEvents(AbstractEventType):
    """
    Objects are a building block to track complex data structures in traces. Since traces
    go for long periods of time, the formative concept for this is a way to identify
    objects in a time varying way. To identify an object, you need to give it an id,
    usually its pointer, e.g. id: "0x1000". But, in a long running trace, or just a trace
    with a clever allocator, a raw pointer 0x1000 might refer to two different objects at
    two different points in time. Object events do not have an args property associated
    with them.

    In the trace event format, we think in terms of object instances.  e.g. in C++, given
    class Foo , new Foo() at 0x1000 is an instance of Foo. There are three trace phase
    types associated with objects they are: N, O, and D, object created, object snapshot
    and object destroyed respectively.

    Object Events Example:
    {"name": "MyObject", "ph": "N", "id": "0x1000", "ts": 0, "pid": 1, "tid": 1},
    {"name": "MyObject", "ph": "D", "id": "0x1000", "ts": 20, "pid": 1, "tid": 1}

    The phases of object events are:
    - N: created
    - O: snapshot
    - D: destroyed
    """

    def __init__(self, name, ph, id, ts, pid, tid):
        self.name = name
        assert ph in ["N", "O", "D"]
        self.ph = ph
        self.id = id
        self.ts = ts
        self.pid = pid
        self.tid = tid


class MemoryDumpEvents(AbstractEventType):
    """
    Memory dump events correspond to memory dumps of (groups of) processes. There are two
    types of memory dump events:
    Global memory dump events, which contain system memory information such as the size of
    RAM, are denoted by the V phase type and  Process memory dump events, which contain
    information about a single process’s memory
    usage (e.g. total allocated memory), are denoted by the v phase type.
    """

    def __init__(self, name, ts, args={}, pid=None):
        self.name = name  # memory dump name
        self.ts = ts
        self.ph = "V"
        self.args = args
        self.pid = pid

    def json(self):
        d = {"name": self.name, "ts": self.ts, "ph": self.ph}
        if self.pid:
            d["pid"] = self.pid
        if self.args:
            d["args"] = self.args
        return d


class MarkEvents(AbstractEventType):
    """
    Mark events are created whenever a corresponding navigation timing API mark is
    created. Currently, this can happen in two ways:

    Automatically at key times in a web page’s lifecycle, like navigationStart, fetchStart,
    and domComplete.
    Programmatically by the user via Javascript. This allows the user to annotate key
    domain-specific times (e.g. searchComplete).

    Mark Event Example:
    [
      {'name': 'firstLayout', 'ts': 10, 'ph': 'R', 'cat': 'blink.user_timing',  'pid': 42, 'tid': 983},
    ]

    """

    pass


class ClockSyncEvents(AbstractEventType):
    """
    Trace Viewer can handle multiple trace logs produced by different tracing agents
    and synchronize their clock domains. Clock sync events are used for clock
    synchronization. Clock sync events are specified by the clock_sync name and the c
    phase type.
    """

    pass


class ContextEvents(AbstractEventType):
    """
    Context events are used to mark sequences of trace events as belonging to a particular
    context (or a tree of contexts). There are two defined context events: enter and
    leave.

    The enter event with phase “(“ adds a context to all following trace events on the same
    thread until a corresponding leave event with phase “)” exits that context.

    Contexts ids refer to context object snapshots. Context objects can also form a tree – see
    FrameBlamer for details.
    """

    pass
