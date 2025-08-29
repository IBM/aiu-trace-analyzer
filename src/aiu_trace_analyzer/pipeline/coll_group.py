# Copyright 2024-2025 IBM Corporation

import re
import copy
import queue

import aiu_trace_analyzer.logger as aiulog
from aiu_trace_analyzer.types import TraceEvent
from aiu_trace_analyzer.pipeline import AbstractContext,EventPairDetectionContext,TwoPhaseWithBarrierContext

_FLOW_SYNC="sync" # the sync=xy string from the event name
_FLOW_IO="io_type" # dmaI, dmaO
_FLOW_STEP="step" # prep, exec

_IO_TYPE_DMAO=0
_IO_TYPE_DMAI=1

_STEP_PREP=0
_STEP_EXEC=1
_STEP_DONE=5

_TYPE_NONE = 0
_TYPE_BCLIST = 1
_TYPE_SEND = 2
_TYPE_MCAST = 3
_TYPE_DONE = 4


_KEY_PEER="Peers" # key that indicates the peer entry
_KEY_TYPE="Type" # key that indicates the request type

_EVENT_FIRST="first"
_EVENT_LATEST="last"

_ALLREDUCE_CHAIN="chain"
_ALLREDUCE_TREE="tree"

_ABSOLUTE_COMP_COMM_END_TIME_DIFF=10000 # time diff to check whether a comp belongs to a comm call

class CollectiveGroupState:
    def __init__(self) -> None:
        self.queue = []
        self.peers = set()
        self.latest_ts = 0.0
        self.first_ts = 0.0
        self.rank_first_event = {}
        self.rank_latest_event = {}
        self.sends = 0

class CollectiveGroupingContext(EventPairDetectionContext):

    def __init__(self, build_coll_event=False) -> None:
        super().__init__()
        self.flow_sequence_id = 1000000
        self.problem_count = 0
        self.flow_div = {}
        self.last_coll_id = 0
        self.build_coll_event = build_coll_event
        self.stale_drop = 0
        self.drop_threshold = 5000000   # allow at least before start considering a flow of just 2 events to be stale
        self.rank_coll_comp_queues = {} # record comp event candidate for collective call on every rank
        self.coll_algo = None

    def __del__(self) -> None:
        if self.problem_count:
            aiulog.log(aiulog.WARN, "FLOW: Number of potential problems detected =", self.problem_count)
            print('src\tdst\trecv_before_send')
            for dv, a, b in self.flow_div.values():
                print(f'{a}\t{b}\t{dv}')
        if self.stale_drop:
            aiulog.log(aiulog.WARN, "FLOW: Incomplete flow groups dropped =", self.stale_drop, " This can be caused by time-synchronization issues. You might try cmdline arg '-S'")

    # collecting all events from the same collection group into one hash
    def queue_hash(self, collgroup, _=0) -> int:
        return hash(collgroup)

    def update_event_tracking(self, event: TraceEvent, event_tracker: dict, tracker_type: str) -> dict:
        if event["pid"] not in event_tracker:
            event_tracker[event["pid"]] = event
            aiulog.log(aiulog.TRACE, f'FLOW: add new {tracker_type} for {event["pid"]} for {event["name"]}')
        else:
            if tracker_type == _EVENT_FIRST:
                if event_tracker[event["pid"]]["ts"] > event["ts"]:
                    event_tracker[event["pid"]] = event
                    aiulog.log(aiulog.TRACE, f'FLOW: update {tracker_type} for {event["pid"]} for {event["name"]}')
            elif tracker_type == _EVENT_LATEST:
                if event_tracker[event["pid"]]["ts"] < event["ts"]:
                    event_tracker[event["pid"]] = event
                    aiulog.log(aiulog.TRACE, f'FLOW: update {tracker_type} for {event["pid"]} for {event["name"]}')
            else:
                aiulog.log(aiulog.WARN, "FLOW: need either _EVENT_FIRST or _EVENT_LATEST for updating event tracker")

        return event_tracker


    # make insertion based on the collgroup entry
    def insert(self, event: TraceEvent, queue_id=None) -> int:
        _group_id = queue_id if queue_id else self.queue_hash(event["cat"])
        aiulog.log(aiulog.TRACE, f'FLOW: event: {event}')
        if _group_id in self.queues:
            group_state = self.queues[_group_id]
        else:
            aiulog.log(aiulog.TRACE, f'FLOW: New group: {event["cat"]} {_group_id}')
            group_state = CollectiveGroupState()
            self.queues[_group_id] = group_state

        group_state.queue.append(event)
        assert "dur" in event and event["dur"] > 0.0
        group_state.latest_ts = max( group_state.latest_ts, event["ts"]+event["dur"])
        if group_state.first_ts == 0.0:
            group_state.first_ts = event["ts"]
        elif _KEY_TYPE in event and event[_KEY_TYPE] == _TYPE_SEND:
            group_state.first_ts = min( group_state.first_ts, event["ts"])

        for p in event[_KEY_PEER]:
            group_state.peers.add(p)
        if _FLOW_IO in event and event[_FLOW_IO] == _IO_TYPE_DMAO:
            group_state.sends += 1

        return _group_id

    def drain(self) -> list[TraceEvent]:
        aiulog.log(aiulog.TRACE, "FLOW: drain")
        revents = []
        self.drop_threshold = 0.0
        aiulog.log(aiulog.TRACE, "FLOW: enter drain ....")
        while len(self.queues.keys()) > 0:
            group_id = (list(self.queues.keys()))[0]
            cg = self.detect_final(group_id)
            aiulog.log(aiulog.DEBUG, "FLOW: checked for final event: ", group_id, cg)
            if cg:
                revents += self.build_flows(group_id)
            else:
                self.check_drop_group(group_id, 1.e30)  # nothing was found in remaining events, so we have to drop this groupID
        return revents

    def check_drop_group(self, group_id, ref_ts) -> None:
        # if 4x the duration of this group is still smaller than the current ts
        # we consider this a stale group that has no chance of closing at all
        group_duration = max(self.queues[group_id].latest_ts - self.queues[group_id].first_ts, self.drop_threshold)
        if self.queues[group_id].latest_ts + 4 * group_duration < ref_ts:
            aiulog.log(aiulog.DEBUG, "FLOW: Dropping unfinished collective group. Considered stale.", group_id, group_duration, ref_ts)
            self.stale_drop += 1
            self.queues.pop(group_id)


    def try_emit_group(self, group_id) -> list[TraceEvent]:
        revents = []
        # check for a 'last event in a flow'
        captured_all = self.detect_final(group_id)
        aiulog.log(aiulog.TRACE, f"FLOW: group: {group_id}, final: {captured_all}")

        # sort the events
        # create f+s events for each subsequent pairs (watch for 1:N and N:1 situations)
        return revents

    # detect candidates that are worth checking for final events
    def group_candidates(self, ts: float) -> list[int]:
        candidates = []
        for k,g in self.queues.items():
            if g.latest_ts < ts:
                aiulog.log(aiulog.TRACE, f"FLOW: final_candidates: g: {k}, ts: {ts}, latest: {g.latest_ts}")
                candidates.append(k)

        return candidates

    def detect_final(self, group_id) -> bool:
        '''
        detecting whether we have collected the last event of a collective group call:
        send/recvs (both single+multicast) have the same sync-data
        * initiated send needs to have a corresponding recv
        * Don't attempt detection of completion before receiving any
            events with ts > end of latest in collected events
        * assume at least 2 sync-groups are needed to build a collective call
        * if all sync-groups of a coll call have been complete: the whole call is complete
        '''

        aiulog.log(aiulog.TRACE, "FLOW: detect final", group_id)
        sync_groups={}
        for event in self.queues[group_id].queue:
            sync_group_hash=hash(event[_FLOW_SYNC])
            if sync_group_hash not in sync_groups:
                sync_groups[sync_group_hash] = (False, False, 0, 0, set()) # keep state (<complete>, <mcast>, <open_sends>, <wdone_closed>, set[peers])
                aiulog.log(aiulog.TRACE, "FLOW: new sync group: ", event[_FLOW_SYNC], event["cat"])

            closed, mcast, count_open, count_close, peers_list = sync_groups[sync_group_hash]
            aiulog.log(aiulog.TRACE, "FLOW: sync group event", event["name"])

            peers_list.add(int(event["pid"]))

            if _KEY_PEER in event:
                for peer in event[_KEY_PEER]:
                    peers_list.add(int(peer))
                mcast |= (len(peers_list) > 2) # the moment we know there are >2 peers, we know it's a multicast

            if _KEY_TYPE in event:
                if event[_KEY_TYPE] == _TYPE_BCLIST: # bclist setup provides all the peers, so set the open-count
                    count_open += len(event[_KEY_PEER])
                    mcast |= True
                elif event[_KEY_TYPE] == _TYPE_MCAST:
                    count_open += 1
                    mcast |= True
                elif event[_KEY_TYPE] == _TYPE_SEND:
                    count_open += 1
                elif event[_KEY_TYPE] == _TYPE_DONE:
                    count_close += 1

            partners = len(peers_list)
            closed = (partners>1) and (count_close>0) # if we don't even have a single peer and no finished receive, we're not finished at all
            if mcast:
                closed &= (2*partners-1 == count_open ) and (count_close == partners-1)
            else:
                closed &= (count_open > 0 ) and (count_close == partners-1)

            sync_groups[sync_group_hash] = (closed, mcast, count_open, count_close, peers_list)
            aiulog.log(aiulog.TRACE, "FLOW: sync group state: ", event[_FLOW_SYNC], sync_groups[sync_group_hash])

        # check the sync-groups for completion
        final = len(sync_groups) > 1
        for _, sg in sync_groups.items():
            final &= sg[0]
        aiulog.log(aiulog.TRACE, "FLOW: current final result: ", final)
        return final

    def simple_flows(self, group_id) -> list[TraceEvent]:
        '''
        create trivial flows (e.g. Prep->Exec)
        '''
        queue = self.queues[group_id]
        # filter the queue for Prep and non-Prep events:
        preps = filter(lambda e: _FLOW_STEP in e and e[_FLOW_STEP] == 0, queue)
        execs = list(filter(lambda e: _FLOW_STEP in e and e[_FLOW_STEP] == 1, queue))
        regex = re.compile("(.*) [PE][rx][e][pc]$")  # everything except Prep/Exec at the end

        flow_pairs = []
        remove = []
        for event in preps:
            aiulog.log(aiulog.TRACE, "FLOW: Search Exec for:", event["name"])
            partner = self.find_exec_partner(event, execs, regex)
            if partner:
                aiulog.log(aiulog.TRACE, "FLOW: Prep-Exec:", event["name"], partner["name"])
                flow_pairs.append((event, partner))
                remove += [ event, partner ]
        self.queues[group_id] = list(filter(lambda e: e not in remove, queue))
        aiulog.log(aiulog.TRACE, "FLOW: remaining queue PE:", len(self.queues[group_id]))


        queue = self.queues[group_id]
        sends = filter(lambda e: _FLOW_SYNC in e and _FLOW_IO in e and e[_FLOW_IO] == 0, queue)
        recvs = list(filter(lambda e: _FLOW_SYNC in e and _FLOW_IO in e and e[_FLOW_IO] == 1, queue))

        remove = []
        for event in sends:
            aiulog.log(aiulog.TRACE, "FLOW: Search Recv for Send:", event["name"])
            partner = self.find_recv_partner(event, recvs)
            if partner:
                aiulog.log(aiulog.TRACE, "FLOW: Send-Recv:", event["name"], partner["name"])
                flow_pairs.append((event, partner))
                remove += [ event, partner ]
        self.queues[group_id] = list(filter(lambda e: e not in remove, queue))
        aiulog.log(aiulog.TRACE, "FLOW: remaining queue SR:", len(self.queues[group_id]))


        revents = []
        for (fp_src, fp_dst) in flow_pairs:
            revents += self.create_flow_events_from_pair(fp_src, fp_dst)
        return revents

    def find_exec_partner(self, event, queue, regex, same_pid=True) -> TraceEvent:
        # check the inbound event for the first pattern of pattern_pair
        ref_name = regex.findall(event["name"])
        if len(ref_name) == 0:
            return None

        # scan the queue for the second pattern of pattern_pair
        for partner in queue:
            p_name = regex.findall(partner["name"])
            if len(p_name) and ref_name[0] == p_name[0] and ( same_pid and event["pid"] == partner["pid"]):
                aiulog.log(aiulog.TRACE, "FLOW: pair:", ref_name, p_name)
                return partner
        return None

    def find_recv_partner(self, event, queue) -> TraceEvent:
        reference=(event[_FLOW_SYNC], _TYPE_DONE, event[_KEY_PEER][0])
        for partner in queue:
            p_match = (partner[_FLOW_SYNC],
                       partner[_KEY_TYPE],
                       partner["pid"])
            if reference == p_match:
                aiulog.log(aiulog.TRACE, "FLOW: pair:", p_match)
                return partner
        return None

    def check_allreduce_algo(self, group_state: CollectiveGroupState) -> str:
        # TP-tree : proc_0      has multicast
        # TP-chain  : proc_(NP-1) has multicast

        # Only need to check once for first run (assume all groups use the same algorithm)
        if self.coll_algo is not None:
            return self.coll_algo

        multicast_rank = None
        partners = len(group_state.peers)
        for event in group_state.queue:
            # only check event name on proc_0 and proc_(NP-1)
            if event['pid'] == 0 or event['pid'] == (partners - 1):
                if event[_KEY_TYPE] == _TYPE_MCAST:
                    aiulog.log(aiulog.TRACE, f'FLOW: AllReduce Rank {event["pid"]} w/ {event["name"]}')
                    multicast_rank = event['pid']
                    break

        if multicast_rank == 0:
            self.coll_algo = _ALLREDUCE_TREE
            aiulog.log(aiulog.TRACE, f'FLOW: AllReduce multicat rank {multicast_rank} TP-tree')

        elif multicast_rank == (partners - 1):
            self.coll_algo = _ALLREDUCE_CHAIN
            aiulog.log(aiulog.TRACE, f'FLOW: AllReduce multicat rank {multicast_rank} TP-chain')

        else:
            aiulog.log(aiulog.WARN, "FLOW: AllReduce algorithm type not supported (chain and tree only")
            self.build_coll_event = False
            self.coll_algo = None

        return self.coll_algo


    def update_rank_first_last_event(self, event: TraceEvent, group_state: CollectiveGroupState) -> bool:
        # track first send and last recv on one rank
        partners = len(group_state.peers)

        if self.coll_algo == _ALLREDUCE_CHAIN:
            if _KEY_TYPE in event and event[_KEY_TYPE] == _TYPE_SEND:
                # rank 0 starts from first send event
                if event["pid"] == 0:
                    self.update_event_tracking(event, group_state.rank_first_event, _EVENT_FIRST)

            elif _KEY_TYPE in event and event[_KEY_TYPE] == _TYPE_MCAST:
                # last rank ends from last send for multicast
                if event["pid"] == (partners - 1):
                    self.update_event_tracking(event, group_state.rank_latest_event, _EVENT_LATEST)

            elif _KEY_TYPE in event and event[_KEY_TYPE] == _TYPE_DONE:
                # except rank 0 other ranks start from first real begin of recv event
                if event["pid"] > 0:
                    self.update_event_tracking(event, group_state.rank_first_event, _EVENT_FIRST)

                # except last rank, other ranks ends from last recv
                if event["pid"] < (partners - 1): # temporary hard code rank counts
                    self.update_event_tracking(event, group_state.rank_latest_event, _EVENT_LATEST)

            return True

        elif self.coll_algo == _ALLREDUCE_TREE:
            aiulog.log(aiulog.WARN, "To be finished, Only chain AllReduce is supported now")
            self.build_coll_event = False
            return False

        else:
            aiulog.log(aiulog.WARN, "Only chain AllReduce is supported now")
            self.build_coll_event = False
            return False


    def update_all_ranks_first_last_event(self, group_state: CollectiveGroupState) -> None:
        for event in group_state.queue:
            self.update_rank_first_last_event(event, group_state)

    def build_flows(self, groupID: int) -> list[TraceEvent]:
        group_state = self.queues.pop(groupID, None)
        if not group_state:
            return []

        event_list = group_state.queue
        revents = []

        for e in event_list:
            if e[_KEY_TYPE] == _TYPE_SEND:
                r = self.find_recv_partner(e, event_list)
                if not r:
                    continue
                flows,flowd = self.create_flow_events_from_pair(e,r)
                revents += [flows, flowd]

        # build artificial collective call duration event
        if self.build_coll_event and len(revents):

            # check chain or tree for allreduce algorithm
            if self.check_allreduce_algo(group_state) is None:
                return revents

            # update last and first event on all ranks
            if self.update_all_ranks_first_last_event(group_state) is False:
                return revents

            for pid in group_state.rank_first_event:

                # get the corresponding compute event
                if pid not in self.rank_coll_comp_queues:
                    continue

                while not self.rank_coll_comp_queues[pid].empty():
                    cmpt_event = self.rank_coll_comp_queues[pid].get()
                    aiulog.log(aiulog.TRACE, f'FLOW: Rank {pid}, dequeue {cmpt_event["name"]}')
                    aiulog.log(aiulog.TRACE, f'FLOW: first_last_event:{pid},  {group_state.rank_first_event[pid]["cat"]}\
                        first: {group_state.rank_first_event[pid]["name"]} starting with {group_state.rank_first_event[pid]["ts"]},  \
                        end: {group_state.rank_latest_event[pid]["name"]}')
                    cmpt_end=cmpt_event["ts"]+cmpt_event["dur"]
                    send_end=group_state.rank_first_event[pid]["ts"] + group_state.rank_first_event[pid]["dur"]

                    # find the compute event happens after the first event and finished before first event
                    if cmpt_event["ts"] > group_state.rank_first_event[pid]["ts"] and abs(cmpt_end - send_end) <= _ABSOLUTE_COMP_COMM_END_TIME_DIFF:
                        call_duration_event_ts = cmpt_end
                        call_duration_event_dur =  group_state.rank_latest_event[pid]["ts"] + group_state.rank_latest_event[pid]["dur"] - cmpt_end
                        aiulog.log(aiulog.TRACE, f'FLOW: Rank {pid}, create a collective call with trimed ts: {call_duration_event_ts}; trimed dur: {call_duration_event_dur}')
                        break
                # To extract the data size (in bytes) from the event name within the same collective group.
                # The name typically ends with a unit like "B" (for bytes).
                # "bytes" represents the amount of data being transferred by a single process, in bytes.
                # subject to change, might be a dedicated size entry in args
                num_bytes=str(group_state.rank_first_event[pid]["name"].split(" ")[1].strip("[]") if 'B' in group_state.rank_first_event[pid]["name"].split(" ")[1].strip("[]") else group_state.rank_latest_event[pid]["name"].split(" ")[1].strip("[]"))
                # NP represents the number of processes
                NP = len(str(group_state.peers).strip('{}').split(','))
                # Calculate the total data size for the collective operation.
                coll_data_size = self._calculate_data_size(NP, int(num_bytes.rstrip('B')))
                # assign to first encountered job (for dialect purposes)
                jobhash = group_state.rank_first_event[pid]["jobhash"]
                '''
                Template for call_duration_event:
                call_duration_event = {
                    "ph": "X",
                    "ts": "cmpt_event end" if cmpt_event["ts"] > group_state.first_ts and cmpt_event_end < group_state.first_ts else "group_state.first_ts",
                    "dur": "group_state.latest_ts - cmpt_event_end" if this.cmpt_event["ts"] > group_state.first_ts and cmpt_event_end < group_state.first_ts,
                    "pid": 0,
                    "tid": "coll" + str(self.last_coll_id),
                    "name": revents[0]["cat"],
                    "args": {
                        "bytes": parsing the data size from the name of receive event,
                        "collective algorithm type": "coll_algo",
                        "peers": str(group_state.peers),
                        "CollGroup": str(revents[0]["cat"]),
                        "rank group compute event": str(cmpt_event["name"]),
                        "Coll_data_size": total transferred data amount
                    }
                }
                '''

                name = revents[0]["cat"]
                if not re.search(r'_\d+$', name):
                    name += '_0'
                call_duration_event = {
                    "ph": "X",
                    "ts": call_duration_event_ts,
                    "dur": call_duration_event_dur,
                    "pid": pid,
                    "tid": "coll"+str(pid),
                    "name": name, #revents[0]["cat"],
                    "args": {
                        "bytes": num_bytes,
                        "collective algorithm type": str(self.coll_algo),
                        "peers": str(group_state.peers),
                        "CollGroup":str(revents[0]["cat"]),
                        "rank group compute event":str(cmpt_event["name"]),
                        "Coll_data_size": coll_data_size,
                        "jobhash": jobhash,
                    }
                }

                self.last_coll_id = (self.last_coll_id + 1) % 2
                revents.append(call_duration_event)

        return revents

    def _calculate_data_size(self, num_procs, data_size_per_process):
        if self.coll_algo == _ALLREDUCE_CHAIN:
            '''
            Bandwidth calculation preparation for the chain algorithm used in AllReduce operations:
            This calculates the total amount of data transferred during the AllReduce operation across all processes.

            - In the chain algorithm for AllReduce, each process sequentially sends its data to the next process, aggregating the results as they propagate.
            - To complete AllReduce, each process sends its data (NP - 1) times to aggregate the data, and another (NP - 1) times to broadcast the result back to all processes.
            - Total data transferred (Coll_data_size) = 2 * (NP - 1) * data size per process
            '''
            # Calculate the total data transferred during the AllReduce operation across all processes.
            return 2 * (num_procs-1) * data_size_per_process

        elif self.coll_algo == _ALLREDUCE_TREE:
            aiulog.log(aiulog.WARN, "To be finished, Only chain AllReduce is supported now")
            self.build_coll_event = False
            return False

        else:
            aiulog.log(aiulog.WARN, "Only chain AllReduce is supported now")
            self.build_coll_event = False
            return False

    def create_flow_events_from_pair(self, src: TraceEvent, dst: TraceEvent) -> tuple[TraceEvent,TraceEvent]:
        '''
        accepts a tuple/pair of events and creates 's' and 'f' flow events
        '''
        src_event = copy.deepcopy(src)
        dst_event = copy.deepcopy(dst)

        src_event["ph"] = "s"
        dst_event["ph"] = "f"

        self.flow_sequence_id += 1
        src_event["name"] = src[_FLOW_SYNC] if _FLOW_SYNC in src else f'flow_{self.flow_sequence_id}'
        dst_event["name"] = src[_FLOW_SYNC] if _FLOW_SYNC in src else f'flow_{self.flow_sequence_id}'
        src_event["id"] = self.flow_sequence_id
        dst_event["id"] = self.flow_sequence_id
        dst_event["bp"] = "e"

        # from where to where the arrow should go:
        src_event["ts"] = src["ts"]
        # dst_event["ts"] = round(dst["ts"] + dst["dur"] - 0.0005, 3)
        dst_event["ts"] = dst["ts"] + dst["dur"] - 0.001

        if src_event["ts"] > dst_event["ts"]:
            aiulog.log(aiulog.DEBUG, "Reverse flow, Send-after-Recv", src_event["name"], src_event["cat"], src_event["ts"], dst_event["ts"])
            self.update_reverse_flow_stats(src_event, dst_event)
            self.problem_count += 1
        # remove keys that are no longer needed
        src_event.pop("args", "")
        dst_event.pop("args", "")
        src_event.pop(_KEY_PEER, "")
        dst_event.pop(_KEY_PEER, "")
        src_event.pop(_KEY_TYPE, "")
        dst_event.pop(_KEY_TYPE, "")
        src_event.pop("dur")


        aiulog.log(aiulog.TRACE, "FLOW create: ", src_event, dst_event)
        return src_event, dst_event

    def update_reverse_flow_stats(self, src_event: TraceEvent, dst_event: TraceEvent) -> None:
        a_to_b_hash = hash(f'{src_event["pid"]}_to_{dst_event["pid"]}')
        if a_to_b_hash not in self.flow_div:
            self.flow_div[a_to_b_hash] = (0.0, src_event["pid"], dst_event["pid"])
        a_to_b_flow_div, a, b = self.flow_div[a_to_b_hash]
        a_to_b_flow_div = max(a_to_b_flow_div, src_event["ts"] - dst_event["ts"])
        self.flow_div[a_to_b_hash] = (a_to_b_flow_div, a, b)


class CommunicationGroupContext(TwoPhaseWithBarrierContext):
    sequence_number_pattern = re.compile(r"[_-](\d+)")

    def __init__(self):
        super().__init__()

    def __del__(self):
        if len(self.queues):
            aiulog.log(aiulog.ERROR, "COMMGROUP: There are unprocessed communication event sequences. Events will be missing from result:", len(self.queues))

    def extract_sequence_number(self, name: str, jobid: int):
        match = self.sequence_number_pattern.search(name)
        if match:
            return int(str(jobid) + match.group(1))
        else:
            return None


    def add_to_sequence(self, event: TraceEvent, sequence: int):
        def _longest_name_overlap(str_a: str, str_b: str) -> str:
            for i, (a,b) in enumerate(zip(str_a+'a', str_b+'b')):
                if a != b:
                    return str_a[:i]
            return "EmptyName"

        if sequence not in self.queues:
            self.queues[sequence] = {
                "count": 1,
                "start_ts": event["ts"],
                "end_ts": event["ts"] + event["dur"],
                "name": event["name"],
                "peers": set([int(event["args"]["Peer"])]) if "args" in event and "Peer" in event["args"] else set()
                }
        else:
            self.queues[sequence]["count"] += 1
            self.queues[sequence]["start_ts"] = min(self.queues[sequence]["start_ts"], event["ts"])
            self.queues[sequence]["end_ts"] = max(self.queues[sequence]["end_ts"], event["ts"] + event["dur"])
            self.queues[sequence]["name"] = _longest_name_overlap(self.queues[sequence]["name"], event["name"])
            if "args" in event and "Peer" in event["args"]:
                self.queues[sequence]["peers"].add(int(event["args"]["Peer"]))


    def apply(self, event: TraceEvent, sequence: int) -> list[TraceEvent]:
        self.queues[sequence]["count"] -= 1
        if self.queues[sequence]["count"] != 0:
            return []

        event_data = self.queues.pop(sequence)
        event["name"] = event_data["name"]
        event["ts"] = event_data["start_ts"]
        event["dur"] = event_data["end_ts"] - event_data["start_ts"]
        event["args"]["Peers"] = [ str(p) for p in event_data["peers"] ]

        return [event]

def communication_event_collection(event: TraceEvent, context: AbstractContext) -> list[TraceEvent]:
    assert( isinstance(context, CommunicationGroupContext))

    if event["ph"] != "X" or "SenRdma" not in event["name"]:
        return [event]

    sequence = context.extract_sequence_number(event["name"], event["args"]["jobhash"])
    if sequence:
        context.add_to_sequence(event, sequence)

    return [event]

def communication_event_apply(event: TraceEvent, context: AbstractContext) -> list[TraceEvent]:
    assert( isinstance(context, CommunicationGroupContext))

    if event["ph"] != "X" or "SenRdma" not in event["name"]:
        return [event]

    sequence = context.extract_sequence_number(event["name"], event["args"]["jobhash"])
    if sequence:
        return context.apply(event, sequence)
    else:
        return [event]




def flow_extraction(event: TraceEvent, context: AbstractContext) -> list[TraceEvent]:

    assert( isinstance(context, CollectiveGroupingContext) )

    if event["ph"] in "F":
        _ = context.insert(event)

        groups_complete = context.group_candidates(event["ts"])
        for g in groups_complete:
            cg = context.detect_final(g)
            aiulog.log(aiulog.DEBUG, "FLOW: checked for final event: ", g, cg)
            if cg:
                revents = context.build_flows(g)
                revents.sort(key=lambda e: e["ts"])
                return revents
            else:
                context.check_drop_group(g, event["ts"])
        return []

    if event['name'].endswith(' Cmpt Exec') and not event['name'].startswith('AllReduce'):
        if event['pid'] not in context.rank_coll_comp_queues:
            context.rank_coll_comp_queues[event['pid']] = queue.Queue()
        context.rank_coll_comp_queues[event['pid']].put(event)
        aiulog.log(aiulog.TRACE, f'FLOW: name extraction: {event["name"]} at Rank {event["pid"]} w/ queue len: {context.rank_coll_comp_queues[event["pid"]].qsize()}')

    # any other event needs to be passed through as a list of a single event, otherwise it gets lost/dropped
    return [event]




_recv_pattern = re.compile(r"[Rr][Ee][Cc][Vv]_(\d+)_")
_bytes_pattern = re.compile(r" \[(\d+[Bb])\]")
_sync_pattern = re.compile(" \\[sync=(.*)\\]")

_rdma_send_pattern = re.compile(r'RdmaSend[^X]+Xseg[^D]+DmaO$')
_rdma_recv_pattern = re.compile(r'RdmaReceive.*DmaI$')
_send_data_pattern = re.compile(r'Send[_\d]+ Data [^\s]+ DmaO$')


_type_bclist_pattern = re.compile("Set BCList")
_type_send_pattern = re.compile("SingleCast")
_type_mcast_pattern = re.compile("MultiCast")
_type_wdone_pattern = re.compile("WDone *Barrier")

_prep_pattern = re.compile(" Prep$")
_exec_pattern = re.compile(" Exec$")

_unify_recv = re.compile("Receive")
_unify_rdma = re.compile("RDMA")

_TYPE_NONE = 0
_TYPE_BCLIST = 1
_TYPE_SEND = 2
_TYPE_MCAST = 3
_TYPE_DONE = 4

_event_type_map ={
    "WDone Barrier": _TYPE_DONE,
    "MultiCast": _TYPE_MCAST,
    "MultiCast XSEG": _TYPE_SEND, # xseg is targeting a single peer, so we consider it like a send here
    "SingleCast": _TYPE_SEND,
    "Set BCList": _TYPE_BCLIST,
}

def flow_prepare_event_data(event: TraceEvent, _: AbstractContext) -> list[TraceEvent]:
    '''
    make a copy of the inbound event and build a temporary event "F" for subsequent creation of flows
    * unify the strings like "RDMA and Rdma -> Rdma"
    * remove the 'args' entry and populate
        * peer(s)
        * sync-group -> category
        * execution step (prep, exec, done)
        * io-class: send/recv
    '''

    def event_updates(event: TraceEvent) -> TraceEvent:
        # eliminate those [Bytes] entries from the names
        data_bytes = _bytes_pattern.findall(event["name"])
        if len(data_bytes)>0 and "Bytes" not in event["args"]:
            event["name"] = _bytes_pattern.sub('', event["name"])

        # Peers and Peer are diffent entries and here we unify to make it "Peers"
        if "Peer" in event["args"]:
            event["args"][_KEY_PEER] = event["args"].pop("Peer")

        return event

    if event["ph"] in "Xbe" and "args" in event:

        # unify name variability (Recv,Receive|RDMA Rdma)
        event = event_updates(event)
        flow_extraction_event = copy.deepcopy(event)

        name = flow_extraction_event["name"]

        # non-standard denomination of Flow-computation event
        flow_extraction_event["ph"] = "F"

        # make Peer entry a temporary primary citizen of the flow event
        if _KEY_PEER in flow_extraction_event["args"]:
            peer_data = flow_extraction_event["args"][_KEY_PEER]
            if isinstance(peer_data, str):
                event_peers = [ int(p) for p in peer_data.split(',') ]
            else:
                event_peers = [ int(peer_data) ]
            flow_extraction_event[_KEY_PEER] = event_peers
        elif _send_data_pattern.findall(name):
            flow_extraction_event[_KEY_PEER] = []  # send-data events will get their peers from preceeding events
        else:
            # add the peer entry to RDMARecv events that don't have a peer entry
            # by extracting the peer from the sync-string of the name
            idstr = _recv_pattern.findall(name)
            if len(idstr)>0 and not _KEY_PEER in flow_extraction_event:
                aiulog.log(aiulog.TRACE, "FLOW: Adding peer", int(idstr[0]), "to", flow_extraction_event["name"])
                flow_extraction_event[_KEY_PEER] = [ int(idstr[0]) ]

        # extract prep/exec type to separate entry
        if _prep_pattern.search(name):
            flow_extraction_event[_FLOW_STEP] = _STEP_PREP
        elif _exec_pattern.search(name):
            flow_extraction_event[_FLOW_STEP] = _STEP_EXEC
        elif _KEY_TYPE in event["args"] and _type_wdone_pattern.search(event["args"][_KEY_TYPE]):
            flow_extraction_event[_FLOW_STEP] = _STEP_DONE

        # turn the collgroup into category of the flow event
        if "CollGroup" in event["args"]:
            flow_extraction_event["cat"] = event["args"]["CollGroup"]
        else:
            flow_extraction_event["cat"] = ""

        # extract sync entry from name
        sync_entry = _sync_pattern.findall(name)
        if len(sync_entry)>0:
            flow_extraction_event[_FLOW_SYNC]=sync_entry[0]  # TODO consider hashing
        else:
            # if no sync-info is in the name, then we have no way of connecting a flow
            return [event]

        # extract DmaI/O into a io_type
        if _rdma_send_pattern.search(name):
            flow_extraction_event[_FLOW_IO] = _IO_TYPE_DMAO
        elif _rdma_recv_pattern.search(name):
            flow_extraction_event[_FLOW_IO] = _IO_TYPE_DMAI

        # preserve the type
        if _KEY_TYPE in event["args"]:
            flow_extraction_event[_KEY_TYPE] = _event_type_map[ event["args"][_KEY_TYPE] ]
            # if flow_extraction_event[_KEY_TYPE] == _TYPE_SEND:
            #     flow_extraction_event["ts"] = flow_extraction_event["args"]["ts_all"][3]
            #     flow_extraction_event["dur"] = flow_extraction_event["args"]["ts_all"][4]-flow_extraction_event["args"]["ts_all"][3]
        else:
            flow_extraction_event[_KEY_TYPE] = _TYPE_NONE


        flow_extraction_event["jobhash"] = flow_extraction_event["args"]["jobhash"]
        # drop the args dict from the flow-prep event
        flow_extraction_event.pop("args","")

        assert _KEY_TYPE in flow_extraction_event
        assert _KEY_PEER in flow_extraction_event, f'Event: {flow_extraction_event}, {event}'
        assert "cat" in flow_extraction_event
        assert "args" not in flow_extraction_event

        return [ event, flow_extraction_event ]

    return [ event ]


def flow_data_cleanup(event: TraceEvent, _: AbstractContext) -> list[TraceEvent]:
    if event["ph"] == "F":
        return []
    # event.pop(_FLOW_SYNC, "")
    # event.pop(_FLOW_IO, "")
    # event.pop(_FLOW_STEP, "")
    return [event]
