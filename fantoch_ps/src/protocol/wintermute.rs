use crate::executor::{GraphExecutionInfo, GraphExecutor};
use crate::protocol::common::byzantine::{ByzQuorumSystem, MGridStrong};
use crate::protocol::common::graph::{
    Dependency, KeyDeps, LockedKeyDeps, QuorumDeps, SequentialKeyDeps,
};
use crate::protocol::common::synod::{Synod, SynodMessage};
use fantoch::command::Command;
use fantoch::config::Config;
use fantoch::id::{Dot, ProcessId, ShardId};
use fantoch::protocol::{
    Action, BaseProcess, Info, MessageIndex, Protocol, ProtocolMetrics,
    SequentialCommandsInfo, VClockGCTrack,
};
use fantoch::time::SysTime;
use fantoch::{singleton, trace};
use fantoch::{HashMap, HashSet};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use threshold::VClock;

pub type WintermuteSequential = Wintermute<SequentialKeyDeps, MGridStrong>;
//pub type WintermuteLocked = Wintermute<LockedKeyDeps>;

#[derive(Debug, Clone)]
pub struct Wintermute<KD: KeyDeps, QS: ByzQuorumSystem> {
    bp: BaseProcess,
    key_deps: KD,
    byz_quorum_system: Option<QS>,
    cmds: SequentialCommandsInfo<WintermuteInfo>,
    gc_track: VClockGCTrack,
    to_processes: Vec<Action<Self>>,
    to_executors: Vec<GraphExecutionInfo>,
    // commit notifications that arrived before the initial `MCollect` message
    // (this may be possible even without network failures due to multiplexing)
    buffered_commits: HashMap<Dot, (ProcessId, ConsensusValue)>,
}

impl<KD: KeyDeps, QS: ByzQuorumSystem> Protocol for Wintermute<KD, QS> {
    type Message = Message;
    type PeriodicEvent = PeriodicEvent;
    type Executor = GraphExecutor;

    fn new(
        process_id: ProcessId,
        shard_id: ShardId,
        config: Config,
    ) -> (Self, Vec<(Self::PeriodicEvent, Duration)>) {
        let (fast_quorum_size, write_quorum_size) =
            config.wintermute_quorum_sizes();

        // create protocol data-structures
        let bp = BaseProcess::new(
            process_id,
            shard_id,
            config,
            fast_quorum_size,
            write_quorum_size,
        );
        let key_deps = KD::new(shard_id);

        //let f = Self::allowed_faults(config.n());
        let f = config.f();
        let cmds = SequentialCommandsInfo::new(
            process_id,
            shard_id,
            config.n(),
            f,
            fast_quorum_size,
            write_quorum_size,
        );
        let gc_track = VClockGCTrack::new(process_id, shard_id, config.n());
        let to_processes = Vec::new();
        let to_executors = Vec::new();
        let buffered_commits = HashMap::new();

        // create `Wintermute`
        let protocol = Self {
            bp,
            key_deps,
            cmds,
            gc_track,
            to_processes,
            to_executors,
            buffered_commits,
            //will build after calling discover
            byz_quorum_system: None,
        };

        // create periodic events
        let events = if let Some(interval) = config.gc_interval() {
            vec![(PeriodicEvent::GarbageCollection, interval)]
        } else {
            vec![]
        };

        // return both
        (protocol, events)
    }

    /// Returns the process identifier.
    fn id(&self) -> ProcessId {
        self.bp.process_id
    }

    /// Returns the shard identifier.
    fn shard_id(&self) -> ShardId {
        self.bp.shard_id
    }

    /// Updates the processes known by this process.
    /// The set of processes provided is already sorted by distance.
    fn discover(
        &mut self,
        processes: Vec<(ProcessId, ShardId)>,
    ) -> (bool, HashMap<ShardId, ProcessId>) {
        let connect_ok = self.bp.simple_discover(processes);
        (connect_ok, self.bp.closest_shard_process().clone())
    }

    /// Submits a command issued by some client.
    fn submit(&mut self, dot: Option<Dot>, cmd: Command, _time: &dyn SysTime) {
        self.handle_submit(dot, cmd);
    }

    /// Handles protocol messages.
    fn handle(
        &mut self,
        from: ProcessId,
        _from_shard_id: ShardId,
        msg: Self::Message,
        time: &dyn SysTime,
    ) {
        match msg {
            Message::MCollect {
                dot,
                cmd,
                quorum,
                deps,
            } => self.handle_mcollect(from, dot, cmd, quorum, deps, time),
            Message::MCollectAck { dot, deps } => {
                self.handle_mcollectack(from, dot, deps, time)
            }
            Message::MCommit { dot, value } => {
                //self.handle_mcommit(from, dot, value, time)
            }
            Message::MConsensus { dot, ballot, value } => {
                //self.handle_mconsensus(from, dot, ballot, value, time)
            }
            Message::MConsensusAck { dot, ballot } => {
                //self.handle_mconsensusack(from, dot, ballot, time)
            }
            Message::MCommitDot { dot } => {
                //self.handle_mcommit_dot(from, dot, time)
            }
            Message::MGarbageCollection { committed } => {
                //self.handle_mgc(from, committed, time)
            }
            Message::MStable { stable } => {
                //self.handle_mstable(from, stable, time)
            }
        }
    }

    /// Handles periodic local events.
    fn handle_event(&mut self, event: Self::PeriodicEvent, time: &dyn SysTime) {
        match event {
            PeriodicEvent::GarbageCollection => {
                self.handle_event_garbage_collection(time)
            }
        }
    }

    /// Returns a new action to be sent to other processes.
    fn to_processes(&mut self) -> Option<Action<Self>> {
        self.to_processes.pop()
    }

    /// Returns new execution info for executors.
    fn to_executors(&mut self) -> Option<GraphExecutionInfo> {
        self.to_executors.pop()
    }

    fn parallel() -> bool {
        KD::parallel()
    }

    fn leaderless() -> bool {
        true
    }

    fn metrics(&self) -> &ProtocolMetrics {
        self.bp.metrics()
    }
}

impl<KD: KeyDeps, QS: ByzQuorumSystem> Wintermute<KD, QS> {
    /// Uses baseprocess attrib to create new BQS instance
    fn build_BQS(&mut self) -> bool {
        let created = QS::new(self.bp.all(), self.bp.config.f());
        self.byz_quorum_system = Some(created);

        self.byz_quorum_system.is_some()
    }

    fn handle_submit(&mut self, dot: Option<Dot>, cmd: Command) {
        // compute the command identifier
        // TODO: find solution to byzantine behavior here.

        // invariant: dot must be unique
        // just show proof of last dot. Let quorum assign dot.
        let dot = dot.unwrap_or_else(|| self.bp.next_dot());

        // compute its deps
        // add_cmd takes past, won't use it.
        //let deps = self.key_deps.add_cmd(dot, &cmd, None);

        // CHECK. What to do if coord \notin Q?
        // Command is not being saved here.

        // generates a quorum to handle this command
        // NOTE: I need to create a mapping from Cmd -> quorum_cmd
        let quorum_picked: HashSet<ProcessId> = self
            .byz_quorum_system
            .as_ref()
            .unwrap_or_else(|| {
                panic!(
                    "process {} should have a BQS registered before",
                    self.id()
                );
            })
            .get_quorum();

        // no past
        let empty_past: HashSet<Dependency> = HashSet::new();

        // create `MCollect` and target
        let mcollect = Message::MCollect {
            dot,
            cmd,
            deps: empty_past.clone(),
            quorum: quorum_picked.clone(),
        };

        // to all actually
        let target = self.bp.all();

        trace!(
            "p{}: HSubmit({:?}, {:?})| time={}",
            self.id(),
            dot,
            cmd,
            time.micros()
        );

        println!(
            "p: {} HSubmit(quorumPicked: {:?})",
            self.id(),
            quorum_picked.clone()
        );

        // save new action
        self.to_processes.push(Action::ToSend {
            target,
            msg: mcollect,
        });
    }

    fn handle_mcollect(
        &mut self,
        from: ProcessId,
        dot: Dot,
        cmd: Command,
        quorum: HashSet<ProcessId>,
        remote_deps: HashSet<Dependency>,
        time: &dyn SysTime,
    ) {
        trace!(
            "p{}: MCollect({:?}, {:?}, {:?}) from {} | time={}",
            self.id(),
            dot,
            cmd,
            remote_deps,
            from,
            time.micros()
        );

        println!("p:{} HMcollect", self.id());

        // get cmd info
        let info = self.cmds.get(dot);

        // discard message if no longer in START
        if info.status != Status::START {
            return;
        }

        // check if it's a message from self (in case the coordinator that submitted is not in Q)
        let message_from_self = from == self.bp.process_id;

        // check if part of fast quorum
        if !quorum.contains(&self.bp.process_id) {
            // if not
            // AND we are NOT coordinator:
            // - simply save the payload and set status to `PAYLOAD`
            // - if we received the `MCommit` before the `MCollect`, handle the
            //   `MCommit` now
            if !message_from_self {
                info.status = Status::PAYLOAD;
                info.cmd = Some(cmd);

                // check if there's a buffered commit notification; if yes, handle
                // the commit again (since now we have the payload)
                /*
                if let Some((from, value)) = self.buffered_commits.remove(&dot) {
                    self.handle_mcommit(from, dot, value, time);
                }
                */
            } else {
                // update command info being coordinator
                info.status = Status::COLLECT;
                info.quorum = quorum;
                info.cmd = Some(cmd);
            }
            return;
        }

        // compute its deps
        let deps = self.key_deps.add_cmd(dot, &cmd, None);

        // update command info
        info.status = Status::COLLECT;
        info.quorum = quorum;
        info.cmd = Some(cmd);

        /*
        TODO:
        // create and set consensus value
        let value = ConsensusValue::with(deps.clone());
        assert!(info.synod.set_if_not_accepted(|| value));
        */

        // create `MCollectAck` and target
        let mcollectack = Message::MCollectAck { dot, deps };
        let target = singleton![from];

        // save new action
        self.to_processes.push(Action::ToSend {
            target,
            msg: mcollectack,
        });
    }

    fn handle_mcollectack(
        &mut self,
        from: ProcessId,
        dot: Dot,
        deps: HashSet<Dependency>,
        _time: &dyn SysTime,
    ) {
        trace!(
            "p{}: MCollectAck({:?}, {:?}) from {} | time={}",
            self.id(),
            dot,
            deps,
            from,
            _time.micros()
        );

        println!(
            "p{}: MCollectAck({:?}, {:?}) from {} | time={}",
            self.id(),
            dot,
            deps,
            from,
            _time.micros()
        );

        // get cmd info
        let info = self.cmds.get(dot);

        // do nothing if we're no longer COLLECT
        if info.status != Status::COLLECT {
            return;
        }

        // update quorum deps
        info.quorum_deps.add(from, deps);

        println!("after add: {:?}", info.quorum_deps);
        // check if we have all necessary replies
        if info.quorum_deps.all() {
            // return only the deps seen `f+1` times
            // this won't work with compaction.
            let final_deps = info.quorum_deps.threshold_union(self.bp.config.f()+1);

            /*
            // create consensus value
            let value = ConsensusValue::with(final_deps);

            self.bp.slow_path();
            // slow path: create `MConsensus`
            let ballot = info.synod.skip_prepare();
            let mconsensus = Message::MConsensus { dot, ballot, value };
            let target = self.bp.write_quorum();
            // save new action
            self.to_processes.push(Action::ToSend {
                target,
                msg: mconsensus,
            });
            */
        }
    }

    fn handle_event_garbage_collection(&mut self, _time: &dyn SysTime) {
        trace!(
            "p{}: PeriodicEvent::GarbageCollection | time={}",
            self.id(),
            _time.micros()
        );

        // retrieve the committed clock
        let committed = self.gc_track.clock().frontier();

        // save new action
        self.to_processes.push(Action::ToSend {
            target: self.bp.all_but_me(),
            msg: Message::MGarbageCollection { committed },
        });
    }

    fn gc_running(&self) -> bool {
        self.bp.config.gc_interval().is_some()
    }
}

// consensus value is a pair where the first component is a flag indicating
// whether this is a noop and the second component is the command's dependencies
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConsensusValue {
    is_noop: bool,
    deps: HashSet<Dependency>,
}

impl ConsensusValue {
    fn bottom() -> Self {
        let is_noop = false;
        let deps = HashSet::new();
        Self { is_noop, deps }
    }

    fn with(deps: HashSet<Dependency>) -> Self {
        let is_noop = false;
        Self { is_noop, deps }
    }
}

fn proposal_gen(_values: HashMap<ProcessId, ConsensusValue>) -> ConsensusValue {
    todo!("recovery not implemented yet")
}

// `WintermuteInfo` contains all information required in the life-cyle of a
// `Command`
#[derive(Debug, Clone)]
struct WintermuteInfo {
    status: Status,
    quorum: HashSet<ProcessId>,
    synod: Synod<ConsensusValue>,
    // `None` if not set yet
    cmd: Option<Command>,
    // `quorum_clocks` is used by the coordinator to compute the threshold
    // clock when deciding whether to take the fast path
    quorum_deps: QuorumDeps,
}

impl Info for WintermuteInfo {
    fn new(
        process_id: ProcessId,
        _shard_id: ShardId,
        n: usize,
        f: usize,
        fast_quorum_size: usize,
        _write_quorum_size: usize,
    ) -> Self {
        // create bottom consensus value
        let initial_value = ConsensusValue::bottom();

        // although the fast quorum size is `fast_quorum_size`, we're going to
        // initialize `QuorumClocks` with `fast_quorum_size - 1` since
        // the clock reported by the coordinator shouldn't be considered
        // in the fast path condition, and this clock is not necessary for
        // correctness; for this to work, `MCollectAck`'s from self should be
        // ignored, or not even created.

        // In Wintermute we handle self MCollectAck, because coord might not be in Q
        // picked by the BQS.

        Self {
            status: Status::START,
            quorum: HashSet::new(),
            synod: Synod::new(process_id, n, f, proposal_gen, initial_value),
            cmd: None,
            quorum_deps: QuorumDeps::new(fast_quorum_size),
        }
    }
}

// `Wintermute` protocol messages
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Message {
    MCollect {
        dot: Dot,
        cmd: Command,
        deps: HashSet<Dependency>,
        quorum: HashSet<ProcessId>,
    },
    MCollectAck {
        dot: Dot,
        deps: HashSet<Dependency>,
    },
    MCommit {
        dot: Dot,
        value: ConsensusValue,
    },
    MConsensus {
        dot: Dot,
        ballot: u64,
        value: ConsensusValue,
    },
    MConsensusAck {
        dot: Dot,
        ballot: u64,
    },
    MCommitDot {
        dot: Dot,
    },
    MGarbageCollection {
        committed: VClock<ProcessId>,
    },
    MStable {
        stable: Vec<(ProcessId, u64, u64)>,
    },
}

impl MessageIndex for Message {
    fn index(&self) -> Option<(usize, usize)> {
        use fantoch::load_balance::{
            worker_dot_index_shift, worker_index_no_shift, GC_WORKER_INDEX,
        };
        match self {
            // Protocol messages
            Self::MCollect { dot, .. } => worker_dot_index_shift(&dot),
            Self::MCollectAck { dot, .. } => worker_dot_index_shift(&dot),
            Self::MCommit { dot, .. } => worker_dot_index_shift(&dot),
            Self::MConsensus { dot, .. } => worker_dot_index_shift(&dot),
            Self::MConsensusAck { dot, .. } => worker_dot_index_shift(&dot),
            // GC messages
            Self::MCommitDot { .. } => worker_index_no_shift(GC_WORKER_INDEX),
            Self::MGarbageCollection { .. } => {
                worker_index_no_shift(GC_WORKER_INDEX)
            }
            Self::MStable { .. } => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PeriodicEvent {
    GarbageCollection,
}

impl MessageIndex for PeriodicEvent {
    fn index(&self) -> Option<(usize, usize)> {
        use fantoch::load_balance::{worker_index_no_shift, GC_WORKER_INDEX};
        match self {
            Self::GarbageCollection => worker_index_no_shift(GC_WORKER_INDEX),
        }
    }
}

/// `Status` of commands.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Status {
    START,
    PAYLOAD,
    COLLECT,
    COMMIT,
}

#[cfg(test)]
mod tests {
    use super::*;
    use fantoch::client::{Client, KeyGen, Workload};
    use fantoch::executor::Executor;
    use fantoch::planet::{Planet, Region};
    use fantoch::sim::Simulation;
    use fantoch::time::SimTime;
    use fantoch::util;

    //cargo test protocol::wintermute::tests::sequential_wintermute_test -- --show-output
    #[test]
    fn sequential_wintermute_test() {
        wintermute_flow::<SequentialKeyDeps, MGridStrong>();
    }

    fn wintermute_flow<KD: KeyDeps, QS: ByzQuorumSystem>() {
        // create simulation

        // need to register processes
        // otherwise it cant infer types
        //let mut simulation = Simulation::<WintermuteSequential>::new();
        let mut simulation = Simulation::new();

        // processes ids
        let v_processes: Vec<ProcessId> = (1..=25).collect();

        // regions
        let europe_west2 = Region::new("europe-west2");
        let us_west1 = Region::new("us-west1");

        // there's a single shard
        let shard_id = 0;

        fn is_odd(n: u64) -> bool {
            n % 2 == 1
        }

        let processes: Vec<_> = v_processes
            .iter()
            .map(|e| match is_odd(*e) {
                true => (*e, shard_id, europe_west2.clone()),
                false => (*e, shard_id, us_west1.clone()),
            })
            .collect();
        // planet
        let planet = Planet::new();

        // create system time
        let time = SimTime::new();

        // n and f
        let n = 25;
        let f = 1;
        let config = Config::new(n, f);

        /*
        // executors
        let executors_n: Vec<_> = v_processes
            .iter()
            .map(|e| GraphExecutor::new(*e, shard_id, config))
            .collect();
        */

        let executor_1 = GraphExecutor::new(1, shard_id, config);
        let executor_2 = GraphExecutor::new(2, shard_id, config);
        let executor_3 = GraphExecutor::new(3, shard_id, config);
        let executor_4 = GraphExecutor::new(4, shard_id, config);
        let executor_5 = GraphExecutor::new(5, shard_id, config);
        let executor_6 = GraphExecutor::new(6, shard_id, config);
        let executor_7 = GraphExecutor::new(7, shard_id, config);
        let executor_8 = GraphExecutor::new(8, shard_id, config);
        let executor_9 = GraphExecutor::new(9, shard_id, config);
        let executor_10 = GraphExecutor::new(10, shard_id, config);
        let executor_11 = GraphExecutor::new(11, shard_id, config);
        let executor_12 = GraphExecutor::new(12, shard_id, config);
        let executor_13 = GraphExecutor::new(13, shard_id, config);
        let executor_14 = GraphExecutor::new(14, shard_id, config);
        let executor_15 = GraphExecutor::new(15, shard_id, config);
        let executor_16 = GraphExecutor::new(16, shard_id, config);
        let executor_17 = GraphExecutor::new(17, shard_id, config);
        let executor_18 = GraphExecutor::new(18, shard_id, config);
        let executor_19 = GraphExecutor::new(19, shard_id, config);
        let executor_20 = GraphExecutor::new(20, shard_id, config);
        let executor_21 = GraphExecutor::new(21, shard_id, config);
        let executor_22 = GraphExecutor::new(22, shard_id, config);
        let executor_23 = GraphExecutor::new(23, shard_id, config);
        let executor_24 = GraphExecutor::new(24, shard_id, config);
        let executor_25 = GraphExecutor::new(25, shard_id, config);
        /*
        let wintermute_protocol: Vec<_> = v_processes
            .iter()
            .map(|e| Wintermute::<KD, QS>::new(*e, shard_id, config))
            .collect();
        */

        // wintermute
        let (mut winter_1, _) = Wintermute::<KD, QS>::new(1, shard_id, config);
        let (mut winter_2, _) = Wintermute::<KD, QS>::new(2, shard_id, config);
        let (mut winter_3, _) = Wintermute::<KD, QS>::new(3, shard_id, config);
        let (mut winter_4, _) = Wintermute::<KD, QS>::new(4, shard_id, config);
        let (mut winter_5, _) = Wintermute::<KD, QS>::new(5, shard_id, config);
        let (mut winter_6, _) = Wintermute::<KD, QS>::new(6, shard_id, config);
        let (mut winter_7, _) = Wintermute::<KD, QS>::new(7, shard_id, config);
        let (mut winter_8, _) = Wintermute::<KD, QS>::new(8, shard_id, config);
        let (mut winter_9, _) = Wintermute::<KD, QS>::new(9, shard_id, config);
        let (mut winter_10, _) =
            Wintermute::<KD, QS>::new(10, shard_id, config);
        let (mut winter_11, _) =
            Wintermute::<KD, QS>::new(11, shard_id, config);
        let (mut winter_12, _) =
            Wintermute::<KD, QS>::new(12, shard_id, config);
        let (mut winter_13, _) =
            Wintermute::<KD, QS>::new(13, shard_id, config);
        let (mut winter_14, _) =
            Wintermute::<KD, QS>::new(14, shard_id, config);
        let (mut winter_15, _) =
            Wintermute::<KD, QS>::new(15, shard_id, config);
        let (mut winter_16, _) =
            Wintermute::<KD, QS>::new(16, shard_id, config);
        let (mut winter_17, _) =
            Wintermute::<KD, QS>::new(17, shard_id, config);
        let (mut winter_18, _) =
            Wintermute::<KD, QS>::new(18, shard_id, config);
        let (mut winter_19, _) =
            Wintermute::<KD, QS>::new(19, shard_id, config);
        let (mut winter_20, _) =
            Wintermute::<KD, QS>::new(20, shard_id, config);
        let (mut winter_21, _) =
            Wintermute::<KD, QS>::new(21, shard_id, config);
        let (mut winter_22, _) =
            Wintermute::<KD, QS>::new(22, shard_id, config);
        let (mut winter_23, _) =
            Wintermute::<KD, QS>::new(23, shard_id, config);
        let (mut winter_24, _) =
            Wintermute::<KD, QS>::new(24, shard_id, config);
        let (mut winter_25, _) =
            Wintermute::<KD, QS>::new(25, shard_id, config);

        // discover processes in all wintermute
        // just reusing code, not useful for now to sort by distance
        // since when building the BQS this is not leveraged for now
        // added function to BaseProcess, simple_discover
        // it doesnt fill fast_quorum or write_quorum

        let sorted = util::sort_processes_by_distance(
            &europe_west2,
            &planet,
            processes.clone(),
        );

        /*
        for p in wintermute_protocol.iter() {
            p.discover(sorted);
        }
        */

        winter_1.discover(sorted.clone());
        winter_2.discover(sorted.clone());
        winter_3.discover(sorted.clone());
        winter_4.discover(sorted.clone());
        winter_5.discover(sorted.clone());
        winter_6.discover(sorted.clone());
        winter_7.discover(sorted.clone());
        winter_8.discover(sorted.clone());
        winter_9.discover(sorted.clone());
        winter_10.discover(sorted.clone());
        winter_11.discover(sorted.clone());
        winter_12.discover(sorted.clone());
        winter_13.discover(sorted.clone());
        winter_14.discover(sorted.clone());
        winter_15.discover(sorted.clone());
        winter_16.discover(sorted.clone());
        winter_17.discover(sorted.clone());
        winter_18.discover(sorted.clone());
        winter_19.discover(sorted.clone());
        winter_20.discover(sorted.clone());
        winter_21.discover(sorted.clone());
        winter_22.discover(sorted.clone());
        winter_23.discover(sorted.clone());
        winter_24.discover(sorted.clone());
        winter_25.discover(sorted.clone());

        // Build BQS now
        winter_1.build_BQS();
        winter_2.build_BQS();
        winter_3.build_BQS();
        winter_4.build_BQS();
        winter_5.build_BQS();
        winter_6.build_BQS();
        winter_7.build_BQS();
        winter_8.build_BQS();
        winter_9.build_BQS();
        winter_10.build_BQS();
        winter_11.build_BQS();
        winter_12.build_BQS();
        winter_13.build_BQS();
        winter_14.build_BQS();
        winter_15.build_BQS();
        winter_16.build_BQS();
        winter_17.build_BQS();
        winter_18.build_BQS();
        winter_19.build_BQS();
        winter_20.build_BQS();
        winter_21.build_BQS();
        winter_22.build_BQS();
        winter_23.build_BQS();
        winter_24.build_BQS();
        winter_25.build_BQS();

        // register processes
        simulation.register_process(winter_1, executor_1);
        simulation.register_process(winter_2, executor_2);
        simulation.register_process(winter_3, executor_3);
        simulation.register_process(winter_4, executor_4);
        simulation.register_process(winter_5, executor_5);
        simulation.register_process(winter_6, executor_6);
        simulation.register_process(winter_7, executor_7);
        simulation.register_process(winter_8, executor_8);
        simulation.register_process(winter_9, executor_9);
        simulation.register_process(winter_10, executor_10);
        simulation.register_process(winter_11, executor_11);
        simulation.register_process(winter_12, executor_12);
        simulation.register_process(winter_13, executor_13);
        simulation.register_process(winter_14, executor_14);
        simulation.register_process(winter_15, executor_15);
        simulation.register_process(winter_16, executor_16);
        simulation.register_process(winter_17, executor_17);
        simulation.register_process(winter_18, executor_18);
        simulation.register_process(winter_19, executor_19);
        simulation.register_process(winter_20, executor_20);
        simulation.register_process(winter_21, executor_21);
        simulation.register_process(winter_22, executor_22);
        simulation.register_process(winter_23, executor_23);
        simulation.register_process(winter_24, executor_24);
        simulation.register_process(winter_25, executor_25);

        println!("Values instantiated");

        // client workload
        let shard_count = 1;
        let key_gen = KeyGen::ConflictPool {
            conflict_rate: 100,
            pool_size: 1,
        };
        let keys_per_command = 1;
        let commands_per_client = 10;
        let payload_size = 100;
        let workload = Workload::new(
            shard_count,
            key_gen,
            keys_per_command,
            commands_per_client,
            payload_size,
        );
        // create client 1 that is connected to wintermute 1
        let client_id = 1;
        let client_region = europe_west2.clone();
        let status_frequency = None;
        let mut client_1 = Client::new(client_id, workload, status_frequency);

        // discover processes in client 1
        let closest =
            util::closest_process_per_shard(&client_region, &planet, processes);
        client_1.connect(closest);

        // start client
        let (target_shard, cmd) = client_1
            .cmd_send(&time)
            .expect("there should be a first operation");
        let target = client_1.shard_process(&target_shard);

        // check that `target` is winter 1
        assert_eq!(target, 1);

        // register client
        simulation.register_client(client_1);

        // register command in executor and submit it in winter 1
        let (process, _, pending, time) = simulation.get_process(target);

        pending.wait_for(&cmd);
        process.submit(None, cmd, time);

        let mut actions: Vec<_> = process.to_processes_iter().collect();

        // there's a single action
        assert_eq!(actions.len(), 1);
        let mcollect = actions.pop().unwrap();

        // check that the mcollect is being sent to *all* processes
        let check_target = |target: &HashSet<ProcessId>| target.len() == n;
        assert!(
            matches!(mcollect.clone(), Action::ToSend{target, ..} if check_target(&target))
        );

        // handle mcollects
        let mut mcollectacks = simulation.forward_to_processes((1, mcollect));

        println!("mcollectacks: {}", mcollectacks.len());

        // check that there's that |mcollectack| = |Q|
        assert_eq!(mcollectacks.len(), 16);

        let mut mcommits = for ack in 1..=16 {
            simulation.forward_to_processes(
                mcollectacks.pop().expect("there should be an mcollect ack"),
            );
        };
        /*
        // there's a commit now
        assert_eq!(mcommits.len(), 1);

        // check that the mcommit is sent to everyone
        let mcommit = mcommits.pop().expect("there should be an mcommit");
        let check_target = |target: &HashSet<ProcessId>| target.len() == n;
        assert!(
            matches!(mcommit.clone(), (_, Action::ToSend {target, ..}) if check_target(&target))
        );

        // all processes handle it
        let to_sends = simulation.forward_to_processes(mcommit);

        // check the MCommitDot
        let check_msg =
            |msg: &Message| matches!(msg, Message::MCommitDot { .. });
        assert!(to_sends.into_iter().all(|(_, action)| {
            matches!(action, Action::ToForward { msg } if check_msg(&msg))
        }));

        // process 1 should have something to the executor
        let (process, executor, pending, time) =
            simulation.get_process(process_id_1);
        let to_executor: Vec<_> = process.to_executors_iter().collect();
        assert_eq!(to_executor.len(), 1);

        // handle in executor and check there's a single command partial
        let mut ready: Vec<_> = to_executor
            .into_iter()
            .flat_map(|info| {
                executor.handle(info, time);
                executor.to_clients_iter().collect::<Vec<_>>()
            })
            .collect();
        assert_eq!(ready.len(), 1);

        // get that command
        let executor_result =
            ready.pop().expect("there should an executor result");
        let cmd_result = pending
            .add_executor_result(executor_result)
            .expect("there should be a command result");

        // handle the previous command result
        let (target, cmd) = simulation
            .forward_to_client(cmd_result)
            .expect("there should a new submit");

        let (process, _, _, time) = simulation.get_process(target);
        process.submit(None, cmd, time);
        let mut actions: Vec<_> = process.to_processes_iter().collect();
        // there's a single action
        assert_eq!(actions.len(), 1);
        let mcollect = actions.pop().unwrap();

        let check_msg = |msg: &Message| matches!(msg, Message::MCollect {dot, ..} if dot == &Dot::new(process_id_1, 2));
        assert!(
            matches!(mcollect, Action::ToSend {msg, ..} if check_msg(&msg))
        );
        */
    }
}
