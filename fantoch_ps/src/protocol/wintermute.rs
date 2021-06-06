use crate::executor::{GraphExecutionInfo, GraphExecutor};
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

pub type WintermuteSequential = Wintermute<SequentialKeyDeps>;
pub type WintermuteLocked = Wintermute<LockedKeyDeps>;

#[derive(Debug, Clone)]
pub struct Wintermute<KD: KeyDeps> {
    bp: BaseProcess,
    key_deps: KD,
    byz_quorum_system: QS,
    cmds: SequentialCommandsInfo<EPaxosInfo>,
    gc_track: VClockGCTrack,
    to_processes: Vec<Action<Self>>,
    to_executors: Vec<GraphExecutionInfo>,
    // commit notifications that arrived before the initial `MCollect` message
    // (this may be possible even without network failures due to multiplexing)
    buffered_commits: HashMap<Dot, (ProcessId, ConsensusValue)>,
}

impl<KD: KeyDeps> Protocol for Wintermute<KD> {
    type Message = Message;
    type PeriodicEvent = PeriodicEvent;
    type Executor = GraphExecutor;

    fn new(
        process_id: ProcessId,
        shard_id: ShardId,
        config: Config,
    ) -> (Self, Vec<(Self::PeriodicEvent, Duration)>){
        // compute fast and write quorum sizes
        let (fast_quorum_size, write_quorum_size) =
            config.epaxos_quorum_sizes();

        // create protocol data-structures
        let bp = BaseProcess::new(
            process_id,
            shard_id,
            config,
            fast_quorum_size,
            write_quorum_size,
        );
        let key_deps = KD::new(shard_id);
        let f = Self::allowed_faults(config.n());
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
}

impl<KD: KeyDeps> Wintermute<KD> {
    /// Depends on Byz Quorum System
    pub fn allowed_faults(n: usize) -> usize {
        0
    }

    fn handle_submit(&mut self, dot: Option<Dot>, cmd: Command) {
        // compute the command identifier
        let dot = dot.unwrap_or_else(|| self.bp.next_dot());

        // compute its deps
        let deps = self.key_deps.add_cmd(dot, &cmd, None);

        // create `MCollect` and target
        /*
        let mcollect = Message::MCollect {
            dot,
            cmd,
            deps,
            quorum: self.bp.fast_quorum(),
        };
        */
        let target = self.bp.all();

        // save new action
        self.to_processes.push(Action::ToSend {
            target,
            msg: mcollect,
        });
    }
}