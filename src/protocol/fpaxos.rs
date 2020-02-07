use crate::command::Command;
use crate::config::Config;
use crate::executor::{Executor, SlotExecutor};
use crate::id::{Dot, ProcessId};
use crate::protocol::common::synod::{MultiSynod, MultiSynodMessage};
use crate::protocol::{BaseProcess, MessageDot, Protocol, ToSend};
use crate::{log, singleton};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::mem;

type ExecutionInfo = <SlotExecutor as Executor>::ExecutionInfo;

#[derive(Clone)]
pub struct FPaxos {
    bp: BaseProcess,
    leader: ProcessId,
    multi_synod: MultiSynod<Command>,
    to_executor: Vec<ExecutionInfo>,
}

impl Protocol for FPaxos {
    type Message = Message;
    type Executor = SlotExecutor;

    /// Creates a new `FPaxos` process.
    fn new(process_id: ProcessId, config: Config) -> Self {
        // compute fast and write quorum sizes
        let fast_quorum_size = 0; // there's no fast quorum as we don't have fast paths
        let write_quorum_size = config.fpaxos_quorum_size();

        // create protocol data-structures
        let bp = BaseProcess::new(
            process_id,
            config,
            fast_quorum_size,
            write_quorum_size,
        );

        // get leader from config
        let initial_leader = config.leader().expect(
            "in a leader-based protocol, the initial leader should be defined",
        );
        // create multi synod
        let multi_synod =
            MultiSynod::new(process_id, initial_leader, config.n(), config.f());
        let to_executor = Vec::new();

        // create `FPaxos`
        Self {
            bp,
            leader: initial_leader,
            multi_synod,
            to_executor,
        }
    }

    /// Returns the process identifier.
    fn id(&self) -> ProcessId {
        self.bp.process_id
    }

    /// Updates the processes known by this process.
    /// The set of processes provided is already sorted by distance.
    fn discover(&mut self, processes: Vec<ProcessId>) -> bool {
        self.bp.discover(processes)
    }

    /// Submits a command issued by some client.
    fn submit(
        &mut self,
        dot: Option<Dot>,
        cmd: Command,
    ) -> ToSend<Self::Message> {
        self.handle_submit(dot, cmd)
    }

    /// Handles protocol messages.
    fn handle(
        &mut self,
        from: ProcessId,
        msg: Self::Message,
    ) -> Option<ToSend<Message>> {
        match msg {
            Message::MForwardSubmit { cmd } => {
                let msg = self.handle_submit(None, cmd);
                Some(msg)
            }
            Message::MAccept { ballot, slot, cmd } => {
                self.handle_maccept(from, ballot, slot, cmd)
            }
            Message::MAccepted { ballot, slot } => {
                self.handle_maccepted(from, ballot, slot)
            }
            Message::MChosen { slot, cmd } => self.handle_mchosen(slot, cmd),
        }
    }

    /// Returns new commands results to be sent to clients.
    fn to_executor(&mut self) -> Vec<ExecutionInfo> {
        mem::take(&mut self.to_executor)
    }

    fn parallel() -> bool {
        true
    }

    fn show_metrics(&self) {
        self.bp.show_metrics();
    }
}

impl FPaxos {
    /// Handles a submit operation by a client.
    fn handle_submit(
        &mut self,
        _dot: Option<Dot>,
        cmd: Command,
    ) -> ToSend<Message> {
        // create `MStore` and target
        match self.multi_synod.submit(cmd) {
            MultiSynodMessage::MAccept(ballot, slot, cmd) => {
                // in this case, we're the leader
                let maccept = Message::MAccept { ballot, slot, cmd };
                let target = self.bp.write_quorum();

                // return `ToSend`
                ToSend {
                    from: self.id(),
                    target,
                    msg: maccept,
                }
            }
            MultiSynodMessage::MForwardSubmit(cmd) => {
                // in this case, we're not the leader and should forward the
                // command to the leader
                let mforward = Message::MForwardSubmit { cmd };
                let target = singleton![self.leader];

                // return `ToSend`
                ToSend {
                    from: self.id(),
                    target,
                    msg: mforward,
                }
            }
            msg => panic!("can't handle {:?} in handle_submit", msg),
        }
    }

    fn handle_maccept(
        &mut self,
        from: ProcessId,
        ballot: u64,
        slot: u64,
        cmd: Command,
    ) -> Option<ToSend<Message>> {
        log!(
            "p{}: MAccept({:?}, {:?}, {:?}) from {}",
            self.id(),
            ballot,
            slot,
            cmd,
            from
        );

        if let Some(msg) = self
            .multi_synod
            .handle(from, MultiSynodMessage::MAccept(ballot, slot, cmd))
        {
            match msg {
                MultiSynodMessage::MAccepted(ballot, slot) => {
                    // create `MAccepted` and target
                    let maccepted = Message::MAccepted { ballot, slot };
                    let target = singleton![from];

                    // return `ToSend`
                    Some(ToSend {
                        from: self.id(),
                        target,
                        msg: maccepted,
                    })
                }
                msg => panic!("can't handle {:?} in handle_maccept", msg),
            }
        } else {
            // TODO maybe warn the leader that it is not longer a leader?
            None
        }
    }

    fn handle_maccepted(
        &mut self,
        from: ProcessId,
        ballot: u64,
        slot: u64,
    ) -> Option<ToSend<Message>> {
        log!(
            "p{}: MAccepted({:?}, {:?}) from {}",
            self.id(),
            ballot,
            slot,
            from
        );

        if let Some(msg) = self
            .multi_synod
            .handle(from, MultiSynodMessage::MAccepted(ballot, slot))
        {
            match msg {
                MultiSynodMessage::MChosen(slot, cmd) => {
                    // create `MChosen`
                    let mcommit = Message::MChosen { slot, cmd };
                    let target = self.bp.all();

                    // return `ToSend`
                    let to_send = ToSend {
                        from: self.id(),
                        target,
                        msg: mcommit,
                    };
                    return Some(to_send);
                }
                msg => panic!("can't handle {:?} in handle_maccepted", msg),
            }
        }

        // nothing to send
        None
    }

    fn handle_mchosen(
        &mut self,
        slot: u64,
        cmd: Command,
    ) -> Option<ToSend<Message>> {
        log!("p{}: MCommit({:?}, {:?})", self.id(), slot, cmd);

        // create execution info
        let execution_info = ExecutionInfo::new(slot, cmd);
        self.to_executor.push(execution_info);

        // nothing to send
        None
    }
}

// `FPaxos` protocol messages
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum Message {
    MForwardSubmit {
        cmd: Command,
    },
    MAccept {
        ballot: u64,
        slot: u64,
        cmd: Command,
    },
    MAccepted {
        ballot: u64,
        slot: u64,
    },
    MChosen {
        slot: u64,
        cmd: Command,
    },
}

impl MessageDot for Message {
    fn dot(&self) -> Option<&Dot> {
        todo!("implementation for MessageDot missing for MultiSynodMessage")
        // match self {
        //     // Self::MChosen { dot, .. } => Some(dot),
        // }
    }
}

// #[cfg(test)]
// mod tests {
//     use super::*;
//     use crate::client::{Client, Workload};
//     use crate::planet::{Planet, Region};
//     use crate::sim::Simulation;
//     use crate::time::SimTime;
//     use crate::util;

//     #[test]
//     fn basic_flow() {
//         // create simulation
//         let mut simulation = Simulation::new();

//         // processes ids
//         let process_id_1 = 1;
//         let process_id_2 = 2;
//         let process_id_3 = 3;

//         // regions
//         let europe_west2 = Region::new("europe-west2");
//         let europe_west3 = Region::new("europe-west2");
//         let us_west1 = Region::new("europe-west2");

//         // processes
//         let processes = vec![
//             (process_id_1, europe_west2.clone()),
//             (process_id_2, europe_west3.clone()),
//             (process_id_3, us_west1.clone()),
//         ];

//         // planet
//         let planet = Planet::new("latency/");

//         // create system time
//         let time = SimTime::new();

//         // n and f
//         let n = 3;
//         let f = 1;
//         let config = Config::new(n, f);

//         // executors
//         let executor_1 = SlotExecutor::new(config);
//         let executor_2 = SlotExecutor::new(config);
//         let executor_3 = SlotExecutor::new(config);

//         // basic
//         let mut basic_1 = FPaxos::new(process_id_1, config);
//         let mut basic_2 = FPaxos::new(process_id_2, config);
//         let mut basic_3 = FPaxos::new(process_id_3, config);

//         // discover processes in all basic
//         let sorted = util::sort_processes_by_distance(
//             &europe_west2,
//             &planet,
//             processes.clone(),
//         );
//         basic_1.discover(sorted);
//         let sorted = util::sort_processes_by_distance(
//             &europe_west3,
//             &planet,
//             processes.clone(),
//         );
//         basic_2.discover(sorted);
//         let sorted = util::sort_processes_by_distance(
//             &us_west1,
//             &planet,
//             processes.clone(),
//         );
//         basic_3.discover(sorted);

//         // register processes
//         simulation.register_process(basic_1, executor_1);
//         simulation.register_process(basic_2, executor_2);
//         simulation.register_process(basic_3, executor_3);

//         // client workload
//         let conflict_rate = 100;
//         let total_commands = 10;
//         let payload_size = 100;
//         let workload =
//             Workload::new(conflict_rate, total_commands, payload_size);

//         // create client 1 that is connected to basic 1
//         let client_id = 1;
//         let client_region = europe_west2.clone();
//         let mut client_1 = Client::new(client_id, workload);

//         // discover processes in client 1
//         let sorted = util::sort_processes_by_distance(
//             &client_region,
//             &planet,
//             processes,
//         );
//         assert!(client_1.discover(sorted));

//         // start client
//         let (target, cmd) = client_1
//             .next_cmd(&time)
//             .expect("there should be a first operation");

//         // check that `target` is basic 1
//         assert_eq!(target, process_id_1);

//         // register client
//         simulation.register_client(client_1);

//         // register command in executor and submit it in basic 1
//         let (process, executor) = simulation.get_process(target);
//         executor.wait_for(&cmd);
//         let mcollect = process.submit(None, cmd);

//         // check that the mcollect is being sent to 2 processes
//         let ToSend { target, .. } = mcollect.clone();
//         assert_eq!(target.len(), 2 * f);
//         assert!(target.contains(&1));
//         assert!(target.contains(&2));

//         // handle mcollects
//         let mut mcollectacks = simulation.forward_to_processes(mcollect);

//         // check that there are 2 mcollectacks
//         assert_eq!(mcollectacks.len(), 2 * f);

//         // handle the first mcollectack
//         let mcommits = simulation.forward_to_processes(
//             mcollectacks.pop().expect("there should be an mcollect ack"),
//         );
//         // no mcommit yet
//         assert!(mcommits.is_empty());

//         // handle the second mcollectack
//         let mut mcommits = simulation.forward_to_processes(
//             mcollectacks.pop().expect("there should be an mcollect ack"),
//         );
//         // there's a commit now
//         assert_eq!(mcommits.len(), 1);

//         // check that the mcommit is sent to everyone
//         let mcommit = mcommits.pop().expect("there should be an mcommit");
//         let ToSend { target, .. } = mcommit.clone();
//         assert_eq!(target.len(), n);

//         // all processes handle it
//         let to_sends = simulation.forward_to_processes(mcommit);

//         // check there's nothing to send
//         assert!(to_sends.is_empty());

//         // process 1 should have something to the executor
//         let (process, executor) = simulation.get_process(process_id_1);
//         let to_executor = process.to_executor();
//         assert_eq!(to_executor.len(), 1);

//         // handle in executor and check there's a single command ready
//         let mut ready: Vec<_> = to_executor
//             .into_iter()
//             .flat_map(|info| executor.handle(info))
//             .map(|result| result.unwrap_ready())
//             .collect();
//         assert_eq!(ready.len(), 1);

//         // get that command
//         let cmd_result = ready.pop().expect("there should a command ready");

//         // handle the previous command result
//         let (target, cmd) = simulation
//             .forward_to_client(cmd_result, &time)
//             .expect("there should a new submit");

//         let (process, _) = simulation.get_process(target);
//         let ToSend { msg, .. } = process.submit(None, cmd);
//         if let Message::MStore { dot, .. } = msg {
//             assert_eq!(dot, Dot::new(process_id_1, 2));
//         } else {
//             panic!("Message::MStore not found!");
//         }
//     }
// }
