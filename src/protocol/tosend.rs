use crate::command::{Command, CommandResult};
use crate::id::ProcessId;

#[derive(Clone, PartialEq, Debug)]
pub enum ToSend<M> {
    // new command to be sent to a coordinator
    ToCoordinator(ProcessId, Command),
    // a protocol message to be sent to a list of processes
    ToProcesses(Vec<ProcessId>, M),
    // a list of command results to be sent to the issuing clients
    ToClients(Vec<CommandResult>),
    // nothing to send
    Nothing,
}

impl<M> ToSend<M> {
    /// Check if it's something to be sent to a coordinator.
    pub fn to_coordinator(&self) -> bool {
        match *self {
            ToSend::ToCoordinator(_, _) => true,
            _ => false,
        }
    }

    /// Check if it' ssomething to be sent to processes.
    pub fn to_processes(&self) -> bool {
        match *self {
            ToSend::ToProcesses(_, _) => true,
            _ => false,
        }
    }

    /// Check if it's something to be sent to clients.
    pub fn to_clients(&self) -> bool {
        match *self {
            ToSend::ToClients(_) => true,
            _ => false,
        }
    }

    /// Check if there's nothing to be sent.
    pub fn is_nothing(&self) -> bool {
        match *self {
            ToSend::Nothing => true,
            _ => false,
        }
    }
}