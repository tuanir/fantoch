use super::Dependency;
use fantoch::id::ProcessId;
use fantoch::{HashMap, HashSet};

type AcceptedSet = HashSet<Dependency>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitteeDeps {
    // committee size
    write_quorum_size: usize,
    // set of processes that have participated in the committee
    participants: HashSet<ProcessId>,
    // accepted deps. Using HashMap< HashSet<Dependency>, usize> didn't work.
    pub accepted_deps: (AcceptedSet, usize),
}

impl CommitteeDeps {
    /// Creates a `CommitteeDeps` instance given the quorum size.
    pub fn new(write_quorum_size: usize) -> Self {
        Self {
            write_quorum_size,
            participants: HashSet::with_capacity(write_quorum_size),
            accepted_deps: (HashSet::new(), 0),
        }
    }

    /// Count how many times accepted deps were reported.
    pub fn add(&mut self, process_id: ProcessId, deps: HashSet<Dependency>) -> bool {
        assert!(self.participants.len() < self.write_quorum_size);

        // record new participant
        self.participants.insert(process_id);

        // add deps to the accepted deps
        if deps == self.accepted_deps.0 {
            self.accepted_deps.1 += 1;
            true
        } else {
            false
        }
    }
}