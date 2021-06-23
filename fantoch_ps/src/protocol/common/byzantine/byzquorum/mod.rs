//This module contains the definition of `MGridStrong`
mod mgridstrong;

// Re-exports.
pub use mgridstrong::MGridStrong;

use fantoch::id::ProcessId;
use fantoch::{HashMap, HashSet};
use serde::{Deserialize, Serialize};
use std::fmt::Debug;

// The idea is to have more attributes here, to implement
// quoracle-style analysis.

pub trait ByzQuorumSystem: Debug + Clone {
    /// Create a new `ByzQuorumSystem` instance.
    /// All processes in the system must have been passed as a parameter
    fn new(system_processes: HashSet<ProcessId>, faults: usize) -> Self;

    /// Finds a quorum according to some strategy
    /// that abides to a certain intersection rule.
    fn get_quorum(&self) -> HashSet<ProcessId>;

    fn get_quorum_size(&self) -> usize;
}

//cargo test --lib byzantine -- --show-output
//remember to test everything in byzantine
//cargo test protocol::common::byzantine::byzquorum::tests::build_mgrid_strong -- --exact --show-output

#[cfg(test)]
mod tests {
    use super::*;
    use fantoch::id::ProcessId;
    use fantoch::HashSet;
    use std::iter::FromIterator;
    #[test]
    fn build_mgrid_strong() {
        let p: HashSet<ProcessId> = HashSet::from_iter(0..49);
        let s = MGridStrong::new(p, 1);

        for i in 0..s.grid.rows() {
            let a = s.grid.get_row(i);
            for j in a.unwrap() {
                print!("{} ", j);
            }
            println!("");
        }

        println!("grid 7x7");

        s.get_quorum();
    }
}
