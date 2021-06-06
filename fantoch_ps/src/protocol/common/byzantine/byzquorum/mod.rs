//This module contains the definition of `MGridStrong`
mod mgridstrong;

// Re-exports.
pub use mgridstrong::MGridStrong;

use fantoch::{HashMap, HashSet};
use fantoch::id::ProcessId;
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

#[cfg(test)]
mod tests {
    use simple_matrix::Matrix;
    use fantoch::id::ProcessId;
    use fantoch::{HashMap, HashSet};
    
    #[test]
    fn build_mgrid_strong() {
       println!("test")
    }
}
