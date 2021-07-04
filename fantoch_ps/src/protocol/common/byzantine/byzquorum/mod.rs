//This module contains the definition of `MGridStrong`
mod mgridstrong;

// Re-exports.
pub use mgridstrong::MGridStrong;

use fantoch::id::ProcessId;
use fantoch::HashSet;
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

    fn get_consensus_quorum(&self) -> HashSet<ProcessId>;

}

//cargo test --lib byzantine -- --show-output
//remember to test everything in byzantine
//cargo test protocol::common::byzantine::byzquorum::tests::build_mgrid_strong -- --exact --show-output

//TODO: do some proper testing, this is absolute garbage.

#[cfg(test)]
mod tests {
    use super::*;
    use fantoch::id::ProcessId;
    use fantoch::HashSet;
    use std::iter::FromIterator;

    #[test]
    fn build_mgrid_strong() {
        let p: HashSet<ProcessId> = HashSet::from_iter(0..49);
        let f = 1;
        //3f+2 <= sqrt(n), for grid 7x7 f must be equal 1
        let s = MGridStrong::new(p, f);

        println!("{:?}",s.grid);

        for i in 0..s.grid.rows() {
            let a = s.grid.get_row(i);
            for j in a.unwrap() {
                print!("{} ", j);
            }
            println!("");
        }

        println!("grid 7x7");

        let q1 = s.get_quorum();
        let q2 = s.get_quorum();

        //let inter: HashSet<ProcessId> = q1.intersection(&q2).map(|e| *e).collect();
        let inter: HashSet<ProcessId> = q1.intersection(&q2).cloned().collect();

        assert!(inter.len() >= 3*f+2);
        //let q_intersection: HashSet<ProcessId> = q1.intersection(&q2).collect();
    }

    #[test]
    fn build_mgrid_strong2() {
        let p: HashSet<ProcessId> = HashSet::from_iter(0..25);
        let f = 1;
        //3f+2 <= sqrt(n), for grid 5x5 f must be equal 1
        let s = MGridStrong::new(p, f);

        for i in 0..s.grid.rows() {
            let a = s.grid.get_row(i);
            for j in a.unwrap() {
                print!("{} ", j);
            }
            println!("");
        }

        println!("grid 5x5");

        let q1 = s.get_quorum();
        let q2 = s.get_quorum();

        //let inter: HashSet<ProcessId> = q1.intersection(&q2).map(|e| *e).collect();
        let inter: HashSet<ProcessId> = q1.intersection(&q2).cloned().collect();
        println!("{:?}", inter);

        assert!(inter.len() >= 3*f+2);
        //let q_intersection: HashSet<ProcessId> = q1.intersection(&q2).collect();
    }
}