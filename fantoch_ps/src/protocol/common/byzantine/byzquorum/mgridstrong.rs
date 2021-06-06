use super::ByzQuorumSystem;
use fantoch::id::ProcessId;
use fantoch::HashSet;
use simple_matrix::Matrix;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MGridStrong {
    pub grid: Matrix<ProcessId>,
    pub faults: usize,
}

impl ByzQuorumSystem for MGridStrong {
    /// Create a new `MGridStrong` instance.
    fn new(system_processes: HashSet<ProcessId>, faults: usize) -> Self {
        let grid = Self::from_set_to_matrix(system_processes);
        Self { grid, faults }
    }

    fn get_quorum(&self) -> HashSet<ProcessId> {
        let temp = HashSet::new();
        temp
    }

    fn get_quorum_size(&self) -> usize {
        0
    }
}

impl MGridStrong {

    // I'm assuming that the n provided in the config is correct
    // Not the best approach...
    pub fn from_set_to_matrix(procs: HashSet<ProcessId>) -> Matrix<ProcessId> {
        let n: usize = procs.len();
        let d = (n as f64).sqrt() as usize;
        let res =  Matrix::from_iter(d, d, procs.into_iter());
        res
    }
}