use super::ByzQuorumSystem;
use fantoch::id::ProcessId;
use fantoch::HashSet;
use simple_matrix::Matrix;
use std::iter::FromIterator;
use rand::distributions::{Distribution, Uniform};

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
        let mut q = HashSet::new();

        // A quorum is `factor`*rows + `factor`columns
        // We define `factor` in a way we can guarantee at least 3f+1 in 
        // the intersection between any two quorums

        let inner = (((3*self.faults) as f64)/2_f64) + 1_f64;
        let factor = (inner.sqrt()).ceil() as usize;

        println!("Factor: {}", factor);

        let mut rng = rand::thread_rng();

        // Be careful, matrix index starts at 0
        let rows_die = Uniform::from(0..self.grid.rows());
        let cols_die = Uniform::from(0..self.grid.cols());


        //The following section can be improved (ofc), but will remain ugly for now.

        // Generate random rows without duplicates
        // rows_used will be useful later as they must be sent as proof in a later iteration of the code
        let mut rows_picked: HashSet<usize> = HashSet::new();
        loop {
            let throw_row: usize = rows_die.sample(&mut rng);
            println!("R: {}",throw_row);
            rows_picked.insert(throw_row);
            if rows_picked.len() == factor {
                break;
            }
        }

        // Generate random cols without duplicates
        // cols_used will be useful later as they must be sent as proof in a later iteration of the code
        let mut cols_picked: HashSet<usize> = HashSet::new();
        loop {
            let throw_col: usize = cols_die.sample(&mut rng);
            println!("C: {}",throw_col);
            cols_picked.insert(throw_col);
            if cols_picked.len() == factor {
                break;
            }
        }

        for i in rows_picked.into_iter() {
            for e in self.grid.get_row(i).unwrap() {
                 q.insert(*e);
             }
        }

        for j in cols_picked.into_iter() {
            for e in self.grid.get_col(j).unwrap() {
                 q.insert(*e);
            }
        }
        println!("Size of Q: {}", q.len());
        q
    }

    fn get_quorum_size(&self) -> usize {
        0
    }
}

impl MGridStrong {

    // I'm assuming that 'n' provided in the config is correct
    // TODO: properly sanitize when `n` and `f` are incompatible.
    pub fn from_set_to_matrix(procs: HashSet<ProcessId>) -> Matrix<ProcessId> {
        let n: usize = procs.len();
        println!("Size of set: {}", n);
        let d = (n as f64).sqrt() as usize;
        println!("Dimension: {}", d);
        //Equal view of the matrix
        let mut v = Vec::from_iter(procs.clone());
        v.sort();
        let res =  Matrix::from_iter(d, d, v.into_iter());
        res
    }

}