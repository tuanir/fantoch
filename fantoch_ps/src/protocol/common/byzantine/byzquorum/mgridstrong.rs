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
        let factor = (((((3*self.faults)/2) + 1) as f64).sqrt()).ceil() as usize;

        println!("Factor: {}", factor);

        let mut rng = rand::thread_rng();

        // Be careful, matrix index starts at 0
        let rows_die = Uniform::from(0..self.grid.rows());
        let cols_die = Uniform::from(0..self.grid.cols());

        
        // FIXME: Note that we might take repeated cols/rows.
        // Get random rows

        //The following section can be improved (ofc), but will remain ugly for now.
        for i in 0..factor {
            let throw_rows = rows_die.sample(&mut rng);
            println!("ROWS, i: {}, r: {}", i, throw_rows);
            for e in self.grid.get_row(throw_rows).unwrap(){
                 q.insert(*e);
                 print!("{} ", e);
             }
             println!("");
        }
        // Get random columns
        for i in 0..factor {
            let throw_cols = cols_die.sample(&mut rng);
            println!("COLS, i: {}, c: {}", i, throw_cols);
            for e in self.grid.get_col(throw_cols).unwrap(){
                 q.insert(*e);
                 print!("{} ", e);
            }
             println!("");
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