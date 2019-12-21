use crate::command::Command;
use crate::id::{Dot, ProcessId};
use crate::log;
use crate::protocol::atlas::queue::VertexIndex;
use std::cmp;
use std::collections::{BTreeSet, HashSet, VecDeque};
use threshold::{AEClock, VClock};

/// commands are sorted inside an SCC given their dot
pub type SCC = BTreeSet<Dot>;

#[derive(PartialEq)]
pub enum FinderResult {
    Found,
    NotFound,
    MissingDependency,
}

pub struct TarjanSCCFinder {
    id: usize,
    stack: VecDeque<Dot>,
    sccs: Vec<SCC>,
}

impl TarjanSCCFinder {
    /// Creates a new SCC finder that employs Tarjan's algorithm.
    pub fn new() -> Self {
        Self {
            id: 0,
            stack: VecDeque::new(),
            sccs: Vec::new(),
        }
    }

    /// Returns a list with the SCCs found and a set with all dots visited.
    /// It also resets the ids of all vertices still on the stack.
    #[must_use]
    pub fn finalize(self, vertex_index: &VertexIndex) -> (Vec<SCC>, HashSet<Dot>) {
        // reset the id of each dot in the stack, while computing the set of visited dots
        let visited = self
            .stack
            .into_iter()
            .map(|dot| {
                log!("Finder::finalize removing {:?} from stack", dot);

                // find vertex and reset its id
                let vertex = vertex_index
                    .get_mut(&dot)
                    .expect("stack member should exist");
                vertex.set_id(0);

                // add dot to set of visited
                dot
            })
            .collect();
        // return SCCs found and visited dots
        (self.sccs, visited)
    }

    /// Tries to find an SCC starting from root `dot`.
    pub fn strong_connect(
        &mut self,
        dot: Dot,
        executed_clock: &AEClock<ProcessId>,
        vertex_index: &VertexIndex,
    ) -> FinderResult {
        // get the vertex
        let vertex = vertex_index.get_mut(&dot).expect("root vertex must exist");

        // update id
        self.id += 1;

        log!("Finder::strong_connect {:?} with id {}", dot, self.id);

        // set id and low for vertex
        vertex.set_id(self.id);
        vertex.update_low(|_| self.id);

        // add to the stack
        vertex.set_on_stack(true);
        self.stack.push_front(dot);

        // for all deps
        for (process_id, eset) in executed_clock.clone().into_iter() {
            // compute non-executed deps for each process
            for dep in vertex.clock().subtract_iter(&process_id, eset) {
                // create dot and find vertex
                let dep_dot = Dot::new(process_id, dep);
                log!("Finder::strong_connect non-executed {:?}", dep_dot);

                match vertex_index.get(&dep_dot) {
                    None => {
                        // not necesserarily a missing dependency, since it may not conflict
                        // with `dot` but we can't be sure until we have it locally
                        log!("Finder::strong_connect missing {:?}", dep_dot);
                        return FinderResult::MissingDependency;
                    }
                    Some(dep_vertex) => {
                        // ignore non-conflicting commands
                        if !vertex.conflicts(&dep_vertex) {
                            log!("Finder::strong_connect non-conflicting {:?}", dep_dot);
                            continue;
                        }

                        // if not visited, visit
                        if dep_vertex.id() == 0 {
                            log!("Finder::strong_connect non-visited {:?}", dep_dot);
                            let result = self.strong_connect(dep_dot, executed_clock, vertex_index);
                            if result == FinderResult::MissingDependency {
                                return result;
                            }

                            // min low with dep low
                            vertex.update_low(|low| cmp::min(low, dep_vertex.low()));
                        } else {
                            // if visited and on the stack
                            if dep_vertex.on_stack() {
                                log!("Finder::strong_connect dependency on stack {:?}", dep_dot);
                                // min low with dep id
                                vertex.update_low(|low| cmp::min(low, dep_vertex.id()));
                            }
                        }
                    }
                }
            }
        }

        // if after visiting all neighbors, an SCC was found if vertex.id == vertex.low
        // - good news: the SCC members are on the stack
        if vertex.id() == vertex.low() {
            let mut scc = SCC::new();

            loop {
                // pop an element from the stack
                let member_dot = self
                    .stack
                    .pop_front()
                    .expect("there should be an SCC member on the stack");

                log!("Finder::strong_connect new SCC member {:?}", member_dot);

                // get its vertex and change its `on_stack` value
                let member_vertex = vertex_index
                    .get_mut(&member_dot)
                    .expect("stack member should exist");
                member_vertex.set_on_stack(false);

                // add it to the SCC and check it wasn't there before
                assert!(scc.insert(member_dot));

                // quit if root is found
                if member_dot == dot {
                    break;
                }
            }
            // add scc to to the set of sccs
            self.sccs.push(scc);
            FinderResult::Found
        } else {
            FinderResult::NotFound
        }
    }
}

pub struct Vertex {
    dot: Dot,
    cmd: Command,
    clock: VClock<ProcessId>,
    // specific to tarjan's algorithm
    id: usize,
    low: usize,
    on_stack: bool,
}

impl Vertex {
    pub fn new(dot: Dot, cmd: Command, clock: VClock<ProcessId>) -> Self {
        Self {
            dot,
            cmd,
            clock,
            id: 0,
            low: 0,
            on_stack: false,
        }
    }

    /// Consumes the vertex, returning its command.
    pub fn into_command(self) -> Command {
        self.cmd
    }

    /// Retrieves vertex's dot.
    pub fn dot(&self) -> Dot {
        self.dot
    }

    /// Retrieves vertex's command.
    pub fn command(&self) -> &Command {
        &self.cmd
    }

    /// Retrieves vertex's clock.
    fn clock(&self) -> &VClock<ProcessId> {
        &self.clock
    }

    /// Retrieves vertex's id.
    fn id(&self) -> usize {
        self.id
    }

    /// Retrieves vertex's low.
    fn low(&self) -> usize {
        self.low
    }

    /// Check if vertex is on the stack.
    fn on_stack(&self) -> bool {
        self.on_stack
    }

    /// Sets vertex's id.
    fn set_id(&mut self, id: usize) {
        self.id = id;
    }

    /// Updates vertex's low.
    fn update_low<F>(&mut self, update: F)
    where
        F: FnOnce(usize) -> usize,
    {
        self.low = update(self.low);
    }

    /// Sets if vertex is on the stack or not.
    fn set_on_stack(&mut self, on_stack: bool) {
        self.on_stack = on_stack;
    }

    /// This vertex conflicts with another vertex by checking if their commands conflict.
    fn conflicts(&self, other: &Vertex) -> bool {
        self.cmd.conflicts(&other.cmd)
    }
}
