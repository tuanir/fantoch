mod compress;
mod dstat;
mod exp_data;
mod results_db;

// Re-exports.
pub use compress::{DstatCompress, LatencyPrecision, MicrosHistogramCompress};
pub use dstat::Dstat;
pub use exp_data::ExperimentData;
pub use results_db::ResultsDB;

use fantoch::client::KeyGen;
use fantoch_exp::Protocol;

#[derive(Debug, Clone, Copy)]
pub struct Search {
    pub n: usize,
    pub f: usize,
    pub protocol: Protocol,
    pub shard_count: Option<usize>,
    pub cpus: Option<usize>,
    pub clients_per_region: Option<usize>,
    pub key_gen: Option<KeyGen>,
    pub keys_per_command: Option<usize>,
    pub payload_size: Option<usize>,
}

impl Search {
    pub fn new(n: usize, f: usize, protocol: Protocol) -> Self {
        Self {
            n,
            f,
            protocol,
            shard_count: None,
            cpus: None,
            clients_per_region: None,
            key_gen: None,
            keys_per_command: None,
            payload_size: None,
        }
    }

    pub fn shard_count(&mut self, shard_count: usize) -> &mut Self {
        self.shard_count = Some(shard_count);
        self
    }

    pub fn cpus(&mut self, cpus: usize) -> &mut Self {
        self.cpus = Some(cpus);
        self
    }

    pub fn clients_per_region(
        &mut self,
        clients_per_region: usize,
    ) -> &mut Self {
        self.clients_per_region = Some(clients_per_region);
        self
    }

    pub fn key_gen(&mut self, key_gen: KeyGen) -> &mut Self {
        self.key_gen = Some(key_gen);
        self
    }

    pub fn keys_per_command(&mut self, keys_per_command: usize) -> &mut Self {
        self.keys_per_command = Some(keys_per_command);
        self
    }

    pub fn payload_size(&mut self, payload_size: usize) -> &mut Self {
        self.payload_size = Some(payload_size);
        self
    }
}
