mod protocol;

use planet_sim::protocol::{Newt, Protocol};
use std::error::Error;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let (process_id, sorted_processes, ip, port, client_port, addresses, config) =
        protocol::parse_args();
    let process = Newt::new(process_id, config);
    planet_sim::run::process(
        process,
        process_id,
        sorted_processes,
        ip,
        port,
        client_port,
        addresses,
        config,
    )
    .await?;
    Ok(())
}