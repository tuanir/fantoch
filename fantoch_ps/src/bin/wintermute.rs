mod common;

use color_eyre::Report;
use fantoch_ps::protocol::WintermuteSequential;

fn main() -> Result<(), Report> {
    common::protocol::run::<WintermuteSequential>()
}
