use crate::log;
use crate::protocol::Protocol;
use crate::run::prelude::*;
use crate::run::rw::Rw;
use futures::future::FutureExt;
use futures::select_biased;
use tokio::fs::File;
use tokio::time::{self, Duration};

const EXECUTION_LOGGER_FLUSH_INTERVAL: u64 = 1000; // flush every second
const EXECUTION_LOGGER_BUFFER_SIZE: usize = 8 * 1024; // 8KB

pub async fn execution_logger_task<P>(
    execution_log: String,
    mut from_workers: ExecutionInfoReceiver<P>,
) where
    P: Protocol,
{
    println!(
        "[execution_logger] started with log {}",
        execution_log
    );

    // create execution log file (truncating it if already exists)
    let file = File::create(execution_log)
        .await
        .expect("it should be possible to create execution log file");

    // create file logger
    let mut logger = Rw::from(
        EXECUTION_LOGGER_BUFFER_SIZE,
        EXECUTION_LOGGER_BUFFER_SIZE,
        file,
    );

    // create interval
    let mut interval =
        time::interval(Duration::from_millis(EXECUTION_LOGGER_FLUSH_INTERVAL));

    loop {
        select_biased! {
            execution_info = from_workers.recv().fuse() => {
                log!("[executor_logger] from parent: {:?}", execution_info);
                if let Some(execution_info) = execution_info {
                    // write execution info to file
                    logger.write(execution_info).await;
                } else {
                    println!("[executor_logger] error while receiving execution info from parent");
                }
            }
            _ = interval.tick().fuse()  => {
                // flush
                logger.flush().await
            }
        }
    }
}