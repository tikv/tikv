#![allow(dead_code)]

use std::thread;
use std::time::Duration;

use mio::{self, NotifyError};

pub mod store;
pub mod errors;
pub mod server;
pub use self::errors::{Result, Error, other};

const MAX_SEND_RETRY_CNT: i32 = 20;

// send_msg wraps Sender and retries some times if queue is full.
pub fn send_msg<M: Send>(sender: &mio::Sender<M>, msg: M) -> Result<()> {
    let mut value: M = msg;
    for _ in 0..MAX_SEND_RETRY_CNT {
        let r = sender.send(value);
        if r.is_ok() {
            return Ok(());
        }

        match r.unwrap_err() {
            NotifyError::Full(m) => {
                warn!("notify queue is full, sleep and retry");
                thread::sleep(Duration::from_millis(100));
                value = m;
                continue;
            }
            e => {
                return Err(other(format!("{:?}", e)));
            }
        }
    }

    // Now sender only supports Send trait, no Sync, so we can' use `try!` directly 
    // for outer use because our result error supports Sync + Send.
    // So here returns result instead of Err(NotifyError::Full(value)).
    // TODO: We can refactor it later if using quick_error.
    Err(other("notify channel is full"))
}
