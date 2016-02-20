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
pub fn send_msg<M: Send>(ch: &mio::Sender<M>, msg: M) -> Result<()> {
    let mut value: M = msg;
    for _ in 0..MAX_SEND_RETRY_CNT {
        let r = ch.send(value);
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

    // TODO: if we refactor with quick_error, we can use NotifyError instead later.
    Err(other("notify channel is full"))
}
