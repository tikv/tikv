// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

#![allow(dead_code)]

use std::thread;
use std::time::Duration;

use mio::{self, NotifyError};

pub mod store;
pub mod errors;
pub mod coprocessor;
pub use self::errors::{Result, Error};

const MAX_SEND_RETRY_CNT: i32 = 20;

// send_msg wraps Sender and retries some times if queue is full.
pub fn send_msg<M: Send>(ch: &mio::Sender<M>, mut msg: M) -> Result<()> {
    for _ in 0..MAX_SEND_RETRY_CNT {
        let r = ch.send(msg);
        if r.is_ok() {
            return Ok(());
        }

        match r.unwrap_err() {
            NotifyError::Full(m) => {
                warn!("notify queue is full, sleep and retry");
                thread::sleep(Duration::from_millis(100));
                msg = m;
                continue;
            }
            e => {
                return Err(box_err!("{:?}", e));
            }
        }
    }

    // TODO: if we refactor with quick_error, we can use NotifyError instead later.
    Err(box_err!("notify channel is full"))
}
