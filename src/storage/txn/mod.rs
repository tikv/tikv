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

mod store;
mod scheduler;
mod mem_rowlock;
mod shard_mutex;

use std::error;
use std::thread;
use std::time::Duration;
use mio::{self, NotifyError};
use std::io::Error as IoError;

pub use self::scheduler::{Scheduler, Msg};
pub use self::store::{SnapshotStore, TxnStore};

const MAX_SEND_RETRY_CNT: i32 = 20;

quick_error! {
    #[derive(Debug)]
    pub enum Error {
        Engine(err: ::storage::engine::Error) {
            from()
            cause(err)
            description(err.description())
        }
        Codec(err: ::util::codec::Error) {
            from()
            cause(err)
            description(err.description())
        }
        Mvcc(err: ::storage::mvcc::Error) {
            from()
            cause(err)
            description(err.description())
        }
        Other(err: Box<error::Error + Sync + Send>) {
            from()
            cause(err.as_ref())
            description(err.description())
            display("{:?}", err)
        }
        Io(err: IoError) {
            from()
            cause(err)
            description(err.description())
        }
    }
}

pub type Result<T> = ::std::result::Result<T, Error>;


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

    Err(box_err!("notify channel is full"))
}

#[derive(Debug)]
pub struct SchedCh {
    ch: mio::Sender<Msg>,
}

impl Clone for SchedCh {
    fn clone(&self) -> SchedCh {
        SchedCh { ch: self.ch.clone() }
    }
}

impl SchedCh {
    pub fn new(ch: mio::Sender<Msg>) -> SchedCh {
        SchedCh { ch: ch }
    }

    pub fn send(&self, msg: Msg) -> Result<()> {
        try!(send_msg(&self.ch, msg));
        Ok(())
    }
}
