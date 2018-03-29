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

mod endpoint;
mod metrics;
mod dag;
mod statistics;
mod checksum;
mod readpool_context;
pub mod local_metrics;
pub mod util;
pub mod codec;

pub use self::endpoint::err_resp;
pub use self::readpool_context::Context as ReadPoolContext;

use std::result;
use std::error;
use std::time::Duration;

use kvproto::{errorpb, kvrpcpb};

use util::time::Instant;
use storage;

const SINGLE_GROUP: &[u8] = b"SingleGroup";

quick_error! {
    #[derive(Debug)]
    pub enum Error {
        Region(err: errorpb::Error) {
            description("region related failure")
            display("region {:?}", err)
        }
        Locked(l: kvrpcpb::LockInfo) {
            description("key is locked")
            display("locked {:?}", l)
        }
        Outdated(elapsed: Duration, tag: &'static str) {
            description("request is outdated")
        }
        Full(allow: usize) {
            description("running queue is full")
        }
        Other(err: Box<error::Error + Send + Sync>) {
            from()
            cause(err.as_ref())
            description(err.description())
            display("unknown error {:?}", err)
        }
    }
}

pub type Result<T> = result::Result<T, Error>;

impl From<storage::engine::Error> for Error {
    fn from(e: storage::engine::Error) -> Error {
        match e {
            storage::engine::Error::Request(e) => Error::Region(e),
            _ => Error::Other(box e),
        }
    }
}

impl From<storage::txn::Error> for Error {
    fn from(e: storage::txn::Error) -> Error {
        match e {
            storage::txn::Error::Mvcc(storage::mvcc::Error::KeyIsLocked {
                primary,
                ts,
                key,
                ttl,
            }) => {
                let mut info = kvrpcpb::LockInfo::new();
                info.set_primary_lock(primary);
                info.set_lock_version(ts);
                info.set_key(key);
                info.set_lock_ttl(ttl);
                Error::Locked(info)
            }
            _ => Error::Other(box e),
        }
    }
}

pub use self::endpoint::{Host as EndPointHost, RequestTask, Task as EndPointTask,
                         DEFAULT_REQUEST_MAX_HANDLE_SECS, REQ_TYPE_CHECKSUM, REQ_TYPE_DAG};
pub use self::dag::{ScanOn, Scanner};

#[derive(Debug)]
pub struct ReqContext {
    pub context: kvrpcpb::Context,
    pub table_scan: bool, // Whether is a table scan request.
    pub txn_start_ts: u64,
    pub start_time: Instant,
    pub deadline: Instant,
}

impl ReqContext {
    pub fn new(ctx: &kvrpcpb::Context, txn_start_ts: u64, table_scan: bool) -> ReqContext {
        let start_time = Instant::now_coarse();
        ReqContext {
            context: ctx.clone(),
            table_scan,
            txn_start_ts,
            start_time,
            deadline: start_time,
        }
    }

    pub fn set_max_handle_duration(&mut self, request_max_handle_duration: Duration) {
        self.deadline = self.start_time + request_max_handle_duration;
    }

    #[inline]
    pub fn get_scan_tag(&self) -> &'static str {
        if self.table_scan {
            "select"
        } else {
            "index"
        }
    }

    pub fn check_if_outdated(&self) -> Result<()> {
        let now = Instant::now_coarse();
        if self.deadline <= now {
            let elapsed = now.duration_since(self.start_time);
            return Err(Error::Outdated(elapsed, self.get_scan_tag()));
        }
        Ok(())
    }
}
