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
mod local_metrics;
mod dag;
mod statistics;
mod util;
pub mod codec;

use std::result;
use std::error;
use std::sync::Arc;
use std::time::Duration;

use kvproto::kvrpcpb;
use kvproto::errorpb;
use kvproto::coprocessor as coppb;

use storage::{engine, mvcc, txn, Snapshot, Statistics};
use util::time::Instant;
use self::local_metrics::ScanCounter;
pub use self::endpoint::Host as EndPointHost;

// If a request has been handled for more than 60 seconds, the client should
// be timeout already, so it can be safely aborted.
const REQUEST_MAX_HANDLE_SECS: u64 = 60;

pub const SINGLE_GROUP: &[u8] = b"SingleGroup";

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
        Outdated(deadline: Instant, now: Instant, tag: String) {
            description("request is outdated")
        }
        Full {
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

impl From<engine::Error> for Error {
    fn from(e: engine::Error) -> Error {
        match e {
            engine::Error::Request(e) => Error::Region(e),
            _ => Error::Other(box e),
        }
    }
}

impl From<txn::Error> for Error {
    fn from(e: txn::Error) -> Error {
        match e {
            txn::Error::Mvcc(mvcc::Error::KeyIsLocked {
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

pub struct CopContext {
    pub tag: String,

    // The deadline before which the task should be responded.
    pub deadline: Instant,
    pub isolation_level: kvrpcpb::IsolationLevel,
    pub fill_cache: bool,
}

impl CopContext {
    fn new(ctx: &kvrpcpb::Context, tag: &str) -> CopContext {
        let deadline = Instant::now_coarse() + Duration::from_secs(REQUEST_MAX_HANDLE_SECS);
        CopContext {
            tag: tag.to_string(),
            deadline,
            isolation_level: ctx.get_isolation_level(),
            fill_cache: !ctx.get_not_fill_cache(),
        }
    }

    pub fn check_if_outdated(&self) -> Result<()> {
        let now = Instant::now_coarse();
        if self.deadline <= now {
            return Err(Error::Outdated(self.deadline, now, self.tag.clone()));
        }
        Ok(())
    }
}

#[derive(Default)]
struct CopStats {
    stats: Statistics,
    scan_counter: ScanCounter,
}

trait CopRequest: Send {
    fn handle(&mut self, snapshot: Box<Snapshot>, stats: &mut CopStats) -> Result<coppb::Response>;
    fn get_context(&self) -> &kvrpcpb::Context;
    fn get_cop_context(&self) -> Arc<CopContext>;
}
