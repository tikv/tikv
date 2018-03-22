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
mod service;
mod util;
mod readpool_context;
pub mod local_metrics;
pub mod codec;

pub use self::readpool_context::Context as ReadPoolContext;

use std::result;
use std::error;
use std::time::Duration;

use kvproto::{coprocessor as coppb, errorpb, kvrpcpb};

use util::time::Instant;
use storage::{self, engine, mvcc, txn};

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

trait RequestHandler: Send {
    fn handle_request(&mut self, batch_row_limit: usize) -> Result<coppb::Response> {
        panic!("unsupported operation");
    }

    fn handle_streaming_request(
        &mut self,
        batch_row_limit: usize,
    ) -> Result<(Option<coppb::Response>, bool)> {
        panic!("unsupported operation");
    }

    fn collect_metrics_into(&mut self, metrics: &mut self::dag::executor::ExecutorMetrics) {
        // Do nothing
    }
}

/// `RequestHandlerBuilder` accepts a `Box<Snapshot>` and builds a `RequestHandler`.
type RequestHandlerBuilder = FnOnce(Box<storage::Snapshot + Send>) -> Result<Box<RequestHandler>>;

#[derive(Debug)]
struct ReqContext {
    pub start_time: Instant,
    pub deadline: Instant,
    pub isolation_level: kvrpcpb::IsolationLevel,
    pub fill_cache: bool,
    pub table_scan: bool, // Whether is a table scan request.
}

impl ReqContext {
    pub fn new(
        ctx: &kvrpcpb::Context,
        table_scan: bool,
        max_handle_duration: Duration,
    ) -> ReqContext {
        let start_time = Instant::now_coarse();
        let deadline = start_time + max_handle_duration;
        ReqContext {
            start_time,
            deadline,
            isolation_level: ctx.get_isolation_level(),
            fill_cache: !ctx.get_not_fill_cache(),
            table_scan,
        }
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
