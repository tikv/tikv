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

mod checksum;
pub mod codec;
mod dag;
mod endpoint;
mod error;
pub mod local_metrics;
mod metrics;
mod readpool_context;
mod statistics;
mod util;

pub use self::endpoint::err_resp;
pub use self::error::{Error, Result};
pub use self::readpool_context::Context as ReadPoolContext;

use std::time::Duration;

use kvproto::kvrpcpb;

use util::time::Instant;

const SINGLE_GROUP: &[u8] = b"SingleGroup";

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

pub use self::dag::{ScanOn, Scanner};
pub use self::endpoint::{Host as EndPointHost, RequestTask, Task as EndPointTask,
                         DEFAULT_REQUEST_MAX_HANDLE_SECS, REQ_TYPE_CHECKSUM, REQ_TYPE_DAG};
