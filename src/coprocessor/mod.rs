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
pub mod local_metrics;
mod metrics;
mod statistics;

pub use self::endpoint::err_resp;
use std::error;
use std::result;
use std::time::Duration;

use kvproto::errorpb;
use kvproto::kvrpcpb::LockInfo;
use tipb::select;

use self::dag::expr;
use storage::{engine, mvcc, txn};
use util;

quick_error! {
    #[derive(Debug)]
    pub enum Error {
        Region(err: errorpb::Error) {
            description("region related failure")
            display("region {:?}", err)
        }
        Locked(l: LockInfo) {
            description("key is locked")
            display("locked {:?}", l)
        }
        Outdated(elapsed: Duration, tag: &'static str) {
            description("request is outdated")
        }
        Full(allow: usize) {
            description("running queue is full")
        }
        Eval(err:select::Error) {
            from()
            description("eval failed")
            display("eval error {:?}",err)
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

impl From<expr::Error> for Error {
    fn from(e: expr::Error) -> Error {
        Error::Eval(e.into())
    }
}

impl From<util::codec::Error> for Error {
    fn from(e: util::codec::Error) -> Error {
        let mut err = select::Error::new();
        err.set_msg(format!("{}", e));
        Error::Eval(err)
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
                let mut info = LockInfo::new();
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

pub use self::dag::{ScanOn, Scanner};
pub use self::endpoint::{Host as EndPointHost, RequestTask, Task as EndPointTask,
                         DEFAULT_REQUEST_MAX_HANDLE_SECS, REQ_TYPE_CHECKSUM, REQ_TYPE_DAG,
                         SINGLE_GROUP};
