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

mod util;
mod collector;
mod endpoint;
mod aggregate;
mod metrics;

use kvproto::kvrpcpb::LockInfo;
use kvproto::errorpb;
use tipb::select::{SelectRequest, Chunk};

use std::{result, error};
use std::collections::HashMap;

use storage::{txn, engine, mvcc};
use ::util::xeval::Evaluator;

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
            txn::Error::Mvcc(mvcc::Error::KeyIsLocked { primary, ts, key }) => {
                let mut info = LockInfo::new();
                info.set_primary_lock(primary);
                info.set_lock_version(ts);
                info.set_key(key);
                Error::Locked(info)
            }
            _ => Error::Other(box e),
        }
    }
}

/// An abstract trait to handle row.
///
/// A common work flow is:
///     1. create a collector;
///     2. collect all qualified rows (pass condition check);
///     3. take the collection.
trait Collector: Sized {
    /// Create a Collector.
    fn create(sel: &SelectRequest) -> Result<Self>;

    /// Collect a row.
    ///
    /// Only qualified row will be delivered to this function.
    /// Returns the number of collected row.
    fn collect(&mut self,
               eval: &mut Evaluator,
               handle: i64,
               val: &HashMap<i64, &[u8]>)
               -> Result<usize>;

    /// Take the collection and finish handling.
    fn take_collection(&mut self) -> Result<Vec<Chunk>>;
}

pub use self::endpoint::{Host as EndPointHost, RequestTask, REQ_TYPE_SELECT, REQ_TYPE_INDEX,
                         Task as EndPointTask};
pub use self::aggregate::SINGLE_GROUP;
