// Copyright 2018 PingCAP, Inc.
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

pub mod kvget;
pub mod kvbatchget;
pub mod coprocessor;

use std::result;
use std::error;
use futures;

use storage;
use kvproto::coprocessor as coppb;

use super::*;

#[derive(Debug, Copy, Clone)]
pub enum Priority {
    ReadNormal,
    ReadLow,
    ReadHigh,
    ReadCritical,
}

quick_error!{
    #[derive(Debug)]
    pub enum Error {
        Storage(err: storage::Error) {
            from()
            cause(err)
            description(err.description())
            display("storage error, err = {:?}", err)
        }
        Other(err: Box<error::Error + Sync + Send>) {
            from()
            cause(err.as_ref())
            description(err.description())
            display("{:?}", err)
        }
    }
}

#[derive(Debug)]
pub enum Value {
    StorageValue(Option<storage::Value>),
    StorageMultiKvpairs(Vec<storage::Result<storage::KvPair>>),
    Coprocessor(coppb::Response),
}

pub type Result = result::Result<Value, Error>;

pub type BoxedFuture = Box<futures::Future<Item = Value, Error = Error> + Send>;

pub trait Task {
    fn build(&mut self, context: &WorkerThreadContext) -> BoxedFuture;
}

#[cfg(test)]
mod tests {
    pub use super::super::tests::*;
}
