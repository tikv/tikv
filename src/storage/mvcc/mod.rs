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

mod reader;
mod txn;
mod lock;
mod write;
mod metrics;

use std::io;
use std::error;
pub use self::txn::{MvccTxn, MAX_TXN_WRITE_SIZE};
pub use self::reader::MvccReader;
pub use self::lock::{Lock, LockType};
pub use self::write::{Write, WriteType};
use util::escape;

quick_error! {
    #[derive(Debug)]
    pub enum Error {
        Engine(err: ::storage::engine::Error) {
            from()
            cause(err)
            description(err.description())
        }
        Io(err: io::Error) {
            from()
            cause(err)
            description(err.description())
        }
        Codec(err: ::util::codec::Error) {
            from()
            cause(err)
            description(err.description())
        }
        KeyIsLocked {key: Vec<u8>, primary: Vec<u8>, ts: u64, ttl: u64} {
            description("key is locked (backoff or cleanup)")
            display("key is locked (backoff or cleanup) {}-{}@{} ttl {}",
                        escape(key),
                        escape(primary),
                        ts,
                        ttl)
        }
        BadFormatLock {description("bad format lock data")}
        BadFormatWrite {description("bad format write data")}
        Committed {commit_ts: u64} {
            description("txn already committed")
            display("txn already committed @{}", commit_ts)
        }
        TxnLockNotFound {start_ts: u64, commit_ts: u64, key: Vec<u8> } {
            description("txn lock not found")
            display("txn lock not found {}-{} key:{:?}", start_ts, commit_ts, key)
        }
        WriteConflict { start_ts: u64, conflict_ts: u64, key: Vec<u8>, primary: Vec<u8> } {
            description("write conflict")
            display("write conflict {} with {}, key:{:?}, primary:{:?}",
             start_ts, conflict_ts, key, primary)
        }
        KeyVersion {description("bad format key(version)")}
        Other(err: Box<error::Error + Sync + Send>) {
            from()
            cause(err.as_ref())
            description(err.description())
            display("{:?}", err)
        }
    }
}

impl Error {
    pub fn maybe_clone(&self) -> Option<Error> {
        match *self {
            Error::Engine(ref e) => e.maybe_clone().map(Error::Engine),
            Error::Codec(ref e) => e.maybe_clone().map(Error::Codec),
            Error::KeyIsLocked {
                ref key,
                ref primary,
                ts,
                ttl,
            } => Some(Error::KeyIsLocked {
                key: key.clone(),
                primary: primary.clone(),
                ts: ts,
                ttl: ttl,
            }),
            Error::BadFormatLock => Some(Error::BadFormatLock),
            Error::BadFormatWrite => Some(Error::BadFormatWrite),
            Error::TxnLockNotFound {
                start_ts,
                commit_ts,
                ref key,
            } => Some(Error::TxnLockNotFound {
                start_ts: start_ts,
                commit_ts: commit_ts,
                key: key.to_owned(),
            }),
            Error::WriteConflict {
                start_ts,
                conflict_ts,
                ref key,
                ref primary,
            } => Some(Error::WriteConflict {
                start_ts: start_ts,
                conflict_ts: conflict_ts,
                key: key.to_owned(),
                primary: primary.to_owned(),
            }),
            Error::KeyVersion => Some(Error::KeyVersion),
            Error::Committed { commit_ts } => Some(Error::Committed {
                commit_ts: commit_ts,
            }),
            Error::Io(_) | Error::Other(_) => None,
        }
    }
}

pub type Result<T> = ::std::result::Result<T, Error>;
