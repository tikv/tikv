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

mod meta;
mod txn;

pub use self::txn::MvccTxn;

quick_error! {
    #[derive(Debug)]
    pub enum Error {
        Engine(err: ::storage::engine::Error) {
            from()
            cause(err)
            description(err.description())
        }
        ProtoBuf(err: ::protobuf::error::ProtobufError) {
            from()
            cause(err)
            description(err.description())
        }
        Codec(err: ::util::codec::Error) {
            from()
            cause(err)
            description(err.description())
        }
        KeyIsLocked {key: Vec<u8>, primary: Vec<u8>, ts: u64} {
            description("key is locked (backoff or cleanup)")
            display("key is locked (backoff or cleanup) {:?}-{:?}@{}", key, primary, ts)
        }
        AlreadyCommitted {commit_ts: u64} {
            description("txn already committed")
            display("txn already committed @{}", commit_ts)
        }
        TxnLockNotFound {description("txn lock not found")}
        WriteConflict {description("write conflict")}
        KeyVersion {description("bad format key(version)")}
        AlreadyRolledback {description("txn already rolled back")}
    }
}

pub type Result<T> = ::std::result::Result<T, Error>;
