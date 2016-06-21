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

pub use self::meta::FIRST_META_INDEX;
pub use self::txn::{MvccTxn, MvccSnapshot, MvccCursor};
use util::escape;

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
            display("key is locked (backoff or cleanup) {}-{}@{}", escape(key), escape(primary), ts)
        }
        AlreadyCommitted {commit_ts: u64} {
            description("txn already committed")
            display("txn already committed @{}", commit_ts)
        }
        TxnLockNotFound {description("txn lock not found")}
        WriteConflict {description("write conflict")}
        KeyVersion {description("bad format key(version)")}
    }
}

pub type Result<T> = ::std::result::Result<T, Error>;

// Make sure meta version in tests could never catch up with key version(timestamp).
pub const TEST_TS_BASE: u64 = 1000000;

use kvproto::kvpb::{Row, Column, Mutation, Op};

pub fn default_cols() -> Vec<Vec<u8>> {
    vec![vec![]]
}

pub fn is_row_value_empty(row: &Row) -> bool {
    row.get_columns().len() == 0
}

pub fn default_row_value(row: &Row) -> Option<Vec<u8>> {
    if is_row_value_empty(row) {
        return None;
    }
    Some(row.get_columns()[0].get_value().to_owned())
}

pub fn default_put(row_key: &[u8], value: &[u8]) -> Mutation {
    let mut mutation = Mutation::new();
    mutation.set_row_key(row_key.to_vec());
    mutation.mut_ops().push(Op::Put);
    let mut col = Column::new();
    col.set_name(vec![]);
    col.set_value(value.to_vec());
    mutation.mut_columns().push(col);
    mutation
}

pub fn default_del(row_key: &[u8]) -> Mutation {
    let mut mutation = Mutation::new();
    mutation.set_row_key(row_key.to_vec());
    mutation.mut_ops().push(Op::Del);
    let mut col = Column::new();
    col.set_name(vec![]);
    mutation.mut_columns().push(col);
    mutation
}

pub fn default_lock(row_key: &[u8]) -> Mutation {
    let mut mutation = Mutation::new();
    mutation.set_row_key(row_key.to_vec());
    mutation.mut_ops().push(Op::Lock);
    let mut col = Column::new();
    col.set_name(vec![]);
    mutation.mut_columns().push(col);
    mutation
}
