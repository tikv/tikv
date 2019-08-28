// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

//! Storage Transactions

mod latch;
mod process;
pub mod sched_pool;
pub mod scheduler;
mod store;

use std::error;
use std::io::Error as IoError;

pub use self::process::{execute_callback, ProcessResult, RESOLVE_LOCK_BATCH_SIZE};
pub use self::scheduler::{Msg, Scheduler};
pub use self::store::{EntryBatch, TxnEntry, TxnEntryScanner, TxnEntryStore};
pub use self::store::{FixtureStore, FixtureStoreScanner};
pub use self::store::{Scanner, SnapshotStore, Store};

quick_error! {
    #[derive(Debug)]
    pub enum Error {
        Engine(err: crate::storage::kv::Error) {
            from()
            cause(err)
            description(err.description())
        }
        Codec(err: tikv_util::codec::Error) {
            from()
            cause(err)
            description(err.description())
        }
        ProtoBuf(err: protobuf::error::ProtobufError) {
            from()
            cause(err)
            description(err.description())
        }
        Mvcc(err: crate::storage::mvcc::Error) {
            from()
            cause(err)
            description(err.description())
        }
        Other(err: Box<dyn error::Error + Sync + Send>) {
            from()
            cause(err.as_ref())
            description(err.description())
            display("{:?}", err)
        }
        Io(err: IoError) {
            from()
            cause(err)
            description(err.description())
        }
        InvalidTxnTso {start_ts: u64, commit_ts: u64} {
            description("Invalid transaction tso")
            display("Invalid transaction tso with start_ts:{},commit_ts:{}",
                        start_ts,
                        commit_ts)
        }
        InvalidReqRange {start: Option<Vec<u8>>,
                        end: Option<Vec<u8>>,
                        lower_bound: Option<Vec<u8>>,
                        upper_bound: Option<Vec<u8>>} {
            description("Invalid request range")
            display("Request range exceeds bound, request range:[{}, end:{}), physical bound:[{}, {})",
                        start.as_ref().map(hex::encode_upper).unwrap_or_else(|| "(none)".to_owned()),
                        end.as_ref().map(hex::encode_upper).unwrap_or_else(|| "(none)".to_owned()),
                        lower_bound.as_ref().map(hex::encode_upper).unwrap_or_else(|| "(none)".to_owned()),
                        upper_bound.as_ref().map(hex::encode_upper).unwrap_or_else(|| "(none)".to_owned()))
        }
    }
}

impl Error {
    pub fn maybe_clone(&self) -> Option<Error> {
        match *self {
            Error::Engine(ref e) => e.maybe_clone().map(Error::Engine),
            Error::Codec(ref e) => e.maybe_clone().map(Error::Codec),
            Error::Mvcc(ref e) => e.maybe_clone().map(Error::Mvcc),
            Error::InvalidTxnTso {
                start_ts,
                commit_ts,
            } => Some(Error::InvalidTxnTso {
                start_ts,
                commit_ts,
            }),
            Error::InvalidReqRange {
                ref start,
                ref end,
                ref lower_bound,
                ref upper_bound,
            } => Some(Error::InvalidReqRange {
                start: start.clone(),
                end: end.clone(),
                lower_bound: lower_bound.clone(),
                upper_bound: upper_bound.clone(),
            }),
            Error::Other(_) | Error::ProtoBuf(_) | Error::Io(_) => None,
        }
    }
}

pub type Result<T> = std::result::Result<T, Error>;
