// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

mod point_getter;
mod reader;
mod scanner;

use txn_types::{TimeStamp, Write, WriteType};

#[cfg(test)]
pub use self::reader::tests as reader_tests;
pub use self::{
    point_getter::{PointGetter, PointGetterBuilder},
    reader::{MvccReader, SnapshotReader},
    scanner::{
        has_data_in_range, near_load_data_by_write, seek_for_valid_write, test_util, DeltaScanner,
        EntryScanner, Scanner, ScannerBuilder,
    },
};

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum NewerTsCheckState {
    Unknown,
    Met,
    NotMetYet,
}

/// The result of `get_txn_commit_record`, which is used to get the status of a specified
/// transaction from write cf.
#[derive(Debug)]
pub enum TxnCommitRecord {
    /// The commit record of the given transaction is not found. But it's possible that there's
    /// another transaction's commit record, whose `commit_ts` equals to the current transaction's
    /// `start_ts`. That kind of record will be returned via the `overlapped_write` field.
    /// In this case, if the current transaction is to be rolled back, the `overlapped_write` must not
    /// be overwritten.
    None {
        overlapped_write: Option<OverlappedWrite>,
    },
    /// Found the transaction's write record.
    SingleRecord { commit_ts: TimeStamp, write: Write },
    /// The transaction's status is found in another transaction's record's `overlapped_rollback`
    /// field. This may happen when the current transaction's `start_ts` is the same as the
    /// `commit_ts` of another transaction on this key.
    OverlappedRollback { commit_ts: TimeStamp },
}

#[derive(Clone, Debug)]
pub struct OverlappedWrite {
    pub write: Write,
    /// GC fence for `overlapped_write`. PTAL at `txn_types::Write::gc_fence`.
    pub gc_fence: TimeStamp,
}

impl TxnCommitRecord {
    pub fn exist(&self) -> bool {
        match self {
            Self::None { .. } => false,
            Self::SingleRecord { .. } | Self::OverlappedRollback { .. } => true,
        }
    }

    pub fn info(&self) -> Option<(TimeStamp, WriteType)> {
        match self {
            Self::None { .. } => None,
            Self::SingleRecord { commit_ts, write } => Some((*commit_ts, write.write_type)),
            Self::OverlappedRollback { commit_ts } => Some((*commit_ts, WriteType::Rollback)),
        }
    }

    pub fn unwrap_single_record(self) -> (TimeStamp, WriteType) {
        match self {
            Self::SingleRecord { commit_ts, write } => (commit_ts, write.write_type),
            _ => panic!("not a single record: {:?}", self),
        }
    }

    pub fn unwrap_overlapped_rollback(self) -> TimeStamp {
        match self {
            Self::OverlappedRollback { commit_ts } => commit_ts,
            _ => panic!("not an overlapped rollback record: {:?}", self),
        }
    }

    pub fn unwrap_none(self) -> Option<OverlappedWrite> {
        match self {
            Self::None { overlapped_write } => overlapped_write,
            _ => panic!("txn record found but not expected: {:?}", self),
        }
    }
}
