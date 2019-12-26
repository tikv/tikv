// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::error;
use std::fmt::{self, Debug, Display, Formatter};
use std::io::Error as IoError;

use crate::storage::{
    kv::{self, Error as EngineError, ErrorInner as EngineErrorInner},
    mvcc::{self, Error as MvccError, ErrorInner as MvccErrorInner},
    txn::{self, Error as TxnError, ErrorInner as TxnErrorInner},
    Result,
};
use kvproto::{errorpb, kvrpcpb};
use txn_types::{KvPair, TimeStamp};

quick_error! {
    #[derive(Debug)]
    pub enum ErrorInner {
        Engine(err: kv::Error) {
            from()
            cause(err)
            description(err.description())
        }
        Txn(err: txn::Error) {
            from()
            cause(err)
            description(err.description())
        }
        Mvcc(err: mvcc::Error) {
            from()
            cause(err)
            description(err.description())
        }
        Closed {
            description("storage is closed.")
        }
        Other(err: Box<dyn error::Error + Send + Sync>) {
            from()
            cause(err.as_ref())
            description(err.description())
        }
        Io(err: IoError) {
            from()
            cause(err)
            description(err.description())
        }
        SchedTooBusy {
            description("scheduler is too busy")
        }
        GcWorkerTooBusy {
            description("gc worker is too busy")
        }
        KeyTooLarge(size: usize, limit: usize) {
            description("max key size exceeded")
            display("max key size exceeded, size: {}, limit: {}", size, limit)
        }
        InvalidCf (cf_name: String) {
            description("invalid cf name")
            display("invalid cf name: {}", cf_name)
        }
        PessimisticTxnNotEnabled {
            description("pessimistic transaction is not enabled")
        }
    }
}

pub struct Error(pub Box<ErrorInner>);

impl fmt::Debug for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&self.0, f)
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

impl std::error::Error for Error {
    fn description(&self) -> &str {
        std::error::Error::description(&self.0)
    }

    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        std::error::Error::source(&self.0)
    }
}

impl From<ErrorInner> for Error {
    #[inline]
    fn from(e: ErrorInner) -> Self {
        Error(Box::new(e))
    }
}

impl<T: Into<ErrorInner>> From<T> for Error {
    #[inline]
    default fn from(err: T) -> Self {
        let err = err.into();
        err.into()
    }
}

pub enum ErrorHeaderKind {
    NotLeader,
    RegionNotFound,
    KeyNotInRegion,
    EpochNotMatch,
    ServerIsBusy,
    StaleCommand,
    StoreNotMatch,
    RaftEntryTooLarge,
    Other,
}

impl ErrorHeaderKind {
    /// TODO: This function is only used for bridging existing & legacy metric tags.
    /// It should be removed once Coprocessor starts using new static metrics.
    pub fn get_str(&self) -> &'static str {
        match *self {
            ErrorHeaderKind::NotLeader => "not_leader",
            ErrorHeaderKind::RegionNotFound => "region_not_found",
            ErrorHeaderKind::KeyNotInRegion => "key_not_in_region",
            ErrorHeaderKind::EpochNotMatch => "epoch_not_match",
            ErrorHeaderKind::ServerIsBusy => "server_is_busy",
            ErrorHeaderKind::StaleCommand => "stale_command",
            ErrorHeaderKind::StoreNotMatch => "store_not_match",
            ErrorHeaderKind::RaftEntryTooLarge => "raft_entry_too_large",
            ErrorHeaderKind::Other => "other",
        }
    }
}

impl Display for ErrorHeaderKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.get_str())
    }
}

const SCHEDULER_IS_BUSY: &str = "scheduler is busy";
const GC_WORKER_IS_BUSY: &str = "gc worker is busy";

pub fn get_error_kind_from_header(header: &errorpb::Error) -> ErrorHeaderKind {
    if header.has_not_leader() {
        ErrorHeaderKind::NotLeader
    } else if header.has_region_not_found() {
        ErrorHeaderKind::RegionNotFound
    } else if header.has_key_not_in_region() {
        ErrorHeaderKind::KeyNotInRegion
    } else if header.has_epoch_not_match() {
        ErrorHeaderKind::EpochNotMatch
    } else if header.has_server_is_busy() {
        ErrorHeaderKind::ServerIsBusy
    } else if header.has_stale_command() {
        ErrorHeaderKind::StaleCommand
    } else if header.has_store_not_match() {
        ErrorHeaderKind::StoreNotMatch
    } else if header.has_raft_entry_too_large() {
        ErrorHeaderKind::RaftEntryTooLarge
    } else {
        ErrorHeaderKind::Other
    }
}

pub fn get_tag_from_header(header: &errorpb::Error) -> &'static str {
    get_error_kind_from_header(header).get_str()
}

pub fn extract_region_error<T>(res: &Result<T>) -> Option<errorpb::Error> {
    match *res {
        // TODO: use `Error::cause` instead.
        Err(Error(box ErrorInner::Engine(EngineError(box EngineErrorInner::Request(ref e)))))
        | Err(Error(box ErrorInner::Txn(TxnError(box TxnErrorInner::Engine(EngineError(
            box EngineErrorInner::Request(ref e),
        ))))))
        | Err(Error(box ErrorInner::Txn(TxnError(box TxnErrorInner::Mvcc(MvccError(
            box MvccErrorInner::Engine(EngineError(box EngineErrorInner::Request(ref e))),
        )))))) => Some(e.to_owned()),
        Err(Error(box ErrorInner::SchedTooBusy)) => {
            let mut err = errorpb::Error::default();
            let mut server_is_busy_err = errorpb::ServerIsBusy::default();
            server_is_busy_err.set_reason(SCHEDULER_IS_BUSY.to_owned());
            err.set_server_is_busy(server_is_busy_err);
            Some(err)
        }
        Err(Error(box ErrorInner::GcWorkerTooBusy)) => {
            let mut err = errorpb::Error::default();
            let mut server_is_busy_err = errorpb::ServerIsBusy::default();
            server_is_busy_err.set_reason(GC_WORKER_IS_BUSY.to_owned());
            err.set_server_is_busy(server_is_busy_err);
            Some(err)
        }
        Err(Error(box ErrorInner::Closed)) => {
            // TiKV is closing, return an RegionError to tell the client that this region is unavailable
            // temporarily, the client should retry the request in other TiKVs.
            let mut err = errorpb::Error::default();
            err.set_message("TiKV is Closing".to_string());
            Some(err)
        }
        _ => None,
    }
}

pub fn extract_committed(err: &Error) -> Option<TimeStamp> {
    match *err {
        Error(box ErrorInner::Txn(TxnError(box TxnErrorInner::Mvcc(MvccError(
            box MvccErrorInner::Committed { commit_ts },
        ))))) => Some(commit_ts),
        _ => None,
    }
}

pub fn extract_key_error(err: &Error) -> kvrpcpb::KeyError {
    let mut key_error = kvrpcpb::KeyError::default();
    match err {
        Error(box ErrorInner::Txn(TxnError(box TxnErrorInner::Mvcc(MvccError(
            box MvccErrorInner::KeyIsLocked(info),
        ))))) => {
            key_error.set_locked(info.clone());
        }
        // failed in prewrite or pessimistic lock
        Error(box ErrorInner::Txn(TxnError(box TxnErrorInner::Mvcc(MvccError(
            box MvccErrorInner::WriteConflict {
                start_ts,
                conflict_start_ts,
                conflict_commit_ts,
                key,
                primary,
                ..
            },
        ))))) => {
            let mut write_conflict = kvrpcpb::WriteConflict::default();
            write_conflict.set_start_ts(start_ts.into_inner());
            write_conflict.set_conflict_ts(conflict_start_ts.into_inner());
            write_conflict.set_conflict_commit_ts(conflict_commit_ts.into_inner());
            write_conflict.set_key(key.to_owned());
            write_conflict.set_primary(primary.to_owned());
            key_error.set_conflict(write_conflict);
            // for compatibility with older versions.
            key_error.set_retryable(format!("{:?}", err));
        }
        Error(box ErrorInner::Txn(TxnError(box TxnErrorInner::Mvcc(MvccError(
            box MvccErrorInner::AlreadyExist { key },
        ))))) => {
            let mut exist = kvrpcpb::AlreadyExist::default();
            exist.set_key(key.clone());
            key_error.set_already_exist(exist);
        }
        // failed in commit
        Error(box ErrorInner::Txn(TxnError(box TxnErrorInner::Mvcc(MvccError(
            box MvccErrorInner::TxnLockNotFound { .. },
        ))))) => {
            warn!("txn conflicts"; "err" => ?err);
            key_error.set_retryable(format!("{:?}", err));
        }
        Error(box ErrorInner::Txn(TxnError(box TxnErrorInner::Mvcc(MvccError(
            box MvccErrorInner::TxnNotFound { start_ts, key },
        ))))) => {
            let mut txn_not_found = kvrpcpb::TxnNotFound::default();
            txn_not_found.set_start_ts(start_ts.into_inner());
            txn_not_found.set_primary_key(key.to_owned());
            key_error.set_txn_not_found(txn_not_found);
        }
        Error(box ErrorInner::Txn(TxnError(box TxnErrorInner::Mvcc(MvccError(
            box MvccErrorInner::Deadlock {
                lock_ts,
                lock_key,
                deadlock_key_hash,
                ..
            },
        ))))) => {
            warn!("txn deadlocks"; "err" => ?err);
            let mut deadlock = kvrpcpb::Deadlock::default();
            deadlock.set_lock_ts(lock_ts.into_inner());
            deadlock.set_lock_key(lock_key.to_owned());
            deadlock.set_deadlock_key_hash(*deadlock_key_hash);
            key_error.set_deadlock(deadlock);
        }
        Error(box ErrorInner::Txn(TxnError(box TxnErrorInner::Mvcc(MvccError(
            box MvccErrorInner::CommitTsExpired {
                start_ts,
                commit_ts,
                key,
                min_commit_ts,
            },
        ))))) => {
            let mut commit_ts_expired = kvrpcpb::CommitTsExpired::default();
            commit_ts_expired.set_start_ts(start_ts.into_inner());
            commit_ts_expired.set_attempted_commit_ts(commit_ts.into_inner());
            commit_ts_expired.set_key(key.to_owned());
            commit_ts_expired.set_min_commit_ts(min_commit_ts.into_inner());
            key_error.set_commit_ts_expired(commit_ts_expired);
        }
        _ => {
            error!("txn aborts"; "err" => ?err);
            key_error.set_abort(format!("{:?}", err));
        }
    }
    key_error
}

pub fn extract_kv_pairs(res: Result<Vec<Result<KvPair>>>) -> Vec<kvrpcpb::KvPair> {
    match res {
        Ok(res) => res
            .into_iter()
            .map(|r| match r {
                Ok((key, value)) => {
                    let mut pair = kvrpcpb::KvPair::default();
                    pair.set_key(key);
                    pair.set_value(value);
                    pair
                }
                Err(e) => {
                    let mut pair = kvrpcpb::KvPair::default();
                    pair.set_error(extract_key_error(&e));
                    pair
                }
            })
            .collect(),
        Err(e) => {
            let mut pair = kvrpcpb::KvPair::default();
            pair.set_error(extract_key_error(&e));
            vec![pair]
        }
    }
}

pub fn extract_key_errors(res: Result<Vec<Result<()>>>) -> Vec<kvrpcpb::KeyError> {
    match res {
        Ok(res) => res
            .into_iter()
            .filter_map(|x| match x {
                Err(e) => Some(extract_key_error(&e)),
                Ok(_) => None,
            })
            .collect(),
        Err(e) => vec![extract_key_error(&e)],
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_extract_key_error_write_conflict() {
        let start_ts = 110.into();
        let conflict_start_ts = 108.into();
        let conflict_commit_ts = 109.into();
        let key = b"key".to_vec();
        let primary = b"primary".to_vec();
        let case = Error::from(TxnError::from(MvccError::from(
            MvccErrorInner::WriteConflict {
                start_ts,
                conflict_start_ts,
                conflict_commit_ts,
                key: key.clone(),
                primary: primary.clone(),
            },
        )));
        let mut expect = kvrpcpb::KeyError::default();
        let mut write_conflict = kvrpcpb::WriteConflict::default();
        write_conflict.set_start_ts(start_ts.into_inner());
        write_conflict.set_conflict_ts(conflict_start_ts.into_inner());
        write_conflict.set_conflict_commit_ts(conflict_commit_ts.into_inner());
        write_conflict.set_key(key);
        write_conflict.set_primary(primary);
        expect.set_conflict(write_conflict);
        expect.set_retryable(format!("{:?}", case));

        let got = extract_key_error(&case);
        assert_eq!(got, expect);
    }
}
