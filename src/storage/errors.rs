// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::error;
use std::fmt::{self, Debug, Display, Formatter};
use std::io::Error as IoError;

use kvproto::errorpb;

use crate::storage::{kv::Error as EngineError, mvcc, txn};

quick_error! {
    #[derive(Debug)]
    pub enum ErrorInner {
        Engine(err: EngineError) {
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
