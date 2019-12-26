// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

#[macro_use]
extern crate quick_error;
#[allow(unused_extern_crates)]
extern crate tikv_alloc;

mod lock;
mod timestamp;
mod types;
mod write;

use std::io;

pub use lock::{Lock, LockType};
pub use timestamp::{TimeStamp, TsSet};
pub use types::{is_short_value, Key, KvPair, Mutation, Value, SHORT_VALUE_MAX_LEN};
pub use write::{Write, WriteRef, WriteType};

quick_error! {
    #[derive(Debug)]
    pub enum Error {
        Io(err: io::Error) {
            from()
            cause(err)
            description(err.description())
        }
        Codec(err: tikv_util::codec::Error) {
            from()
            cause(err)
            description(err.description())
        }
        BadFormatLock { description("bad format lock data") }
        BadFormatWrite { description("bad format write data") }
        KeyIsLocked(info: kvproto::kvrpcpb::LockInfo) {
            description("key is locked (backoff or cleanup)")
            display("key is locked (backoff or cleanup) {:?}", info)
        }
    }
}

impl Error {
    pub fn maybe_clone(&self) -> Option<Error> {
        match self {
            Error::Codec(e) => e.maybe_clone().map(Error::Codec),
            Error::BadFormatLock => Some(Error::BadFormatLock),
            Error::BadFormatWrite => Some(Error::BadFormatWrite),
            Error::KeyIsLocked(info) => Some(Error::KeyIsLocked(info.clone())),
            Error::Io(_) => None,
        }
    }
}

pub type Result<T> = std::result::Result<T, Error>;
