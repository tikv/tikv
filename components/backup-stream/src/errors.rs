// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    error::Error as StdError, fmt::Display, io::Error as IoError, result::Result as StdResult,
};

use error_code::ErrorCodeExt;
use etcd_client::Error as EtcdError;
use kvproto::{errorpb::Error as StoreError, metapb::*};
use pd_client::Error as PdError;
use protobuf::ProtobufError;
use raftstore::Error as RaftStoreError;
use thiserror::Error as ThisError;
use tikv::storage::txn::Error as TxnError;
use tikv_util::{error, warn, worker::ScheduleError};

use crate::{endpoint::Task, metrics};

#[derive(ThisError, Debug)]
pub enum Error {
    #[error("Etcd meet error {0}")]
    Etcd(#[from] EtcdError),
    #[error("Protobuf meet error {0}")]
    Protobuf(#[from] ProtobufError),
    #[error("No such task {task_name:?}")]
    NoSuchTask { task_name: String },
    #[error("Observe have already canceled for region {0} (version = {1:?})")]
    ObserveCanceled(u64, RegionEpoch),
    #[error("Malformed metadata {0}")]
    MalformedMetadata(String),
    #[error("I/O Error: {0}")]
    Io(#[from] IoError),
    #[error("Txn error: {0}")]
    Txn(#[from] TxnError),
    #[error("TiKV scheduler error: {0}")]
    Sched(#[from] ScheduleError<Task>),
    #[error("PD client meet error: {0}")]
    Pd(#[from] PdError),
    #[error("Error during requesting raftstore: {0:?}")]
    RaftRequest(StoreError),
    #[error("Error from raftstore: {0}")]
    RaftStore(#[from] RaftStoreError),
    #[error("{context}: {inner_error}")]
    Contextual {
        context: String,
        inner_error: Box<Self>,
    },
    #[error("Other Error: {0}")]
    Other(#[from] Box<dyn StdError + Send + Sync + 'static>),
}

impl ErrorCodeExt for Error {
    fn error_code(&self) -> error_code::ErrorCode {
        use error_code::backup_stream::*;
        match self {
            Error::Etcd(_) => ETCD,
            Error::Protobuf(_) => PROTO,
            Error::NoSuchTask { .. } => NO_SUCH_TASK,
            Error::MalformedMetadata(_) => MALFORMED_META,
            Error::Io(_) => IO,
            Error::Txn(_) => TXN,
            Error::Sched(_) => SCHED,
            Error::Pd(_) => PD,
            Error::RaftRequest(_) => RAFTREQ,
            Error::Contextual { inner_error, .. } => inner_error.error_code(),
            Error::Other(_) => OTHER,
            Error::RaftStore(_) => RAFTSTORE,
            Error::ObserveCanceled(..) => OBSERVE_CANCELED,
        }
    }
}

impl<'a> ErrorCodeExt for &'a Error {
    fn error_code(&self) -> error_code::ErrorCode {
        Error::error_code(*self)
    }
}

pub type Result<T> = StdResult<T, Error>;

impl From<StoreError> for Error {
    fn from(e: StoreError) -> Self {
        Self::RaftRequest(e)
    }
}

pub trait ContextualResultExt<T>
where
    Self: Sized,
{
    /// attache a context to the error.
    fn context(self, context: impl ToString) -> Result<T>;

    fn context_with(self, context: impl Fn() -> String) -> Result<T>;
}

impl<T, E> ContextualResultExt<T> for StdResult<T, E>
where
    E: Into<Error>,
{
    #[inline(always)]
    fn context(self, context: impl ToString) -> Result<T> {
        self.map_err(|err| Error::Contextual {
            context: context.to_string(),
            inner_error: Box::new(err.into()),
        })
    }

    #[inline(always)]
    fn context_with(self, context: impl Fn() -> String) -> Result<T> {
        self.map_err(|err| Error::Contextual {
            context: context(),
            inner_error: Box::new(err.into()),
        })
    }
}

/// Like `errors.Annotate` in Go.
/// Wrap an unknown error with [`Error::Other`].
#[macro_export(crate)]
macro_rules! annotate {
    ($inner: expr, $message: expr) => {
        Error::Other(tikv_util::box_err!("{}: {}", $message, $inner))
    };
    ($inner: expr, $format: literal, $($args: expr),+) => {
        annotate!($inner, format_args!($format, $($args),+))
    }
}

impl Error {
    pub fn report(&self, context: impl Display) {
        warn!("backup stream meet error"; "context" => %context, "err" => %self);
        metrics::STREAM_ERROR
            .with_label_values(&[self.kind()])
            .inc()
    }

    pub fn report_fatal(&self) {
        error!(%self; "backup stream meet fatal error");
        metrics::STREAM_FATAL_ERROR
            .with_label_values(&[self.kind()])
            .inc()
    }
    /// remove all context added to the error.
    pub fn without_context(&self) -> &Self {
        match self {
            Error::Contextual { inner_error, .. } => inner_error.without_context(),
            _ => self,
        }
    }

    /// add some context to the error.
    pub fn context(self, msg: impl Display) -> Self {
        Self::Contextual {
            inner_error: Box::new(self),
            context: msg.to_string(),
        }
    }

    fn kind(&self) -> &'static str {
        self.error_code().code
    }
}

#[cfg(test)]
mod test {
    extern crate test;

    use std::io::{self, ErrorKind};

    use error_code::ErrorCodeExt;

    use super::{ContextualResultExt, Error, Result};

    #[test]
    fn test_contextual_error() {
        let err = Error::Io(io::Error::new(
            ErrorKind::Other,
            "the absence of error messages, is also a kind of error message",
        ));
        let result: Result<()> = Err(err);
        let result = result.context(format_args!(
            "a cat named {} cut off the power wire",
            "neko"
        ));

        let err = result.unwrap_err();
        assert_eq!(
            err.to_string(),
            "a cat named neko cut off the power wire: I/O Error: the absence of error messages, is also a kind of error message"
        );

        assert_eq!(err.error_code(), error_code::backup_stream::IO,);
    }

    // Bench: Pod at Intel(R) Xeon(R) Gold 6240 CPU @ 2.60GHz
    //        With CPU Claim = 16 cores.

    #[bench]
    // 2,685 ns/iter (+/- 194)
    fn contextual_add_format_strings_directly(b: &mut test::Bencher) {
        b.iter(|| {
            let err = Error::Io(io::Error::new(
                ErrorKind::Other,
                "basement, it is the fundamental basement.",
            ));
            let result: Result<()> = Err(err);
            let lucky_number = rand::random::<u8>();
            let result = result.context(format!("lucky: the number is {}", lucky_number));
            assert_eq!(
                result.unwrap_err().to_string(),
                format!(
                    "lucky: the number is {}: I/O Error: basement, it is the fundamental basement.",
                    lucky_number
                )
            )
        })
    }

    #[bench]
    // 1,922 ns/iter (+/- 273)
    fn contextual_add_format_strings(b: &mut test::Bencher) {
        b.iter(|| {
            let err = Error::Io(io::Error::new(
                ErrorKind::Other,
                "basement, it is the fundamental basement.",
            ));
            let result: Result<()> = Err(err);
            let lucky_number = rand::random::<u8>();
            let result = result.context(format_args!("lucky: the number is {}", lucky_number));
            assert_eq!(
                result.unwrap_err().to_string(),
                format!(
                    "lucky: the number is {}: I/O Error: basement, it is the fundamental basement.",
                    lucky_number
                )
            )
        })
    }

    #[bench]
    // 1,988 ns/iter (+/- 89)
    fn contextual_add_closure(b: &mut test::Bencher) {
        b.iter(|| {
            let err = Error::Io(io::Error::new(
                ErrorKind::Other,
                "basement, it is the fundamental basement.",
            ));
            let result: Result<()> = Err(err);
            let lucky_number = rand::random::<u8>();
            let result = result.context_with(|| format!("lucky: the number is {}", lucky_number));
            assert_eq!(
                result.unwrap_err().to_string(),
                format!(
                    "lucky: the number is {}: I/O Error: basement, it is the fundamental basement.",
                    lucky_number
                )
            )
        })
    }

    #[bench]
    // 773 ns/iter (+/- 8)
    fn baseline(b: &mut test::Bencher) {
        b.iter(|| {
            let err = Error::Io(io::Error::new(
                ErrorKind::Other,
                "basement, it is the fundamental basement.",
            ));
            let result: Result<()> = Err(err);
            let _lucky_number = rand::random::<u8>();
            assert_eq!(
                result.unwrap_err().to_string(),
                "I/O Error: basement, it is the fundamental basement.".to_string(),
            )
        })
    }

    #[bench]
    // 3 ns/iter (+/- 0)
    fn contextual_ok(b: &mut test::Bencher) {
        b.iter(|| {
            let result: Result<()> = Ok(());
            let lucky_number = rand::random::<u8>();
            let result = result.context_with(|| format!("lucky: the number is {}", lucky_number));
            assert!(result.is_ok());
        })
    }
}
