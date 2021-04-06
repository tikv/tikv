// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use error_code::{self, ErrorCode, ErrorCodeExt};
use openssl::error::ErrorStack as CrypterError;
use protobuf::ProtobufError;
use std::io::{Error as IoError, ErrorKind};
use std::{error, result};

/// The error type for encryption.
#[derive(Debug, Fail)]
pub enum Error {
<<<<<<< HEAD
    #[fail(display = "Other error {}", _0)]
    Other(Box<dyn error::Error + Sync + Send>),
    #[fail(display = "RocksDB error {}", _0)]
=======
    #[error("Other error {0}")]
    Other(#[from] Box<dyn error::Error + Sync + Send>),
    // Only when the parsing record is the last one, and the
    // length is insufficient or the crc checksum fails.
    #[error("Recoverable tail record corruption while parsing file dictionary")]
    TailRecordParseIncomplete,
    // Currently only in use by cloud KMS
    #[error("Cloud KMS error {0}")]
    RetryCodedError(Box<dyn RetryCodedError>),
    #[error("RocksDB error {0}")]
>>>>>>> 4b328ed55... encryption: Ignore log record parse error caused by write failure before (#9938)
    Rocks(String),
    #[fail(display = "IO error {}", _0)]
    Io(IoError),
    #[fail(display = "OpenSSL error {}", _0)]
    Crypter(CrypterError),
    #[fail(display = "Protobuf error {}", _0)]
    Proto(ProtobufError),
    #[fail(display = "Unknown encryption error")]
    UnknownEncryption,
    #[fail(display = "Wrong master key error {}", _0)]
    WrongMasterKey(Box<dyn error::Error + Sync + Send>),
    #[fail(
        display = "Both master key failed, current key {}, previous key {}.",
        _0, _1
    )]
    BothMasterKeyFail(
        Box<dyn error::Error + Sync + Send>,
        Box<dyn error::Error + Sync + Send>,
    ),
}

macro_rules! impl_from {
    ($($inner:ty => $container:ident,)+) => {
        $(
            impl From<$inner> for Error {
                fn from(inr: $inner) -> Error {
                    Error::$container(inr)
                }
            }
        )+
    };
}

impl_from! {
    Box<dyn error::Error + Sync + Send> => Other,
    String => Rocks,
    IoError => Io,
    CrypterError => Crypter,
    ProtobufError => Proto,
}

impl From<Error> for IoError {
    fn from(err: Error) -> IoError {
        match err {
            Error::Io(e) => e,
            other => IoError::new(ErrorKind::Other, format!("{}", other)),
        }
    }
}

pub type Result<T> = result::Result<T, Error>;

impl ErrorCodeExt for Error {
    fn error_code(&self) -> ErrorCode {
        match self {
<<<<<<< HEAD
=======
            Error::RetryCodedError(err) => (*err).error_code(),
            Error::TailRecordParseIncomplete => error_code::encryption::PARSE_INCOMPLETE,
>>>>>>> 4b328ed55... encryption: Ignore log record parse error caused by write failure before (#9938)
            Error::Rocks(_) => error_code::encryption::ROCKS,
            Error::Io(_) => error_code::encryption::IO,
            Error::Crypter(_) => error_code::encryption::CRYPTER,
            Error::Proto(_) => error_code::encryption::PROTO,
            Error::UnknownEncryption => error_code::encryption::UNKNOWN_ENCRYPTION,
            Error::WrongMasterKey(_) => error_code::encryption::WRONG_MASTER_KEY,
            Error::BothMasterKeyFail(_, _) => error_code::encryption::BOTH_MASTER_KEY_FAIL,
            Error::Other(_) => error_code::UNKNOWN,
        }
    }
}
<<<<<<< HEAD
=======

impl RetryError for Error {
    fn is_retryable(&self) -> bool {
        // This should be refined.
        // However, only Error::Tls should be encountered
        match self {
            Error::RetryCodedError(err) => err.is_retryable(),
            Error::TailRecordParseIncomplete => true,
            Error::Rocks(_) => true,
            Error::Io(_) => true,
            Error::Crypter(_) => true,
            Error::Proto(_) => true,
            Error::UnknownEncryption => true,
            Error::WrongMasterKey(_) => false,
            Error::BothMasterKeyFail(_, _) => false,
            Error::Other(_) => true,
        }
    }
}
>>>>>>> 4b328ed55... encryption: Ignore log record parse error caused by write failure before (#9938)
