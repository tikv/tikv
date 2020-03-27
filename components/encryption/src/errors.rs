// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use openssl::error::ErrorStack as CrypterError;
use protobuf::ProtobufError;
use std::io::{Error as IoError, ErrorKind};
use std::{error, result};

/// The error type for encryption.
#[derive(Debug, Fail)]
pub enum Error {
    #[fail(display = "Other error {}", _0)]
    Other(Box<dyn error::Error + Sync + Send>),
    #[fail(display = "RocksDB error {}", _0)]
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
