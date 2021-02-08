use std::fmt::{Debug, Display};
use std::io::{Error as IoError, ErrorKind};
use std::{error, result};

use openssl::error::ErrorStack as SslError;
use protobuf::ProtobufError;
use rusoto_core::request::TlsError;

use tikv_util::stream::RetryError;

use error_code::{self, ErrorCode, ErrorCodeExt};

pub type Result<T> = result::Result<T, Error>;

pub trait ErrorTrait: Debug + Display + Send + Sync + 'static {}

/// The error type for the cloud.
#[derive(Debug, Fail)]
pub enum Error {
    #[fail(display = "Other error {}", _0)]
    Other(Box<dyn error::Error + Sync + Send>),
    #[fail(display = "IO error {}", _0)]
    Io(IoError),
    #[fail(display = "OpenSSL error {}", _0)]
    Ssl(SslError),
    #[fail(display = "Protobuf error {}", _0)]
    Proto(ProtobufError),
    #[fail(display = "Wrong master key error {}", _0)]
    WrongMasterKey(Box<dyn error::Error + Sync + Send>),
    #[fail(display = "Key error: {}", _0)]
    KeyError(KeyError),
}

impl Error {
    pub fn boxed(self) -> Box<dyn ErrorTrait> {
        Box::new(self) as Box<dyn ErrorTrait>
    }
}

impl ErrorTrait for Error {}

#[derive(Debug, Fail)]
pub enum KeyError {
    #[fail(display = "Wrong master key {}", _0)]
    WrongMasterKey(Box<dyn error::Error + Sync + Send>),
    #[fail(display = "Empty key {}", _0)]
    EmptyContents(String),
}

pub fn empty_key_contents(msg: &str) -> Error {
    Error::KeyError(KeyError::EmptyContents(msg.to_owned()))
}

impl From<Error> for IoError {
    fn from(err: Error) -> IoError {
        match err {
            Error::Io(e) => e,
            other => IoError::new(ErrorKind::Other, format!("{}", other)),
        }
    }
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
    IoError => Io,
    ProtobufError => Proto,
}

impl std::convert::From<rusoto_core::request::TlsError> for Error {
    fn from(err: rusoto_core::request::TlsError) -> Error {
        Error::Other(box_err!(format!("{}", err)))
    }
}

/*
impl ErrorCodeExt for Error {
    fn error_code(&self) -> ErrorCode {
        match self {
            Error::Io(_) => error_code::encryption::IO,
            Error::Ssl(_) => error_code::encryption::CRYPTER,
            Error::Proto(_) => error_code::encryption::PROTO,
            Error::WrongMasterKey(_) => error_code::encryption::WRONG_MASTER_KEY,
            Error::Other(_) => error_code::UNKNOWN,
            Error::Tls(_) => error_code::encryption::CRYPTER,
            Error::KeyError(e) => e.error_code(),
        }
    }
}

impl RetryError for Error {
    fn is_retryable(&self) -> bool {
        match self {
            Error::Io(_) => true,
            Error::Ssl(_) => true,
            Error::Proto(_) => true,
            Error::WrongMasterKey(_) => false,
            Error::Other(_) => true,
            Error::Tls(_) => true,
            Error::KeyError(e) => e.is_retryable(),
        }
    }
}

impl RetryError for KeyError {
    fn is_retryable(&self) -> bool {
        match self {
            KeyError::WrongMasterKey(_) => false,
            KeyError::EmptyContents(_) => false,
        }
    }
}
