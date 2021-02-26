use std::fmt::{Debug, Display};
use std::io::{Error as IoError, ErrorKind};
use std::{error, result};

use protobuf::ProtobufError;

use error_code::{self, ErrorCode, ErrorCodeExt};
use tikv_util::stream::RetryError;

pub type Result<T> = result::Result<T, Error>;

pub trait ErrorTrait: Debug + Display + ErrorCodeExt + RetryError + Send + Sync + 'static {}

/// The error type for the cloud.
#[derive(Debug, Fail)]
pub enum Error {
    #[fail(display = "Other error {}", _0)]
    Other(Box<dyn error::Error + Sync + Send>),
    #[fail(display = "IO error {}", _0)]
    Io(IoError),
    #[fail(display = "Protobuf error {}", _0)]
    Proto(ProtobufError),
    #[fail(display = "API Timeout error: {}", _0)]
    ApiTimeout(Box<dyn error::Error + Sync + Send>),
    #[fail(display = "API internal error: {}", _0)]
    ApiInternal(Box<dyn error::Error + Sync + Send>),
    #[fail(display = "API not found: {}", _0)]
    ApiNotFound(Box<dyn error::Error + Sync + Send>),
    #[fail(display = "API auth: {}", _0)]
    ApiAuthentication(Box<dyn error::Error + Sync + Send>),
    #[fail(display = "Key error: {}", _0)]
    KmsError(KmsError),
}

impl ErrorTrait for Error {}

#[derive(Debug, Fail)]
pub enum KmsError {
    #[fail(display = "Wrong master key {}", _0)]
    WrongMasterKey(Box<dyn error::Error + Sync + Send>),
    #[fail(display = "Empty key {}", _0)]
    EmptyKey(String),
    #[fail(display = "Kms error {}", _0)]
    Other(Box<dyn error::Error + Sync + Send>),
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

impl ErrorCodeExt for KmsError {
    fn error_code(&self) -> ErrorCode {
        match self {
            KmsError::WrongMasterKey(_) => error_code::cloud::WRONG_MASTER_KEY,
            KmsError::EmptyKey(_) => error_code::cloud::INVALID_INPUT,
            KmsError::Other(_) => error_code::cloud::UNKNOWN,
        }
    }
}

impl ErrorCodeExt for Error {
    fn error_code(&self) -> ErrorCode {
        match self {
            Error::Io(_) => error_code::cloud::IO,
            Error::Proto(_) => error_code::cloud::PROTO,
            Error::Other(_) => error_code::cloud::UNKNOWN,
            Error::ApiTimeout(_) => error_code::cloud::TIMEOUT,
            Error::ApiInternal(_) => error_code::cloud::API_INTERNAL,
            Error::ApiNotFound(_) => error_code::cloud::API_NOT_FOUND,
            Error::ApiAuthentication(_) => error_code::cloud::API_AUTHENTICATION,
            Error::KmsError(e) => e.error_code(),
        }
    }
}

impl RetryError for Error {
    fn is_retryable(&self) -> bool {
        // This should be refined.
        // However, only Error::Tls should be encountered
        match self {
            Error::Io(_) => true,
            Error::Proto(_) => true,
            Error::Other(_) => true,
            Error::ApiTimeout(_) => true,
            Error::ApiInternal(_) => true,
            Error::ApiNotFound(_) => false,
            Error::ApiAuthentication(_) => false,
            Error::KmsError(e) => e.is_retryable(),
        }
    }
}

impl RetryError for KmsError {
    fn is_retryable(&self) -> bool {
        match self {
            KmsError::WrongMasterKey(_) => false,
            KmsError::EmptyKey(_) => false,
            KmsError::Other(_) => true,
        }
    }
}
