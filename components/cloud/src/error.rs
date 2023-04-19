// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    error,
    fmt::{Debug, Display},
    io::{Error as IoError, ErrorKind},
    result,
};

use error_code::{self, ErrorCode, ErrorCodeExt};
use protobuf::ProtobufError;
use thiserror::Error;
use tikv_util::stream::RetryError;

pub type Result<T> = result::Result<T, Error>;

pub trait ErrorTrait: Debug + Display + ErrorCodeExt + RetryError + Send + Sync + 'static {}

/// The error type for the cloud.
#[derive(Debug, Error)]
pub enum Error {
    #[error("Other error {0}")]
    Other(#[from] Box<dyn error::Error + Sync + Send>),
    #[error("IO error {0}")]
    Io(#[from] IoError),
    #[error("Protobuf error {0}")]
    Proto(#[from] ProtobufError),
    #[error("API Timeout error: {0}")]
    ApiTimeout(Box<dyn error::Error + Sync + Send>),
    #[error("API internal error: {0}")]
    ApiInternal(Box<dyn error::Error + Sync + Send>),
    #[error("API not found: {0}")]
    ApiNotFound(Box<dyn error::Error + Sync + Send>),
    #[error("API auth: {0}")]
    ApiAuthentication(Box<dyn error::Error + Sync + Send>),
    #[error("Key error: {0}")]
    KmsError(KmsError),
}

impl ErrorTrait for Error {}

#[derive(Debug, Error)]
pub enum KmsError {
    #[error("Wrong master key {0}")]
    WrongMasterKey(Box<dyn error::Error + Sync + Send>),
    #[error("Empty key {0}")]
    EmptyKey(String),
    #[error("Kms error {0}")]
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
