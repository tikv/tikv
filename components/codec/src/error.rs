// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use std::io;

use error_code::{self, ErrorCode, ErrorCodeExt};
use static_assertions::const_assert;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ErrorInner {
    #[error("{0}")]
    Io(#[from] io::Error),
    #[error("bad format key(length)")]
    KeyLength,
    #[error("bad format key(padding)")]
    KeyPadding,
    #[error("key not found")]
    KeyNotFound,
    #[error("bad format value(length)")]
    ValueLength,
    #[error("bad format value(meta)")]
    ValueMeta,
}

impl ErrorInner {
    pub fn maybe_clone(&self) -> Option<ErrorInner> {
        match *self {
            ErrorInner::KeyLength => Some(ErrorInner::KeyLength),
            ErrorInner::KeyPadding => Some(ErrorInner::KeyPadding),
            ErrorInner::KeyNotFound => Some(ErrorInner::KeyNotFound),
            ErrorInner::ValueLength => Some(ErrorInner::ValueLength),
            ErrorInner::ValueMeta => Some(ErrorInner::ValueMeta),
            ErrorInner::Io(_) => None,
        }
    }

    #[inline]
    pub fn eof() -> ErrorInner {
        io::Error::new(io::ErrorKind::UnexpectedEof, "Unexpected EOF").into()
    }

    #[inline]
    pub fn key_padding() -> ErrorInner {
        ErrorInner::KeyPadding
    }
}

// Box the error so that the it can be as small as possible
#[derive(Debug, Error)]
#[error(transparent)]
pub struct Error(#[from] pub Box<ErrorInner>);

impl Error {
    pub fn maybe_clone(&self) -> Option<Error> {
        self.0.maybe_clone().map(Error::from)
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

pub type Result<T> = std::result::Result<T, Error>;

const_assert!(8 == std::mem::size_of::<Result<()>>());

impl ErrorCodeExt for Error {
    fn error_code(&self) -> ErrorCode {
        match self.0.as_ref() {
            ErrorInner::Io(_) => error_code::codec::IO,
            ErrorInner::KeyLength => error_code::codec::KEY_LENGTH,
            ErrorInner::KeyPadding => error_code::codec::BAD_PADDING,
            ErrorInner::KeyNotFound => error_code::codec::KEY_NOT_FOUND,
            ErrorInner::ValueLength => error_code::codec::VALUE_LENGTH,
            ErrorInner::ValueMeta => error_code::codec::VALUE_META,
        }
    }
}
