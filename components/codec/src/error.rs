// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use std::io;

use error_code::{self, ErrorCode, ErrorCodeExt};
use static_assertions::const_assert;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ErrorInner {
    #[error("Io error: {0}")]
    Io(#[from] io::Error),

    #[error("Data padding is incorrect")]
    BadPadding,
}

impl ErrorInner {
    #[inline]
    pub(crate) fn eof() -> ErrorInner {
        io::Error::new(io::ErrorKind::UnexpectedEof, "Unexpected EOF").into()
    }

    #[inline]
    pub(crate) fn bad_padding() -> ErrorInner {
        ErrorInner::BadPadding
    }
}

// ====== The code below is to box the error so that the it can be as small as possible ======

#[derive(Debug, Error)]
#[error(transparent)]
pub struct Error(#[from] pub Box<ErrorInner>);

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
            ErrorInner::BadPadding => error_code::codec::BAD_PADDING,
        }
    }
}
