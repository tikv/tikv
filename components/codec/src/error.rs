// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use std::io;

use failure::{Backtrace, Fail};

#[derive(Debug, Fail)]
pub enum ErrorInner {
    #[fail(display = "Io error: {}", _0)]
    Io(#[fail(cause)] io::Error),

    #[fail(display = "Data padding is incorrect")]
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

impl From<io::Error> for ErrorInner {
    #[inline]
    fn from(e: io::Error) -> Self {
        ErrorInner::Io(e)
    }
}

// ====== The code below is to box the error so that the it can be as small as possible ======

impl Fail for Box<ErrorInner> {
    #[inline]
    fn cause(&self) -> Option<&dyn Fail> {
        (**self).cause()
    }

    #[inline]
    fn backtrace(&self) -> Option<&Backtrace> {
        (**self).backtrace()
    }
}

#[derive(Debug, Fail)]
#[fail(display = "{}", _0)]
pub struct Error(#[fail(cause)] pub Box<ErrorInner>);

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
