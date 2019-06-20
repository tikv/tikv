// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use std::io;

#[derive(Debug, Fail)]
pub enum Error {
    #[fail(display = "Io error: {}", _0)]
    Io(#[fail(cause)] io::Error),

    #[fail(display = "Data padding is incorrect")]
    BadPadding,
}

impl Error {
    pub(crate) fn eof() -> Box<Error> {
        io::Error::new(io::ErrorKind::UnexpectedEof, "Unexpected EOF").into()
    }

    pub(crate) fn bad_padding() -> Box<Error> {
        Error::BadPadding.into()
    }
}

impl From<io::Error> for Box<Error> {
    fn from(e: io::Error) -> Self {
        Error::Io(e).into()
    }
}

// Box the error in case of large data structure when there is no error.
pub type Result<T> = std::result::Result<T, Box<Error>>;

const_assert!(8 == std::mem::size_of::<Result<()>>());
