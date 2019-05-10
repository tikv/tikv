// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use std::io;

#[derive(Debug, Fail)]
pub enum Error {
    #[fail(display = "Io error")]
    Io(#[fail(cause)] Box<io::Error>),

    #[fail(display = "Data padding is incorrect")]
    BadPadding,
}

impl Error {
    pub(crate) fn new_eof_error() -> Error {
        io::Error::new(io::ErrorKind::UnexpectedEof, "Unexpected EOF").into()
    }
}

impl From<io::Error> for Error {
    fn from(e: io::Error) -> Self {
        Error::Io(Box::new(e))
    }
}

// Box the error in case of large data structure when there is no error.
pub type Result<T> = std::result::Result<T, Error>;
