use std::{result, io, fmt};

pub enum Error {
    Io(io::Error),
    NotFound,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Error::NotFound => write!(f, "not found"),
            Error::Io(ref error) => fmt::Display::fmt(error, f),
        }
    }
}


pub type Result<T> = result::Result<T, Error>;
