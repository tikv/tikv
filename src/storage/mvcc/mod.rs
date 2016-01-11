use std::{error, fmt, result};
use storage::engine;

pub use self::meta::Meta;
pub use self::codec::{encode_key, decode_key};

mod meta;
mod codec;

#[derive(Debug)]
pub enum Error {
    Engine(engine::Error),
    Mvcc(MvccErrorKind),
}

#[derive(Debug, Copy, Clone, PartialEq)]
enum MvccErrorKind {
    MetaDataLength,
    MetaDataFlag,
    MetaDataVersion,
    KeyLength,
    KeyVersion,
}

impl MvccErrorKind {
    fn description(self) -> &'static str {
        match self {
            MvccErrorKind::MetaDataLength => "bad format meta data(length)",
            MvccErrorKind::MetaDataFlag => "bad format meta data(flag)",
            MvccErrorKind::MetaDataVersion => "bad format meta data(version)",
            MvccErrorKind::KeyLength => "bad format key(length)",
            MvccErrorKind::KeyVersion => "bad format key(version)",
        }
    }

    fn as_result<T>(self) -> Result<T> {
        Err(Error::Mvcc(self))
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Error::Engine(ref err) => err.fmt(f),
            Error::Mvcc(kind) => kind.description().fmt(f),
        }
    }
}

impl error::Error for Error {
    fn description(&self) -> &str {
        match *self {
            Error::Engine(ref err) => err.description(),
            Error::Mvcc(kind) => kind.description(),
        }
    }

    fn cause(&self) -> Option<&error::Error> {
        match *self {
            Error::Engine(ref err) => Some(err),
            Error::Mvcc(kind) => None,
        }
    }
}

impl From<engine::Error> for Error {
    fn from(err: engine::Error) -> Error {
        Error::Engine(err)
    }
}

pub type Result<T> = result::Result<T, Error>;
