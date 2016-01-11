use std::{result, io, fmt};
use std::error;
use std::boxed::Box;

use protobuf::ProtobufError;

#[derive(Debug)]
pub enum Error {
    Io(io::Error),
    NotFound,
    Store(StorageError),
    Other(Box<error::Error + Send + Sync>),
}


#[derive(Debug)]
pub enum StorageError {
    Compacted,
    Unavailable,
    SnapshotOutOfDate,
}

impl StorageError {
    pub fn string(&self) -> &str {
        match self {
            &StorageError::Compacted => "log compacted",
            &StorageError::Unavailable => "log unavailable",
            &StorageError::SnapshotOutOfDate => "log unavailable",
        }
    }
}


impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            &Error::Io(ref e) => fmt::Display::fmt(e, f),
            &Error::Store(ref e) => fmt::Display::fmt(e, f),
            &Error::Other(ref e) => fmt::Display::fmt(e, f),
            &Error::NotFound => fmt::Display::fmt("not found", f),
        }
    }
}

impl fmt::Display for StorageError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        fmt::Display::fmt(self.string(), f)
    }
}


impl error::Error for StorageError {
    fn description(&self) -> &str {
        self.string()
    }

    fn cause(&self) -> Option<&error::Error> {
        None
    }
}

impl error::Error for Error {
    fn description(&self) -> &str {
        match self {
            // not sure that cause should be included in message
            &Error::Io(ref e) => e.description(),
            &Error::Store(ref e) => e.description(),
            &Error::Other(ref e) => e.description(),
            &Error::NotFound => "not found",
        }
    }

    fn cause(&self) -> Option<&error::Error> {
        match self {
            &Error::Io(ref e) => Some(e),
            &Error::Other(ref e) => e.cause(),
            _ => None,
        }
    }
}

// Other can convert e to Error::Other.
pub fn Other<E>(e: E) -> Error
    where E: Into<Box<error::Error + Send + Sync>>
{
    Error::Other(e.into())
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Error {
        Error::Io(err)
    }
}

impl From<ProtobufError> for Error {
    fn from(err: ProtobufError) -> Error {
        Other(err)
    }
}

pub type Result<T> = result::Result<T, Error>;

#[cfg(test)]
mod tests {
    use std::error;
    use protobuf::ProtobufError;

    use super::*;

    #[test]
    fn test_error() {
        let e = ProtobufError::WireError("a error".to_string());
        let err: Error = Other(e);
        assert!(error::Error::cause(&err).is_none());

        let err1 = Other("hello world");
        assert!(error::Error::cause(&err1).is_none());
    }
}
