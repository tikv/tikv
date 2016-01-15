#![allow(dead_code)]
use std::{cmp, result, io, fmt};
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


#[derive(Debug, PartialEq)]
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
            &StorageError::SnapshotOutOfDate => "snapshot out of date",
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

impl cmp::PartialEq for Error {
    fn eq(&self, other: &Error) -> bool {
        match (self, other) {
            (&Error::NotFound, &Error::NotFound) => true,
            (&Error::Store(ref e1), &Error::Store(ref e2)) => e1 == e2,
            (&Error::Io(ref e1), &Error::Io(ref e2)) => e1.kind() == e2.kind(),
            // should not compare directly
            (&Error::Other(ref e1), &Error::Other(ref e2)) => {
                panic!("Shoud not compare boxed error directly {}， {}", e1, e2)
            }
            _ => false, 
        }
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

// other can convert e to Error::Other.
pub fn other<E>(e: E) -> Error
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
        other(err)
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
        let err: Error = other(e);
        assert!(error::Error::cause(&err).is_none());

        let err1 = other("hello world");
        assert!(error::Error::cause(&err1).is_none());
    }

    fn test_equal() {
        assert_eq!(Error::NotFound, Error::NotFound);
        assert!(Error::NotFound != Error::Store(StorageError::Compacted));
        assert_eq!(Error::Store(StorageError::Compacted),
                   Error::Store(StorageError::Compacted));
    }
}
