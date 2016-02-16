#![allow(dead_code)]
use std::{cmp, result, io};
use std::error;
use protobuf::error::ProtobufError;

quick_error! {
    #[derive(Debug)]
    pub enum Error {
        Io(err: io::Error) {
            from()
            cause(err)
            description(err.description())
        }
        Store(err: StorageError) {
            from()
            cause(err)
            description(err.description())
        }
        StepLocalMsg {
            description("raft: cannot step raft local message")
        }
        StepPeerNotFound {
            description("raft: cannot step as peer not found")
        }
        ConfigInvalid(desc: String) {
            description(desc)
        }
        Other(err: Box<error::Error + Sync + Send>) {
            cause(err.as_ref())
            description(err.description())
        }
    }
}

impl Error {
    pub fn other<T>(err: T) -> Error
        where T: Into<Box<error::Error + Sync + Send + 'static>>
    {
        Error::Other(err.into())
    }
}

impl cmp::PartialEq for Error {
    #[allow(match_same_arms)]
    fn eq(&self, other: &Error) -> bool {
        match (self, other) {
            (&Error::StepPeerNotFound, &Error::StepPeerNotFound) => true,
            (&Error::Store(ref e1), &Error::Store(ref e2)) => e1 == e2,
            (&Error::Io(ref e1), &Error::Io(ref e2)) => e1.kind() == e2.kind(),
            (&Error::StepLocalMsg, &Error::StepLocalMsg) => true,
            (&Error::ConfigInvalid(ref e1), &Error::ConfigInvalid(ref e2)) => e1 == e2,
            _ => false,
        }
    }
}

impl From<ProtobufError> for Error {
    fn from(e: ProtobufError) -> Error {
        Error::other(e)
    }
}

quick_error! {
    #[derive(Debug)]
    pub enum StorageError {
        Compacted {
            description("log compacted")
        }
        Unavailable {
            description("log unavailable")
        }
        SnapshotOutOfDate {
            description("snapshot out of date")
        }
        SnapshotTemporarilyUnavailable {
            description("snapshot is temporarily unavailable")
        }
        Other(err: Box<error::Error + Sync + Send>) {
            cause(err.as_ref())
            description(err.description())
        }
    }
}

impl StorageError {
    pub fn other<T>(err: T) -> StorageError
        where T: Into<Box<error::Error + Sync + Send + 'static>>
    {
        StorageError::Other(err.into())
    }
}

impl cmp::PartialEq for StorageError {
    #[allow(match_same_arms)]
    fn eq(&self, other: &StorageError) -> bool {
        match (self, other) {
            (&StorageError::Compacted, &StorageError::Compacted) => true,
            (&StorageError::Unavailable, &StorageError::Unavailable) => true,
            (&StorageError::SnapshotOutOfDate,
             &StorageError::SnapshotOutOfDate) => true,
            (&StorageError::SnapshotTemporarilyUnavailable,
             &StorageError::SnapshotTemporarilyUnavailable) => true,
            _ => false,
        }
    }
}

pub type Result<T> = result::Result<T, Error>;

#[cfg(test)]
mod tests {
    use std::io;
    use super::*;

    #[test]
    fn test_error_equal() {
        assert_eq!(Error::StepPeerNotFound, Error::StepPeerNotFound);
        assert!(Error::StepPeerNotFound != Error::Store(StorageError::Compacted));
        assert_eq!(Error::Store(StorageError::Compacted),
                   Error::Store(StorageError::Compacted));
        assert_eq!(Error::StepLocalMsg, Error::StepLocalMsg);
        assert_eq!(Error::from(io::Error::new(io::ErrorKind::Other, "oh no!")),
                   Error::from(io::Error::new(io::ErrorKind::Other, "oh yes!")));
    }

    #[test]
    fn test_storage_error_equal() {
        assert_eq!(StorageError::Compacted, StorageError::Compacted);
        assert!(StorageError::Compacted != StorageError::Unavailable);
        assert_eq!(StorageError::Unavailable, StorageError::Unavailable);
        assert_eq!(StorageError::SnapshotOutOfDate,
                   StorageError::SnapshotOutOfDate);
        assert_eq!(StorageError::SnapshotTemporarilyUnavailable,
                   StorageError::SnapshotTemporarilyUnavailable);
        assert!(StorageError::other(Error::StepLocalMsg) != StorageError::Unavailable);
    }

    #[test]
    fn test_other() {
        // just for check compile.
        Error::other("a simple string error");
        StorageError::other("a simple string error");
        Error::other(io::Error::new(io::ErrorKind::Other, "hello io"));
        StorageError::other(io::Error::new(io::ErrorKind::Other, "hello io"));
    }
}
