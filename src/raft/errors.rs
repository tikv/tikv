#![allow(dead_code)]
use std::{cmp, result, io};

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
    }
}

quick_error! {
    #[derive(Debug, PartialEq)]
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
    }
}

impl cmp::PartialEq for Error {
    fn eq(&self, other: &Error) -> bool {
        match (self, other) {
            (&Error::StepPeerNotFound, &Error::StepPeerNotFound) => true,
            (&Error::Store(ref e1), &Error::Store(ref e2)) => e1 == e2,
            (&Error::Io(ref e1), &Error::Io(ref e2)) => e1.kind() == e2.kind(),
            (&Error::StepLocalMsg, &Error::StepLocalMsg) => true,
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
    fn test_equal() {
        assert_eq!(Error::StepPeerNotFound, Error::StepPeerNotFound);
        assert!(Error::StepPeerNotFound != Error::Store(StorageError::Compacted));
        assert_eq!(Error::Store(StorageError::Compacted),
                   Error::Store(StorageError::Compacted));
        assert_eq!(Error::StepLocalMsg, Error::StepLocalMsg);
        assert_eq!(Error::from(io::Error::new(io::ErrorKind::Other, "oh no!")),
                   Error::from(io::Error::new(io::ErrorKind::Other, "oh yes!")));
    }
}
