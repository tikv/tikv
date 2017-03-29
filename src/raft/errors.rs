// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.


use std::{cmp, result, io};
use std::error;

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
            from()
            cause(err.as_ref())
            description(err.description())
            display("unknown error {:?}", err)
        }

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
            from()
            cause(err.as_ref())
            description(err.description())
            display("unknown error {:?}", err)
        }
    }
}

impl cmp::PartialEq for StorageError {
    #[allow(match_same_arms)]
    fn eq(&self, other: &StorageError) -> bool {
        match (self, other) {
            (&StorageError::Compacted, &StorageError::Compacted) => true,
            (&StorageError::Unavailable, &StorageError::Unavailable) => true,
            (&StorageError::SnapshotOutOfDate, &StorageError::SnapshotOutOfDate) => true,
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
        assert_eq!(Error::Store(StorageError::Compacted),
                   Error::Store(StorageError::Compacted));
        assert_eq!(Error::Io(io::Error::new(io::ErrorKind::UnexpectedEof, "oh no!")),
                   Error::Io(io::Error::new(io::ErrorKind::UnexpectedEof, "oh yes!")));
        assert_ne!(Error::Io(io::Error::new(io::ErrorKind::NotFound, "error")),
                   Error::Io(io::Error::new(io::ErrorKind::BrokenPipe, "error")));
        assert_eq!(Error::StepLocalMsg, Error::StepLocalMsg);
        assert_eq!(Error::ConfigInvalid(String::from("config error")),
                   Error::ConfigInvalid(String::from("config error")));
        assert_ne!(Error::ConfigInvalid(String::from("config error")),
                   Error::ConfigInvalid(String::from("other error")));
        assert_eq!(Error::from(io::Error::new(io::ErrorKind::Other, "oh no!")),
                   Error::from(io::Error::new(io::ErrorKind::Other, "oh yes!")));
        assert_ne!(Error::StepPeerNotFound,
                   Error::Store(StorageError::Compacted));
        assert_ne!(Error::Other(box Error::StepLocalMsg), Error::StepLocalMsg);
    }

    #[test]
    fn test_storage_error_equal() {
        assert_eq!(StorageError::Compacted, StorageError::Compacted);
        assert_eq!(StorageError::Unavailable, StorageError::Unavailable);
        assert_eq!(StorageError::SnapshotOutOfDate,
                   StorageError::SnapshotOutOfDate);
        assert_eq!(StorageError::SnapshotTemporarilyUnavailable,
                   StorageError::SnapshotTemporarilyUnavailable);
        assert_ne!(StorageError::Compacted, StorageError::Unavailable);
        assert_ne!(StorageError::Other(box StorageError::Unavailable),
                   StorageError::Unavailable);
    }
}
