//! This module contains things about disk snapshot.

use raftstore::store::{snapshot_backup::SnapshotBrHandle, SignificantRouter};

struct Controller<SR: SnapshotBrHandle> {
    handle: SR,
}
