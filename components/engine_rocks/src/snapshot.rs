// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    convert::Infallible,
    fmt::{self, Debug, Formatter},
    sync::Arc,
};

use engine_traits::{
    self, CfNamesExt, IterOptions, Iterable, Peekable, ReadOptions, Result, Snapshot,
    SnapshotMiscExt,
};
use parking_lot::Mutex;
use rocksdb::{DB, DBIterator, rocksdb_options::UnsafeSnap};

use crate::{
    RocksEngineIterator, db_vector::RocksDbVector, options::RocksReadOptions, r2e,
    util::get_cf_handle,
};

pub struct RocksSnapshot {
    db: Arc<DB>,
    snap: UnsafeSnap,
}

/// The latest snapshot sequence number observed on one live RocksDB instance.
///
/// Issue #19891 showed that concurrent external SST ingestion and foreground
/// writes could make RocksDB's DB-global sequence number move backwards. A
/// mutex is used instead of a load/check/store atomic sequence so snapshot
/// creation and publication have one order: otherwise a later-created snapshot
/// could publish first and make an earlier-created snapshot look like a
/// regression. `RocksEngine` clones share this object, and a newly opened DB
/// gets a new object because sequence numbers from different DB lifetimes are
/// not necessarily comparable. The tracker is enabled by default in debug
/// builds and disabled by default in release builds, and can be overridden by
/// the RocksDB configuration knob.
#[derive(Debug)]
pub(crate) struct SnapshotSequenceNumber {
    db_identity: String,
    latest: Mutex<Option<u64>>,
}

impl SnapshotSequenceNumber {
    pub(crate) fn new(db_identity: String) -> Self {
        Self {
            db_identity,
            latest: Mutex::new(None),
        }
    }

    fn with_new_snapshot<T, E>(
        &self,
        create: impl FnOnce() -> std::result::Result<T, E>,
        sequence_number: impl FnOnce(&T) -> u64,
    ) -> std::result::Result<T, E> {
        // Keep the lock across creation, observation, and publication. This
        // makes the checked order match the order in which RocksDB snapshots
        // are created, and adds only one uncontended mutex on the hot path.
        let mut latest = self.latest.lock();
        let snapshot = create()?;
        let sequence_number = sequence_number(&snapshot);
        if let Some(previous) = *latest {
            assert!(
                sequence_number >= previous,
                "RocksDB snapshot sequence number regressed: new_sequence_number={}, previous_sequence_number={}, db={}",
                sequence_number,
                previous,
                self.db_identity,
            );
        }
        // A failed creation or a failed invariant check must leave the last
        // successfully published sequence number unchanged.
        *latest = Some(sequence_number);
        Ok(snapshot)
    }

    #[cfg(test)]
    pub(crate) fn latest(&self) -> Option<u64> {
        *self.latest.lock()
    }
}

unsafe impl Send for RocksSnapshot {}
unsafe impl Sync for RocksSnapshot {}

impl RocksSnapshot {
    pub(crate) fn new(db: Arc<DB>) -> Self {
        unsafe {
            let snap = db.unsafe_snap();
            assert!(
                !snap.get_inner().is_null(),
                "RocksDB failed to create snapshot: db={}",
                db.path(),
            );
            RocksSnapshot { snap, db }
        }
    }

    pub(crate) fn new_checked(db: Arc<DB>, sequence_number: &SnapshotSequenceNumber) -> Self {
        let result = sequence_number.with_new_snapshot(
            || Ok::<_, Infallible>(RocksSnapshot::new(db)),
            RocksSnapshot::sequence_number,
        );
        match result {
            Ok(snapshot) => snapshot,
            Err(never) => match never {},
        }
    }
}

impl Snapshot for RocksSnapshot {}

impl Debug for RocksSnapshot {
    fn fmt(&self, fmt: &mut Formatter<'_>) -> fmt::Result {
        write!(fmt, "Engine Snapshot Impl")
    }
}

impl Drop for RocksSnapshot {
    fn drop(&mut self) {
        unsafe {
            self.db.release_snap(&self.snap);
        }
    }
}

impl Iterable for RocksSnapshot {
    type Iterator = RocksEngineIterator;

    fn iterator_opt(&self, cf: &str, opts: IterOptions) -> Result<Self::Iterator> {
        let opt: RocksReadOptions = opts.into();
        let mut opt = opt.into_raw();
        unsafe {
            opt.set_snapshot(&self.snap);
        }
        let handle = get_cf_handle(self.db.as_ref(), cf)?;
        Ok(RocksEngineIterator::from_raw(DBIterator::new_cf(
            self.db.clone(),
            handle,
            opt,
        )))
    }
}

impl Peekable for RocksSnapshot {
    type DbVector = RocksDbVector;

    fn get_value_opt(&self, opts: &ReadOptions, key: &[u8]) -> Result<Option<RocksDbVector>> {
        let opt: RocksReadOptions = opts.into();
        let mut opt = opt.into_raw();
        unsafe {
            opt.set_snapshot(&self.snap);
        }
        let v = self.db.get_opt(key, &opt).map_err(r2e)?;
        Ok(v.map(RocksDbVector::from_raw))
    }

    fn get_value_cf_opt(
        &self,
        opts: &ReadOptions,
        cf: &str,
        key: &[u8],
    ) -> Result<Option<RocksDbVector>> {
        let opt: RocksReadOptions = opts.into();
        let mut opt = opt.into_raw();
        unsafe {
            opt.set_snapshot(&self.snap);
        }
        let handle = get_cf_handle(self.db.as_ref(), cf)?;
        let v = self.db.get_cf_opt(handle, key, &opt).map_err(r2e)?;
        Ok(v.map(RocksDbVector::from_raw))
    }
}

impl CfNamesExt for RocksSnapshot {
    fn cf_names(&self) -> Vec<&str> {
        self.db.cf_names()
    }
}

impl SnapshotMiscExt for RocksSnapshot {
    fn sequence_number(&self) -> u64 {
        unsafe { self.snap.get_sequence_number() }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        panic::{AssertUnwindSafe, catch_unwind},
        sync::{
            Arc, Barrier,
            atomic::{AtomicU64, Ordering},
        },
        thread,
    };

    use engine_traits::{KvEngine, SnapshotMiscExt, SyncMutable};
    use rocksdb::{DB, DBOptions};
    use tempfile::Builder;

    use super::SnapshotSequenceNumber;
    use crate::util;

    #[derive(Debug)]
    struct TestSnapshot(u64);

    fn create_snapshot(sequence_number: &SnapshotSequenceNumber, sequence: u64) -> TestSnapshot {
        sequence_number
            .with_new_snapshot(
                || Ok::<_, ()>(TestSnapshot(sequence)),
                |snapshot| snapshot.0,
            )
            .unwrap()
    }

    #[test]
    fn test_snapshot_sequence_number_invariant() {
        let sequence_number = SnapshotSequenceNumber::new("test-db".to_owned());

        create_snapshot(&sequence_number, 10);
        create_snapshot(&sequence_number, 11);
        create_snapshot(&sequence_number, 11);
        assert_eq!(sequence_number.latest(), Some(11));

        let failed = sequence_number.with_new_snapshot(
            || Err::<TestSnapshot, _>("snapshot creation failed"),
            |snapshot| snapshot.0,
        );
        assert_eq!(failed.unwrap_err(), "snapshot creation failed");
        assert_eq!(sequence_number.latest(), Some(11));

        let regression = catch_unwind(AssertUnwindSafe(|| {
            create_snapshot(&sequence_number, 9);
        }));
        let panic = regression.unwrap_err();
        let message = panic
            .downcast_ref::<String>()
            .map(String::as_str)
            .or_else(|| panic.downcast_ref::<&str>().copied())
            .unwrap();
        assert!(message.contains("new_sequence_number=9"), "{message}");
        assert!(message.contains("previous_sequence_number=11"), "{message}");
        assert!(message.contains("db=test-db"), "{message}");
        assert_eq!(sequence_number.latest(), Some(11));
    }

    #[test]
    fn test_rocks_snapshot_sequence_number_increases_or_stays_equal() {
        let path = Builder::new()
            .prefix("snapshot-sequence")
            .tempdir()
            .unwrap();
        let engine = util::new_default_engine(path.path().to_str().unwrap()).unwrap();

        let first = engine.snapshot();
        let equal = engine.snapshot();
        assert_eq!(first.sequence_number(), equal.sequence_number());

        engine.put(b"key", b"value").unwrap();
        let increased = engine.snapshot();
        assert!(increased.sequence_number() > equal.sequence_number());
    }

    #[test]
    fn test_concurrent_snapshot_sequence_number_invariant() {
        const THREADS: u64 = 8;
        const SNAPSHOTS_PER_THREAD: u64 = 100;

        let sequence_number =
            Arc::new(SnapshotSequenceNumber::new("concurrent-test-db".to_owned()));
        let next_sequence = Arc::new(AtomicU64::new(0));
        let start = Arc::new(Barrier::new(THREADS as usize));
        let mut handles = Vec::with_capacity(THREADS as usize);

        for _ in 0..THREADS {
            let sequence_number = sequence_number.clone();
            let next_sequence = next_sequence.clone();
            let start = start.clone();
            handles.push(thread::spawn(move || {
                start.wait();
                for _ in 0..SNAPSHOTS_PER_THREAD {
                    sequence_number
                        .with_new_snapshot(
                            || {
                                let sequence = next_sequence.fetch_add(1, Ordering::SeqCst) + 1;
                                thread::yield_now();
                                Ok::<_, ()>(TestSnapshot(sequence))
                            },
                            |snapshot| snapshot.0,
                        )
                        .unwrap();
                }
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }
        assert_eq!(
            sequence_number.latest(),
            Some(THREADS * SNAPSHOTS_PER_THREAD)
        );
    }

    #[test]
    fn test_snapshot_sequence_numbers_are_scoped_to_db_instance() {
        let first_db = SnapshotSequenceNumber::new("first-db".to_owned());
        let reopened_db = SnapshotSequenceNumber::new("reopened-db".to_owned());

        create_snapshot(&first_db, 100);
        create_snapshot(&reopened_db, 1);

        assert_eq!(first_db.latest(), Some(100));
        assert_eq!(reopened_db.latest(), Some(1));
    }

    #[test]
    fn test_snapshot_sequence_state_is_reset_with_db_instance() {
        let parent = Builder::new()
            .prefix("snapshot-sequence-reopen")
            .tempdir()
            .unwrap();
        let db_path = parent.path().join("db");
        let db_path = db_path.to_str().unwrap();

        {
            let engine = util::new_default_engine(db_path).unwrap();
            engine.put(b"key", b"value").unwrap();
            assert!(engine.snapshot().sequence_number() > 0);
        }

        DB::destroy(&DBOptions::new(), db_path).unwrap();
        let reopened = util::new_default_engine(db_path).unwrap();
        assert_eq!(reopened.snapshot().sequence_number(), 0);
    }
}
