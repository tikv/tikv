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

mod lock;
mod metrics;
mod reader;
mod txn;
mod write;

pub use self::lock::{Lock, LockType};
pub use self::reader::MvccReader;
pub use self::reader::{BackwardScanner, BackwardScannerBuilder};
pub use self::reader::{ForwardScanner, ForwardScannerBuilder};
pub use self::txn::{MvccTxn, MAX_TXN_WRITE_SIZE};
pub use self::write::{Write, WriteType};
use std::error;
use std::io;
use util::escape;

quick_error! {
    #[derive(Debug)]
    pub enum Error {
        Engine(err: ::storage::engine::Error) {
            from()
            cause(err)
            description(err.description())
        }
        Io(err: io::Error) {
            from()
            cause(err)
            description(err.description())
        }
        Codec(err: ::util::codec::Error) {
            from()
            cause(err)
            description(err.description())
        }
        KeyIsLocked {key: Vec<u8>, primary: Vec<u8>, ts: u64, ttl: u64} {
            description("key is locked (backoff or cleanup)")
            display("key is locked (backoff or cleanup) {}-{}@{} ttl {}",
                        escape(key),
                        escape(primary),
                        ts,
                        ttl)
        }
        BadFormatLock {description("bad format lock data")}
        BadFormatWrite {description("bad format write data")}
        Committed {commit_ts: u64} {
            description("txn already committed")
            display("txn already committed @{}", commit_ts)
        }
        TxnLockNotFound {start_ts: u64, commit_ts: u64, key: Vec<u8> } {
            description("txn lock not found")
            display("txn lock not found {}-{} key:{:?}", start_ts, commit_ts, escape(key))
        }
        WriteConflict { start_ts: u64, conflict_start_ts: u64, conflict_commit_ts: u64, key: Vec<u8>, primary: Vec<u8> } {
            description("write conflict")
            display("write conflict, start_ts:{}, conflict_start_ts:{}, conflict_commit_ts:{}, key:{:?}, primary:{:?}",
             start_ts, conflict_start_ts, conflict_commit_ts, escape(key), escape(primary))
        }
        KeyVersion {description("bad format key(version)")}
        Other(err: Box<error::Error + Sync + Send>) {
            from()
            cause(err.as_ref())
            description(err.description())
            display("{:?}", err)
        }
    }
}

impl Error {
    pub fn maybe_clone(&self) -> Option<Error> {
        match *self {
            Error::Engine(ref e) => e.maybe_clone().map(Error::Engine),
            Error::Codec(ref e) => e.maybe_clone().map(Error::Codec),
            Error::KeyIsLocked {
                ref key,
                ref primary,
                ts,
                ttl,
            } => Some(Error::KeyIsLocked {
                key: key.clone(),
                primary: primary.clone(),
                ts,
                ttl,
            }),
            Error::BadFormatLock => Some(Error::BadFormatLock),
            Error::BadFormatWrite => Some(Error::BadFormatWrite),
            Error::TxnLockNotFound {
                start_ts,
                commit_ts,
                ref key,
            } => Some(Error::TxnLockNotFound {
                start_ts,
                commit_ts,
                key: key.to_owned(),
            }),
            Error::WriteConflict {
                start_ts,
                conflict_start_ts,
                conflict_commit_ts,
                ref key,
                ref primary,
            } => Some(Error::WriteConflict {
                start_ts,
                conflict_start_ts,
                conflict_commit_ts,
                key: key.to_owned(),
                primary: primary.to_owned(),
            }),
            Error::KeyVersion => Some(Error::KeyVersion),
            Error::Committed { commit_ts } => Some(Error::Committed { commit_ts }),
            Error::Io(_) | Error::Other(_) => None,
        }
    }
}

pub type Result<T> = ::std::result::Result<T, Error>;

#[cfg(test)]
pub mod tests {
    use kvproto::kvrpcpb::{Context, IsolationLevel};

    use storage::CF_WRITE;
    use storage::{Engine, Key, Modify, Mutation, Options, ScanMode, Snapshot};

    use super::*;

    fn write<E: Engine>(engine: &E, ctx: &Context, modifies: Vec<Modify>) {
        if !modifies.is_empty() {
            engine.write(ctx, modifies).unwrap();
        }
    }

    pub fn must_get<E: Engine>(engine: &E, key: &[u8], ts: u64, expect: &[u8]) {
        let ctx = Context::new();
        let snapshot = engine.snapshot(&ctx).unwrap();
        let mut reader = MvccReader::new(snapshot, None, true, None, None, IsolationLevel::SI);
        assert_eq!(
            reader.get(&Key::from_raw(key), ts).unwrap().unwrap(),
            expect
        );
    }

    pub fn must_get_rc<E: Engine>(engine: &E, key: &[u8], ts: u64, expect: &[u8]) {
        let ctx = Context::new();
        let snapshot = engine.snapshot(&ctx).unwrap();
        let mut reader = MvccReader::new(snapshot, None, true, None, None, IsolationLevel::RC);
        assert_eq!(
            reader.get(&Key::from_raw(key), ts).unwrap().unwrap(),
            expect
        );
    }

    pub fn must_get_none<E: Engine>(engine: &E, key: &[u8], ts: u64) {
        let ctx = Context::new();
        let snapshot = engine.snapshot(&ctx).unwrap();
        let mut reader = MvccReader::new(snapshot, None, true, None, None, IsolationLevel::SI);
        assert!(reader.get(&Key::from_raw(key), ts).unwrap().is_none());
    }

    pub fn must_get_err<E: Engine>(engine: &E, key: &[u8], ts: u64) {
        let ctx = Context::new();
        let snapshot = engine.snapshot(&ctx).unwrap();
        let mut reader = MvccReader::new(snapshot, None, true, None, None, IsolationLevel::SI);
        assert!(reader.get(&Key::from_raw(key), ts).is_err());
    }

    pub fn must_prewrite_put<E: Engine>(engine: &E, key: &[u8], value: &[u8], pk: &[u8], ts: u64) {
        let ctx = Context::new();
        let snapshot = engine.snapshot(&ctx).unwrap();
        let mut txn = MvccTxn::new(snapshot, ts, true).unwrap();
        txn.prewrite(
            Mutation::Put((Key::from_raw(key), value.to_vec())),
            pk,
            &Options::default(),
        ).unwrap();
        write(engine, &ctx, txn.into_modifies());
    }

    pub fn must_prewrite_delete<E: Engine>(engine: &E, key: &[u8], pk: &[u8], ts: u64) {
        let ctx = Context::new();
        let snapshot = engine.snapshot(&ctx).unwrap();
        let mut txn = MvccTxn::new(snapshot, ts, true).unwrap();
        txn.prewrite(
            Mutation::Delete(Key::from_raw(key)),
            pk,
            &Options::default(),
        ).unwrap();
        engine.write(&ctx, txn.into_modifies()).unwrap();
    }

    pub fn must_prewrite_lock<E: Engine>(engine: &E, key: &[u8], pk: &[u8], ts: u64) {
        let ctx = Context::new();
        let snapshot = engine.snapshot(&ctx).unwrap();
        let mut txn = MvccTxn::new(snapshot, ts, true).unwrap();
        txn.prewrite(Mutation::Lock(Key::from_raw(key)), pk, &Options::default())
            .unwrap();
        engine.write(&ctx, txn.into_modifies()).unwrap();
    }

    pub fn must_prewrite_lock_err<E: Engine>(engine: &E, key: &[u8], pk: &[u8], ts: u64) {
        let ctx = Context::new();
        let snapshot = engine.snapshot(&ctx).unwrap();
        let mut txn = MvccTxn::new(snapshot, ts, true).unwrap();
        assert!(
            txn.prewrite(Mutation::Lock(Key::from_raw(key)), pk, &Options::default())
                .is_err()
        );
    }

    pub fn must_commit<E: Engine>(engine: &E, key: &[u8], start_ts: u64, commit_ts: u64) {
        let ctx = Context::new();
        let snapshot = engine.snapshot(&ctx).unwrap();
        let mut txn = MvccTxn::new(snapshot, start_ts, true).unwrap();
        txn.commit(Key::from_raw(key), commit_ts).unwrap();
        write(engine, &ctx, txn.into_modifies());
    }

    pub fn must_commit_err<E: Engine>(engine: &E, key: &[u8], start_ts: u64, commit_ts: u64) {
        let ctx = Context::new();
        let snapshot = engine.snapshot(&ctx).unwrap();
        let mut txn = MvccTxn::new(snapshot, start_ts, true).unwrap();
        assert!(txn.commit(Key::from_raw(key), commit_ts).is_err());
    }

    pub fn must_rollback<E: Engine>(engine: &E, key: &[u8], start_ts: u64) {
        let ctx = Context::new();
        let snapshot = engine.snapshot(&ctx).unwrap();
        let mut txn = MvccTxn::new(snapshot, start_ts, true).unwrap();
        txn.collapse_rollback(false);
        txn.rollback(Key::from_raw(key)).unwrap();
        write(engine, &ctx, txn.into_modifies());
    }

    pub fn must_rollback_collapsed<E: Engine>(engine: &E, key: &[u8], start_ts: u64) {
        let ctx = Context::new();
        let snapshot = engine.snapshot(&ctx).unwrap();
        let mut txn = MvccTxn::new(snapshot, start_ts, true).unwrap();
        txn.rollback(Key::from_raw(key)).unwrap();
        write(engine, &ctx, txn.into_modifies());
    }

    pub fn must_rollback_err<E: Engine>(engine: &E, key: &[u8], start_ts: u64) {
        let ctx = Context::new();
        let snapshot = engine.snapshot(&ctx).unwrap();
        let mut txn = MvccTxn::new(snapshot, start_ts, true).unwrap();
        assert!(txn.rollback(Key::from_raw(key)).is_err());
    }

    pub fn must_gc<E: Engine>(engine: &E, key: &[u8], safe_point: u64) {
        let ctx = Context::new();
        let snapshot = engine.snapshot(&ctx).unwrap();
        let mut txn = MvccTxn::new(snapshot, 0, true).unwrap();
        txn.gc(Key::from_raw(key), safe_point).unwrap();
        write(engine, &ctx, txn.into_modifies());
    }

    pub fn must_locked<E: Engine>(engine: &E, key: &[u8], start_ts: u64) {
        let snapshot = engine.snapshot(&Context::new()).unwrap();
        let mut reader = MvccReader::new(snapshot, None, true, None, None, IsolationLevel::SI);
        let lock = reader.load_lock(&Key::from_raw(key)).unwrap().unwrap();
        assert_eq!(lock.ts, start_ts);
    }

    pub fn must_unlocked<E: Engine>(engine: &E, key: &[u8]) {
        let snapshot = engine.snapshot(&Context::new()).unwrap();
        let mut reader = MvccReader::new(snapshot, None, true, None, None, IsolationLevel::SI);
        assert!(reader.load_lock(&Key::from_raw(key)).unwrap().is_none());
    }

    pub fn must_written<E: Engine>(
        engine: &E,
        key: &[u8],
        start_ts: u64,
        commit_ts: u64,
        tp: WriteType,
    ) {
        let snapshot = engine.snapshot(&Context::new()).unwrap();
        let k = Key::from_raw(key).append_ts(commit_ts);
        let v = snapshot.get_cf(CF_WRITE, &k).unwrap().unwrap();
        let write = Write::parse(&v).unwrap();
        assert_eq!(write.start_ts, start_ts);
        assert_eq!(write.write_type, tp);
    }

    pub fn must_seek_write_none<E: Engine>(engine: &E, key: &[u8], ts: u64) {
        let snapshot = engine.snapshot(&Context::new()).unwrap();
        let mut reader = MvccReader::new(snapshot, None, true, None, None, IsolationLevel::SI);
        assert!(
            reader
                .seek_write(&Key::from_raw(key), ts)
                .unwrap()
                .is_none()
        );
    }

    pub fn must_seek_write<E: Engine>(
        engine: &E,
        key: &[u8],
        ts: u64,
        start_ts: u64,
        commit_ts: u64,
        write_type: WriteType,
    ) {
        let snapshot = engine.snapshot(&Context::new()).unwrap();
        let mut reader = MvccReader::new(snapshot, None, true, None, None, IsolationLevel::SI);
        let (t, write) = reader.seek_write(&Key::from_raw(key), ts).unwrap().unwrap();
        assert_eq!(t, commit_ts);
        assert_eq!(write.start_ts, start_ts);
        assert_eq!(write.write_type, write_type);
    }

    pub fn must_reverse_seek_write_none<E: Engine>(engine: &E, key: &[u8], ts: u64) {
        let snapshot = engine.snapshot(&Context::new()).unwrap();
        let mut reader = MvccReader::new(snapshot, None, true, None, None, IsolationLevel::SI);
        assert!(
            reader
                .reverse_seek_write(&Key::from_raw(key), ts)
                .unwrap()
                .is_none()
        );
    }

    pub fn must_reverse_seek_write<E: Engine>(
        engine: &E,
        key: &[u8],
        ts: u64,
        start_ts: u64,
        commit_ts: u64,
        write_type: WriteType,
    ) {
        let snapshot = engine.snapshot(&Context::new()).unwrap();
        let mut reader = MvccReader::new(snapshot, None, true, None, None, IsolationLevel::SI);
        let (t, write) = reader
            .reverse_seek_write(&Key::from_raw(key), ts)
            .unwrap()
            .unwrap();
        assert_eq!(t, commit_ts);
        assert_eq!(write.start_ts, start_ts);
        assert_eq!(write.write_type, write_type);
    }

    pub fn must_get_commit_ts<E: Engine>(engine: &E, key: &[u8], start_ts: u64, commit_ts: u64) {
        let snapshot = engine.snapshot(&Context::new()).unwrap();
        let mut reader = MvccReader::new(snapshot, None, true, None, None, IsolationLevel::SI);
        let (ts, write_type) = reader
            .get_txn_commit_info(&Key::from_raw(key), start_ts)
            .unwrap()
            .unwrap();
        assert_ne!(write_type, WriteType::Rollback);
        assert_eq!(ts, commit_ts);
    }

    pub fn must_get_commit_ts_none<E: Engine>(engine: &E, key: &[u8], start_ts: u64) {
        let snapshot = engine.snapshot(&Context::new()).unwrap();
        let mut reader = MvccReader::new(snapshot, None, true, None, None, IsolationLevel::SI);

        let ret = reader.get_txn_commit_info(&Key::from_raw(key), start_ts);
        assert!(ret.is_ok());
        match ret.unwrap() {
            None => {}
            Some((_, write_type)) => {
                assert_eq!(write_type, WriteType::Rollback);
            }
        }
    }

    pub fn must_get_rollback_ts<E: Engine>(engine: &E, key: &[u8], start_ts: u64) {
        let snapshot = engine.snapshot(&Context::new()).unwrap();
        let mut reader = MvccReader::new(snapshot, None, true, None, None, IsolationLevel::SI);

        let (ts, write_type) = reader
            .get_txn_commit_info(&Key::from_raw(key), start_ts)
            .unwrap()
            .unwrap();
        assert_eq!(ts, start_ts);
        assert_eq!(write_type, WriteType::Rollback);
    }

    pub fn must_get_rollback_ts_none<E: Engine>(engine: &E, key: &[u8], start_ts: u64) {
        let snapshot = engine.snapshot(&Context::new()).unwrap();
        let mut reader = MvccReader::new(snapshot, None, true, None, None, IsolationLevel::SI);

        let ret = reader
            .get_txn_commit_info(&Key::from_raw(key), start_ts)
            .unwrap();
        assert_eq!(ret, None);
    }

    pub fn must_scan_keys<E: Engine>(
        engine: &E,
        start: Option<&[u8]>,
        limit: usize,
        keys: Vec<&[u8]>,
        next_start: Option<&[u8]>,
    ) {
        let expect = (
            keys.into_iter().map(Key::from_raw).collect(),
            next_start.map(|x| Key::from_raw(x).append_ts(0)),
        );
        let snapshot = engine.snapshot(&Context::new()).unwrap();
        let mut reader = MvccReader::new(
            snapshot,
            Some(ScanMode::Mixed),
            false,
            None,
            None,
            IsolationLevel::SI,
        );
        assert_eq!(
            reader.scan_keys(start.map(Key::from_raw), limit).unwrap(),
            expect
        );
    }
}
