// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use crate::storage::kv::{Snapshot, WriteData};
use crate::storage::lock_manager::{self, Lock, LockManager, WaitTimeout};
use crate::storage::mvcc::{
    Error as MvccError, ErrorInner as MvccErrorInner, Lock as MvccLock, MvccReader, ReleasedLock,
    TimeStamp, Write,
};
use crate::storage::txn::{Error, ErrorInner, ProcessResult, Result};
use crate::storage::{
    Error as StorageError, ErrorInner as StorageErrorInner, Result as StorageResult,
};
use kvproto::kvrpcpb::Context;
use txn_types::{Key, Value};

// To resolve a key, the write size is about 100~150 bytes, depending on key and value length.
// The write batch will be around 32KB if we scan 256 keys each time.
pub const RESOLVE_LOCK_BATCH_SIZE: usize = 256;

#[derive(Default)]
pub struct ReleasedLocks {
    start_ts: TimeStamp,
    commit_ts: TimeStamp,
    hashes: Vec<u64>,
    pessimistic: bool,
}

impl ReleasedLocks {
    pub fn new(start_ts: TimeStamp, commit_ts: TimeStamp) -> Self {
        Self {
            start_ts,
            commit_ts,
            ..Default::default()
        }
    }

    pub fn push(&mut self, lock: Option<ReleasedLock>) {
        if let Some(lock) = lock {
            self.hashes.push(lock.hash);
            if !self.pessimistic {
                self.pessimistic = lock.pessimistic;
            }
        }
    }

    // Wake up pessimistic transactions that waiting for these locks.
    pub fn wake_up<L: LockManager>(self, lock_mgr: &L) {
        lock_mgr.wake_up(self.start_ts, self.hashes, self.commit_ts, self.pessimistic);
    }
}

pub fn extract_lock_from_result<T>(res: &StorageResult<T>) -> Lock {
    match res {
        Err(StorageError(box StorageErrorInner::Txn(Error(box ErrorInner::Mvcc(MvccError(
            box MvccErrorInner::KeyIsLocked(info),
        )))))) => Lock {
            ts: info.get_lock_version().into(),
            hash: Key::from_raw(info.get_key()).gen_hash(),
        },
        _ => panic!("unexpected mvcc error"),
    }
}

pub struct WriteResult {
    pub ctx: Context,
    pub to_be_write: WriteData,
    pub rows: usize,
    pub pr: ProcessResult,
    // (lock, is_first_lock, wait_timeout)
    pub lock_info: Option<(lock_manager::Lock, bool, Option<WaitTimeout>)>,
}

type LockWritesVals = (
    Option<MvccLock>,
    Vec<(TimeStamp, Write)>,
    Vec<(TimeStamp, Value)>,
);

pub fn find_mvcc_infos_by_key<S: Snapshot>(
    reader: &mut MvccReader<S>,
    key: &Key,
    mut ts: TimeStamp,
) -> Result<LockWritesVals> {
    let mut writes = vec![];
    let mut values = vec![];
    let lock = reader.load_lock(key)?;
    loop {
        let opt = reader.seek_write(key, ts)?;
        match opt {
            Some((commit_ts, write)) => {
                ts = commit_ts.prev();
                writes.push((commit_ts, write));
            }
            None => break,
        };
    }
    for (ts, v) in reader.scan_values_in_default(key)? {
        values.push((ts, v));
    }
    Ok((lock, writes, values))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::kv::{Engine, Snapshot, Statistics, TestEngineBuilder};
    use crate::storage::txn::commands::FORWARD_MIN_MUTATIONS_NUM;
    use crate::storage::txn::commands::{Commit, Prewrite};
    use crate::storage::txn::LockInfo;
    use crate::storage::{mvcc::Mutation, DummyLockManager, PrewriteResult};
    use engine_traits::CF_WRITE;
    use kvproto::kvrpcpb::ExtraOp;
    use pd_client::DummyPdClient;
    use std::sync::Arc;

    #[test]
    fn test_extract_lock_from_result() {
        let raw_key = b"key".to_vec();
        let key = Key::from_raw(&raw_key);
        let ts = 100;
        let mut info = LockInfo::default();
        info.set_key(raw_key);
        info.set_lock_version(ts);
        info.set_lock_ttl(100);
        let case = StorageError::from(StorageErrorInner::Txn(Error::from(ErrorInner::Mvcc(
            MvccError::from(MvccErrorInner::KeyIsLocked(info)),
        ))));
        let lock = extract_lock_from_result::<()>(&Err(case));
        assert_eq!(lock.ts, ts.into());
        assert_eq!(lock.hash, key.gen_hash());
    }

    fn inner_test_prewrite_skip_constraint_check(pri_key_number: u8, write_num: usize) {
        let mut mutations = Vec::default();
        let pri_key = &[pri_key_number];
        for i in 0..write_num {
            mutations.push(Mutation::Insert((
                Key::from_raw(&[i as u8]),
                b"100".to_vec(),
            )));
        }
        let mut statistic = Statistics::default();
        let engine = TestEngineBuilder::new().build().unwrap();
        prewrite(
            &engine,
            &mut statistic,
            vec![Mutation::Put((
                Key::from_raw(&[pri_key_number]),
                b"100".to_vec(),
            ))],
            pri_key.to_vec(),
            99,
        )
        .unwrap();
        assert_eq!(1, statistic.write.seek);
        let e = prewrite(
            &engine,
            &mut statistic,
            mutations.clone(),
            pri_key.to_vec(),
            100,
        )
        .err()
        .unwrap();
        assert_eq!(2, statistic.write.seek);
        match e {
            Error(box ErrorInner::Mvcc(MvccError(box MvccErrorInner::KeyIsLocked(_)))) => (),
            _ => panic!("error type not match"),
        }
        commit(
            &engine,
            &mut statistic,
            vec![Key::from_raw(&[pri_key_number])],
            99,
            102,
        )
        .unwrap();
        assert_eq!(2, statistic.write.seek);
        let e = prewrite(
            &engine,
            &mut statistic,
            mutations.clone(),
            pri_key.to_vec(),
            101,
        )
        .err()
        .unwrap();
        match e {
            Error(box ErrorInner::Mvcc(MvccError(box MvccErrorInner::WriteConflict {
                ..
            }))) => (),
            _ => panic!("error type not match"),
        }
        let e = prewrite(
            &engine,
            &mut statistic,
            mutations.clone(),
            pri_key.to_vec(),
            104,
        )
        .err()
        .unwrap();
        match e {
            Error(box ErrorInner::Mvcc(MvccError(box MvccErrorInner::AlreadyExist { .. }))) => (),
            _ => panic!("error type not match"),
        }

        statistic.write.seek = 0;
        let ctx = Context::default();
        engine
            .delete_cf(
                &ctx,
                CF_WRITE,
                Key::from_raw(&[pri_key_number]).append_ts(102.into()),
            )
            .unwrap();
        prewrite(
            &engine,
            &mut statistic,
            mutations.clone(),
            pri_key.to_vec(),
            104,
        )
        .unwrap();
        // All keys are prewrited successful with only one seek operations.
        assert_eq!(1, statistic.write.seek);
        let keys: Vec<Key> = mutations.iter().map(|m| m.key().clone()).collect();
        commit(&engine, &mut statistic, keys.clone(), 104, 105).unwrap();
        let snap = engine.snapshot(&ctx).unwrap();
        for k in keys {
            let v = snap.get_cf(CF_WRITE, &k.append_ts(105.into())).unwrap();
            assert!(v.is_some());
        }
    }

    #[test]
    fn test_prewrite_skip_constraint_check() {
        inner_test_prewrite_skip_constraint_check(0, FORWARD_MIN_MUTATIONS_NUM + 1);
        inner_test_prewrite_skip_constraint_check(5, FORWARD_MIN_MUTATIONS_NUM + 1);
        inner_test_prewrite_skip_constraint_check(
            FORWARD_MIN_MUTATIONS_NUM as u8,
            FORWARD_MIN_MUTATIONS_NUM + 1,
        );
    }

    fn prewrite<E: Engine>(
        engine: &E,
        statistics: &mut Statistics,
        mutations: Vec<Mutation>,
        primary: Vec<u8>,
        start_ts: u64,
    ) -> Result<()> {
        let ctx = Context::default();
        let snap = engine.snapshot(&ctx)?;
        let mut cmd = Prewrite::with_defaults(mutations, primary, TimeStamp::from(start_ts));
        let ret = cmd.cmd.write_command_mut().process_write(
            snap,
            &DummyLockManager {},
            Arc::new(DummyPdClient::new()),
            ExtraOp::Noop,
            statistics,
            false,
        )?;
        if let ProcessResult::PrewriteResult {
            result: PrewriteResult { locks, .. },
        } = ret.pr
        {
            if !locks.is_empty() {
                let info = LockInfo::default();
                return Err(Error::from(ErrorInner::Mvcc(MvccError::from(
                    MvccErrorInner::KeyIsLocked(info),
                ))));
            }
        }
        let ctx = Context::default();
        engine.write(&ctx, ret.to_be_write).unwrap();
        Ok(())
    }

    fn commit<E: Engine>(
        engine: &E,
        statistics: &mut Statistics,
        keys: Vec<Key>,
        lock_ts: u64,
        commit_ts: u64,
    ) -> Result<()> {
        let ctx = Context::default();
        let snap = engine.snapshot(&ctx)?;
        let mut cmd = Commit::new(
            keys,
            TimeStamp::from(lock_ts),
            TimeStamp::from(commit_ts),
            ctx,
        );

        let ret = cmd.cmd.write_command_mut().process_write(
            snap,
            &DummyLockManager {},
            Arc::new(DummyPdClient::new()),
            ExtraOp::Noop,
            statistics,
            false,
        )?;
        let ctx = Context::default();
        engine.write(&ctx, ret.to_be_write).unwrap();
        Ok(())
    }
}
