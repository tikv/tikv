// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

mod mutation;
mod txn_props;

use crate::storage::{
    mvcc::{metrics::MVCC_CONFLICT_COUNTER, ErrorInner, MvccTxn, Result},
    Snapshot,
};
use fail::fail_point;
use mutation::{LockStatus, PrewriteMutation};
pub use txn_props::{CommitKind, TransactionKind, TransactionProperties};
use txn_types::{Key, Mutation, TimeStamp};

/// Prewrite a single mutation by creating and storing a lock and value.
pub fn prewrite<S: Snapshot>(
    txn: &mut MvccTxn<S>,
    txn_props: &TransactionProperties,
    mutation: Mutation,
    secondary_keys: &Option<Vec<Vec<u8>>>,
    is_pessimistic_lock: bool,
) -> Result<TimeStamp> {
    let mut mutation = PrewriteMutation::from_mutation(mutation, secondary_keys, txn_props)?;

    fail_point!("prewrite", |err| Err(
        crate::storage::mvcc::txn::make_txn_error(err, &mutation.key, mutation.txn_props.start_ts)
            .into()
    ));

    let lock_status = match txn.reader.load_lock(&mutation.key)? {
        Some(lock) => mutation.check_lock(lock, is_pessimistic_lock)?,
        None if is_pessimistic_lock => {
            amend_pessimistic_lock(&mutation.key, txn)?;
            LockStatus::None
        }
        None => LockStatus::None,
    };

    if let LockStatus::Locked(ts) = lock_status {
        return Ok(ts);
    }

    let prev_write = if !mutation.skip_constraint_check() {
        mutation.check_for_newer_version(txn)?
    } else {
        None
    };

    if mutation.should_not_write {
        return Ok(TimeStamp::zero());
    }

    txn.check_extra_op(&mutation.key, mutation.mutation_type, prev_write)?;

    let final_min_commit_ts = mutation.write_lock(lock_status, txn)?;

    fail_point!("after_prewrite_one_key");

    Ok(final_min_commit_ts)
}

// TiKV may fails to write pessimistic locks due to pipelined process.
// If the data is not changed after acquiring the lock, we can still prewrite the key.
fn amend_pessimistic_lock<S: Snapshot>(key: &Key, txn: &mut MvccTxn<S>) -> Result<()> {
    if let Some((commit_ts, _)) = txn.reader.seek_write(key, TimeStamp::max())? {
        // The invariants of pessimistic locks are:
        //   1. lock's for_update_ts >= key's latest commit_ts
        //   2. lock's for_update_ts >= txn's start_ts
        //   3. If the data is changed after acquiring the pessimistic lock, key's new commit_ts > lock's for_update_ts
        //
        // So, if the key's latest commit_ts is still less than or equal to lock's for_update_ts, the data is not changed.
        // However, we can't get lock's for_update_ts in current implementation (txn's for_update_ts is updated for each DML),
        // we can only use txn's start_ts to check -- If the key's commit_ts is less than txn's start_ts, it's less than
        // lock's for_update_ts too.
        if commit_ts >= txn.start_ts {
            warn!(
                "prewrite failed (pessimistic lock not found)";
                "start_ts" => txn.start_ts,
                "commit_ts" => commit_ts,
                "key" => %key
            );
            MVCC_CONFLICT_COUNTER
                .pipelined_acquire_pessimistic_lock_amend_fail
                .inc();
            return Err(ErrorInner::PessimisticLockNotFound {
                start_ts: txn.start_ts,
                key: key.clone().into_raw()?,
            }
            .into());
        }
    }
    // Used pipelined pessimistic lock acquiring in this txn but failed
    // Luckily no other txn modified this lock, amend it by treat it as optimistic txn.
    MVCC_CONFLICT_COUNTER
        .pipelined_acquire_pessimistic_lock_amend_success
        .inc();
    Ok(())
}

pub mod tests {
    use super::*;
    use crate::storage::{mvcc::tests::*, Engine};
    #[cfg(test)]
    use crate::storage::{
        mvcc::Error,
        txn::{commands::prewrite::fallback_1pc_locks, tests::must_acquire_pessimistic_lock},
    };
    use concurrency_manager::ConcurrencyManager;
    use kvproto::kvrpcpb::Context;
    use txn_types::Key;

    fn optimistic_txn_props(primary: &[u8], start_ts: TimeStamp) -> TransactionProperties<'_> {
        TransactionProperties {
            start_ts,
            kind: TransactionKind::Optimistic(false),
            commit_kind: CommitKind::TwoPc,
            primary,
            txn_size: 0,
            lock_ttl: 0,
            min_commit_ts: TimeStamp::default(),
        }
    }

    #[cfg(test)]
    fn optimistic_async_props(
        primary: &[u8],
        start_ts: TimeStamp,
        max_commit_ts: TimeStamp,
        txn_size: u64,
        one_pc: bool,
    ) -> TransactionProperties<'_> {
        TransactionProperties {
            start_ts,
            kind: TransactionKind::Optimistic(false),
            commit_kind: if one_pc {
                CommitKind::OnePc(max_commit_ts)
            } else {
                CommitKind::Async(max_commit_ts)
            },
            primary,
            txn_size,
            lock_ttl: 2000,
            min_commit_ts: 10.into(),
        }
    }

    // Insert has a constraint that key should not exist
    pub fn try_prewrite_insert<E: Engine>(
        engine: &E,
        key: &[u8],
        value: &[u8],
        pk: &[u8],
        ts: impl Into<TimeStamp>,
    ) -> Result<()> {
        let ctx = Context::default();
        let snapshot = engine.snapshot(Default::default()).unwrap();
        let ts = ts.into();
        let cm = ConcurrencyManager::new(ts);
        let mut txn = MvccTxn::new(snapshot, ts, true, cm);

        prewrite(
            &mut txn,
            &optimistic_txn_props(pk, ts),
            Mutation::Insert((Key::from_raw(key), value.to_vec())),
            &None,
            false,
        )?;
        write(engine, &ctx, txn.into_modifies());
        Ok(())
    }

    pub fn try_prewrite_check_not_exists<E: Engine>(
        engine: &E,
        key: &[u8],
        pk: &[u8],
        ts: impl Into<TimeStamp>,
    ) -> Result<()> {
        let snapshot = engine.snapshot(Default::default()).unwrap();
        let ts = ts.into();
        let cm = ConcurrencyManager::new(ts);
        let mut txn = MvccTxn::new(snapshot, ts, true, cm);

        prewrite(
            &mut txn,
            &optimistic_txn_props(pk, ts),
            Mutation::CheckNotExists(Key::from_raw(key)),
            &None,
            false,
        )?;
        Ok(())
    }

    #[test]
    fn test_async_commit_prewrite_check_max_commit_ts() {
        let engine = crate::storage::TestEngineBuilder::new().build().unwrap();
        let cm = ConcurrencyManager::new(42.into());

        let snapshot = engine.snapshot(Default::default()).unwrap();

        let mut txn = MvccTxn::new(snapshot, 10.into(), false, cm.clone());
        // calculated commit_ts = 43 ≤ 50, ok
        prewrite(
            &mut txn,
            &optimistic_async_props(b"k1", 10.into(), 50.into(), 2, false),
            Mutation::Put((Key::from_raw(b"k1"), b"v1".to_vec())),
            &Some(vec![b"k2".to_vec()]),
            false,
        )
        .unwrap();

        cm.update_max_ts(60.into());
        // calculated commit_ts = 61 > 50, err
        let err = prewrite(
            &mut txn,
            &optimistic_async_props(b"k1", 10.into(), 50.into(), 1, false),
            Mutation::Put((Key::from_raw(b"k2"), b"v2".to_vec())),
            &Some(vec![]),
            false,
        )
        .unwrap_err();
        assert!(matches!(
            err,
            Error(box ErrorInner::CommitTsTooLarge { .. })
        ));

        let modifies = txn.into_modifies();
        assert_eq!(modifies.len(), 2); // the mutation that meets CommitTsTooLarge still exists
        write(&engine, &Default::default(), modifies);
        assert!(must_locked(&engine, b"k1", 10).use_async_commit);
        // The written lock should not have use_async_commit flag.
        assert!(!must_locked(&engine, b"k2", 10).use_async_commit);
    }

    #[test]
    fn test_async_commit_prewrite_min_commit_ts() {
        let engine = crate::storage::TestEngineBuilder::new().build().unwrap();
        let cm = ConcurrencyManager::new(42.into());
        let snapshot = engine.snapshot(Default::default()).unwrap();

        // min_commit_ts must be > max_ts
        let mut txn = MvccTxn::new(snapshot.clone(), 10.into(), false, cm.clone());
        let min_ts = prewrite(
            &mut txn,
            &optimistic_async_props(b"k1", 10.into(), 50.into(), 2, false),
            Mutation::Put((Key::from_raw(b"k1"), b"v1".to_vec())),
            &Some(vec![b"k2".to_vec()]),
            false,
        )
        .unwrap();
        assert!(min_ts > 42.into());
        assert!(min_ts < 50.into());

        // min_commit_ts must be > start_ts
        let mut txn = MvccTxn::new(snapshot, 44.into(), false, cm);
        let min_ts = prewrite(
            &mut txn,
            &optimistic_async_props(b"k3", 44.into(), 50.into(), 2, false),
            Mutation::Put((Key::from_raw(b"k3"), b"v1".to_vec())),
            &Some(vec![b"k4".to_vec()]),
            false,
        )
        .unwrap();
        assert!(min_ts > 44.into());
        assert!(min_ts < 50.into());

        // min_commit_ts must be > for_update_ts
        let mut props = optimistic_async_props(b"k5", 44.into(), 50.into(), 2, false);
        props.kind = TransactionKind::Pessimistic(45.into());
        let min_ts = prewrite(
            &mut txn,
            &props,
            Mutation::Put((Key::from_raw(b"k5"), b"v1".to_vec())),
            &Some(vec![b"k6".to_vec()]),
            false,
        )
        .unwrap();
        assert!(min_ts > 45.into());
        assert!(min_ts < 50.into());

        // min_commit_ts must be >= txn min_commit_ts
        let mut props = optimistic_async_props(b"k7", 44.into(), 50.into(), 2, false);
        props.min_commit_ts = 46.into();
        let min_ts = prewrite(
            &mut txn,
            &props,
            Mutation::Put((Key::from_raw(b"k7"), b"v1".to_vec())),
            &Some(vec![b"k8".to_vec()]),
            false,
        )
        .unwrap();
        assert!(min_ts >= 46.into());
        assert!(min_ts < 50.into());
    }

    #[test]
    fn test_1pc_check_max_commit_ts() {
        let engine = crate::storage::TestEngineBuilder::new().build().unwrap();
        let cm = ConcurrencyManager::new(42.into());

        let snapshot = engine.snapshot(Default::default()).unwrap();

        let mut txn = MvccTxn::new(snapshot, 10.into(), false, cm.clone());
        // calculated commit_ts = 43 ≤ 50, ok
        prewrite(
            &mut txn,
            &optimistic_async_props(b"k1", 10.into(), 50.into(), 2, true),
            Mutation::Put((Key::from_raw(b"k1"), b"v1".to_vec())),
            &None,
            false,
        )
        .unwrap();

        cm.update_max_ts(60.into());
        // calculated commit_ts = 61 > 50, err
        let err = prewrite(
            &mut txn,
            &optimistic_async_props(b"k1", 10.into(), 50.into(), 1, true),
            Mutation::Put((Key::from_raw(b"k2"), b"v2".to_vec())),
            &None,
            false,
        )
        .unwrap_err();
        assert!(matches!(
            err,
            Error(box ErrorInner::CommitTsTooLarge { .. })
        ));

        fallback_1pc_locks(&mut txn);
        let modifies = txn.into_modifies();
        assert_eq!(modifies.len(), 2); // the mutation that meets CommitTsTooLarge still exists
        write(&engine, &Default::default(), modifies);
        // success 1pc prewrite needs to be transformed to locks
        assert!(!must_locked(&engine, b"k1", 10).use_async_commit);
        assert!(!must_locked(&engine, b"k2", 10).use_async_commit);
    }

    pub fn try_pessimistic_prewrite_check_not_exists<E: Engine>(
        engine: &E,
        key: &[u8],
        pk: &[u8],
        ts: impl Into<TimeStamp>,
    ) -> Result<()> {
        let snapshot = engine.snapshot(Default::default()).unwrap();
        let ts = ts.into();
        let cm = ConcurrencyManager::new(ts);
        let mut txn = MvccTxn::new(snapshot, ts, true, cm);

        prewrite(
            &mut txn,
            &TransactionProperties {
                start_ts: ts,
                kind: TransactionKind::Pessimistic(TimeStamp::default()),
                commit_kind: CommitKind::TwoPc,
                primary: pk,
                txn_size: 0,
                lock_ttl: 0,
                min_commit_ts: TimeStamp::default(),
            },
            Mutation::CheckNotExists(Key::from_raw(key)),
            &None,
            false,
        )?;
        Ok(())
    }

    #[test]
    fn test_async_commit_pessimistic_prewrite_check_max_commit_ts() {
        let engine = crate::storage::TestEngineBuilder::new().build().unwrap();
        let cm = ConcurrencyManager::new(42.into());

        must_acquire_pessimistic_lock(&engine, b"k1", b"k1", 10, 10);
        must_acquire_pessimistic_lock(&engine, b"k2", b"k1", 10, 10);

        let snapshot = engine.snapshot(Default::default()).unwrap();

        let mut txn = MvccTxn::new(snapshot, 10.into(), false, cm.clone());
        let txn_props = TransactionProperties {
            start_ts: 10.into(),
            kind: TransactionKind::Pessimistic(20.into()),
            commit_kind: CommitKind::Async(50.into()),
            primary: b"k1",
            txn_size: 2,
            lock_ttl: 2000,
            min_commit_ts: 10.into(),
        };
        // calculated commit_ts = 43 ≤ 50, ok
        prewrite(
            &mut txn,
            &txn_props,
            Mutation::Put((Key::from_raw(b"k1"), b"v1".to_vec())),
            &Some(vec![b"k2".to_vec()]),
            true,
        )
        .unwrap();

        cm.update_max_ts(60.into());
        // calculated commit_ts = 61 > 50, ok
        prewrite(
            &mut txn,
            &txn_props,
            Mutation::Put((Key::from_raw(b"k2"), b"v2".to_vec())),
            &Some(vec![]),
            true,
        )
        .unwrap_err();
    }

    #[test]
    fn test_1pc_pessimistic_prewrite_check_max_commit_ts() {
        let engine = crate::storage::TestEngineBuilder::new().build().unwrap();
        let cm = ConcurrencyManager::new(42.into());

        must_acquire_pessimistic_lock(&engine, b"k1", b"k1", 10, 10);
        must_acquire_pessimistic_lock(&engine, b"k2", b"k1", 10, 10);

        let snapshot = engine.snapshot(Default::default()).unwrap();

        let mut txn = MvccTxn::new(snapshot, 10.into(), false, cm.clone());
        let txn_props = TransactionProperties {
            start_ts: 10.into(),
            kind: TransactionKind::Pessimistic(20.into()),
            commit_kind: CommitKind::OnePc(50.into()),
            primary: b"k1",
            txn_size: 2,
            lock_ttl: 2000,
            min_commit_ts: 10.into(),
        };
        // calculated commit_ts = 43 ≤ 50, ok
        prewrite(
            &mut txn,
            &txn_props,
            Mutation::Put((Key::from_raw(b"k1"), b"v1".to_vec())),
            &None,
            true,
        )
        .unwrap();

        cm.update_max_ts(60.into());
        // calculated commit_ts = 61 > 50, ok
        prewrite(
            &mut txn,
            &txn_props,
            Mutation::Put((Key::from_raw(b"k2"), b"v2".to_vec())),
            &None,
            true,
        )
        .unwrap_err();
    }
}
