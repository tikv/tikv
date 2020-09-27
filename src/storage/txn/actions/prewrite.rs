use crate::storage::mvcc::{
    metrics::{MVCC_CONFLICT_COUNTER, MVCC_DUPLICATE_CMD_COUNTER_VEC},
    ErrorInner, LockType, MvccTxn, Result as MvccResult,
};
use crate::storage::txn::actions::shared::prewrite_key_value;
use crate::storage::Snapshot;
use txn_types::{Key, Mutation, TimeStamp, WriteType};

pub fn prewrite<S: Snapshot>(
    txn: &mut MvccTxn<S>,
    mutation: Mutation,
    primary: &[u8],
    secondary_keys: &Option<Vec<Vec<u8>>>,
    skip_constraint_check: bool,
    lock_ttl: u64,
    txn_size: u64,
    min_commit_ts: TimeStamp,
) -> MvccResult<TimeStamp> {
    let lock_type = LockType::from_mutation(&mutation);
    // For the insert/checkNotExists operation, the old key should not be in the system.
    let should_not_exist = mutation.should_not_exists();
    let should_not_write = mutation.should_not_write();
    let mutation_type = mutation.mutation_type();
    let (key, value) = mutation.into_key_value();

    fail_point!("prewrite", |err| Err(
        crate::storage::mvcc::txn::make_txn_error(err, &key, txn.start_ts,).into()
    ));

    let mut prev_write = None;
    // Check whether there is a newer version.
    if !skip_constraint_check {
        if let Some((commit_ts, write)) = txn.reader.seek_write(&key, TimeStamp::max())? {
            // Abort on writes after our start timestamp ...
            // If exists a commit version whose commit timestamp is larger than current start
            // timestamp, we should abort current prewrite.
            if commit_ts > txn.start_ts {
                MVCC_CONFLICT_COUNTER.prewrite_write_conflict.inc();
                return Err(ErrorInner::WriteConflict {
                    start_ts: txn.start_ts,
                    conflict_start_ts: write.start_ts,
                    conflict_commit_ts: commit_ts,
                    key: key.into_raw()?,
                    primary: primary.to_vec(),
                }
                .into());
            }
            // If there's a write record whose commit_ts equals to our start ts, the current
            // transaction is ok to continue, unless the record means that the current
            // transaction has been rolled back.
            if commit_ts == txn.start_ts
                && (write.write_type == WriteType::Rollback || write.has_overlapped_rollback)
            {
                MVCC_CONFLICT_COUNTER.rolled_back.inc();
                // TODO: Maybe we need to add a new error for the rolled back case.
                return Err(ErrorInner::WriteConflict {
                    start_ts: txn.start_ts,
                    conflict_start_ts: write.start_ts,
                    conflict_commit_ts: commit_ts,
                    key: key.into_raw()?,
                    primary: primary.to_vec(),
                }
                .into());
            }
            txn.check_data_constraint(should_not_exist, &write, commit_ts, &key)?;
            prev_write = Some(write);
        }
    }
    if should_not_write {
        return Ok(TimeStamp::zero());
    }
    // Check whether the current key is locked at any timestamp.
    if let Some(lock) = txn.reader.load_lock(&key)? {
        if lock.ts != txn.start_ts {
            return Err(ErrorInner::KeyIsLocked(lock.into_lock_info(key.into_raw()?)).into());
        }
        // TODO: remove it in future
        if lock.lock_type == LockType::Pessimistic {
            return Err(ErrorInner::LockTypeNotMatch {
                start_ts: txn.start_ts,
                key: key.into_raw()?,
                pessimistic: true,
            }
            .into());
        }
        // Duplicated command. No need to overwrite the lock and data.
        MVCC_DUPLICATE_CMD_COUNTER_VEC.prewrite.inc();
        return Ok(lock.min_commit_ts);
    }

    txn.check_extra_op(&key, mutation_type, prev_write)?;
    prewrite_key_value(
        txn,
        key,
        lock_type.unwrap(),
        primary,
        secondary_keys,
        value,
        lock_ttl,
        TimeStamp::zero(),
        txn_size,
        min_commit_ts,
    )
}

pub mod tests {
    use super::*;
    use crate::storage::mvcc::tests::*;
    use crate::storage::mvcc::MvccTxn;
    use crate::storage::Engine;
    use concurrency_manager::ConcurrencyManager;
    use kvproto::kvrpcpb::Context;
    use txn_types::TimeStamp;

    // Insert has a constraint that key should not exist
    pub fn try_prewrite_insert<E: Engine>(
        engine: &E,
        key: &[u8],
        value: &[u8],
        pk: &[u8],
        ts: impl Into<TimeStamp>,
    ) -> MvccResult<()> {
        let ctx = Context::default();
        let snapshot = engine.snapshot(&ctx).unwrap();
        let ts = ts.into();
        let cm = ConcurrencyManager::new(ts);
        let mut txn = MvccTxn::new(snapshot, ts, true, cm);

        prewrite(
            &mut txn,
            Mutation::Insert((Key::from_raw(key), value.to_vec())),
            pk,
            &None,
            false,
            0,
            0,
            TimeStamp::default(),
        )?;
        write(engine, &ctx, txn.into_modifies());
        Ok(())
    }

    pub fn try_prewrite_check_not_exists<E: Engine>(
        engine: &E,
        key: &[u8],
        pk: &[u8],
        ts: impl Into<TimeStamp>,
    ) -> MvccResult<()> {
        let ctx = Context::default();
        let snapshot = engine.snapshot(&ctx).unwrap();
        let ts = ts.into();
        let cm = ConcurrencyManager::new(ts);
        let mut txn = MvccTxn::new(snapshot, ts, true, cm);

        prewrite(
            &mut txn,
            Mutation::CheckNotExists(Key::from_raw(key)),
            pk,
            &None,
            false,
            0,
            0,
            TimeStamp::default(),
        )?;
        Ok(())
    }
}
