// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use crate::storage::mvcc::{seek_for_valid_write, MvccTxn, Result as MvccResult};
use crate::storage::Snapshot;
use kvproto::kvrpcpb::ExtraOp;
use txn_types::{Key, MutationType, OldValue, Write, WriteType};

// Check and execute the extra operation.
// Currently we use it only for reading the old value for CDC.
pub fn check_extra_op<S: Snapshot>(
    txn: &mut MvccTxn<S>,
    key: &Key,
    mutation_type: MutationType,
    prev_write: Option<Write>,
) -> MvccResult<()> {
    if txn.extra_op == ExtraOp::ReadOldValue
        && (mutation_type == MutationType::Put || mutation_type == MutationType::Delete)
    {
        let old_value = if let Some(w) = prev_write {
            // If write is Rollback or Lock, seek for valid write record.
            get_old_value(txn, key, w)?
        } else {
            None
        };
        // If write is None or cannot find a previously valid write record.
        txn.writes.extra.add_old_value(
            key.clone().append_ts(txn.start_ts),
            old_value,
            mutation_type,
        );
    }
    Ok(())
}

fn get_old_value<S: Snapshot>(
    txn: &mut MvccTxn<S>,
    key: &Key,
    prev_write: Write,
) -> MvccResult<Option<OldValue>> {
    if prev_write.write_type == WriteType::Rollback || prev_write.write_type == WriteType::Lock {
        let write_cursor = txn.reader.write_cursor.as_mut().unwrap();
        // Skip the current write record.
        write_cursor.next(&mut txn.reader.statistics.write);
        let write = seek_for_valid_write(
            write_cursor,
            key,
            txn.start_ts,
            txn.start_ts,
            &mut txn.reader.statistics,
        )?;
        Ok(write.map(|w| OldValue {
            short_value: w.short_value,
            start_ts: w.start_ts,
        }))
    } else if prev_write
        .as_ref()
        .check_gc_fence_as_latest_version(txn.start_ts)
    {
        Ok(Some(OldValue {
            short_value: prev_write.short_value,
            start_ts: prev_write.start_ts,
        }))
    } else {
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::kv::WriteData;
    use crate::storage::mvcc::txn::tests::txn_props;
    use crate::storage::txn::{acquire_pessimistic_lock, commit, prewrite, CommitKind};
    use crate::storage::{Engine, TestEngineBuilder};
    use concurrency_manager::ConcurrencyManager;
    use kvproto::kvrpcpb::Context;
    use txn_types::{Mutation, TimeStamp};
    #[test]
    fn test_extra_op_old_value() {
        let engine = TestEngineBuilder::new().build().unwrap();
        let key = Key::from_raw(b"key");
        let ctx = Context::default();

        let new_old_value = |short_value, start_ts| OldValue {
            short_value,
            start_ts,
        };

        let cases = vec![
            (
                Mutation::Put((key.clone(), b"v0".to_vec())),
                false,
                5,
                5,
                None,
                true,
            ),
            (
                Mutation::Put((key.clone(), b"v1".to_vec())),
                false,
                6,
                6,
                Some(new_old_value(Some(b"v0".to_vec()), 5.into())),
                true,
            ),
            (Mutation::Lock(key.clone()), false, 7, 7, None, false),
            (
                Mutation::Lock(key.clone()),
                false,
                8,
                8,
                Some(new_old_value(Some(b"v1".to_vec()), 6.into())),
                false,
            ),
            (
                Mutation::Put((key.clone(), vec![b'0'; 5120])),
                false,
                9,
                9,
                Some(new_old_value(Some(b"v1".to_vec()), 6.into())),
                true,
            ),
            (
                Mutation::Put((key.clone(), b"v3".to_vec())),
                false,
                10,
                10,
                Some(new_old_value(None, 9.into())),
                true,
            ),
            (
                Mutation::Put((key.clone(), b"v4".to_vec())),
                true,
                11,
                11,
                None,
                true,
            ),
        ];

        let write = |modifies| {
            engine.write(&ctx, modifies).unwrap();
        };

        let new_txn = |start_ts, cm| {
            let snapshot = engine.snapshot(Default::default()).unwrap();
            MvccTxn::new(snapshot, start_ts, true, cm)
        };

        for case in cases {
            let (mutation, is_pessimistic, start_ts, commit_ts, old_value, check_old_value) = case;
            let mutation_type = mutation.mutation_type();
            let cm = ConcurrencyManager::new(start_ts.into());
            let mut txn = new_txn(start_ts.into(), cm.clone());

            txn.extra_op = ExtraOp::ReadOldValue;
            if is_pessimistic {
                acquire_pessimistic_lock(
                    &mut txn,
                    key.clone(),
                    b"key",
                    false,
                    0,
                    start_ts.into(),
                    false,
                    TimeStamp::zero(),
                )
                .unwrap();
                write(WriteData::from_modifies(txn.into_modifies()));
                txn = new_txn(start_ts.into(), cm.clone());
                txn.extra_op = ExtraOp::ReadOldValue;
                prewrite(
                    &mut txn,
                    &txn_props(
                        start_ts.into(),
                        b"key",
                        CommitKind::TwoPc,
                        Some(TimeStamp::default()),
                        0,
                        false,
                    ),
                    mutation,
                    &None,
                    true,
                )
                .unwrap();
            } else {
                prewrite(
                    &mut txn,
                    &txn_props(start_ts.into(), b"key", CommitKind::TwoPc, None, 0, false),
                    mutation,
                    &None,
                    false,
                )
                .unwrap();
            }
            if check_old_value {
                let extra = txn.take_extra();
                let ts_key = key.clone().append_ts(start_ts.into());
                assert!(
                    extra.old_values.get(&ts_key).is_some(),
                    "{}@{}",
                    ts_key,
                    start_ts
                );
                assert_eq!(extra.old_values[&ts_key], (old_value, mutation_type));
            }
            write(WriteData::from_modifies(txn.into_modifies()));
            let mut txn = new_txn(start_ts.into(), cm);
            commit(&mut txn, key.clone(), commit_ts.into()).unwrap();
            engine
                .write(&ctx, WriteData::from_modifies(txn.into_modifies()))
                .unwrap();
        }
    }
}
