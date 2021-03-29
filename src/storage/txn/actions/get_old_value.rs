// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use crate::storage::mvcc::{MvccTxn, Result as MvccResult};
use crate::storage::Snapshot;
use txn_types::{Key, OldValue, Write};

/// Read the old value for key for CDC.
/// `prev_write` stands for the previous write record of the key
/// it must be read in the caller and be passed in for optimization
pub fn get_old_value<S: Snapshot>(
    txn: &mut MvccTxn<S>,
    _key: &Key,
    prev_write: Option<Write>,
) -> MvccResult<OldValue> {
    match prev_write {
        Some(w)
            if w.may_have_old_value()
                && w.as_ref().check_gc_fence_as_latest_version(txn.start_ts) =>
        {
            Ok(OldValue::Value {
                short_value: w.short_value,
                start_ts: w.start_ts,
            })
        }
        _ => Ok(OldValue::None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::mvcc::tests::write;
    use crate::storage::{Engine, TestEngineBuilder};
    use concurrency_manager::ConcurrencyManager;
    use kvproto::kvrpcpb::Context;
    use txn_types::{TimeStamp, WriteType};

    #[test]
    fn test_get_old_value() {
        struct Case {
            expected: OldValue,

            // (write_record, put_ts)
            // all data to write to the engine
            // current write_cursor will be on the last record in `written`
            // which also means prev_write is `Write` in the record
            written: Vec<(Write, TimeStamp)>,
        }
        let cases = vec![
            // prev_write is None
            Case {
                expected: OldValue::None,
                written: vec![],
            },
            // prev_write is not Rollback or Lock, check_gc_fence_as_latest_version is true
            Case {
                expected: OldValue::Value {
                    short_value: None,
                    start_ts: TimeStamp::new(7),
                },
                written: vec![(
                    Write::new(WriteType::Put, TimeStamp::new(7), None)
                        .set_overlapped_rollback(true, Some(27.into())),
                    TimeStamp::new(5),
                )],
            },
            // prev_write is not Rollback or Lock, check_gc_fence_as_latest_version is false
            Case {
                expected: OldValue::None,
                written: vec![(
                    Write::new(WriteType::Put, TimeStamp::new(4), None)
                        .set_overlapped_rollback(true, Some(3.into())),
                    TimeStamp::new(5),
                )],
            },
        ];
        for case in cases {
            let engine = TestEngineBuilder::new().build().unwrap();
            let cm = ConcurrencyManager::new(42.into());
            let snapshot = engine.snapshot(Default::default()).unwrap();
            let mut txn = MvccTxn::new(snapshot, TimeStamp::new(10), true, cm.clone());
            for (write_record, put_ts) in case.written.iter() {
                txn.put_write(
                    Key::from_raw(b"a"),
                    *put_ts,
                    write_record.as_ref().to_bytes(),
                );
            }
            write(&engine, &Context::default(), txn.into_modifies());
            let snapshot = engine.snapshot(Default::default()).unwrap();
            let mut txn = MvccTxn::new(snapshot, TimeStamp::new(25), true, cm);
            let prev_write = if case.written.is_empty() {
                None
            } else {
                Some(
                    txn.reader
                        .seek_write(&Key::from_raw(b"a"), case.written.last().unwrap().1)
                        .unwrap()
                        .unwrap()
                        .1,
                )
            };
            let result = get_old_value(&mut txn, &Key::from_raw(b"a"), prev_write).unwrap();
            assert_eq!(result, case.expected);
        }
    }
}
