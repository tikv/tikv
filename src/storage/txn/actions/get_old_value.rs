// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use crate::storage::mvcc::{seek_for_valid_write, MvccTxn, Result as MvccResult};
use crate::storage::Snapshot;
use txn_types::{Key, OldValue, Write, WriteType};

pub fn get_old_value<S: Snapshot>(
    txn: &mut MvccTxn<S>,
    key: &Key,
    prev_write: Write,
) -> MvccResult<Option<OldValue>> {
    debug_assert_eq!(
        &Key::from_encoded(
            txn.reader
                .write_cursor
                .as_ref()
                .unwrap()
                .key(&mut txn.reader.statistics.write)
                .to_vec()
        )
        .truncate_ts()
        .unwrap(),
        key
    );
    debug_assert_eq!(
        txn_types::WriteRef::parse(
            txn.reader
                .write_cursor
                .as_ref()
                .unwrap()
                .value(&mut txn.reader.statistics.write)
        )
        .unwrap()
        .to_owned(),
        prev_write
    );
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
    use crate::storage::mvcc::tests::write;
    use crate::storage::{Engine, TestEngineBuilder};
    use concurrency_manager::ConcurrencyManager;
    use kvproto::kvrpcpb::Context;
    use txn_types::TimeStamp;

    #[test]
    fn test_get_old_value() {
        struct Case {
            expected: Option<OldValue>,

            // (write_record, put_ts)
            // all data to write to the engine
            // current write_cursor will be on the last record in `written`
            // which also means prev_write is `Write` in the record
            written: Vec<(Write, TimeStamp)>,
        }
        let cases = vec![
            // prev_write is Rollback, and there exists a more previous valid write
            Case {
                expected: Some(OldValue {
                    short_value: None,
                    start_ts: TimeStamp::new(4),
                }),

                written: vec![
                    (
                        Write::new(WriteType::Put, TimeStamp::new(4), None),
                        TimeStamp::new(6),
                    ),
                    (
                        Write::new(WriteType::Rollback, TimeStamp::new(5), None),
                        TimeStamp::new(7),
                    ),
                ],
            },
            // prev_write is Rollback, and there isn't a more previous valid write
            Case {
                expected: None,

                written: vec![(
                    Write::new(WriteType::Rollback, TimeStamp::new(5), None),
                    TimeStamp::new(6),
                )],
            },
            // prev_write is Lock, and there exists a more previous valid write
            Case {
                expected: Some(OldValue {
                    short_value: None,
                    start_ts: TimeStamp::new(3),
                }),

                written: vec![
                    (
                        Write::new(WriteType::Put, TimeStamp::new(3), None),
                        TimeStamp::new(6),
                    ),
                    (
                        Write::new(WriteType::Lock, TimeStamp::new(5), None),
                        TimeStamp::new(7),
                    ),
                ],
            },
            // prev_write is Lock, and there isn't a more previous valid write
            Case {
                expected: None,

                written: vec![(
                    Write::new(WriteType::Lock, TimeStamp::new(5), None),
                    TimeStamp::new(6),
                )],
            },
            // prev_write is not Rollback or Lock, check_gc_fence_as_latest_version is true
            Case {
                expected: Some(OldValue {
                    short_value: None,
                    start_ts: TimeStamp::new(7),
                }),
                written: vec![(
                    Write::new(WriteType::Put, TimeStamp::new(7), None)
                        .set_overlapped_rollback(true, Some(27.into())),
                    TimeStamp::new(5),
                )],
            },
            // prev_write is not Rollback or Lock, check_gc_fence_as_latest_version is false
            Case {
                expected: None,
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
            let prev_write = txn
                .reader
                .seek_write(&Key::from_raw(b"a"), case.written.last().unwrap().1)
                .unwrap()
                .unwrap()
                .1;
            let result = get_old_value(&mut txn, &Key::from_raw(b"a"), prev_write).unwrap();
            assert_eq!(result, case.expected);
        }
    }
}
