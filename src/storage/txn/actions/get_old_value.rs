// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use crate::storage::mvcc::{MvccTxn, Result as MvccResult};
use crate::storage::Snapshot;
use txn_types::{Key, OldValue, TimeStamp, Write, WriteType};

/// Read the old value for key for CDC.
/// `prev_write` stands for the previous write record of the key
/// it must be read in the caller and be passed in for optimization
pub fn get_old_value<S: Snapshot>(
    txn: &mut MvccTxn<S>,
    key: &Key,
    start_ts: TimeStamp,
    prev_write_loaded: bool,
    prev_write: Option<Write>,
) -> MvccResult<OldValue> {
    if prev_write_loaded && prev_write.is_none() {
        return Ok(OldValue::None);
    }
    let reader = &mut txn.reader;
    if let Some(prev_write) = prev_write {
        if !prev_write
            .as_ref()
            .check_gc_fence_as_latest_version(start_ts)
        {
            return Ok(OldValue::None);
        }

        match prev_write.write_type {
            WriteType::Put => {
                // For Put, there must be an old value either in its
                // short value or in the default CF.
                return Ok(match prev_write.short_value {
                    Some(value) => OldValue::Value { value },
                    None => OldValue::ValueTimeStamp {
                        start_ts: prev_write.start_ts,
                    },
                });
            }
            WriteType::Delete => {
                // For Delete, no old value.
                return Ok(OldValue::None);
            }
            // For Rollback and Lock, it's unknown whether there is a more
            // previous valid write. Call `get_write` to get a valid
            // previous write.
            WriteType::Rollback | WriteType::Lock => (),
        }
    }
    Ok(match reader.get_write(key, start_ts, Some(start_ts))? {
        Some(write) => match write.short_value {
            Some(value) => OldValue::Value { value },
            None => OldValue::ValueTimeStamp {
                start_ts: write.start_ts,
            },
        },
        None => OldValue::None,
    })
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
            // prev_write is Rollback, and there exists a more previous valid write
            Case {
                expected: OldValue::ValueTimeStamp {
                    start_ts: TimeStamp::new(4),
                },

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
            Case {
                expected: OldValue::Value {
                    value: b"v".to_vec(),
                },

                written: vec![
                    (
                        Write::new(WriteType::Put, TimeStamp::new(4), Some(b"v".to_vec())),
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
                expected: OldValue::None,
                written: vec![(
                    Write::new(WriteType::Rollback, TimeStamp::new(5), None),
                    TimeStamp::new(6),
                )],
            },
            // prev_write is Lock, and there exists a more previous valid write
            Case {
                expected: OldValue::ValueTimeStamp {
                    start_ts: TimeStamp::new(3),
                },

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
                expected: OldValue::None,
                written: vec![(
                    Write::new(WriteType::Lock, TimeStamp::new(5), None),
                    TimeStamp::new(6),
                )],
            },
            // prev_write is not Rollback or Lock, check_gc_fence_as_latest_version is true
            Case {
                expected: OldValue::ValueTimeStamp {
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
            // prev_write is Delete, check_gc_fence_as_latest_version is true
            Case {
                expected: OldValue::None,
                written: vec![
                    (
                        Write::new(WriteType::Put, TimeStamp::new(3), None),
                        TimeStamp::new(6),
                    ),
                    (
                        Write::new(WriteType::Delete, TimeStamp::new(7), None),
                        TimeStamp::new(8),
                    ),
                ],
            },
            // prev_write is Delete, check_gc_fence_as_latest_version is false
            Case {
                expected: OldValue::None,
                written: vec![
                    (
                        Write::new(WriteType::Put, TimeStamp::new(3), None),
                        TimeStamp::new(6),
                    ),
                    (
                        Write::new(WriteType::Delete, TimeStamp::new(7), None)
                            .set_overlapped_rollback(true, Some(6.into())),
                        TimeStamp::new(8),
                    ),
                ],
            },
        ];
        for (i, case) in cases.into_iter().enumerate() {
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
            if !case.written.is_empty() {
                let prev_write = txn
                    .reader
                    .seek_write(&Key::from_raw(b"a"), case.written.last().unwrap().1)
                    .unwrap()
                    .map(|w| w.1);
                let prev_write_loaded = true;
                let result = get_old_value(
                    &mut txn,
                    &Key::from_raw(b"a"),
                    TimeStamp::new(25),
                    prev_write_loaded,
                    prev_write,
                )
                .unwrap();
                assert_eq!(result, case.expected, "case #{}", i);
            }
        }
    }
}
