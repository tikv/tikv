// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use tikv_kv::{ScanMode, Snapshot};
use txn_types::{Key, TimeStamp, Value, Write};

use crate::storage::mvcc::{Lock as MvccLock, MvccReader};

type LockWritesVals = (
    Option<MvccLock>,
    Vec<(TimeStamp, Write)>,
    Vec<(TimeStamp, Value)>,
);

pub fn find_mvcc_infos_by_key<S: Snapshot>(
    reader: &mut MvccReader<S>,
    key: &Key,
    // TODO: add some argument such as `limit` to limit the max size of mvcc info
) -> crate::storage::txn::Result<LockWritesVals> {
    let mut writes = vec![];
    let mut values = vec![];
    let lock = reader.load_lock(key)?;
    let mut ts = TimeStamp::max();
    loop {
        let opt = reader.seek_write(key, ts)?;
        match opt {
            Some((commit_ts, write)) => {
                writes.push((commit_ts, write));
                if commit_ts.is_zero() {
                    break;
                }
                ts = commit_ts.prev();
            }
            None => break,
        };
    }
    for (ts, v) in reader.scan_values_in_default(key)? {
        values.push((ts, v));
    }
    Ok((lock, writes, values))
}

pub fn collect_mvcc_info_for_debug<S: Snapshot>(snapshot: S, key: &Key) -> Option<LockWritesVals> {
    let mut reader = MvccReader::new(snapshot, Some(ScanMode::Forward), false);
    match find_mvcc_infos_by_key(&mut reader, key) {
        Ok(mvcc_info) => Some(mvcc_info),
        Err(e) => {
            warn!(
                "failed to collect mvcc as debug info";
                "key" => %key,
                "err" => format!("{:?}", e),
            );
            None
        }
    }
}

pub mod tests {
    #[cfg(test)]
    use std::array;

    use tikv_kv::Engine;
    #[cfg(test)]
    use tikv_kv::Snapshot;
    use txn_types::Key;
    #[cfg(test)]
    use txn_types::{Lock, SHORT_VALUE_MAX_LEN, TimeStamp, Value, Write};

    use crate::storage::txn::actions::mvcc::{LockWritesVals, MvccReader, find_mvcc_infos_by_key};
    #[cfg(test)]
    use crate::storage::{
        TestEngineBuilder,
        mvcc::SnapshotReader,
        txn::{
            actions::mvcc::collect_mvcc_info_for_debug,
            tests::{must_commit, must_prewrite_put},
        },
    };

    #[cfg(test)]
    const LONG_VALUE_MIN_LEN: usize = SHORT_VALUE_MAX_LEN + 1;

    pub fn must_find_mvcc_infos<E: Engine>(engine: &mut E, key: &[u8]) -> LockWritesVals {
        let snapshot = engine.snapshot(Default::default()).unwrap();
        let mut reader = MvccReader::new(snapshot, None, true);
        find_mvcc_infos_by_key(&mut reader, &Key::from_raw(key)).unwrap()
    }

    #[test]
    fn test_find_mvcc_infos_by_key() {
        let mut engine = TestEngineBuilder::new().build().unwrap();

        fn check_lock<S: Snapshot>(
            lock: Lock,
            key: &[u8],
            start_ts: impl Into<TimeStamp>,
            reader: &mut SnapshotReader<S>,
        ) {
            let expected_lock = reader.load_lock(&Key::from_raw(key)).unwrap().unwrap();
            assert_eq!(lock.ts, start_ts.into());
            assert_eq!(lock, expected_lock);
        }

        fn check_write<S: Snapshot>(
            write: (TimeStamp, Write),
            key: &[u8],
            start_ts: impl Into<TimeStamp>,
            commit_ts: impl Into<TimeStamp>,
            val: &[u8],
            data: Option<(TimeStamp, Value)>,
            reader: &mut SnapshotReader<S>,
        ) {
            let expected_start_ts = start_ts.into();
            let expected_commit_ts = commit_ts.into();
            let expected_value = Value::from(val.to_vec());
            let (commit_ts, write) = write;
            let tag = format!(
                "check_write, start_ts: {:?}, commit_ts: {:?}",
                expected_start_ts, expected_commit_ts
            );
            assert_eq!(commit_ts, expected_commit_ts, "{}", tag);
            assert_eq!(write.start_ts, expected_start_ts, "{}", tag);

            let expected_write = reader
                .get_write_with_commit_ts(&Key::from_raw(key), commit_ts)
                .unwrap()
                .unwrap();
            assert_eq!((write.clone(), commit_ts), expected_write, "{}", tag);
            match data {
                Some((ts, value)) => {
                    assert_eq!(ts, expected_start_ts, "{}", tag);
                    assert_eq!(value, expected_value, "{}", tag);
                    let data_cf_value = reader
                        .load_data(&Key::from_raw(key), write.clone())
                        .unwrap();
                    assert_eq!(data_cf_value, expected_value, "{}", tag);
                    assert_eq!(write.short_value, None, "{}", tag);
                }
                None => {
                    assert_eq!(write.short_value.unwrap(), expected_value, "{}", tag);
                }
            }
        }

        // Construct mvcc
        // LockCF:
        //   k1 => 21
        // WriteCF:
        //   k1@20 => 11
        //   k1@10 => 1
        // DataCF:
        //   k1@11 => v2
        let k = b"k1";
        let big_val: &[u8] = &array::from_fn::<_, LONG_VALUE_MIN_LEN, _>(|i| i as u8);
        must_prewrite_put(&mut engine, k, big_val, k, 1);
        must_commit(&mut engine, k, 1, 10);
        must_prewrite_put(&mut engine, k, b"v2", k, 11);
        must_commit(&mut engine, k, 11, 20);
        must_prewrite_put(&mut engine, k, b"v3", k, 21);
        let snapshot = engine.snapshot(Default::default()).unwrap();
        let mut reader = SnapshotReader::new(TimeStamp::max(), snapshot.clone(), true);

        let (lock, writes, values) = must_find_mvcc_infos(&mut engine, k);
        assert!(lock.is_some());
        assert_eq!(writes.len(), 2);
        assert_eq!(values.len(), 1);
        check_lock(lock.clone().unwrap(), k, 21, &mut reader);
        for (i, (start_ts, commit_ts, val)) in [(11, 20, b"v2" as &[u8]), (1, 10, big_val)]
            .iter()
            .enumerate()
        {
            check_write(
                writes[i].clone(),
                k,
                *start_ts,
                *commit_ts,
                val,
                if val.len() > SHORT_VALUE_MAX_LEN {
                    Some(values[0].clone())
                } else {
                    None
                },
                &mut reader,
            );
        }

        // collect_mvcc_info_for_debug should collect all mvcc info
        assert_eq!(
            collect_mvcc_info_for_debug(snapshot, &Key::from_raw(k)).unwrap(),
            (lock, writes, values),
        )
    }
}
