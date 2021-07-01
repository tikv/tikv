// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::{IterOptions, DATA_KEY_PREFIX_LEN};
use engine_traits::{CF_DEFAULT, CF_WRITE};
use kvproto::import_sstpb::{DuplicateDetectResponse, KvPair};
use sst_importer::{Error, Result};
use tikv_kv::{Iterator as kvIterator, Snapshot};
use tikv_util::keybuilder::KeyBuilder;
use txn_types::{Key, TimeStamp, WriteRef, WriteType};

#[cfg(test)]
const MAX_SCAN_BATCH_COUNT: usize = 256;

#[cfg(not(test))]
const MAX_SCAN_BATCH_COUNT: usize = 4096;

const MAX_SCAN_BATCH_SIZE: usize = 1024 * 1024; // 1MB

pub struct DuplicateDetector<S: Snapshot> {
    snapshot: S,
    iter: S::Iter,
    key_only: bool,
    valid: bool,
    min_commit_ts: TimeStamp,
}

impl<S: Snapshot> DuplicateDetector<S> {
    pub fn new(
        snapshot: S,
        start_key: Vec<u8>,
        end_key: Option<Vec<u8>>,
        min_commit_ts: u64,
        key_only: bool,
    ) -> Result<DuplicateDetector<S>> {
        let start_key = Key::from_raw(&start_key);
        let l_bound =
            KeyBuilder::from_vec(start_key.clone().into_encoded(), DATA_KEY_PREFIX_LEN, 0);
        let u_bound = end_key.map(|k| {
            let key = Key::from_raw(&k);
            KeyBuilder::from_vec(key.into_encoded(), DATA_KEY_PREFIX_LEN, 0)
        });
        let mut iter_opt = IterOptions::new(Some(l_bound), u_bound, false);
        iter_opt.set_key_only(key_only);
        let mut iter = snapshot
            .iter_cf(CF_WRITE, iter_opt)
            .map_err(from_kv_error)?;
        iter.seek(&start_key).map_err(from_kv_error)?;
        Ok(DuplicateDetector {
            snapshot,
            iter,
            key_only,
            min_commit_ts: TimeStamp::new(min_commit_ts),
            valid: true,
        })
    }

    pub fn try_next(&mut self) -> Result<Option<Vec<KvPair>>> {
        let mut ret = vec![];
        let mut last_key = Vec::with_capacity(256);
        let mut last_value = Vec::with_capacity(256);
        let mut last_commit_ts = TimeStamp::zero();
        let mut first_duplicate = true;
        let mut batch_size = 0;
        let mut has_delete_key_before_import = false;
        let mut has_put_key_before_import = false;
        while self.iter.valid().map_err(from_kv_error)? {
            let key = self.iter.key();
            let (user_key, commit_ts) = Key::split_on_ts_for(key)?;
            let value = self.iter.value();
            let write = WriteRef::parse(value).map_err(from_txn_types_error)?;
            match write.write_type {
                WriteType::Delete => {
                    if commit_ts > self.min_commit_ts {
                        return Err(Error::Engine(box_err!(
                            "we find a delete key with commits ts {} larger than min_commit_ts of importer {}",
                            commit_ts,
                            self.min_commit_ts
                        )));
                    }
                    if has_delete_key_before_import || has_put_key_before_import {
                        self.iter.next().map_err(from_kv_error)?;
                        continue;
                    }
                    last_value.clear();
                    last_key.extend_from_slice(user_key);
                    has_delete_key_before_import = true;
                    last_commit_ts = commit_ts;
                    self.iter.next().map_err(from_kv_error)?;
                    continue;
                }
                WriteType::Lock | WriteType::Rollback => {
                    self.iter.next().map_err(from_kv_error)?;
                    continue;
                }
                WriteType::Put => {
                    if !last_key.is_empty() && user_key.eq(&last_key) {
                        if has_delete_key_before_import || has_put_key_before_import {
                            // Do not need to detect deleted keys.
                            // If there are multiple versions of one key before import, we only detect
                            // whether the first one is the same with keys of import.
                            self.iter.next().map_err(from_kv_error)?;
                            continue;
                        }
                        batch_size += user_key.len();
                        if first_duplicate {
                            batch_size += last_key.len();
                            if self.key_only {
                                ret.push(self.make_kv_pair(&last_key, None, last_commit_ts)?);
                            } else {
                                let last_write =
                                    WriteRef::parse(&last_value).map_err(from_txn_types_error)?;
                                batch_size += last_write
                                    .short_value
                                    .as_ref()
                                    .map(|v| v.len())
                                    .unwrap_or(0);
                                ret.push(self.make_kv_pair(
                                    &last_key,
                                    Some(last_write),
                                    last_commit_ts,
                                )?);
                            }
                        }
                        let w = if self.key_only {
                            None
                        } else {
                            batch_size += write.short_value.as_ref().map(|v| v.len()).unwrap_or(0);
                            Some(write)
                        };
                        ret.push(self.make_kv_pair(&user_key, w, commit_ts)?);
                        first_duplicate = false;
                        if commit_ts <= self.min_commit_ts {
                            has_delete_key_before_import = true;
                        }
                    } else {
                        if ret.len() >= MAX_SCAN_BATCH_COUNT || batch_size >= MAX_SCAN_BATCH_SIZE {
                            return Ok(Some(ret));
                        }
                        first_duplicate = true;
                        has_delete_key_before_import = false;
                        if commit_ts > self.min_commit_ts {
                            has_put_key_before_import = false;
                        } else {
                            has_put_key_before_import = true;
                        }
                        if !self.key_only {
                            last_value.clear();
                            last_value.extend_from_slice(value);
                        }
                    }
                }
            }

            last_key.clear();
            last_key.extend_from_slice(user_key);
            last_commit_ts = commit_ts;
            self.iter.next().map_err(from_kv_error)?;
        }
        if ret.is_empty() {
            return Ok(None);
        }
        Ok(Some(ret))
    }

    fn make_kv_pair<'a>(
        &self,
        key: &[u8],
        write: Option<WriteRef<'a>>,
        ts: TimeStamp,
    ) -> Result<KvPair> {
        let mut pair = KvPair::default();
        let user_key = Key::from_encoded_slice(key);
        pair.set_key(user_key.to_raw()?);
        pair.set_commit_ts(ts.into_inner());
        if let Some(write) = write {
            match write.short_value {
                Some(value) => pair.set_value(value.to_vec()),
                None => {
                    let value = self
                        .snapshot
                        .get_cf(CF_DEFAULT, &user_key.append_ts(write.start_ts))
                        .map_err(from_kv_error)?;
                    match value {
                        Some(val) => pair.set_value(val.to_vec()),
                        None => return Err(Error::RocksDB("Not found defaultcf value".to_owned())),
                    }
                }
            }
        }
        Ok(pair)
    }
}

impl<S: Snapshot> Iterator for DuplicateDetector<S> {
    type Item = DuplicateDetectResponse;

    fn next(&mut self) -> Option<Self::Item> {
        if !self.valid {
            return None;
        }
        let mut resp = DuplicateDetectResponse::default();
        match self.try_next() {
            Ok(Some(pairs)) => {
                resp.set_pairs(pairs.into());
            }
            Err(e) => {
                resp.set_key_error(e.into());
                self.valid = false;
            }
            Ok(None) => {
                return None;
            }
        }
        Some(resp)
    }
}

fn from_kv_error(e: tikv_kv::Error) -> Error {
    match e {
        tikv_kv::Error(box tikv_kv::ErrorInner::Other(err)) => Error::Engine(err),
        _ => Error::RocksDB("unkown error when request rocksdb".to_owned()),
    }
}

fn from_txn_types_error(e: txn_types::Error) -> Error {
    match e {
        txn_types::Error(box txn_types::ErrorInner::Codec(err)) => Error::CodecError(err),
        _ => Error::BadFormat("parse write type error".to_owned()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::lock_manager::{DummyLockManager, LockManager};
    use crate::storage::txn::commands;
    use crate::storage::{Storage, TestStorageBuilder};
    use kvproto::kvrpcpb::Context;
    use std::sync::mpsc::channel;
    use tikv_kv::Engine;
    use txn_types::Mutation;

    fn prewrite_data<E: Engine, L: LockManager>(
        storage: &Storage<E, L>,
        primary: Vec<u8>,
        data: Vec<(Vec<u8>, Vec<u8>)>,
        start_ts: u64,
    ) {
        let cmd = commands::Prewrite::with_defaults(
            data.into_iter()
                .map(|(key, value)| {
                    if value.is_empty() {
                        Mutation::Delete(Key::from_raw(&key))
                    } else {
                        Mutation::Put((Key::from_raw(&key), value))
                    }
                })
                .collect(),
            primary,
            start_ts.into(),
        );
        let (tx, rx) = channel();
        storage
            .sched_txn_command(
                cmd,
                Box::new(move |x| {
                    x.unwrap();
                    tx.send(()).unwrap();
                }),
            )
            .unwrap();
        rx.recv().unwrap();
    }

    fn rollback_data<E: Engine, L: LockManager>(
        storage: &Storage<E, L>,
        data: Vec<Vec<u8>>,
        start_ts: u64,
    ) {
        let cmd = commands::Rollback::new(
            data.into_iter().map(|key| Key::from_raw(&key)).collect(),
            start_ts.into(),
            Context::default(),
        );
        let (tx, rx) = channel();
        storage
            .sched_txn_command(
                cmd,
                Box::new(move |x| {
                    x.unwrap();
                    tx.send(()).unwrap();
                }),
            )
            .unwrap();
        rx.recv().unwrap();
    }

    fn write_data<E: Engine, L: LockManager>(
        storage: &Storage<E, L>,
        data: Vec<(Vec<u8>, Vec<u8>)>,
        ts: u64,
    ) {
        let primary = data[0].0.clone();
        let start_ts = ts - 1;
        let keys: Vec<Key> = data.iter().map(|(key, _)| Key::from_raw(key)).collect();
        prewrite_data(storage, primary, data, start_ts);
        let cmd = commands::Commit::new(keys, start_ts.into(), ts.into(), Context::default());
        let (tx, rx) = channel();
        storage
            .sched_txn_command(
                cmd,
                Box::new(move |x| {
                    x.unwrap();
                    tx.send(()).unwrap();
                }),
            )
            .unwrap();
        rx.recv().unwrap();
    }

    fn check_duplicate_data<S: Snapshot>(
        mut detector: DuplicateDetector<S>,
        expected_kvs: Vec<(Vec<u8>, Vec<u8>, u64)>,
    ) {
        let mut base = 0;
        while let Some(resp) = detector.try_next().unwrap() {
            let data: Vec<(Vec<u8>, Vec<u8>, u64)> = resp
                .into_iter()
                .map(|mut p| (p.take_key(), p.take_value(), p.get_commit_ts()))
                .collect();
            assert!(expected_kvs.len() >= base + data.len());
            for i in base..(base + data.len()) {
                assert_eq!(
                    expected_kvs[i],
                    data[i - base],
                    "base {}, the {} data,  {}, {}",
                    base,
                    i,
                    String::from_utf8(expected_kvs[i].0.clone()).unwrap(),
                    String::from_utf8(data[i - base].0.clone()).unwrap(),
                );
            }
            base += data.len();
        }
    }

    #[test]
    fn test_duplicate_detect() {
        let storage = TestStorageBuilder::new(DummyLockManager {}, false)
            .build()
            .unwrap();
        let mut data = vec![];
        for i in 0..1000 {
            let key = format!("{}", i);
            let value = format!("{}", i);
            data.push((key.as_bytes().to_vec(), value.as_bytes().to_vec()))
        }
        write_data(&storage, data, 3);
        let mut data = vec![];
        let big_value = vec![1u8; 300];
        for i in 0..1000 {
            let key = format!("{}", i * 2);
            data.push((key.as_bytes().to_vec(), big_value.clone()))
        }
        write_data(&storage, data, 5);
        // We have to do the prewrite manually so that the mem locks don't get released.
        let snapshot = storage.engine.snapshot(Default::default()).unwrap();
        let start = format!("{}", 0);
        let end = format!("{}", 800);
        let detector = DuplicateDetector::new(
            snapshot,
            start.as_bytes().to_vec(),
            Some(end.as_bytes().to_vec()),
            0,
            false,
        )
        .unwrap();
        let mut expected_kvs = vec![];
        for i in 0..400 {
            let key = format!("{}", i * 2);
            let value = format!("{}", i * 2);
            expected_kvs.push((key.as_bytes().to_vec(), big_value.clone(), 5));
            expected_kvs.push((key.as_bytes().to_vec(), value.as_bytes().to_vec(), 3));
        }
        expected_kvs.sort_by(|a, b| {
            if a.0.eq(&b.0) {
                b.2.cmp(&a.2)
            } else {
                a.0.cmp(&b.0)
            }
        });
        check_duplicate_data(detector, expected_kvs);
    }

    // There are 40 key-value pairs in db, there are
    //  [100, 101, 102, 103, 104, 105, 106, 107, 108, 109] with commit timestamp 10
    //  [104, 105, 106, 107, 108, 109, 110, 111, 112, 113] with commit timestamp 14, these 20 keys
    //   have existed in db before importing. So we do not think (105,10) is repeated with (105,14).
    //  [108, 109, 110, 111, 112, 113, 114, 115, 116, 117] with commit timestamp 18
    //  [112, 113, 114, 115, 116, 117, 118, 119, 120, 121] with commit timestamp 22, these 20 keys
    // are imported by lightning. So (108,18) is repeated with (108,14), but (108,18) is not repeated
    // with (108,10).
    #[test]
    fn test_duplicate_detect_incremental() {
        let storage = TestStorageBuilder::new(DummyLockManager {}, false)
            .build()
            .unwrap();
        for &start in &[100, 104, 108, 112] {
            let end = start + 10;
            let mut data = vec![];
            for i in start..end {
                let key = format!("{}", i);
                let value = format!("{}", i);
                data.push((key.as_bytes().to_vec(), value.as_bytes().to_vec()))
            }
            write_data(&storage, data, start - 90);
        }

        // We have to do the prewrite manually so that the mem locks don't get released.
        let snapshot = storage.engine.snapshot(Default::default()).unwrap();
        let start = format!("{}", 0);
        let detector =
            DuplicateDetector::new(snapshot, start.as_bytes().to_vec(), None, 16, false).unwrap();
        let mut expected_kvs = vec![];
        for &(i, ts) in &[
            (108u64, 14),
            (108, 18),
            (109, 14),
            (109, 18),
            (110, 14),
            (110, 18),
            (111, 14),
            (111, 18),
            (112, 14),
            (112, 18),
            (112, 22),
            (113, 14),
            (113, 18),
            (113, 22),
            (114, 18),
            (114, 22),
            (115, 18),
            (115, 22),
            (116, 18),
            (116, 22),
            (117, 18),
            (117, 22),
        ] {
            let key = format!("{}", i);
            let value = format!("{}", i);
            expected_kvs.push((key.as_bytes().to_vec(), value.as_bytes().to_vec(), ts));
        }

        expected_kvs.sort_by(|a, b| {
            if a.0.eq(&b.0) {
                b.2.cmp(&a.2)
            } else {
                a.0.cmp(&b.0)
            }
        });
        check_duplicate_data(detector, expected_kvs);
    }

    #[test]
    fn test_duplicate_detect_rollback_and_delete() {
        let storage = TestStorageBuilder::new(DummyLockManager {}, false)
            .build()
            .unwrap();
        let data = vec![
            (b"100".to_vec(), b"100".to_vec()),
            (b"101".to_vec(), b"101".to_vec()),
            (b"102".to_vec(), b"102".to_vec()),
        ];
        write_data(&storage, data.clone(), 10);
        prewrite_data(&storage, b"100".to_vec(), data[..2].to_vec(), 11);
        rollback_data(&storage, vec![b"100".to_vec(), b"101".to_vec()], 11);
        write_data(&storage, vec![(b"102".to_vec(), vec![])], 12);
        write_data(&storage, data, 14);
        let expected_kvs = vec![
            (b"100".to_vec(), b"100".to_vec(), 14),
            (b"100".to_vec(), b"100".to_vec(), 10),
            (b"101".to_vec(), b"101".to_vec(), 14),
            (b"101".to_vec(), b"101".to_vec(), 10),
        ];
        let snapshot = storage.engine.snapshot(Default::default()).unwrap();
        let detector = DuplicateDetector::new(snapshot, b"0".to_vec(), None, 13, false).unwrap();
        check_duplicate_data(detector, expected_kvs);
    }
}
