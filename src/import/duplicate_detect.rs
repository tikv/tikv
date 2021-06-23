// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::{IterOptions, DATA_KEY_PREFIX_LEN};
use engine_traits::{CF_DEFAULT, CF_WRITE};
use kvproto::import_sstpb::{DuplicateDetectResponse, KvPair};
use sst_importer::{Error, Result};
use tikv_kv::{Iterator as kvIterator, Snapshot};
use tikv_util::keybuilder::KeyBuilder;
use txn_types::{Key, TimeStamp, WriteRef, WriteType};

#[cfg(test)]
const MAX_SCAN_BATCH_SIZE: usize = 256;

#[cfg(not(test))]
const MAX_SCAN_BATCH_SIZE: usize = 4096;

pub struct DuplicateDetector<S: Snapshot> {
    snapshot: S,
    iter: S::Iter,
    key_only: bool,
    valid: bool,
}

impl<S: Snapshot> DuplicateDetector<S> {
    pub fn new(
        snapshot: S,
        start_key: Vec<u8>,
        end_key: Option<Vec<u8>>,
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
            valid: true,
        })
    }

    pub fn try_next(&mut self) -> Result<Option<Vec<KvPair>>> {
        let mut ret = vec![];
        let mut last_key = Vec::with_capacity(256);
        let mut last_value = Vec::with_capacity(256);
        let mut last_commit_ts = TimeStamp::zero();
        let mut first_duplicate = true;
        while self.iter.valid().map_err(from_kv_error)? {
            let key = self.iter.key();
            let (user_key, commit_ts) = Key::split_on_ts_for(key)?;
            let value = self.iter.value();
            let write = WriteRef::parse(value).map_err(from_txn_types_error)?;
            match write.write_type {
                WriteType::Delete => {
                    last_key.clear();
                    self.iter.next().map_err(from_kv_error)?;
                    continue;
                }
                WriteType::Lock | WriteType::Rollback => {
                    self.iter.next().map_err(from_kv_error)?;
                    continue;
                }
                WriteType::Put => {
                    if !last_key.is_empty() && user_key.eq(&last_key) {
                        if first_duplicate {
                            // println!("last value lenth: {}, ts: {:?}", last_value.len(), last_commit_ts);
                            if self.key_only {
                                ret.push(self.make_kv_pair(&last_key, None, last_commit_ts)?);
                            } else {
                                let last_write =
                                    WriteRef::parse(&last_value).map_err(from_txn_types_error)?;
                                ret.push(self.make_kv_pair(
                                    &last_key,
                                    Some(last_write),
                                    last_commit_ts,
                                )?);
                            }
                        }
                        // println!("value lenth: {}, ts: {:?}", value.len(), commit_ts);
                        let w = if self.key_only { None } else { Some(write) };
                        ret.push(self.make_kv_pair(&user_key, w, commit_ts)?);
                        first_duplicate = false;
                    } else {
                        if ret.len() >= MAX_SCAN_BATCH_SIZE {
                            return Ok(Some(ret));
                        }
                        first_duplicate = true;
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
        return Ok(Some(ret));
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

    fn write_data<E: Engine, L: LockManager>(
        storage: &Storage<E, L>,
        data: Vec<(Vec<u8>, Vec<u8>)>,
        ts: u64,
    ) {
        let primary = data[0].0.clone();
        let start_ts = ts - 1;
        let keys: Vec<Key> = data.iter().map(|(key, _)| Key::from_raw(key)).collect();
        let cmd = commands::Prewrite::with_defaults(
            data.into_iter()
                .map(|(key, value)| Mutation::Put((Key::from_raw(&key), value)))
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
        for i in 0..1000 {
            let key = format!("{}", i * 2);
            let value = format!("{}", i * 2);
            data.push((key.as_bytes().to_vec(), value.as_bytes().to_vec()))
        }
        write_data(&storage, data, 5);
        // We have to do the prewrite manually so that the mem locks don't get released.
        let snapshot = storage.engine.snapshot(Default::default()).unwrap();
        let start = format!("{}", 0);
        let end = format!("{}", 800);
        let mut detector = DuplicateDetector::new(
            snapshot,
            start.as_bytes().to_vec(),
            Some(end.as_bytes().to_vec()),
            false,
        )
        .unwrap();
        let mut expected_kvs = vec![];
        for i in 0..400 {
            let key = format!("{}", i * 2);
            let value = format!("{}", i * 2);
            expected_kvs.push((key.as_bytes().to_vec(), value.as_bytes().to_vec(), 5));
            expected_kvs.push((key.as_bytes().to_vec(), value.as_bytes().to_vec(), 3));
        }
        expected_kvs.sort_by(|a, b| {
            if a.0.eq(&b.0) {
                b.2.cmp(&a.2)
            } else {
                a.0.cmp(&b.0)
            }
        });
        let mut base = 0;
        while let Some(mut resp) = detector.try_next().unwrap() {
            let data: Vec<(Vec<u8>, Vec<u8>, u64)> = resp
                .into_iter()
                .map(|mut p| (p.take_key(), p.take_value(), p.get_commit_ts()))
                .collect();
            println!("len {}", data.len());
            assert!(expected_kvs.len() >= base + data.len());
            for i in base..(base + data.len()) {
                assert_eq!(expected_kvs[i], data[i - base], "the {} data", i);
            }
            base += data.len();
        }
    }
}
