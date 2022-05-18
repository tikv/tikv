// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    cmp::Ordering,
    collections::VecDeque,
    fmt::format,
    ops::{Deref, DerefMut},
    sync::Arc,
};

use collections::{HashMap, HashSet};
use engine_rocks::RocksSstReader;
use engine_traits::{Error, Iterator as TraitIterator, SstReader, CF_DEFAULT, CF_WRITE};
use kvengine::{table::Value, Iterator};
use kvproto::{import_sstpb::SstMeta, raft_cmdpb::RaftCmdRequest};
use sst_importer::SstImporter;
use tikv_util::{codec, error, info};
use txn_types::{WriteRef, WriteType};

use crate::{mvcc, RaftRouter, UserMeta};

pub(crate) fn convert_sst(
    router: &RaftRouter,
    kv: kvengine::Engine,
    importer: Arc<SstImporter>,
    req: &RaftCmdRequest,
) -> crate::Result<kvenginepb::ChangeSet> {
    info!("convert sst {:?}", req);
    let region_id = req.get_header().get_region_id();
    let region_ver = req.get_header().get_region_epoch().get_version();
    let mut sst_iters = vec![];
    for import_req in req.get_requests() {
        let sst_meta = import_req.get_ingest_sst().get_sst().clone();
        let reader = importer.get_reader(&sst_meta)?;
        let sst_iter = SstIterator::new(sst_meta, reader);
        sst_iters.push(sst_iter);
    }
    sst_iters.sort_unstable_by(|a, b| {
        let a_start = a.meta.get_range().get_start();
        let b_start = b.meta.get_range().get_start();
        let cmp = a_start.cmp(b_start);
        if !cmp.is_eq() {
            return cmp;
        }
        if a.meta.get_cf_name() == CF_DEFAULT {
            Ordering::Less
        } else {
            Ordering::Greater
        }
    });
    let ingest_id = sst_iters.first().as_ref().unwrap().meta.get_uuid().to_vec();
    let mut default_sst_iter = None;
    let mut combined_iters = vec![];
    for iter in sst_iters.drain(..) {
        if iter.meta.get_cf_name() == CF_DEFAULT {
            default_sst_iter = Some(iter);
            continue;
        }
        assert_eq!(iter.meta.get_cf_name(), CF_WRITE);
        let combined_iter = CombinedIterator::new(iter, default_sst_iter.take());
        combined_iters.push(combined_iter);
    }
    let concat_iter = ConcatIterator::new(combined_iters);
    let mut level = 0;
    let shard = kv.get_shard_with_ver(region_id, region_ver)?;
    let stats = shard.get_stats();
    if stats.l0_table_count == 0 {
        let cf_stats = &stats.cfs[0];
        for l in cf_stats.levels.iter() {
            if l.num_tables == 0 {
                level = l.level as u32;
            }
        }
    }
    let cs = kv.build_ingest_files(
        region_id,
        region_ver,
        Box::new(concat_iter),
        level,
        ingest_id,
    )?;
    Ok(cs)
}

struct SstIterator {
    meta: SstMeta,
    iter: engine_rocks::RocksSstIterator,
}

impl Deref for SstIterator {
    type Target = engine_rocks::RocksSstIterator;

    fn deref(&self) -> &Self::Target {
        &self.iter
    }
}

impl DerefMut for SstIterator {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.iter
    }
}

impl SstIterator {
    fn new(meta: SstMeta, reader: RocksSstReader) -> Self {
        let iter = reader.iter();
        Self { meta, iter }
    }
}

struct CombinedIterator {
    write_iter: SstIterator,
    default_iter: Option<SstIterator>,
    current_key: Vec<u8>,
    current_value: Vec<u8>,
    start_ts: u64,
    commit_ts: u64,
}

impl CombinedIterator {
    fn new(write_iter: SstIterator, default_iter: Option<SstIterator>) -> Self {
        Self {
            write_iter,
            default_iter,
            current_key: vec![],
            current_value: vec![],
            start_ts: 0,
            commit_ts: 0,
        }
    }

    fn rewind(&mut self) -> crate::Result<bool> {
        let valid = self.write_iter.seek_to_first()?;
        if !valid {
            return Ok(false);
        }
        if let Some(default_iter) = self.default_iter.as_mut() {
            default_iter.seek_to_first()?;
        }
        self.update_current()
    }

    fn update_current(&mut self) -> crate::Result<bool> {
        let (key, ts) = parse_rocksdb_key(self.write_iter.key())?;
        self.current_key = key;
        self.commit_ts = ts;
        let write_ref = WriteRef::parse(self.write_iter.value())?;
        self.start_ts = write_ref.start_ts.into_inner();
        let user_meta = self.get_user_meta();
        match write_ref.write_type {
            WriteType::Put => match write_ref.short_value {
                Some(short_val) => {
                    self.current_value = encode_table_value(user_meta, short_val);
                }
                None => {
                    let mut default_iter = self.default_iter.as_mut().unwrap();
                    if !default_iter.valid()? {
                        let err_msg =
                            format!("default value not found for key {:?}", &self.current_key);
                        return Err(sst_importer::Error::BadFormat(err_msg).into());
                    }
                    let (default_key, start_ts) = parse_rocksdb_key(default_iter.key())?;
                    if default_key.ne(&self.current_key) || start_ts != self.start_ts {
                        let err_msg = format!(
                            "default key {:?} not match write key {:?}",
                            &default_key, &self.current_key
                        );
                        return Err(sst_importer::Error::BadFormat(err_msg).into());
                    }
                    self.current_value = encode_table_value(user_meta, default_iter.value());
                    default_iter.next()?;
                }
            },
            WriteType::Delete => {
                self.current_value = encode_table_value(user_meta, &[]);
            }
            _ => panic!("unexpected write type"),
        }
        Ok(true)
    }

    fn next(&mut self) -> crate::Result<bool> {
        let valid = self.write_iter.next()?;
        if !valid {
            return Ok(false);
        }
        self.update_current()
    }

    fn get_user_meta(&self) -> mvcc::UserMeta {
        mvcc::UserMeta::new(self.start_ts, self.commit_ts)
    }

    fn valid(&self) -> std::result::Result<bool, engine_traits::Error> {
        self.write_iter.valid()
    }
}

fn parse_rocksdb_key(data_key: &[u8]) -> codec::Result<(Vec<u8>, u64)> {
    let origin_key = keys::origin_key(data_key);
    let key = txn_types::Key::from_encoded(origin_key.to_vec());
    let ts = key.decode_ts()?.into_inner();
    let raw_key = key.to_raw()?;
    Ok((raw_key, ts))
}

fn encode_table_value(user_meta: UserMeta, val: &[u8]) -> Vec<u8> {
    Value::encode_buf(0, &user_meta.to_array(), user_meta.commit_ts, val)
}

struct ConcatIterator {
    iterators: Vec<CombinedIterator>,
    idx: usize,
}

impl ConcatIterator {
    fn new(iterators: Vec<CombinedIterator>) -> Self {
        Self { iterators, idx: 0 }
    }
}

impl ConcatIterator {
    fn get_current(&self) -> &CombinedIterator {
        &self.iterators[self.idx]
    }

    fn mut_current(&mut self) -> &mut CombinedIterator {
        &mut self.iterators[self.idx]
    }
}

impl kvengine::table::Iterator for ConcatIterator {
    fn next(&mut self) {
        let iter_len = self.iterators.len();
        let current = self.mut_current();
        let valid = current.next().unwrap_or_else(|err| {
            error!("next error {:?}", err);
            false
        });
        if valid || self.idx + 1 == iter_len {
            return;
        }
        self.idx += 1;
        let current = self.mut_current();
        current.rewind().unwrap_or_else(|err| {
            error!("rewind error {:?}", err);
            false
        });
    }

    fn next_version(&mut self) -> bool {
        unreachable!()
    }

    fn rewind(&mut self) {
        self.idx = 0;
        if let Err(err) = self.iterators[0].rewind() {
            error!("rewind error {:?}", err);
        }
    }

    fn seek(&mut self, key: &[u8]) {
        unreachable!()
    }

    fn key(&self) -> &[u8] {
        let current = self.get_current();
        current.current_key.as_slice()
    }

    fn value(&self) -> Value {
        let current = self.get_current();
        Value::decode(&current.current_value)
    }

    fn valid(&self) -> bool {
        let valid = self.get_current().valid().unwrap_or(false);
        valid
    }
}
