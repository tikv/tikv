// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use collections::HashMap;
use engine_rocks::RocksSstReader;
use engine_traits::{Iterable, Iterator as TraitIterator, SstReader, CF_DEFAULT, CF_WRITE};
use kvengine::{table::Value, ShardMeta};
use kvproto::{import_sstpb::SstMeta, raft_cmdpb::RaftCmdRequest};
use sst_importer::SstImporter;
use tikv_util::{codec, error, info};
use txn_types::{WriteRef, WriteType};

use crate::UserMeta;

pub(crate) fn convert_sst(
    kv: kvengine::Engine,
    importer: Arc<SstImporter>,
    req: &RaftCmdRequest,
    shard_meta: ShardMeta,
) -> crate::Result<kvenginepb::ChangeSet> {
    info!("{} convert sst {:?}", shard_meta.tag(), req);
    let region_id = req.get_header().get_region_id();
    let region_ver = req.get_header().get_region_epoch().get_version();
    let concat_iter = build_concat_iterator(&importer, req)?;
    let ingest_id = concat_iter
        .iterators
        .first()
        .map(|iter| iter.meta.get_uuid().to_vec())
        .unwrap();
    let cs = kv.build_ingest_files(
        region_id,
        region_ver,
        Box::new(concat_iter),
        ingest_id,
        shard_meta,
    )?;
    Ok(cs)
}

fn collect_default_values(
    importer: &SstImporter,
    req: &RaftCmdRequest,
) -> crate::Result<HashMap<(Vec<u8>, u64), Vec<u8>>> {
    let mut default_values = HashMap::default();
    for import_req in req.get_requests() {
        let sst_meta = import_req.get_ingest_sst().get_sst();
        if sst_meta.get_cf_name() != CF_DEFAULT {
            continue;
        }
        let reader = importer.get_reader(sst_meta)?;
        reader.scan(&[], &[], false, |rocks_key, v| {
            let key = parse_rocksdb_key(rocks_key)?;
            default_values.insert(key, v.to_vec());
            Ok(true)
        })?;
    }
    Ok(default_values)
}

fn build_concat_iterator(
    importer: &SstImporter,
    req: &RaftCmdRequest,
) -> crate::Result<ConcatIterator> {
    let default_values = Arc::new(collect_default_values(importer, req)?);
    let mut sst_iters = vec![];
    for import_req in req.get_requests() {
        let sst_meta = import_req.get_ingest_sst().get_sst();
        if sst_meta.get_cf_name() != CF_WRITE {
            continue;
        }
        let reader = importer.get_reader(sst_meta)?;
        let sst_iter = SstIterator::new(sst_meta.clone(), reader, default_values.clone());
        sst_iters.push(sst_iter);
    }
    sst_iters.sort_by(|a, b| {
        a.meta
            .get_range()
            .get_start()
            .cmp(b.meta.get_range().get_start())
    });
    Ok(ConcatIterator::new(sst_iters))
}

struct SstIterator {
    meta: SstMeta,
    iter: engine_rocks::RocksSstIterator,
    current_key: Vec<u8>,
    current_value: Vec<u8>,
    start_ts: u64,
    commit_ts: u64,
    default_values: Arc<HashMap<(Vec<u8>, u64), Vec<u8>>>,
}

impl SstIterator {
    fn new(
        meta: SstMeta,
        reader: RocksSstReader,
        default_values: Arc<HashMap<(Vec<u8>, u64), Vec<u8>>>,
    ) -> Self {
        let iter = reader.iter();
        Self {
            meta,
            iter,
            current_key: vec![],
            current_value: vec![],
            start_ts: 0,
            commit_ts: 0,
            default_values,
        }
    }

    fn update_current(&mut self) -> crate::Result<bool> {
        let (key, ts) = parse_rocksdb_key(self.iter.key())?;
        self.commit_ts = ts;
        let write_ref = WriteRef::parse(self.iter.value())?;
        self.start_ts = write_ref.start_ts.into_inner();
        let default_key = (key, self.start_ts);
        let default_value = self.default_values.get(&default_key);
        self.current_key = default_key.0;
        let user_meta = UserMeta::new(self.start_ts, self.commit_ts);
        match write_ref.write_type {
            WriteType::Put => match write_ref.short_value {
                Some(short_val) => {
                    self.current_value = encode_table_value(user_meta, short_val);
                }
                None => {
                    self.current_value = encode_table_value(user_meta, default_value.unwrap());
                }
            },
            WriteType::Delete => {
                self.current_value = encode_table_value(user_meta, &[]);
            }
            _ => panic!("unexpected write type"),
        }
        Ok(true)
    }

    fn rewind(&mut self) -> crate::Result<bool> {
        let valid = self.iter.seek_to_first()?;
        if !valid {
            return Ok(false);
        }
        self.update_current()
    }

    fn next(&mut self) -> crate::Result<bool> {
        let valid = self.iter.next()?;
        if !valid {
            return Ok(false);
        }
        self.update_current()
    }

    fn valid(&self) -> std::result::Result<bool, engine_traits::Error> {
        self.iter.valid()
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
    iterators: Vec<SstIterator>,
    idx: usize,
}

impl ConcatIterator {
    fn new(iterators: Vec<SstIterator>) -> Self {
        Self { iterators, idx: 0 }
    }
}

impl ConcatIterator {
    fn get_current(&self) -> &SstIterator {
        &self.iterators[self.idx]
    }

    fn mut_current(&mut self) -> &mut SstIterator {
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

    fn seek(&mut self, _key: &[u8]) {
        unreachable!()
    }

    fn key(&self) -> &[u8] {
        let current = self.get_current();
        current.current_key.as_slice()
    }

    fn value(&self) -> Value {
        let current = self.get_current();
        if current.current_value.len() == 0 {
            panic!("current value is empty");
        }
        Value::decode(&current.current_value)
    }

    fn valid(&self) -> bool {
        let valid = self.get_current().valid().unwrap_or(false);
        valid
    }
}
