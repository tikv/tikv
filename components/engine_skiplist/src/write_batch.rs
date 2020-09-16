// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crate::engine::SkiplistEngine;
use crate::metrics::*;
use engine_traits::{
    CfName, Error, KvEngine, Mutable, MvccPropertiesExt, Result, SyncMutable, WriteBatch,
    WriteBatchExt, WriteOptions, CF_DEFAULT,
};
use std::sync::atomic;
use std::time::Duration;
use tikv_util::time::Instant;

impl WriteBatchExt for SkiplistEngine {
    type WriteBatch = SkiplistWriteBatch;
    type WriteBatchVec = SkiplistWriteBatch;

    const WRITE_BATCH_MAX_KEYS: usize = 256;

    fn write_opt(&self, wb: &Self::WriteBatch, opts: &WriteOptions) -> Result<()> {
        let _timer = SKIPLIST_ACTION_HISTOGRAM_VEC
            .with_label_values(&[self.name, "write"])
            .start_coarse_timer();
        for e in wb.actions.clone() {
            match e {
                WriteAction::Put((cf, key, value)) => {
                    self.put_cf(&cf, &key, &value);
                }
                WriteAction::Delete((cf, key)) => {
                    self.delete_cf(&cf, &key);
                }
                WriteAction::DeleteRange((cf, begin_key, end_key)) => {
                    self.delete_range_cf(&cf, &begin_key, &end_key);
                }
            }
        }
        SKIPLIST_WRITE_SIZE_HISTOGRAM_VEC
            .with_label_values(&[self.name])
            .observe(wb.data_size as f64);
        SKIPLIST_WRITE_KEYS_HISTOGRAM_VEC
            .with_label_values(&[self.name])
            .observe(wb.written_keys as f64);
        Ok(())
    }

    fn support_write_batch_vec(&self) -> bool {
        false
    }

    fn write_vec_opt(&self, wb: &Self::WriteBatchVec, opts: &WriteOptions) -> Result<()> {
        self.write_opt(wb, opts)
    }

    fn write_batch(&self) -> Self::WriteBatch {
        SkiplistWriteBatch::with_capacity(self, 0)
    }
    fn write_batch_with_cap(&self, cap: usize) -> Self::WriteBatch {
        SkiplistWriteBatch::with_capacity(self, cap)
    }
}

#[derive(Clone)]
enum WriteAction {
    Put((String, Vec<u8>, Vec<u8>)),
    Delete((String, Vec<u8>)),
    DeleteRange((String, Vec<u8>, Vec<u8>)),
}

pub struct SkiplistWriteBatch {
    data_size: usize,
    written_keys: usize,
    actions: Vec<WriteAction>,
    safe_points: Vec<usize>,
    engine: SkiplistEngine,
}

impl SkiplistWriteBatch {
    pub fn new(e: &SkiplistEngine) -> SkiplistWriteBatch {
        SkiplistWriteBatch::with_capacity(e, 0)
    }
}

impl WriteBatch<SkiplistEngine> for SkiplistWriteBatch {
    fn with_capacity(e: &SkiplistEngine, cap: usize) -> SkiplistWriteBatch {
        Self {
            data_size: 0,
            written_keys: 0,
            actions: Vec::with_capacity(cap),
            safe_points: Vec::default(),
            engine: e.clone(),
        }
    }
    fn write_to_engine(&self, e: &SkiplistEngine, opts: &WriteOptions) -> Result<()> {
        e.write_opt(self, opts)
    }
}

impl Mutable for SkiplistWriteBatch {
    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        self.data_size += key.len() + value.len();
        self.written_keys += 1;
        self.actions.push(WriteAction::Put((
            CF_DEFAULT.to_owned(),
            key.to_vec(),
            value.to_vec(),
        )));
        Ok(())
    }
    fn put_cf(&mut self, cf: &str, key: &[u8], value: &[u8]) -> Result<()> {
        self.data_size += key.len() + value.len();
        self.written_keys += 1;
        self.actions.push(WriteAction::Put((
            cf.to_owned(),
            key.to_vec(),
            value.to_vec(),
        )));
        Ok(())
    }

    fn delete(&mut self, key: &[u8]) -> Result<()> {
        self.actions
            .push(WriteAction::Delete((CF_DEFAULT.to_owned(), key.to_vec())));
        Ok(())
    }
    fn delete_cf(&mut self, cf: &str, key: &[u8]) -> Result<()> {
        self.actions
            .push(WriteAction::Delete((cf.to_owned(), key.to_vec())));
        Ok(())
    }
    fn delete_range_cf(&mut self, cf: &str, begin_key: &[u8], end_key: &[u8]) -> Result<()> {
        self.actions.push(WriteAction::DeleteRange((
            cf.to_owned(),
            begin_key.to_vec(),
            end_key.to_vec(),
        )));
        Ok(())
    }
    fn data_size(&self) -> usize {
        self.data_size
    }
    fn count(&self) -> usize {
        self.actions.len()
    }
    fn is_empty(&self) -> bool {
        self.actions.is_empty()
    }
    fn should_write_to_engine(&self) -> bool {
        self.count() > SkiplistEngine::WRITE_BATCH_MAX_KEYS
    }
    fn clear(&mut self) {
        self.actions.clear();
        self.data_size = 0;
        self.written_keys = 0;
        self.safe_points.clear();
    }
    fn set_save_point(&mut self) {
        self.safe_points.push(self.actions.len());
    }
    fn pop_save_point(&mut self) -> Result<()> {
        self.safe_points.pop();
        Ok(())
    }
    fn rollback_to_save_point(&mut self) -> Result<()> {
        let p = self
            .safe_points
            .pop()
            .ok_or_else(|| Error::Engine("no save point".to_owned()))?;
        self.actions.truncate(p);
        Ok(())
    }
}
