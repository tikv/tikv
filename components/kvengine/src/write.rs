// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::{collections::HashMap, intrinsics::transmute, sync::atomic::Ordering, time::Instant};

use byteorder::{ByteOrder, LittleEndian};
use bytes::{Buf, BytesMut};
use slog_global::info;

use crate::table::{self, memtable};
use crate::*;
use crossbeam_epoch as epoch;

pub struct WriteBatch {
    shard_id: u64,
    cf_conf: [CFConfig; NUM_CFS],
    cf_batches: [memtable::WriteBatch; NUM_CFS],
    properties: HashMap<String, BytesMut>,
    sequence: u64,
}

impl WriteBatch {
    pub fn new(shard_id: u64, cf_conf: [CFConfig; NUM_CFS]) -> Self {
        let cf_batches = [
            memtable::WriteBatch::new(),
            memtable::WriteBatch::new(),
            memtable::WriteBatch::new(),
        ];
        Self {
            shard_id,
            cf_conf,
            cf_batches,
            properties: HashMap::new(),
            sequence: 1,
        }
    }

    pub fn put(
        &mut self,
        cf: usize,
        key: &[u8],
        val: &[u8],
        meta: u8,
        user_meta: &[u8],
        version: u64,
    ) {
        self.validate_version(cf, version);
        self.get_cf_mut(cf).put(key, meta, user_meta, version, val);
    }

    fn validate_version(&self, cf: usize, version: u64) {
        if self.cf_conf[cf].managed {
            if version == 0 {
                panic!("version is zero for managed CF")
            }
        } else {
            if version != 0 {
                panic!("version is not zero for not managed CF")
            }
        }
    }

    pub fn delete(&mut self, cf: usize, key: &[u8], version: u64) {
        self.validate_version(cf, version);
        self.get_cf_mut(cf)
            .put(key, table::BIT_DELETE, &[], version, &[]);
    }

    pub fn set_property(&mut self, key: &str, val: &[u8]) {
        self.properties.insert(key.to_string(), BytesMut::from(val));
    }

    pub fn set_sequence(&mut self, seq: u64) {
        self.sequence = seq;
    }

    pub fn estimated_size(&self) -> usize {
        let mut size = 0;
        for wb in &self.cf_batches {
            size += wb.estimated_size();
        }
        size
    }

    pub fn num_entries(&self) -> usize {
        let mut num = 0;
        for wb in &self.cf_batches {
            num += wb.len();
        }
        num
    }

    pub fn reset(&mut self) {
        for wb in &mut self.cf_batches {
            wb.reset();
        }
        self.sequence = 0;
        self.properties.clear();
    }

    pub fn get_cf_mut(&mut self, cf: usize) -> &mut memtable::WriteBatch {
        &mut self.cf_batches[cf]
    }

    pub fn cf_len(&self, cf: usize) -> usize {
        self.cf_batches[cf].len()
    }
}

impl Engine {
    pub(crate) fn switch_mem_table(&self, shard: &Shard, version: u64) -> memtable::CFTable {
        let g = &epoch::pin();
        let mut writable = shard.get_writable_mem_table(g).clone();
        if writable.is_empty() {
            writable = memtable::CFTable::new();
        } else {
            let new_tbl = memtable::CFTable::new();
            shard.atomic_add_mem_table(g, new_tbl);
        }
        writable.set_version(version);
        info!(
            "shard {}:{} set mem-table version {}, empty {}, size {}",
            shard.id,
            shard.ver,
            version,
            writable.is_empty(),
            writable.size()
        );
        writable
    }

    pub fn write(&self, wb: &mut WriteBatch) {
        let g = &epoch::pin();
        let shard = self.get_shard(wb.shard_id).unwrap();
        let version = shard.base_version + wb.sequence;
        self.update_write_batch_version(wb, version);
        if shard.is_splitting() {
            if shard.ingest_pre_split_seq == 0 || wb.sequence > shard.ingest_pre_split_seq {
                let split_ctx = shard.get_split_ctx(g);
                self.write_splitting(wb, &shard, split_ctx);
                store_u64(&shard.write_sequence, wb.sequence);
                return;
            }
            // Recover the shard to the pre-split stage when this shard is ingested.
        }
        let mut mem_tbl = shard.get_writable_mem_table(g);
        if mem_tbl.size() + wb.estimated_size() > shard.get_max_mem_table_size() as usize {
            let old_mem_tbl = self.switch_mem_table(&shard, version);
            self.schedule_flush_task(&shard, old_mem_tbl);
            mem_tbl = shard.get_writable_mem_table(g);
        }

        for cf in 0..NUM_CFS {
            mem_tbl.get_cf(cf).put_batch(wb.get_cf_mut(cf));
        }
        for (k, v) in &wb.properties {
            shard.properties.set(k.as_str(), v.chunk());
            if k == MEM_TABLE_SIZE_KEY {
                let max_mem_tbl_size = LittleEndian::read_u64(v.chunk());
                shard.set_max_mem_table_size(max_mem_tbl_size);
                info!(
                    "shard {}:{}, mem size changed to {}",
                    shard.id, shard.ver, max_mem_tbl_size
                );
            }
        }
        store_u64(&shard.write_sequence, wb.sequence);
    }

    fn update_write_batch_version(&self, wb: &mut WriteBatch, version: u64) {
        for cf in 0..NUM_CFS {
            if !self.opts.cfs[cf].managed {
                wb.get_cf_mut(cf).iterate(|e, _| {
                    e.version = version;
                });
            };
        }
    }

    fn write_splitting<'a>(
        &self,
        wb: &mut WriteBatch,
        shard: &'a Shard,
        split_ctx: &'a SplitContext,
    ) {
        for cf in 0..NUM_CFS {
            let cf_wb = wb.get_cf_mut(cf);
            cf_wb.iterate(|entry, buf| {
                split_ctx.write(cf, entry, buf);
            });
        }
        for (key, val) in &wb.properties {
            shard.properties.set(key.as_str(), val.chunk());
        }
    }

    pub(crate) fn schedule_flush_task(&self, shard: &Shard, mut mem_tbl: memtable::CFTable) {
        let last_switch_val = shard.last_switch_time.load(Ordering::Acquire);
        let last_switch_instant: Instant = unsafe { transmute(last_switch_val) };
        let now_val: u64 = unsafe { transmute(Instant::now()) };
        shard.last_switch_time.store(now_val, Ordering::Release);

        let props = shard.properties.to_pb(shard.id);
        mem_tbl.set_properties(props);
        let mut stage = shard.get_split_stage();
        if stage == kvenginepb::SplitStage::PreSplit {
            stage = kvenginepb::SplitStage::PreSplitFlushDone;
        }
        if shard.is_active() {
            let mut next_mem_tbl_size = 0;
            if !mem_tbl.is_empty()
                && self.opts.dynamic_mem_table_size
                && load_bool(&shard.initial_flushed)
            {
                next_mem_tbl_size =
                    shard.next_mem_table_size(mem_tbl.size() as u64, last_switch_instant);
            }
            let task = FlushTask {
                shard_id: shard.id,
                shard_ver: shard.ver,
                split_stage: stage,
                next_mem_tbl_size,
                mem_tbl,
            };
            self.flush_tx.send(task).unwrap();
        }
    }
}
