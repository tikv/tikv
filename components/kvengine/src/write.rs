// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::{collections::HashMap, time::Instant};

use byteorder::{ByteOrder, LittleEndian};
use bytes::{Buf, BytesMut};
use slog_global::info;

use crate::{
    table::{self, memtable},
    *,
};

pub struct WriteBatch {
    shard_id: u64,
    cf_batches: [memtable::WriteBatch; NUM_CFS],
    properties: HashMap<String, BytesMut>,
    sequence: u64,
}

impl WriteBatch {
    pub fn new(shard_id: u64) -> Self {
        let cf_batches = [
            memtable::WriteBatch::new(),
            memtable::WriteBatch::new(),
            memtable::WriteBatch::new(),
        ];
        Self {
            shard_id,
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
        if CF_MANAGED[cf] {
            if version == 0 {
                panic!("version is zero for managed CF")
            }
        } else if version != 0 {
            panic!("version is not zero for not managed CF")
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
    pub(crate) fn switch_mem_table(&self, shard: &Shard, version: u64) {
        let data = shard.get_data();
        let mem_table = data.get_writable_mem_table();
        if mem_table.is_empty() {
            return;
        }
        mem_table.set_version(version);
        let new_tbl = memtable::CFTable::new();
        let mut new_mem_tbls = Vec::with_capacity(data.mem_tbls.len() + 1);
        new_mem_tbls.push(new_tbl);
        new_mem_tbls.extend_from_slice(data.mem_tbls.as_slice());
        let new_data = ShardData::new(
            shard.start.clone(),
            shard.end.clone(),
            new_mem_tbls,
            data.l0_tbls.clone(),
            data.cfs.clone(),
        );
        shard.set_data(new_data);
        info!(
            "shard {}:{} set mem-table version {}, size {}",
            shard.id,
            shard.ver,
            version,
            mem_table.size()
        );
        let mut guard = shard.last_switch_time.write().unwrap();
        let last_switch_instant = *guard;
        *guard = Instant::now();
        let props = shard.properties.to_pb(shard.id);
        mem_table.set_properties(props);
        if shard.is_active() && shard.get_initial_flushed() && self.opts.dynamic_mem_table_size {
            let next_mem_tbl_size =
                shard.next_mem_table_size(mem_table.size() as u64, last_switch_instant);
            let mut change_size = new_change_set(shard.id, shard.ver);
            change_size.set_next_mem_table_size(next_mem_tbl_size);
            self.meta_change_listener.on_change_set(change_size);
        }
    }

    pub fn write(&self, wb: &mut WriteBatch) {
        let shard = self.get_shard(wb.shard_id).unwrap();
        let snap = shard.new_snap_access();
        let version = shard.base_version + wb.sequence;
        self.update_write_batch_version(wb, version);
        let data = shard.get_data();
        let mem_tbl = data.get_writable_mem_table();
        for cf in 0..NUM_CFS {
            mem_tbl.get_cf(cf).put_batch(wb.get_cf_mut(cf), &snap, cf);
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
        if mem_tbl.size() > shard.get_max_mem_table_size() as usize {
            self.switch_mem_table(&shard, version);
            if shard.is_active() && shard.get_initial_flushed() {
                self.trigger_flush(&shard);
            }
        }
    }

    fn update_write_batch_version(&self, wb: &mut WriteBatch, version: u64) {
        for cf in 0..NUM_CFS {
            if !CF_MANAGED[cf] {
                wb.get_cf_mut(cf).iterate(|e, _| {
                    e.version = version;
                });
            };
        }
    }
}
