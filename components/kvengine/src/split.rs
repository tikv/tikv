// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use crate::*;
use byteorder::{ByteOrder, LittleEndian};
use bytes::Buf;
use dashmap::mapref::entry::Entry;
use kvenginepb as pb;
use slog_global::{info, warn};
use std::iter::Iterator as StdIterator;
use std::sync::atomic::Ordering::Release;
use std::sync::Arc;

impl Engine {
    pub fn get_shard_with_ver(&self, shard_id: u64, shard_ver: u64) -> Result<Arc<Shard>> {
        let shard = self.get_shard(shard_id).ok_or(Error::ShardNotFound)?;
        if shard.ver != shard_ver {
            warn!(
                "shard {} version not match, current {}, request {}",
                shard_id, shard.ver, shard_ver
            );
            return Err(Error::ShardNotMatch);
        }
        Ok(shard)
    }

    pub fn split(&self, mut cs: pb::ChangeSet, initial_seq: u64) -> Result<()> {
        let split = cs.take_split();
        let sequence = cs.get_sequence();

        let old_shard = self.get_shard_with_ver(cs.shard_id, cs.shard_ver)?;
        old_shard.write_sequence.store(sequence, Release);
        let version = old_shard.load_mem_table_version();
        // Switch the old shard mem-table, so the first mem-table is always empty.
        // ignore the read-only mem-table to be flushed. let the new shard handle it.
        let _ = self.switch_mem_table(&old_shard, version);

        let mut new_shards = vec![];
        let new_shard_props = split.get_new_shards();
        let new_ver = old_shard.ver + new_shard_props.len() as u64 - 1;
        for i in 0..=split.keys.len() {
            let (start_key, end_key) = get_splitting_start_end(
                old_shard.start.chunk(),
                old_shard.end.chunk(),
                split.get_keys(),
                i,
            );
            let mut new_shard = Shard::new(
                &new_shard_props[i],
                new_ver,
                start_key,
                end_key,
                self.opts.clone(),
            );
            new_shard.parent_id = old_shard.id;
            {
                let mut guard = new_shard.parent_snap.write().unwrap();
                *guard = Some(cs.get_snapshot().clone());
            }
            if new_shard.id == old_shard.id {
                new_shard.set_active(old_shard.is_active());
                new_shard.base_version = old_shard.base_version;
                store_u64(&new_shard.meta_seq, sequence);
                store_u64(&new_shard.write_sequence, sequence);
                // derived shard need larger mem-table size.
                let mem_size = Shard::bounded_mem_size(self.opts.base_size / 2);
                let size_bin = &mut [0u8; 8][..];
                LittleEndian::write_u64(size_bin, mem_size as u64);
                new_shard.set_property(MEM_TABLE_SIZE_KEY, size_bin);
                store_u64(&new_shard.max_mem_table_size, mem_size);
            } else {
                new_shard.base_version = old_shard.base_version + sequence;
                store_u64(&new_shard.meta_seq, initial_seq);
                store_u64(&new_shard.write_sequence, initial_seq);
            }
            new_shards.push(Arc::new(new_shard));
        }
        let mem_tbls = old_shard.get_mem_tbls();
        for new_shard in &new_shards {
            new_shard.set_split_mem_tables(mem_tbls.tbls.as_ref());
        }
        let l0s = old_shard.get_l0_tbls();
        for l0 in l0s.tbls.iter().rev() {
            for new_shard in &new_shards {
                if new_shard.overlap_table(l0.smallest(), l0.biggest()) {
                    new_shard.atomic_add_l0_table(l0.clone());
                }
            }
        }
        for cf in 0..NUM_CFS {
            let old_scf = old_shard.get_cf(cf);
            let mut new_scfs = Vec::new();
            new_scfs.resize_with(new_shards.len(), || ShardCFBuilder::new(cf));
            for lh in old_scf.levels.as_ref() {
                for tbl in lh.tables.iter() {
                    for (idx, new_shard) in new_shards.iter().enumerate() {
                        if new_shard.overlap_table(tbl.smallest(), tbl.biggest()) {
                            let new_scf = &mut new_scfs[idx];
                            new_scf.add_table(tbl.clone(), lh.level);
                        }
                    }
                }
            }
            for i in 0..new_shards.len() {
                new_shards[i].set_cf(cf, new_scfs[i].build());
            }
        }
        for shard in new_shards.drain(..) {
            shard.refresh_estimated_size();
            shard.refresh_compaction_priority();
            let id = shard.id;
            if id != old_shard.id {
                match self.shards.entry(id) {
                    Entry::Occupied(_) => {
                        // The shard already exists, it must be created by ingest, and it maybe
                        // newer than this one, we avoid insert it.
                        continue;
                    }
                    Entry::Vacant(entry) => {
                        entry.insert(shard.clone());
                    }
                }
            } else {
                self.shards.insert(id, shard.clone());
            }
            let all_files = shard.get_all_files();
            info!(
                "new shard {}:{}, start {:x}, end {:x} mem-version {}， all files {:?}",
                shard.id, shard.ver, shard.start, shard.end, version, all_files
            );
        }
        Ok(())
    }
}

pub fn get_split_shard_index(split_keys: &[Vec<u8>], key: &[u8]) -> usize {
    for i in 0..split_keys.len() {
        if key < split_keys[i].as_slice() {
            return i;
        }
    }
    return split_keys.len();
}
