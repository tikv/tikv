// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::{atomic::Ordering::Release, Arc};

use bytes::Buf;
use dashmap::mapref::entry::Entry;
use kvenginepb as pb;
use slog_global::info;

use crate::*;

impl Engine {
    pub fn split(&self, mut cs: pb::ChangeSet, initial_seq: u64) -> Result<()> {
        let split = cs.take_split();
        let sequence = cs.get_sequence();

        let old_shard = self.get_shard_with_ver(cs.shard_id, cs.shard_ver)?;
        old_shard.write_sequence.store(sequence, Release);
        let version = old_shard.load_mem_table_version();
        // Switch the old shard mem-table, so the first mem-table is always empty.
        // ignore the read-only mem-table to be flushed. let the new shard handle it.
        self.switch_mem_table(&old_shard, version);
        self.flush_tx.send(FlushMsg::Clear(cs.shard_id)).unwrap();
        self.compact_tx
            .send(CompactMsg::Clear(IDVer::new(cs.shard_id, cs.shard_ver)))
            .unwrap();

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
                self.get_engine_id(),
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
            } else {
                new_shard.base_version = old_shard.base_version + sequence;
                store_u64(&new_shard.meta_seq, initial_seq);
                store_u64(&new_shard.write_sequence, initial_seq);
            }
            new_shards.push(Arc::new(new_shard));
        }
        let old_data = old_shard.get_data();
        for new_shard in &new_shards {
            let new_mem_tbls = new_shard.split_mem_tables(&old_data.mem_tbls);
            let mut new_l0s = vec![];
            for l0 in &old_data.l0_tbls {
                if new_shard.overlap_table(l0.smallest(), l0.biggest()) {
                    new_l0s.push(l0.clone());
                }
            }
            let mut new_cfs = [ShardCF::new(0), ShardCF::new(1), ShardCF::new(2)];
            for cf in 0..NUM_CFS {
                let old_scf = old_data.get_cf(cf);
                for lh in &old_scf.levels {
                    let mut new_level_tbls = vec![];
                    for tbl in lh.tables.as_slice() {
                        if new_shard.overlap_table(tbl.smallest(), tbl.biggest()) {
                            new_level_tbls.push(tbl.clone());
                        }
                    }
                    let new_level = LevelHandler::new(lh.level, new_level_tbls);
                    new_cfs[cf].set_level(new_level);
                }
            }
            let new_del_prefixes = old_data
                .del_prefixes
                .build_split(&new_shard.start, &new_shard.end);
            let new_data = ShardData::new(
                new_shard.start.clone(),
                new_shard.end.clone(),
                new_del_prefixes,
                new_mem_tbls,
                new_l0s,
                new_cfs,
            );
            new_shard.set_data(new_data);
        }
        for shard in new_shards.drain(..) {
            self.refresh_shard_states(&shard);
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
                "new shard {}, start {:x}, end {:x} mem-version {}， all files {:?}",
                shard.tag(),
                shard.start,
                shard.end,
                version,
                all_files
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
    split_keys.len()
}
