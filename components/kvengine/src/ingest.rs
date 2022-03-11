// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use crate::table::sstable::{L0Table, SSTable};
use crate::*;
use dashmap::mapref::entry::Entry;
use kvenginepb::Snapshot;
use std::iter::Iterator;
use std::sync::Arc;

pub struct IngestTree {
    pub change_set: kvenginepb::ChangeSet,
    pub active: bool,
}

impl Engine {
    pub fn ingest(&self, tree: IngestTree) -> Result<()> {
        let (l0s, mut scfs) = self.create_snapshot_tables(
            tree.change_set.shard_id,
            tree.change_set.shard_ver,
            tree.change_set.get_snapshot(),
        )?;
        let shard = Shard::new_for_ingest(tree.change_set, self.opts.clone());
        shard.set_active(tree.active);
        *shard.l0_tbls.write().unwrap() = l0s;
        for (cf, scf) in scfs.drain(..).enumerate() {
            shard.set_cf(cf, scf);
        }
        shard.refresh_estimated_size();
        match self.shards.entry(shard.id) {
            Entry::Occupied(entry) => {
                let old = entry.get();
                let old_total_seq = old.get_write_sequence() + old.get_meta_sequence();
                let new_total_seq = shard.get_write_sequence() + shard.get_meta_sequence();
                if new_total_seq > old_total_seq {
                    entry.replace_entry(Arc::new(shard));
                } else {
                    info!("ingest found shard already exists with higher sequence, skip insert");
                }
            }
            Entry::Vacant(entry) => {
                entry.insert(Arc::new(shard));
            }
        }
        Ok(())
    }

    pub(crate) fn create_snapshot_tables(
        &self,
        id: u64,
        ver: u64,
        snap: &Snapshot,
    ) -> Result<(L0Tables, Vec<ShardCF>)> {
        let mut l0_tbls = vec![];
        let fs_opts = dfs::Options::new(id, ver);
        for l0_create in snap.get_l0_creates() {
            let file = self.fs.open(l0_create.id, fs_opts)?;
            let l0_tbl = L0Table::new(file, Some(self.cache.clone()))?;
            l0_tbls.push(l0_tbl);
        }
        l0_tbls.sort_by(|a, b| b.version().cmp(&a.version()));
        let mut scf_builders = vec![];
        for cf in 0..NUM_CFS {
            let scf = ShardCFBuilder::new(cf);
            scf_builders.push(scf);
        }
        for table_create in snap.get_table_creates() {
            let file = self.fs.open(table_create.id, fs_opts)?;
            let tbl = SSTable::new(file, Some(self.cache.clone()))?;
            let scf = &mut scf_builders.as_mut_slice()[table_create.cf as usize];
            scf.add_table(tbl, table_create.level as usize);
        }
        let mut scfs = vec![];
        for cf in 0..NUM_CFS {
            let scf = &mut scf_builders.as_mut_slice()[cf];
            scfs.push(scf.build());
        }
        Ok((L0Tables::new(l0_tbls), scfs))
    }
}
