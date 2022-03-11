// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use crate::*;
use crate::{
    meta::is_move_down,
    table::sstable::{self, SSTable},
};
use kvenginepb as pb;
use std::collections::HashMap;
use std::iter::Iterator as StdIterator;

impl Engine {
    pub fn apply_change_set(&self, cs: pb::ChangeSet) -> Result<()> {
        info!("{}:{} apply change set {:?}", cs.shard_id, cs.shard_ver, &cs);
        self.pre_load_files(&cs)?;
        let shard = self.get_shard(cs.shard_id);
        if shard.is_none() {
            return Err(Error::ShardNotFound);
        }
        let shard = shard.unwrap();
        if shard.ver != cs.shard_ver {
            return Err(Error::ShardNotMatch);
        }
        let seq = load_u64(&shard.meta_seq);
        if seq >= cs.sequence {
            warn!(
                "{}:{} skip duplicated shard seq:{}, change seq:{}",
                shard.id, shard.ver, seq, cs.sequence
            );
            return Ok(());
        } else {
            store_u64(&shard.meta_seq, cs.sequence);
        }
        if cs.has_flush() {
            self.apply_flush(&shard, cs)?
        } else if cs.has_compaction() {
            let result = self.apply_compaction(&shard, cs);
            store_bool(&shard.compacting, false);
            if result.is_err() {
                return result;
            }
        } else if cs.has_initial_flush() {
            self.apply_initial_flush(&shard, cs)?;
        }
        shard.refresh_estimated_size();
        shard.refresh_compaction_priority();
        Ok(())
    }

    pub fn apply_flush(&self, shard: &Shard, cs: pb::ChangeSet) -> Result<()> {
        let flush = cs.get_flush();
        if flush.has_l0_create() {
            let opts = dfs::Options::new(shard.id, shard.ver);
            let file = self.fs.open(flush.get_l0_create().id, opts)?;
            let l0_tbl = sstable::L0Table::new(file, Some(self.cache.clone()))?;
            shard.atomic_add_l0_table(l0_tbl);
            shard.atomic_remove_mem_table();
        }
        if flush.is_initial_flush {
            store_bool(&shard.initial_flushed, true);
        }
        Ok(())
    }

    pub fn apply_initial_flush(&self, shard: &Shard, cs: pb::ChangeSet) -> Result<()> {
        let initial_flush = cs.get_initial_flush();
        let (l0s, mut scfs) = self.create_snapshot_tables(shard.id, shard.ver, initial_flush)?;
        // sync the files from bottom to top.
        for (cf, scf) in scfs.drain(..).enumerate() {
            shard.set_cf(cf, scf);
        }
        let mut guard = shard.l0_tbls.write().unwrap();
        *guard = l0s;
        drop(guard);
        while let Some(last_read_only) = shard.get_last_read_only_mem_table() {
            if last_read_only.get_version() <= initial_flush.base_version + initial_flush.data_sequence {
                shard.atomic_remove_mem_table();
            } else {
                break;
            }
        }
        store_bool(&shard.initial_flushed, true);
        Ok(())
    }

    fn apply_compaction(&self, shard: &Shard, mut cs: pb::ChangeSet) -> Result<()> {
        let comp = cs.take_compaction();
        let mut del_files = HashMap::new();
        if comp.conflicted {
            if is_move_down(&comp) {
                return Ok(());
            }
            for create in comp.get_table_creates() {
                let cover = shard.cover_full_table(&create.smallest, &create.biggest);
                del_files.insert(create.id, cover);
            }
            self.remove_dfs_files(shard, del_files);
            return Ok(());
        }
        if comp.level == 0 {
            let l0_tbls = shard.get_l0_tbls();
            for tbl in l0_tbls.tbls.as_ref() {
                let id = tbl.id();
                if comp.top_deletes.contains(&id) {
                    del_files.insert(id, shard.cover_full_table(tbl.smallest(), tbl.biggest()));
                }
            }
            for cf in 0..NUM_CFS {
                self.compaction_update_level_handler(
                    shard,
                    cf,
                    1,
                    comp.get_table_creates(),
                    comp.get_bottom_deletes(),
                    &mut del_files,
                )?;
            }
            shard.atomic_remove_l0_tables(comp.top_deletes.len());
        } else {
            let cf = comp.cf as usize;
            self.compaction_update_level_handler(
                shard,
                cf,
                comp.level + 1,
                comp.get_table_creates(),
                comp.get_bottom_deletes(),
                &mut del_files,
            )?;
            self.compaction_update_level_handler(
                shard,
                cf,
                comp.level,
                &[],
                comp.get_top_deletes(),
                &mut del_files,
            )?;
            // For move down operation, the TableCreates may contains TopDeletes, we don't want to delete them.
            for create in comp.get_table_creates() {
                del_files.remove(&create.id);
            }
        }
        self.remove_dfs_files(shard, del_files);
        Ok(())
    }

    fn remove_dfs_files(&self, shard: &Shard, del_files: HashMap<u64, bool>) {
        let fs = self.fs.clone();
        let opts = dfs::Options::new(shard.id, shard.ver);
        let runtime = fs.get_runtime();
        for (id, cover) in del_files {
            if cover {
                let fs_n = fs.clone();
                runtime.spawn(async move {
                    fs_n.remove(id, opts).await
                });
            }
        }
    }

    fn compaction_update_level_handler(
        &self,
        shard: &Shard,
        cf: usize,
        level: u32,
        creates: &[pb::TableCreate],
        del_ids: &[u64],
        del_files: &mut HashMap<u64, bool>,
    ) -> Result<()> {
        let opts = dfs::Options::new(shard.id, shard.ver);
        let old_scf = shard.get_cf(cf);
        let mut new_levels = vec![];
        for l in old_scf.levels.as_slice() {
            new_levels.push(l.clone());
        }
        let level_idx = level as usize - 1;
        let mut new_level = &mut new_levels[level_idx];
        let old_level = &old_scf.levels.as_slice()[level_idx];
        new_level.total_size = 0;
        new_level.tables.truncate(0);
        let mut need_update = false;
        for create in creates {
            if create.cf as usize != cf {
                continue;
            }
            let file = self.fs.open(create.id, opts)?;
            let tbl = sstable::SSTable::new(file, Some(self.cache.clone()))?;
            new_level.total_size += tbl.size();
            new_level.tables.push(tbl);
            need_update = true;
        }

        for old_tbl in old_level.tables.iter() {
            let id = old_tbl.id();
            if del_ids.contains(&id) {
                del_files.insert(id, shard.cover_full_table(old_tbl.smallest(), old_tbl.biggest()));
                need_update = true;
            } else {
                new_level.total_size += old_tbl.size();
                new_level.tables.push(old_tbl.clone());
            }
        }
        if !need_update {
            return Ok(());
        }
        new_level
            .tables
            .sort_by(|a, b| a.smallest().cmp(b.smallest()));
        if !check_tables_order(&new_level.tables) {
            panic!(
                "invalid table order, shard:[{}:{}][{:?},{:?}][{:?}], cf:{}, level:{}, creates:{:?}, deletes:{:?}, tables:{:?}",
                shard.id,
                shard.ver,
                &shard.start,
                &shard.end,
                shard.get_all_files(),
                cf,
                level,
                creates,
                del_ids,
                new_level
                    .tables
                    .iter()
                    .map(|t| t.id())
                    .collect::<Vec<u64>>(),
            );
        };
        shard.set_cf(cf, ShardCF::new_with_levels(new_levels));
        Ok(())
    }

    pub fn pre_load_files(&self, cs: &pb::ChangeSet) -> Result<()> {
        let mut ids = vec![];
        if cs.has_flush() {
            let flush = cs.get_flush();
            if flush.has_l0_create() {
                ids.push(flush.get_l0_create().id);
            }
        }
        if cs.has_compaction() {
            let comp = cs.get_compaction();
            if !is_move_down(comp) {
                for tbl in &comp.table_creates {
                    ids.push(tbl.id);
                }
            }
        }
        if cs.has_snapshot() {
            self.collect_snap_ids(cs.get_snapshot(), &mut ids);
        }
        if cs.has_initial_flush() {
            self.collect_snap_ids(cs.get_initial_flush(), &mut ids);
        }
        pre_load_files_by_ids(self.fs.clone(), cs.shard_id, cs.shard_ver, ids)
    }

    pub fn collect_snap_ids(&self, snap: &pb::Snapshot, ids: &mut Vec<u64>) {
        for l0 in snap.get_l0_creates() {
            ids.push(l0.id);
        }
        for ln in snap.get_table_creates() {
            ids.push(ln.id);
        }
    }
}

pub(crate) fn check_tables_order(tables: &Vec<SSTable>) -> bool {
    if tables.len() <= 1 {
        return true;
    }
    for i in 0..(tables.len() - 1) {
        let ti = &tables[i];
        let tj = &tables[i + 1];
        if ti.smallest() > ti.biggest()
            || ti.smallest() >= tj.smallest()
            || ti.biggest() >= tj.biggest()
        {
            return false;
        }
    }
    true
}
