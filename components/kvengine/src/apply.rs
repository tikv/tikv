use crate::*;
use crate::{
    meta::is_move_down,
    table::{
        search,
        sstable::{self, SSTable},
    },
};
use crossbeam_epoch as epoch;
use kvenginepb as pb;
use slog_global::info;
use std::collections::HashSet;
use std::fs;
use std::sync::{mpsc, Arc};

impl Engine {
    pub fn apply_change_set(&self, cs: pb::ChangeSet) -> Result<()> {
        let g = &crossbeam_epoch::pin();
        let shard = self.get_shard(cs.shard_id, g);
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
            self.apply_flush(shard, g, cs)?
        } else if cs.has_compaction() {
            let resut = self.apply_compaction(shard, g, cs);
            store_bool(&shard.compacting, false);
            if resut.is_err() {
                return resut;
            }
        } else if cs.has_split_files() {
            self.apply_split_files(shard, g, cs)?
        }
        shard.refresh_estimated_size();
        Ok(())
    }

    pub fn apply_flush<'a>(
        &self,
        shard: &'a Shard,
        g: &'a epoch::Guard,
        cs: pb::ChangeSet,
    ) -> Result<()> {
        let flush = cs.get_flush();
        if flush.has_l0_create() {
            let l0 = flush.get_l0_create();
            let (tx, rx) = mpsc::sync_channel(1);
            let afs = self.fs.clone();
            let opts = dfs::Options::new(shard.id, shard.ver);
            let id = l0.id;
            self.fs.get_future_pool().spawn_ok(async move {
                tx.send(afs.open(id, opts).await).unwrap();
            });
            let file = rx.recv().unwrap()?;
            let l0_tbl = sstable::L0Table::new(file, self.cache.clone())?;
            shard.atomic_add_l0_table(g, l0_tbl);
            shard.atomic_remove_mem_table(g);
        }
        shard.set_split_stage(cs.stage);
        store_bool(&shard.initial_flushed, true);
        Ok(())
    }

    fn apply_compaction<'a>(
        &self,
        shard: &'a Shard,
        g: &'a epoch::Guard,
        mut cs: pb::ChangeSet,
    ) -> Result<()> {
        let comp = cs.take_compaction();
        let mut del_files = HashSet::new();
        if comp.conflicted {
            if is_move_down(&comp) {
                return Ok(());
            }
            for create in comp.get_table_creates() {
                del_files.insert(create.id);
            }
            self.remove_dfs_files(shard, g, del_files);
            return Ok(());
        }
        if comp.level == 0 {
            let l0_tbls = shard.get_l0_tbls(g);
            for tbl in &l0_tbls.tbls {
                let id = tbl.id();
                if comp.top_deletes.contains(&id) {
                    del_files.insert(id);
                }
            }
            for cf in 0..NUM_CFS {
                self.compaction_update_level_handler(
                    shard,
                    g,
                    cf,
                    1,
                    comp.get_table_creates(),
                    comp.get_bottom_deletes(),
                    &mut del_files,
                )?;
            }
            shard.atomic_remove_l0_tables(g, comp.top_deletes.len());
        } else {
            info!("apply Ln compaction");
            let cf = comp.cf as usize;
            self.compaction_update_level_handler(
                shard,
                g,
                cf,
                comp.level + 1,
                comp.get_table_creates(),
                comp.get_bottom_deletes(),
                &mut del_files,
            )?;
            self.compaction_update_level_handler(
                shard,
                g,
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
        self.remove_dfs_files(shard, g, del_files);
        Ok(())
    }

    fn remove_dfs_files<'a>(&self, shard: &'a Shard, g: &'a epoch::Guard, del_files: HashSet<u64>) {
        let fs = self.fs.clone();
        let opts = dfs::Options::new(shard.id, shard.ver);
        g.defer(move || {
            for id in del_files {
                fs.remove(id, opts)
            }
        });
    }

    fn compaction_update_level_handler<'a>(
        &self,
        shard: &'a Shard,
        g: &'a epoch::Guard,
        cf: usize,
        level: u32,
        creates: &[pb::TableCreate],
        del_ids: &[u64],
        del_files: &mut HashSet<u64>,
    ) -> Result<()> {
        let (tx, rx) = mpsc::sync_channel(creates.len());
        let opts = dfs::Options::new(shard.id, shard.ver);
        let mut cnt = 0;
        for tbl in creates {
            if tbl.cf != cf as i32 {
                continue;
            }
            let afs = self.fs.clone();
            let atx = tx.clone();
            let aid = tbl.id;
            self.fs.get_future_pool().spawn_ok(async move {
                atx.send(afs.open(aid, opts).await).unwrap();
            });
            cnt += 1;
        }
        if cnt == 0 {
            return Ok(());
        }
        let shared = shard.cfs[cf].load(std::sync::atomic::Ordering::Acquire, g);
        let old_scf = shard.get_cf(cf, g);
        let mut new_scf = old_scf.clone();
        let level_idx = level as usize - 1;
        let mut new_level = &mut new_scf.levels[level_idx];
        let old_level = &old_scf.levels[level_idx];
        new_level.tables.truncate(0);
        new_level.total_size = 0;
        for _ in 0..cnt {
            let file = rx.recv().unwrap()?;
            let tbl = sstable::SSTable::new(file, self.cache.clone())?;
            new_level.total_size += tbl.size();
            new_level.tables.push(tbl);
        }

        for old_tbl in &old_level.tables {
            let id = old_tbl.id();
            if del_ids.contains(&id) {
                del_files.insert(id);
            } else {
                new_level.total_size += old_tbl.size();
                new_level.tables.push(old_tbl.clone());
            }
        }
        new_level
            .tables
            .sort_by(|a, b| a.smallest().cmp(b.smallest()));
        assert_tables_order(&new_level.tables);

        if !cas_resource(&shard.cfs[cf], g, shared, new_scf) {
            error!("there maybe concurrent apply compaction.");
            panic!("failed to update level_handler")
        }
        Ok(())
    }

    fn apply_split_files<'a>(
        &self,
        shard: &'a Shard,
        g: &'a epoch::Guard,
        cs: pb::ChangeSet,
    ) -> Result<()> {
        todo!()
    }
}

pub(crate) fn assert_tables_order(tables: &Vec<SSTable>) {
    for i in 0..(tables.len() - 1) {
        let ti = &tables[i];
        let tj = &tables[i + 1];
        if ti.smallest() > ti.biggest()
            || ti.smallest() >= tj.smallest()
            || ti.biggest() >= tj.biggest()
        {
            error!(
                "ti[{:x?},{:x?}], tj[{:x?}, {:x?}]",
                ti.smallest(),
                ti.biggest(),
                tj.smallest(),
                tj.biggest()
            );
            panic!("the order of tables is invalid")
        }
    }
}
