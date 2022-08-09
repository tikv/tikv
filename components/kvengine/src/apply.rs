// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    collections::HashMap,
    fmt::{Debug, Formatter},
    ops::Deref,
    sync::Arc,
};

use bytes::{Buf, Bytes};
use moka::sync::SegmentedCache;

use crate::{
    meta::is_move_down,
    table::sstable::{BlockCacheKey, L0Table, LocalFile, SSTable},
    *,
};

pub struct ChangeSet {
    pub change_set: kvenginepb::ChangeSet,
    pub l0_tables: HashMap<u64, L0Table>,
    pub ln_tables: HashMap<u64, SSTable>,
}

impl Deref for ChangeSet {
    type Target = kvenginepb::ChangeSet;

    fn deref(&self) -> &Self::Target {
        &self.change_set
    }
}

impl Debug for ChangeSet {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.change_set.fmt(f)
    }
}

impl ChangeSet {
    pub fn new(change_set: kvenginepb::ChangeSet) -> Self {
        Self {
            change_set,
            l0_tables: HashMap::new(),
            ln_tables: HashMap::new(),
        }
    }

    pub fn add_file(
        &mut self,
        id: u64,
        file: LocalFile,
        is_l0: bool,
        cache: SegmentedCache<BlockCacheKey, Bytes>,
    ) -> Result<()> {
        if is_l0 {
            let l0_table = L0Table::new(Arc::new(file), Some(cache))?;
            self.l0_tables.insert(id, l0_table);
        } else {
            let ln_table = SSTable::new(Arc::new(file), Some(cache))?;
            self.ln_tables.insert(id, ln_table);
        }
        Ok(())
    }
}

impl EngineCore {
    pub fn apply_change_set(&self, cs: ChangeSet) -> Result<()> {
        let shard = self.get_shard(cs.shard_id);
        if shard.is_none() {
            return Err(Error::ShardNotFound);
        }
        let shard = shard.unwrap();
        info!("{} apply change set {:?}", shard.tag(), &cs);
        if shard.ver != cs.shard_ver {
            return Err(Error::ShardNotMatch);
        }
        let seq = load_u64(&shard.meta_seq);
        if seq >= cs.sequence {
            warn!(
                "{} skip duplicated shard seq:{}, change seq:{}",
                shard.tag(),
                seq,
                cs.sequence
            );
            return Ok(());
        } else {
            store_u64(&shard.meta_seq, cs.sequence);
        }
        if cs.has_flush() {
            self.apply_flush(&shard, &cs);
        } else if cs.has_compaction() || cs.has_destroy_range() {
            if cs.has_compaction() {
                self.apply_compaction(&shard, &cs);
            } else {
                self.apply_destroy_range(&shard, &cs);
            }
            store_bool(&shard.compacting, false);
            self.compact_tx
                .send(CompactMsg::Applied(IDVer::new(shard.id, shard.ver)))
                .unwrap();
        } else if cs.has_initial_flush() {
            self.apply_initial_flush(&shard, &cs);
        } else if cs.has_ingest_files() {
            self.apply_ingest_files(&shard, &cs)?;
        }
        self.refresh_shard_states(&shard);
        Ok(())
    }

    fn apply_flush(&self, shard: &Shard, cs: &ChangeSet) {
        let flush = cs.get_flush();
        if flush.has_l0_create() {
            let id = flush.get_l0_create().get_id();
            let l0_tbl = cs.l0_tables.get(&id).unwrap().clone();
            let old_data = shard.get_data();
            let mut new_mem_tbls = old_data.mem_tbls.clone();
            let last = new_mem_tbls.pop().unwrap();
            assert_eq!(last.get_version(), l0_tbl.version(), "{}", shard.tag());
            let mut new_l0_tbls = Vec::with_capacity(old_data.l0_tbls.len() + 1);
            new_l0_tbls.push(l0_tbl);
            new_l0_tbls.extend_from_slice(old_data.l0_tbls.as_slice());
            let new_data = ShardData::new(
                shard.start.clone(),
                shard.end.clone(),
                old_data.del_prefixes.clone(),
                new_mem_tbls,
                new_l0_tbls,
                old_data.cfs.clone(),
            );
            shard.set_data(new_data);
            self.free_tx.send(last).unwrap();
        }
    }

    fn apply_initial_flush(&self, shard: &Shard, cs: &ChangeSet) {
        let initial_flush = cs.get_initial_flush();
        let data = shard.get_data();
        let mut mem_tbls = data.mem_tbls.clone();
        let (l0s, scfs) = self.create_snapshot_tables(initial_flush, cs);
        mem_tbls.retain(|x| {
            let version = x.get_version();
            let flushed =
                version > 0 && version <= initial_flush.base_version + initial_flush.data_sequence;
            if flushed {
                self.free_tx.send(x.clone()).unwrap();
            }
            !flushed
        });
        let new_data = ShardData::new(
            shard.start.clone(),
            shard.end.clone(),
            data.del_prefixes.clone(),
            mem_tbls,
            l0s,
            scfs,
        );
        shard.set_data(new_data);
        store_bool(&shard.initial_flushed, true);
        // Switched memtables can't be flushed until initial flush finished, so we trigger it
        // actively.
        self.trigger_flush(shard);
    }

    fn apply_compaction(&self, shard: &Shard, cs: &ChangeSet) {
        let comp = cs.get_compaction();
        let mut del_files = HashMap::new();
        if comp.conflicted {
            if is_move_down(comp) {
                return;
            }
            for create in comp.get_table_creates() {
                let cover = shard.cover_full_table(&create.smallest, &create.biggest);
                del_files.insert(create.id, cover);
            }
            self.remove_dfs_files(shard, del_files);
            return;
        }
        let data = shard.get_data();
        let mut new_l0s = data.l0_tbls.clone();
        let mut new_cfs = data.cfs.clone();
        if comp.level == 0 {
            new_l0s.retain(|x| {
                let is_deleted = comp.get_top_deletes().contains(&x.id());
                if is_deleted {
                    del_files.insert(x.id(), shard.cover_full_table(x.smallest(), x.biggest()));
                }
                !is_deleted
            });
            for cf in 0..NUM_CFS {
                let new_l1 = self.new_level(shard, cs, &data, cf, &mut del_files, true);
                new_cfs[cf].set_level(new_l1);
            }
        } else {
            let cf = comp.cf as usize;
            let new_top_level = self.new_level(shard, cs, &data, cf, &mut del_files, false);
            new_cfs[cf].set_level(new_top_level);
            let new_bottom_level = self.new_level(shard, cs, &data, cf, &mut del_files, true);
            new_cfs[cf].set_level(new_bottom_level);
            // For move down operation, the TableCreates may contains TopDeletes, we don't want to delete them.
            for create in comp.get_table_creates() {
                del_files.remove(&create.id);
            }
        }
        let new_data = ShardData::new(
            shard.start.clone(),
            shard.end.clone(),
            data.del_prefixes.clone(),
            data.mem_tbls.clone(),
            new_l0s,
            new_cfs,
        );
        shard.set_data(new_data);
        self.remove_dfs_files(shard, del_files);
    }

    fn apply_destroy_range(&self, shard: &Shard, cs: &ChangeSet) {
        assert!(cs.has_destroy_range());
        let dr = cs.get_destroy_range();
        let data = shard.get_data();
        let mut new_l0s = data.l0_tbls.clone();
        let mut new_cfs = data.cfs.clone();
        // Group files by cf and level.
        let mut grouped = HashMap::new();
        for deleted in dr.get_table_deletes() {
            grouped
                .entry((deleted.get_cf() as usize, deleted.get_level() as usize))
                .or_insert_with(|| (Vec::new(), Vec::new()))
                .0
                .push(deleted.get_id());
        }
        for created in dr.get_table_creates() {
            grouped
                .entry((created.get_cf() as usize, created.get_level() as usize))
                .or_insert_with(|| (Vec::new(), Vec::new()))
                .1
                .push(created.get_id());
        }

        for ((cf, level), (deletes, creates)) in grouped {
            if level == 0 {
                new_l0s.retain(|l0| !deletes.contains(&l0.id()));
                new_l0s.extend(
                    creates
                        .into_iter()
                        .map(|id| cs.l0_tables.get(&id).unwrap().clone()),
                );
                new_l0s.sort_by(|a, b| b.version().cmp(&a.version()));
            } else {
                let old_level = new_cfs[cf].get_level(level);
                let mut new_level_tables = old_level.tables.as_ref().clone();
                new_level_tables.retain(|t| !deletes.contains(&t.id()));
                new_level_tables.extend(
                    creates
                        .into_iter()
                        .map(|id| cs.ln_tables.get(&id).unwrap().clone()),
                );
                new_level_tables.sort_by(|a, b| a.smallest().cmp(b.smallest()));
                let new_level = LevelHandler::new(level, new_level_tables);
                new_cfs[cf].set_level(new_level);
            }
        }
        assert_eq!(cs.get_property_key(), DEL_PREFIXES_KEY);
        let done = DeletePrefixes::unmarshal(cs.get_property_value());
        let new_data = ShardData::new(
            shard.start.clone(),
            shard.end.clone(),
            data.del_prefixes.split(&done),
            data.mem_tbls.clone(),
            new_l0s,
            new_cfs,
        );
        shard.set_data(new_data);
        let del_files = dr
            .get_table_deletes()
            .iter()
            .map(|deleted| (deleted.get_id(), true))
            .collect();
        self.remove_dfs_files(shard, del_files);
    }

    fn new_level(
        &self,
        shard: &Shard,
        cs: &ChangeSet,
        data: &ShardData,
        cf: usize,
        del_files: &mut HashMap<u64, bool>,
        is_bottom: bool,
    ) -> LevelHandler {
        let old_scf = data.get_cf(cf);
        let comp = cs.get_compaction();
        let level = if is_bottom {
            comp.get_level() as usize + 1
        } else {
            comp.get_level() as usize
        };
        let deletes = if is_bottom {
            comp.get_bottom_deletes()
        } else {
            comp.get_top_deletes()
        };
        let mut new_level_tables = old_scf.get_level(level).tables.as_ref().clone();
        new_level_tables.retain(|x| {
            let is_deleted = deletes.contains(&x.id());
            if is_deleted {
                del_files.insert(x.id(), shard.cover_full_table(x.smallest(), x.biggest()));
            }
            !is_deleted
        });
        if is_bottom {
            for new_tbl_create in comp.get_table_creates() {
                if new_tbl_create.cf as usize == cf {
                    let new_tbl = if is_move_down(comp) {
                        let old_top_level = old_scf.get_level(level - 1);
                        old_top_level.get_table_by_id(new_tbl_create.id).unwrap()
                    } else {
                        cs.ln_tables.get(&new_tbl_create.get_id()).unwrap().clone()
                    };
                    new_level_tables.push(new_tbl);
                }
            }
            new_level_tables.sort_by(|a, b| a.smallest().cmp(b.smallest()));
        }
        let new_level = LevelHandler::new(level, new_level_tables);
        new_level.check_order(cf, shard.tag());
        new_level
    }

    fn remove_dfs_files(&self, shard: &Shard, del_files: HashMap<u64, bool>) {
        let fs = self.fs.clone();
        let opts = dfs::Options::new(shard.id, shard.ver);
        let runtime = fs.get_runtime();
        for (id, cover) in del_files {
            if cover {
                self.remove_local_file(id);
                let fs_n = fs.clone();
                runtime.spawn(async move { fs_n.remove(id, opts).await });
            }
        }
    }

    fn remove_local_file(&self, file_id: u64) {
        let local_file_path = self.local_file_path(file_id);
        if let Err(err) = std::fs::remove_file(&local_file_path) {
            error!("failed to remove local file {:?}", err);
        }
    }

    pub(crate) fn create_snapshot_tables(
        &self,
        snap: &kvenginepb::Snapshot,
        tables: &ChangeSet,
    ) -> (Vec<L0Table>, [ShardCF; 3]) {
        let mut l0_tbls = vec![];
        for l0_create in snap.get_l0_creates() {
            let l0_tbl = tables.l0_tables.get(&l0_create.id).unwrap().clone();
            l0_tbls.push(l0_tbl);
        }
        l0_tbls.sort_by(|a, b| b.version().cmp(&a.version()));
        let mut scf_builders = vec![];
        for cf in 0..NUM_CFS {
            let scf = ShardCFBuilder::new(cf);
            scf_builders.push(scf);
        }
        for table_create in snap.get_table_creates() {
            let tbl = tables.ln_tables.get(&table_create.id).unwrap().clone();
            let scf = &mut scf_builders.as_mut_slice()[table_create.cf as usize];
            scf.add_table(tbl, table_create.level as usize);
        }
        let mut scfs = [ShardCF::new(0), ShardCF::new(1), ShardCF::new(2)];
        for cf in 0..NUM_CFS {
            let scf = &mut scf_builders.as_mut_slice()[cf];
            scfs[cf] = scf.build();
        }
        (l0_tbls, scfs)
    }

    fn apply_ingest_files(&self, shard: &Shard, cs: &ChangeSet) -> Result<()> {
        let ingest_files = cs.get_ingest_files();
        let ingest_id = get_shard_property(INGEST_ID_KEY, ingest_files.get_properties()).unwrap();
        if let Some(old_ingest_id) = shard.get_property(INGEST_ID_KEY) {
            if old_ingest_id.chunk() == ingest_id.as_slice() {
                // skip duplicated ingest files.
                return Ok(());
            }
        }
        let old_data = shard.get_data();
        let mut new_l0s = old_data.l0_tbls.clone();
        for l0_create in ingest_files.get_l0_creates() {
            let l0_table = cs.l0_tables.get(&l0_create.get_id()).unwrap().clone();
            new_l0s.push(l0_table);
        }
        new_l0s.sort_unstable_by(|a, b| b.version().cmp(&a.version()));
        let mut scf_builder = ShardCFBuilder::new(0);
        for level in &old_data.cfs[0].levels {
            for old_tbl in level.tables.as_ref() {
                scf_builder.add_table(old_tbl.clone(), level.level);
            }
        }
        for tbl_create in ingest_files.get_table_creates() {
            let table = cs.ln_tables.get(&tbl_create.get_id()).unwrap().clone();
            scf_builder.add_table(table, tbl_create.level as usize);
        }
        let new_cf = scf_builder.build();
        let mut new_cfs = old_data.cfs.clone();
        new_cfs[0] = new_cf;
        let new_data = ShardData::new(
            shard.start.clone(),
            shard.end.clone(),
            old_data.del_prefixes.clone(),
            old_data.mem_tbls.clone(),
            new_l0s,
            new_cfs,
        );
        shard.set_data(new_data);
        Ok(())
    }
}
