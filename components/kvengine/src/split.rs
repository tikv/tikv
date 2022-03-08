// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;
use std::{collections::HashSet, thread, time};

use crate::{table::sstable, *};
use byteorder::{ByteOrder, LittleEndian};
use bytes::{Buf, Bytes, BytesMut};
use dashmap::mapref::entry::Entry;
use kvenginepb as pb;
use protobuf::ProtobufEnum;
use slog_global::{info, warn};

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

    // Sets the split keys, then all new entries are written to separated mem-tables.
    pub fn pre_split(
        &self,
        shard_id: u64,
        shard_ver: u64,
        keys: &[Vec<u8>],
        sequence: u64,
    ) -> Result<()> {
        let shard = self.get_shard_with_ver(shard_id, shard_ver)?;
        if !shard.set_split_keys(keys) {
            return Err(Error::AlreadySplitting);
        }
        let version = shard.base_version + sequence;
        let mem_tbl = self.switch_mem_table(&shard, version);
        self.schedule_flush_task(&shard, mem_tbl);
        Ok(())
    }

    pub fn split_shard_files(&self, shard_id: u64, shard_ver: u64) -> Result<pb::ChangeSet> {
        let shard = self.get_shard_with_ver(shard_id, shard_ver)?;
        if shard.get_split_stage() != pb::SplitStage::PreSplit {
            return Err(Error::WrongSplitStage(shard.get_split_stage().value()));
        }
        let mut cs = new_change_set(shard_id, shard_ver, pb::SplitStage::SplitFileDone);

        let _guard = shard.compact_lock.lock().unwrap();
        self.wait_for_pre_split_stage(&shard);
        self.split_shard_l0_files(&shard, cs.mut_split_files())?;

        let dfs_opts = dfs::Options::new(shard_id, shard_ver);
        for cf in 0..NUM_CFS {
            let scf = shard.get_cf(cf);
            for lh in scf.levels.as_ref() {
                let split_keys = shard.get_split_keys();
                self.split_tables(dfs_opts, cf, lh, &split_keys, cs.mut_split_files())?;
            }
        }
        Ok(cs)
    }

    fn wait_for_pre_split_stage(&self, shard: &Shard) {
        loop {
            info!(
                "shard {}:{} split stage {:?}",
                shard.id,
                shard.ver,
                shard.get_split_stage()
            );
            match shard.get_split_stage() {
                pb::SplitStage::PreSplit | pb::SplitStage::SplitFileDone => {
                    return;
                }
                // _ => time::sleep(time::Duration::from_millis(100)).await,
                _ => thread::sleep(time::Duration::from_millis(100)),
            }
        }
    }

    fn split_shard_l0_files(&self, shard: &Shard, split_files: &mut pb::SplitFiles) -> Result<()> {
        let l0s = shard.get_l0_tbls();
        let split_keys = shard.get_split_keys();
        for l0 in l0s.tbls.as_ref() {
            if !need_split_l0(&split_keys, l0) {
                continue;
            }
            let mut new_l0s = self.split_shard_l0_table(shard, l0, &split_keys)?;
            for new_l0 in new_l0s.drain(..) {
                split_files.mut_l0_creates().push(new_l0);
            }
            split_files.mut_table_deletes().push(l0.id());
        }
        Ok(())
    }

    fn split_shard_l0_table(
        &self,
        shard: &Shard,
        l0: &sstable::L0Table,
        split_keys: &Vec<Bytes>,
    ) -> Result<Vec<pb::L0Create>> {
        let mut iters = Vec::new();
        for cf in 0..NUM_CFS {
            let iter = match l0.get_cf(cf) {
                Some(tbl) => {
                    let mut iter = tbl.new_iterator(false);
                    iter.rewind();
                    Some(iter)
                }
                None => None,
            };
            iters.push(iter)
        }
        let mut end_keys = split_keys.clone();
        end_keys.push(shard.end.clone());

        let mut l0_creates = Vec::new();
        let mut l0_datas = Vec::new();
        let mut ids = self
            .id_allocator
            .alloc_id(end_keys.len())
            .map_err(|err| Error::ErrAllocID(err))?;
        for key in &end_keys {
            if let Some((create, data)) =
                self.build_shard_l0_before_key(&mut iters, key, ids.pop().unwrap(), l0.version())?
            {
                l0_creates.push(create);
                l0_datas.push(data);
            }
        }
        let dfs_opts = dfs::Options::new(shard.id, shard.ver);
        let rt = self.fs.get_runtime();
        let (tx, rx) = tikv_util::mpsc::bounded(l0_creates.len());
        for (idx, l0_create) in l0_creates.iter().enumerate() {
            let fs = self.fs.clone();
            let id = l0_create.get_id();
            let data = l0_datas[idx].clone();
            let f_tx = tx.clone();
            let future = async move {
                let res = fs.create(id, data, dfs_opts).await;
                let _ = f_tx.send(res);
            };
            rt.spawn(future);
        }
        let mut errors = vec![];
        for _ in 0..l0_creates.len() {
            if let Err(err) = rx.recv().unwrap() {
                errors.push(err);
            }
        }
        if errors.len() > 0 {
            return Err(errors.pop().unwrap().into());
        }
        Ok(l0_creates)
    }

    fn build_shard_l0_before_key(
        &self,
        iters: &mut Vec<Option<Box<sstable::TableIterator>>>,
        key: &Bytes,
        id: u64,
        version: u64,
    ) -> Result<Option<(pb::L0Create, Bytes)>> {
        let mut builder = sstable::L0Builder::new(id, self.opts.table_builder_options, version);
        let mut has_data = false;
        for cf in 0..NUM_CFS {
            if let Some(iter) = &mut iters[cf] {
                while iter.valid() {
                    if iter.key() >= key {
                        break;
                    }
                    builder.add(cf, iter.key(), iter.value());
                    has_data = true;
                    iter.next_all_version();
                }
            }
        }
        if !has_data {
            return Ok(None);
        }
        let data = builder.finish();
        let (smallest, biggest) = builder.smallest_biggest();
        let mut l0_create = pb::L0Create::new();
        l0_create.set_id(id);
        l0_create.set_smallest(smallest.to_vec());
        l0_create.set_biggest(biggest.to_vec());
        Ok(Some((l0_create, data)))
    }

    fn split_tables(
        &self,
        dfs_opts: dfs::Options,
        cf: usize,
        lh: &LevelHandler,
        keys: &Vec<Bytes>,
        split_files: &mut pb::SplitFiles,
    ) -> Result<()> {
        let mut to_del_ids = HashSet::new();
        let mut related_keys = vec![];
        let rt = self.fs.get_runtime();
        let (tx, rx) = tikv_util::mpsc::unbounded();
        let mut future_cnt = 0;
        for tbl in lh.tables.iter() {
            related_keys.truncate(0);
            for key in keys {
                if tbl.smallest() < key && key <= tbl.biggest() {
                    related_keys.push(key.clone());
                }
            }
            if related_keys.len() == 0 {
                continue;
            }
            to_del_ids.insert(tbl.id());
            // append an end key to build the last table.
            related_keys.push(GLOBAL_SHARD_END_KEY.clone());

            let mut ids = self
                .id_allocator
                .alloc_id(related_keys.len())
                .map_err(|e| Error::ErrAllocID(e))?;

            let mut iter = tbl.new_iterator(false);
            iter.rewind();
            for related_key in &related_keys {
                let id = ids.pop().unwrap();
                if let Some((create, data)) = self.build_table_before_key(
                    id,
                    cf,
                    lh.level,
                    &mut iter,
                    related_key,
                    self.opts.table_builder_options,
                )? {
                    split_files.mut_table_creates().push(create);
                    let fs = self.fs.clone();
                    let f_tx = tx.clone();
                    let future = async move {
                        let res = fs.create(id, data, dfs_opts).await;
                        let _ = f_tx.send(res);
                    };
                    rt.spawn(future);
                    future_cnt += 1;
                }
            }
            split_files.mut_table_deletes().push(tbl.id());
        }
        let mut errors = vec![];
        for _ in 0..future_cnt {
            if let Err(err) = rx.recv().unwrap() {
                errors.push(err);
            }
        }
        if errors.len() > 0 {
            return Err(errors.pop().unwrap().into());
        }
        Ok(())
    }

    fn build_table_before_key(
        &self,
        id: u64,
        cf: usize,
        level: usize,
        iter: &mut Box<sstable::TableIterator>,
        key: &Bytes,
        opts: sstable::TableBuilderOptions,
    ) -> Result<Option<(pb::TableCreate, Bytes)>> {
        let mut b = sstable::Builder::new(id, opts);
        while iter.valid() {
            if key.len() > 0 && iter.key() >= key {
                break;
            }
            b.add(iter.key(), iter.value());
            iter.next_all_version();
        }
        if b.is_empty() {
            return Ok(None);
        }
        let mut buf = BytesMut::with_capacity(b.estimated_size());
        let result = b.finish(0, &mut buf);
        let mut table_create = pb::TableCreate::new();
        table_create.set_id(id);
        table_create.set_cf(cf as i32);
        table_create.set_level(level as u32);
        table_create.set_smallest(result.smallest);
        table_create.set_biggest(result.biggest);

        Ok(Some((table_create, buf.freeze())))
    }

    pub fn finish_split(&self, cs: pb::ChangeSet, initial_seq: u64) -> Result<()> {
        let shard = self.get_shard_with_ver(cs.shard_id, cs.shard_ver)?;
        if shard.get_split_stage() != pb::SplitStage::SplitFileDone {
            return Err(Error::WrongSplitStage(shard.get_split_stage().value()));
        }
        let split = cs.get_split();
        let split_ctx = shard.get_split_ctx();
        assert_eq!(split.get_new_shards().len(), split_ctx.mem_tbls.len());
        self.build_split_shards(&shard, split, cs.get_sequence(), initial_seq)
    }

    fn build_split_shards(
        &self,
        old_shard: &Shard,
        split: &pb::Split,
        sequence: u64,
        initial_seq: u64,
    ) -> Result<()> {
        let split_ctx = old_shard.get_split_ctx();
        let mut new_shards = vec![];
        let new_shard_props = split.get_new_shards();
        let new_ver = old_shard.ver + new_shard_props.len() as u64 - 1;
        for (i, mem_tbl) in split_ctx.mem_tbls.iter().enumerate() {
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
            new_shard.atomic_add_mem_table(mem_tbl.clone());
            new_shard.atomic_remove_mem_table();
            new_shards.push(Arc::new(new_shard));
        }
        let l0s = old_shard.get_l0_tbls();
        for l0 in l0s.tbls.iter().rev() {
            let idx = get_split_shard_index(split.get_keys(), l0.smallest());
            let new_shard = &new_shards[idx];
            new_shard.atomic_add_l0_table(l0.clone());
        }
        for cf in 0..NUM_CFS {
            let old_scf = old_shard.get_cf(cf);
            let mut new_scfs = Vec::new();
            new_scfs.resize_with(new_shards.len(), || ShardCFBuilder::new(cf));
            for lh in old_scf.levels.as_ref() {
                for tbl in lh.tables.iter() {
                    self.insert_table_to_shard(
                        tbl,
                        lh.level,
                        &mut new_scfs,
                        &new_shards,
                        split.get_keys(),
                    );
                }
            }
            for i in 0..new_shards.len() {
                new_shards[i].set_cf(cf, new_scfs[i].build());
            }
        }
        for shard in new_shards.drain(..) {
            shard.refresh_estimated_size();
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
            let version = shard.load_mem_table_version();
            let mem_tbl = self.switch_mem_table(&shard, version);
            self.schedule_flush_task(&shard, mem_tbl);
            let all_files = shard.get_all_files();
            info!(
                "new shard {}:{}, start {:x}, end {:x} mem-version {}， all files {:?}",
                shard.id, shard.ver, shard.start, shard.end, version, all_files
            );
        }
        Ok(())
    }

    fn insert_table_to_shard(
        &self,
        tbl: &sstable::SSTable,
        level: usize,
        scf_builders: &mut Vec<ShardCFBuilder>,
        new_shards: &Vec<Arc<Shard>>,
        keys: &[Vec<u8>],
    ) {
        let idx = get_split_shard_index(keys, tbl.smallest());
        let new_shard = &new_shards[idx];
        if !new_shard.overlap_key(tbl.smallest()) || !new_shard.overlap_key(tbl.biggest()) {
            panic!(
                "shard {}:{} start:{:x}, end:{:x}, tbl smallest:{:x?}, biggest:{:x?}",
                new_shard.id,
                new_shard.ver,
                new_shard.start,
                new_shard.end,
                tbl.smallest(),
                tbl.biggest()
            );
        }
        let scf = &mut scf_builders[idx];
        scf.add_table(tbl.clone(), level);
    }
}

pub(crate) fn need_split_l0(split_keys: &Vec<Bytes>, l0: &sstable::L0Table) -> bool {
    for key in split_keys {
        for cf in 0..NUM_CFS {
            if let Some(tbl) = l0.get_cf(cf) {
                if tbl.smallest() < key && key <= tbl.biggest() {
                    return true;
                }
            }
        }
    }
    false
}

pub fn get_split_shard_index(split_keys: &[Vec<u8>], key: &[u8]) -> usize {
    for i in 0..split_keys.len() {
        if key < split_keys[i].as_slice() {
            return i;
        }
    }
    return split_keys.len();
}
