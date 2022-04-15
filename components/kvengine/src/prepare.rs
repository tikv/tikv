// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use crate::apply::ChangeSet;
use crate::table::sstable::{L0Table, SSTable};
use crate::EngineCore;
use crate::*;
use bytes::{Buf, Bytes};
use file_system::{IOOp, IOType};
use std::collections::HashMap;
use std::io::Write;
use std::path::PathBuf;

impl EngineCore {
    pub fn prepare_change_set(
        &self,
        cs: kvenginepb::ChangeSet,
        use_direct_io: bool,
    ) -> Result<ChangeSet> {
        let mut ids = HashMap::new();
        let mut cs = ChangeSet::new(cs);
        if cs.has_flush() {
            let flush = cs.get_flush();
            if flush.has_l0_create() {
                ids.insert(flush.get_l0_create().id, true);
            }
        }
        if cs.has_compaction() {
            let comp = cs.get_compaction();
            if !is_move_down(comp) {
                for tbl in &comp.table_creates {
                    ids.insert(tbl.id, false);
                }
            }
        }
        if cs.has_snapshot() {
            self.collect_snap_ids(cs.get_snapshot(), &mut ids);
        }
        if cs.has_initial_flush() {
            self.collect_snap_ids(cs.get_initial_flush(), &mut ids);
        }
        self.load_tables_by_ids(cs.shard_id, cs.shard_ver, ids, &mut cs, use_direct_io)?;
        Ok(cs)
    }

    pub(crate) fn collect_snap_ids(
        &self,
        snap: &kvenginepb::Snapshot,
        ids: &mut HashMap<u64, bool>,
    ) {
        for l0 in snap.get_l0_creates() {
            ids.insert(l0.id, true);
        }
        for ln in snap.get_table_creates() {
            ids.insert(ln.id, false);
        }
    }

    pub(crate) fn load_tables_by_ids(
        &self,
        shard_id: u64,
        shard_ver: u64,
        ids: HashMap<u64, bool>,
        cs: &mut ChangeSet,
        use_direct_io: bool,
    ) -> Result<()> {
        let (result_tx, result_rx) = tikv_util::mpsc::bounded(ids.len());
        let runtime = self.fs.get_runtime();
        let opts = dfs::Options::new(shard_id, shard_ver);
        let mut msg_count = 0;
        for (id, _) in &ids {
            let local_path = self.local_file_path(*id);
            if local_path.exists() {
                continue;
            }
            let fs = self.fs.clone();
            let tx = result_tx.clone();
            let id = *id;
            runtime.spawn(async move {
                let res = fs.read_file(id, opts).await;
                tx.send(res.map(|data| (id, data))).unwrap();
            });
            msg_count += 1;
        }
        let mut errors = vec![];
        for _ in 0..msg_count {
            match result_rx.recv().unwrap() {
                Ok((id, data)) => {
                    if let Err(err) = self.write_local_file(id, data, use_direct_io) {
                        error!("write local file failed {:?}", &err);
                        errors.push(err.into());
                    }
                }
                Err(err) => {
                    error!("prefetch failed {:?}", &err);
                    errors.push(err);
                }
            }
        }
        if errors.len() > 0 {
            return Err(errors.pop().unwrap().into());
        }
        let opts = dfs::Options::new(shard_id, shard_ver);
        for (id, is_l0) in ids {
            let file = self.fs.open(id, opts)?;
            if is_l0 {
                let l0_table = L0Table::new(file, Some(self.cache.clone()))?;
                cs.l0_tables.insert(id, l0_table);
            } else {
                let ln_table = SSTable::new(file, Some(self.cache.clone()))?;
                cs.ln_tables.insert(id, ln_table);
            }
        }
        Ok(())
    }

    fn write_local_file(&self, id: u64, data: Bytes, use_direct_io: bool) -> std::io::Result<()> {
        let local_file_name = self.local_file_path(id);
        let tmp_file_name = self.tmp_file_path(id);
        if use_direct_io {
            let mut writer =
                file_system::DirectWriter::new(self.rate_limiter.clone(), IOType::Compaction);
            writer.write_to_file(data.chunk(), &tmp_file_name)?;
        } else {
            let mut file = std::fs::File::create(&tmp_file_name)?;
            let mut start_off = 0;
            let write_batch_size = 256 * 1024;
            while start_off < data.len() {
                self.rate_limiter
                    .request(IOType::Compaction, IOOp::Write, write_batch_size);
                let end_off = std::cmp::min(start_off + write_batch_size, data.len());
                file.write(&data[start_off..end_off])?;
                file.sync_data()?;
                start_off = end_off;
            }
        }
        std::fs::rename(&tmp_file_name, &local_file_name)
    }

    pub(crate) fn local_file_path(&self, file_id: u64) -> PathBuf {
        self.opts.local_dir.join(new_filename(file_id))
    }

    fn tmp_file_path(&self, file_id: u64) -> PathBuf {
        let tmp_id = self
            .tmp_file_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.opts.local_dir.join(new_tmp_filename(file_id, tmp_id))
    }
}
