// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use crate::table::{memtable, sstable};
use crate::*;
use bytes::BytesMut;
use kvenginepb as pb;
use slog_global::info;
use tikv_util::mpsc;

pub(crate) struct FlushTask {
    pub(crate) shard_id: u64,
    pub(crate) shard_ver: u64,
    pub(crate) start: Vec<u8>,
    pub(crate) end: Vec<u8>,
    pub(crate) normal: Option<NormalFlush>,
    pub(crate) initial: Option<InitialFlush>,
}

impl FlushTask {
    pub(crate) fn overlap_table(&self, start_key: &[u8], end_key: &[u8]) -> bool {
        self.start.as_slice() <= end_key && start_key < self.end.as_slice()
    }
}

pub(crate) struct NormalFlush {
    pub(crate) mem_tbl: memtable::CFTable,
    pub(crate) next_mem_tbl_size: u64,
}

pub(crate) struct InitialFlush {
    pub(crate) parent_snap: pb::Snapshot,
    pub(crate) mem_tbls: Vec<memtable::CFTable>,
    pub(crate) base_version: u64,
    pub(crate) data_sequence: u64,
}

pub(crate) type FlushResultRx = mpsc::Receiver<FlushResult>;

pub(crate) struct FlushResult {
    pub(crate) change_set: pb::ChangeSet,
    pub(crate) err: Option<Error>,
    pub(crate) task: FlushTask,
}

impl Engine {
    pub(crate) fn run_flush_worker(
        &self,
        rx: mpsc::Receiver<FlushTask>,
        result_tx: mpsc::Sender<FlushResultRx>,
    ) {
        loop {
            if let Ok(task) = rx.recv() {
                let result_task = if task.normal.is_some() {
                    self.flush_normal(task)
                } else {
                    self.flush_initial(task)
                };
                if let Err(err) = result_tx.send(result_task) {
                    error!("flush worker failed to send result {:?}", err);
                }
            } else {
                break;
            }
        }
    }

    pub(crate) fn flush_normal(&self, task: FlushTask) -> FlushResultRx {
        let (res_tx, res_rx) = mpsc::bounded(1);
        let mut cs = new_change_set(task.shard_id, task.shard_ver);
        let mem_tbl = &task.normal.as_ref().unwrap().mem_tbl;

        let flush_version = mem_tbl.get_version();
        info!(
            "{}:{} flush normal version {}",
            task.shard_id, task.shard_ver, flush_version
        );
        cs.mut_flush().set_version(flush_version);
        if let Some(props) = mem_tbl.get_properties() {
            cs.mut_flush().set_properties(props);
        }
        let mut result = FlushResult {
            change_set: cs,
            err: None,
            task,
        };
        let task = &result.task;
        let m = &task.normal.as_ref().unwrap().mem_tbl;
        if m.is_empty() {
            res_tx.send(result).unwrap();
            return res_rx;
        }
        // TODO: handle alloc_id error.
        let fid = self.id_allocator.alloc_id(1).unwrap().pop().unwrap();

        let mut l0_builder =
            sstable::L0Builder::new(fid, self.opts.table_builder_options, m.get_version());
        for cf in 0..NUM_CFS {
            let skl = m.get_cf(cf);
            if skl.is_empty() {
                continue;
            }
            let mut it = skl.new_iterator(false);
            // If CF is not managed, we only need to keep the latest version.
            let rc = !CF_MANAGED[cf];
            let mut prev_key = BytesMut::new();
            it.seek(&task.start);
            while it.valid() {
                if it.key() >= task.end.as_slice() {
                    break;
                }
                if rc && prev_key == it.key() {
                    // For read committed CF, we can discard all the old versions.
                } else {
                    l0_builder.add(cf, it.key(), it.value());
                    if rc {
                        prev_key.truncate(0);
                        prev_key.extend_from_slice(it.key());
                    }
                }
                it.next_all_version();
            }
        }
        if l0_builder.is_empty() {
            res_tx.send(result).unwrap();
            return res_rx;
        }
        let l0_data = l0_builder.finish();
        let (smallest, biggest) = l0_builder.smallest_biggest();
        info!(
            "shard {}:{} flush memtable id:{}, size:{}, l0_size:{}, version:{}",
            task.shard_id,
            task.shard_ver,
            fid,
            m.size(),
            l0_data.len(),
            m.get_version()
        );
        let fs = self.fs.clone();
        self.fs.get_runtime().spawn(async move {
            let task = &result.task;
            if let Err(err) = fs
                .create(
                    fid,
                    l0_data,
                    dfs::Options::new(task.shard_id, task.shard_ver),
                )
                .await
            {
                result.err = Some(Error::DFSError(err));
            } else {
                let mut l0_create = pb::L0Create::new();
                l0_create.set_id(fid);
                l0_create.set_smallest(smallest.to_vec());
                l0_create.set_biggest(biggest.to_vec());
                result.change_set.mut_flush().set_l0_create(l0_create);
            }
            res_tx.send(result).unwrap();
        });
        res_rx
    }

    pub(crate) fn flush_initial(&self, task: FlushTask) -> FlushResultRx {
        let (res_tx, res_rx) = mpsc::bounded(1);
        let flush = task.initial.as_ref().unwrap();
        let mut cs = new_change_set(task.shard_id, task.shard_ver);
        let initial_flush = cs.mut_initial_flush();
        initial_flush.set_start(task.start.to_vec());
        initial_flush.set_end(task.end.to_vec());
        initial_flush.set_base_version(flush.base_version);
        initial_flush.set_data_sequence(flush.data_sequence);
        for tbl_create in flush.parent_snap.get_table_creates() {
            if task.overlap_table(tbl_create.get_smallest(), tbl_create.get_biggest()) {
                initial_flush.mut_table_creates().push(tbl_create.clone());
            }
        }
        for l0_create in flush.parent_snap.get_l0_creates() {
            if task.overlap_table(l0_create.get_smallest(), l0_create.get_biggest()) {
                initial_flush.mut_l0_creates().push(l0_create.clone());
            }
        }
        let mut l0_builders = vec![];
        for m in &flush.mem_tbls {
            let fid = self.id_allocator.alloc_id(1).unwrap().pop().unwrap();
            let mut l0_builder =
                sstable::L0Builder::new(fid, self.opts.table_builder_options, m.get_version());
            for cf in 0..NUM_CFS {
                let skl = m.get_cf(cf);
                if skl.is_empty() {
                    continue;
                }
                let mut it = skl.new_iterator(false);
                // If CF is not managed, we only need to keep the latest version.
                let rc = !CF_MANAGED[cf];
                let mut prev_key = BytesMut::new();
                it.seek(&task.start);
                while it.valid() {
                    if it.key() >= task.end.as_slice() {
                        break;
                    }
                    if rc && prev_key == it.key() {
                        // For read committed CF, we can discard all the old versions.
                    } else {
                        l0_builder.add(cf, it.key(), it.value());
                        if rc {
                            prev_key.truncate(0);
                            prev_key.extend_from_slice(it.key());
                        }
                    }
                    it.next_all_version();
                }
            }
            l0_builders.push(l0_builder);
        }
        let mut result = FlushResult {
            change_set: cs,
            err: None,
            task,
        };
        let fs = self.fs.clone();
        self.fs.get_runtime().spawn(async move {
            let task = &result.task;
            for mut l0_builder in l0_builders {
                let l0_data = l0_builder.finish();
                let (smallest, biggest) = l0_builder.smallest_biggest();
                if let Err(err) = fs
                    .create(
                        l0_builder.get_fid(),
                        l0_data,
                        dfs::Options::new(task.shard_id, task.shard_ver),
                    )
                    .await
                {
                    result.err = Some(Error::DFSError(err));
                    break;
                } else {
                    let mut l0_create = pb::L0Create::new();
                    l0_create.set_id(l0_builder.get_fid());
                    l0_create.set_smallest(smallest.to_vec());
                    l0_create.set_biggest(biggest.to_vec());
                    result
                        .change_set
                        .mut_initial_flush()
                        .mut_l0_creates()
                        .push(l0_create);
                }
            }
            res_tx.send(result).unwrap();
        });
        res_rx
    }

    pub(crate) fn run_flush_result(&self, rx: mpsc::Receiver<FlushResultRx>) {
        loop {
            if let Ok(result) = rx.recv() {
                let result = result.recv().unwrap();
                if let Some(err) = result.err {
                    // TODO: handle DFS error by queue the failed operation and retry.
                    panic!("{:?}", err);
                }
                let id = result.change_set.shard_id;
                let ver = result.change_set.shard_ver;
                debug!(
                    "engine on flush result {}:{} {:?}",
                    id, ver, &result.change_set
                );
                self.meta_change_listener.on_change_set(result.change_set);
                let task = result.task;
                if let Some(normal) = &task.normal {
                    if normal.next_mem_tbl_size > 0 {
                        let mut change_size = new_change_set(task.shard_id, task.shard_ver);
                        change_size.set_next_mem_table_size(normal.next_mem_tbl_size);
                        self.meta_change_listener.on_change_set(change_size);
                    }
                }
            }
        }
    }
}
