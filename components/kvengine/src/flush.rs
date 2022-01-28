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
    pub(crate) split_stage: pb::SplitStage,
    pub(crate) mem_tbl: memtable::CFTable,
    pub(crate) next_mem_tbl_size: u64,
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
                let result_task = self.flush_mem_table(task);
                if let Err(err) = result_tx.send(result_task) {
                    error!("flush worker failed to send result {:?}", err);
                }
            } else {
                break;
            }
        }
    }

    pub(crate) fn flush_mem_table(&self, task: FlushTask) -> FlushResultRx {
        let (res_tx, res_rx) = mpsc::bounded(1);
        let mut cs = new_change_set(task.shard_id, task.shard_ver, task.split_stage);
        let flush_version = task.mem_tbl.get_version();
        debug!(
            "{}:{} flush version {}",
            flush_version, task.shard_id, task.shard_ver
        );
        cs.mut_flush().set_version(flush_version);
        if let Some(props) = task.mem_tbl.get_properties() {
            cs.mut_flush().set_properties(props);
        }
        let mut result = FlushResult {
            change_set: cs,
            err: None,
            task,
        };
        let task = &result.task;
        if task.mem_tbl.is_empty() {
            res_tx.send(result).unwrap();
            return res_rx;
        }
        // TODO: handle alloc_id error.
        let fid = self.id_allocator.alloc_id(1).unwrap().pop().unwrap();
        let m = &task.mem_tbl;

        let mut l0_builder = sstable::L0Builder::new(
            fid,
            self.opts.table_builder_options,
            task.mem_tbl.get_version(),
        );
        for cf in 0..NUM_CFS {
            let skl = m.get_cf(cf);
            if skl.is_empty() {
                continue;
            }
            let mut it = skl.new_iterator(false);
            // If CF is not managed, we only need to keep the latest version.
            let rc = !CF_MANAGED[cf];
            let mut prev_key = BytesMut::new();
            it.rewind();
            while it.valid() {
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
                if task.next_mem_tbl_size > 0 {
                    let mut change_size =
                        new_change_set(task.shard_id, task.shard_ver, task.split_stage);
                    change_size.set_next_mem_table_size(task.next_mem_tbl_size);
                    self.meta_change_listener.on_change_set(change_size);
                }
            }
        }
    }
}
