// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::{collections::HashMap, time::Duration};

use bytes::BytesMut;
use kvenginepb as pb;
use slog_global::info;
use tikv_util::{mpsc, time::Instant};

use crate::{
    table::{memtable, memtable::CFTable, sstable, sstable::L0Builder},
    *,
};

pub(crate) struct FlushTask {
    pub(crate) shard_id: u64,
    pub(crate) shard_ver: u64,
    pub(crate) start: Vec<u8>,
    pub(crate) end: Vec<u8>,
    pub(crate) normal: Option<memtable::CFTable>,
    pub(crate) initial: Option<InitialFlush>,
}

impl FlushTask {
    pub(crate) fn overlap_table(&self, start_key: &[u8], end_key: &[u8]) -> bool {
        self.start.as_slice() <= end_key && start_key < self.end.as_slice()
    }
}

pub(crate) struct InitialFlush {
    pub(crate) parent_snap: pb::Snapshot,
    pub(crate) mem_tbls: Vec<memtable::CFTable>,
    pub(crate) base_version: u64,
    pub(crate) data_sequence: u64,
}

#[derive(Copy, Clone, Debug, Default, Hash, Eq, PartialEq)]
pub(crate) struct FlushStateKey {
    shard_id: u64,
    shard_ver: u64,
    tbl_version: u64,
}

const FLUSH_DEDUP_DURATION: Duration = Duration::from_secs(30);

impl Engine {
    pub(crate) fn run_flush_worker(&self, rx: mpsc::Receiver<FlushTask>) {
        let mut flush_states = HashMap::new();
        while let Ok(task) = rx.recv() {
            let tbl_version = match task.normal.as_ref() {
                None => 0,
                Some(tbl) => tbl.get_version(),
            };
            let flush_state_key = FlushStateKey {
                shard_id: task.shard_id,
                shard_ver: task.shard_ver,
                tbl_version,
            };
            let now = Instant::now();
            let mut min_flush_time = now - FLUSH_DEDUP_DURATION;
            flush_states.retain(|_, flush_time: &mut Instant| flush_time.gt(&&mut min_flush_time));
            if flush_states.get(&flush_state_key).is_some() {
                continue;
            }
            flush_states.insert(flush_state_key, now);
            let engine = self.clone();
            std::thread::spawn(move || {
                let res = if task.normal.is_some() {
                    engine.flush_normal(task)
                } else {
                    engine.flush_initial(task)
                };
                match res {
                    Ok(cs) => {
                        engine.meta_change_listener.on_change_set(cs);
                    }
                    Err(err) => {
                        // TODO: handle DFS error by queue the failed operation and retry.
                        panic!("{:?}", err);
                    }
                }
            });
        }
    }

    pub(crate) fn flush_normal(&self, task: FlushTask) -> Result<pb::ChangeSet> {
        let (shard_id, shard_ver) = (task.shard_id, task.shard_ver);
        let mut cs = new_change_set(shard_id, shard_ver);
        let m = task.normal.as_ref().unwrap();
        let flush_version = m.get_version();
        info!(
            "{}:{} flush mem-table version {}, size {}",
            shard_id,
            shard_ver,
            flush_version,
            m.size(),
        );
        let flush = cs.mut_flush();
        flush.set_version(flush_version);
        if let Some(props) = m.get_properties() {
            flush.set_properties(props);
        }
        let l0_builder = self.build_l0_table(m, task.start.as_slice(), task.end.as_slice());
        let (tx, rx) = tikv_util::mpsc::bounded(1);
        self.persist_l0_table(l0_builder, tx, shard_id, shard_ver);
        let l0_create = rx.recv().unwrap()?;
        flush.set_l0_create(l0_create);
        Ok(cs)
    }

    pub(crate) fn flush_initial(&self, task: FlushTask) -> Result<pb::ChangeSet> {
        let (shard_id, shard_ver) = (task.shard_id, task.shard_ver);
        let flush = task.initial.as_ref().unwrap();
        info!(
            "{}:{} initial flush {} mem-tables, base_version {}, data_sequence {}",
            shard_id,
            shard_ver,
            flush.mem_tbls.len(),
            flush.base_version,
            flush.data_sequence
        );
        let mut cs = new_change_set(shard_id, shard_ver);
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
            let l0_builder = self.build_l0_table(m, &task.start, &task.end);
            l0_builders.push(l0_builder);
        }
        let num_mem_tables = l0_builders.len();
        let (tx, rx) = tikv_util::mpsc::bounded(num_mem_tables);
        for l0_builder in l0_builders {
            self.persist_l0_table(l0_builder, tx.clone(), shard_id, shard_ver);
        }
        let mut errs = vec![];
        for _ in 0..num_mem_tables {
            match rx.recv().unwrap() {
                Ok(l0_create) => {
                    initial_flush.mut_l0_creates().push(l0_create);
                }
                Err(err) => {
                    errs.push(err);
                }
            }
        }
        if errs.is_empty() {
            return Ok(cs);
        }
        Err(errs.pop().unwrap())
    }

    pub(crate) fn build_l0_table(&self, m: &CFTable, start: &[u8], end: &[u8]) -> L0Builder {
        // TODO: handle alloc_id error.
        let fid = self.id_allocator.alloc_id(1).unwrap().pop().unwrap();

        let mut l0_builder = sstable::L0Builder::new(
            fid,
            self.opts.table_builder_options.block_size,
            m.get_version(),
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
            it.seek(start);
            while it.valid() {
                if it.key() >= end {
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
        l0_builder
    }

    pub(crate) fn persist_l0_table(
        &self,
        mut l0_builder: L0Builder,
        tx: tikv_util::mpsc::Sender<Result<pb::L0Create>>,
        shard_id: u64,
        shard_ver: u64,
    ) {
        let l0_data = l0_builder.finish();
        let (smallest, biggest) = l0_builder.smallest_biggest();
        let fs = self.fs.clone();
        self.fs.get_runtime().spawn(async move {
            let res = fs
                .create(
                    l0_builder.get_fid(),
                    l0_data,
                    dfs::Options::new(shard_id, shard_ver),
                )
                .await;
            if let Err(e) = res {
                tx.send(Err(e.into())).unwrap();
                return;
            }
            let mut l0_create = pb::L0Create::new();
            l0_create.set_id(l0_builder.get_fid());
            l0_create.set_smallest(smallest.to_vec());
            l0_create.set_biggest(biggest.to_vec());
            tx.send(Ok(l0_create)).unwrap();
        });
    }
}
