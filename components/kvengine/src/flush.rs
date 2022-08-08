// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::collections::{HashMap, VecDeque};

use bytes::BytesMut;
use fail::fail_point;
use kvenginepb as pb;
use slog_global::info;
use tikv_util::{
    mpsc,
    time::{monotonic_raw_now, timespec_to_ns},
    Either,
};

use crate::{
    table::{memtable, memtable::CFTable, sstable, sstable::L0Builder},
    *,
};

#[derive(Clone)]
pub(crate) struct FlushTask {
    pub(crate) id_ver: IDVer,
    pub(crate) start: Vec<u8>,
    pub(crate) end: Vec<u8>,
    pub(crate) normal: Option<memtable::CFTable>,
    pub(crate) initial: Option<InitialFlush>,
}

impl FlushTask {
    fn new(
        shard: &Shard,
        normal: Option<memtable::CFTable>,
        initial: Option<InitialFlush>,
    ) -> Self {
        Self {
            id_ver: IDVer::new(shard.id, shard.ver),
            start: shard.start.to_vec(),
            end: shard.end.to_vec(),
            normal,
            initial,
        }
    }

    pub(crate) fn new_normal(shard: &Shard, mem_tbl: memtable::CFTable) -> Self {
        Self::new(shard, Some(mem_tbl), None)
    }

    pub(crate) fn new_initial(shard: &Shard, initial: InitialFlush) -> Self {
        Self::new(shard, None, Some(initial))
    }

    pub(crate) fn overlap_table(&self, start_key: &[u8], end_key: &[u8]) -> bool {
        self.start.as_slice() <= end_key && start_key < self.end.as_slice()
    }

    pub(crate) fn table_version(&self) -> u64 {
        match (&self.normal, &self.initial) {
            (Some(normal), None) => normal.get_version(),
            (None, Some(initial)) => initial.table_version(),
            _ => unreachable!(),
        }
    }
}

#[derive(Clone)]
pub(crate) struct InitialFlush {
    pub(crate) parent_snap: pb::Snapshot,
    pub(crate) mem_tbls: Vec<memtable::CFTable>,
    pub(crate) base_version: u64,
    pub(crate) data_sequence: u64,
}

impl InitialFlush {
    fn table_version(&self) -> u64 {
        self.base_version + self.data_sequence
    }
}

impl Engine {
    pub(crate) fn run_flush_worker(&self, rx: mpsc::Receiver<FlushMsg>) {
        let mut worker = FlushWorker {
            shards: Default::default(),
            receiver: rx,
            engine: self.clone(),
        };
        worker.run();
    }

    pub(crate) fn flush_normal(&self, task: FlushTask) -> Result<pb::ChangeSet> {
        fail_point!("kvengine_flush_normal", |_| Err(dfs::Error::Io(
            "injected error".to_string()
        )
        .into()));

        let mut cs = new_change_set(task.id_ver.id, task.id_ver.ver);
        let m = task.normal.as_ref().unwrap();
        let flush_version = m.get_version();
        let tag = ShardTag::new(self.get_engine_id(), task.id_ver);
        info!(
            "{} flush mem-table version {}, size {}",
            tag,
            flush_version,
            m.size(),
        );
        let flush = cs.mut_flush();
        flush.set_version(flush_version);
        if let Some(props) = m.get_properties() {
            flush.set_properties(props);
        }
        let l0_builder = self.build_l0_table(m, task.start.as_slice(), task.end.as_slice());
        if l0_builder.is_empty() {
            return Ok(cs);
        }
        let (tx, rx) = tikv_util::mpsc::bounded(1);
        self.persist_l0_table(l0_builder, tx, task.id_ver);
        let l0_create = rx.recv().unwrap()?;
        flush.set_l0_create(l0_create);
        Ok(cs)
    }

    pub(crate) fn flush_initial(&self, task: FlushTask) -> Result<pb::ChangeSet> {
        let flush = task.initial.as_ref().unwrap();
        let tag = ShardTag::new(self.get_engine_id(), task.id_ver);
        info!(
            "{} initial flush {} mem-tables, base_version {}, data_sequence {}",
            tag,
            flush.mem_tbls.len(),
            flush.base_version,
            flush.data_sequence
        );
        let mut cs = new_change_set(task.id_ver.id, task.id_ver.ver);
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
            self.persist_l0_table(l0_builder, tx.clone(), task.id_ver);
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
        let fid = self.id_allocator.alloc_id(1).pop().unwrap();
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
        id_ver: IDVer,
    ) {
        let l0_data = l0_builder.finish();
        let (smallest, biggest) = l0_builder.smallest_biggest();
        let fs = self.fs.clone();
        self.fs.get_runtime().spawn(async move {
            let res = fs
                .create(
                    l0_builder.get_fid(),
                    l0_data,
                    dfs::Options::new(id_ver.id, id_ver.ver),
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

pub(crate) enum FlushMsg {
    /// Task is send when trigger_flush is called.
    Task(FlushTask),

    /// Result is sent from the background flush thread when a flush task is finished.
    Result(FlushResult),

    /// Committed message is sent when a flush is committed to the raft group, so we
    /// can notify the next finished task.
    Committed((IDVer, u64)),

    /// Clear message is sent when a shard changed its version or set to inactive.
    /// Then all the previous tasks will be discarded.
    /// This simplifies the logic, avoid race condition.
    Clear(u64),
}

// FlushManager manages the flush tasks, make them concurrent and ensure the order for each shard.
pub(crate) struct FlushWorker {
    shards: HashMap<u64, ShardTaskManager>,
    receiver: mpsc::Receiver<FlushMsg>,
    engine: Engine,
}

impl FlushWorker {
    fn run(&mut self) {
        while let Ok(msg) = self.receiver.recv() {
            match msg {
                FlushMsg::Task(task) => {
                    let task_manager = self.get_shard_task_manager(task.id_ver.id);
                    if task_manager.enqueue_task(&task) {
                        let term = task_manager.term;
                        self.spawn_flush_task(task, term);
                    }
                }
                FlushMsg::Result(res) => {
                    let tag = ShardTag::new(self.engine.get_engine_id(), res.id_ver);
                    let task_manager = self.get_shard_task_manager(res.id_ver.id);
                    match task_manager.handle_flush_result(tag, res) {
                        Either::Left(finished) => {
                            if let Some(finished) = finished {
                                self.engine.meta_change_listener.on_change_set(finished);
                            }
                        }
                        Either::Right(task) => {
                            let term = task_manager.term;
                            self.spawn_flush_task(task, term);
                        }
                    }
                }
                FlushMsg::Committed((id_ver, table_version)) => {
                    let task_manager = self.get_shard_task_manager(id_ver.id);
                    if let Some(finished) = task_manager.handle_committed(table_version) {
                        self.engine.meta_change_listener.on_change_set(finished);
                    }
                }
                FlushMsg::Clear(shard_id) => {
                    self.shards.remove(&shard_id);
                }
            }
        }
    }

    fn get_shard_task_manager(&mut self, id: u64) -> &mut ShardTaskManager {
        self.shards
            .entry(id)
            .or_insert_with(|| ShardTaskManager::new())
    }

    fn spawn_flush_task(&mut self, task: FlushTask, term: u64) {
        let engine = self.engine.clone();
        std::thread::spawn(move || {
            let table_version = task.table_version();
            let id_ver = task.id_ver;
            let res = if task.normal.is_some() {
                engine.flush_normal(task)
            } else {
                engine.flush_initial(task)
            };
            engine
                .flush_tx
                .send(FlushMsg::Result(FlushResult {
                    id_ver,
                    table_version,
                    term,
                    res,
                }))
                .unwrap();
        });
    }
}

/// ShardTaskManager manage flush tasks for a shard.
#[derive(Default)]
pub(crate) struct ShardTaskManager {
    /// When a Shard is set to inactive, all the running tasks should be discarded, then when
    /// it is set to active again, old result may arrive and conflict with the new tasks.
    /// So we use term to detect and discard obsolete tasks.
    term: u64,
    /// task_queue contains the running tasks.
    /// Incoming flush tasks are pushed back to the queue.
    task_queue: VecDeque<FlushTask>,
    /// finished contains tasks that successfully flushed, but not yet notified to the meta listener.
    finished: HashMap<u64, kvenginepb::ChangeSet>,
    /// The flush notified the meta listener but not yet committed.
    notified: Option<kvenginepb::ChangeSet>,
    /// committed_table_version is updated when the notified change set is committed
    /// in the raft group.
    committed_table_version: u64,
}

impl ShardTaskManager {
    fn new() -> Self {
        let term = timespec_to_ns(monotonic_raw_now());
        Self {
            term,
            ..Default::default()
        }
    }

    fn enqueue_task(&mut self, task: &FlushTask) -> bool {
        if task.table_version() <= self.last_enqueued_table_version() {
            return false;
        }
        self.task_queue.push_back(task.clone());
        true
    }

    fn last_enqueued_table_version(&self) -> u64 {
        if let Some(task) = self.task_queue.back() {
            task.table_version()
        } else if let Some(notified) = self.notified.as_ref() {
            change_set_table_version(notified)
        } else {
            self.committed_table_version
        }
    }

    fn handle_flush_result(
        &mut self,
        tag: ShardTag,
        res: FlushResult,
    ) -> Either<Option<kvenginepb::ChangeSet>, FlushTask> {
        if self.term != res.term {
            info!("{} discard old term flush result {:?}", tag, res.res);
            return Either::Left(None);
        }
        let table_version = res.table_version;
        match res.res {
            Ok(cs) => {
                self.finished.insert(table_version, cs);
                Either::Left(
                    self.notified
                        .is_none()
                        .then(|| self.take_finished_task_for_notify())
                        .flatten(),
                )
            }
            Err(err) => {
                info!(
                    "{} flush task failed, table version {}, error {:?}, retrying",
                    tag, res.table_version, err
                );
                let task = self
                    .task_queue
                    .iter()
                    .find(|task| task.table_version() == table_version)
                    .expect("failed task should exist")
                    .clone();
                Either::Right(task)
            }
        }
    }

    fn handle_committed(&mut self, table_version: u64) -> Option<kvenginepb::ChangeSet> {
        if self.committed_table_version < table_version {
            self.committed_table_version = table_version;
        }
        if let Some(notified) = self.notified.take() {
            let notified_table_version = change_set_table_version(&notified);
            if notified_table_version == table_version {
                return self.take_finished_task_for_notify();
            }
            self.notified = Some(notified);
        }
        None
    }

    fn take_finished_task_for_notify(&mut self) -> Option<kvenginepb::ChangeSet> {
        if let Some(task) = self.task_queue.front() {
            if let Some(cs) = self.finished.remove(&task.table_version()) {
                self.task_queue.pop_front();
                self.notified = Some(cs.clone());
                return Some(cs);
            }
        }
        None
    }
}

pub(crate) fn change_set_table_version(cs: &kvenginepb::ChangeSet) -> u64 {
    if cs.has_flush() {
        return cs.get_flush().version;
    } else if cs.has_initial_flush() {
        let initial_flush = cs.get_initial_flush();
        return initial_flush.base_version + initial_flush.data_sequence;
    }
    unreachable!("unexpected change set {:?}", cs);
}

pub(crate) struct FlushResult {
    id_ver: IDVer,
    table_version: u64,
    term: u64,
    res: Result<kvenginepb::ChangeSet>,
}
