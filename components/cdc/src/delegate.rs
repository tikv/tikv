// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    collections::btree_map::{BTreeMap, Entry as BTreeMapEntry},
    fmt,
    ops::Bound,
    result::Result as StdResult,
    string::String,
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

use api_version::{ApiV2, KeyMode, KvFormat};
use collections::HashMap;
use crossbeam::atomic::AtomicCell;
use fail::fail_point;
use futures::{
    channel::mpsc::{self, UnboundedReceiver, UnboundedSender},
    lock::Mutex,
    StreamExt,
};
use kvproto::{
    cdcpb::{ChangeDataRequestKvApi, Error as ErrorEvent, EventLogType, EventRow, EventRowOpType},
    kvrpcpb::ExtraOp as TxnExtraOp,
    metapb::{Region, RegionEpoch},
    raft_cmdpb::{AdminCmdType, AdminRequest, AdminResponse, CmdType, PutRequest, Request},
};
use raftstore::{
    coprocessor::{Cmd, CmdBatch, ObserveHandle, ObserveId},
    store::util::compare_region_epoch,
    Error as RaftStoreError,
};
use tikv::storage::{txn::TxnEntry, Statistics};
use tikv_util::{
    debug, info,
    memory::{HeapSize, MemoryQuota},
    time::Instant,
    warn,
    worker::Scheduler,
};
use txn_types::{Key, Lock, LockType, TimeStamp, WriteBatchFlags, WriteRef, WriteType};

use crate::{
    channel::{DownstreamSink, CDC_EVENT_MAX_BYTES},
    endpoint::{Deregister, Task},
    initializer::KvEntry,
    metrics::*,
    old_value::{OldValueCache, OldValueCallback},
    service::{ConnId, RequestId},
    txn_source::TxnSource,
    Error, Result,
};

static DOWNSTREAM_ID_ALLOC: AtomicUsize = AtomicUsize::new(0);

/// A unique identifier of a Downstream.
#[derive(Clone, Copy, Debug, PartialEq, Hash)]
pub struct DownstreamId(usize);

impl DownstreamId {
    pub fn new() -> DownstreamId {
        DownstreamId(DOWNSTREAM_ID_ALLOC.fetch_add(1, Ordering::SeqCst))
    }
}

impl Default for DownstreamId {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum DownstreamState {
    /// It's just created and rejects change events and resolved timestamps.
    Uninitialized,
    /// It has got a snapshot for incremental scan, and change events will be
    /// accepted. However, it still rejects resolved timestamps.
    Initializing,
    /// Incremental scan is finished so that resolved timestamps are acceptable
    /// now.
    Normal,
    Stopped,
}

impl Default for DownstreamState {
    fn default() -> Self {
        Self::Uninitialized
    }
}

/// Should only be called when it's uninitialized or stopped. Return false if
/// it's stopped.
pub(crate) fn on_init_downstream(s: &AtomicCell<DownstreamState>) -> bool {
    s.compare_exchange(
        DownstreamState::Uninitialized,
        DownstreamState::Initializing,
    )
    .is_ok()
}

/// Should only be called when it's initializing or stopped. Return false if
/// it's stopped.
pub(crate) fn post_init_downstream(s: &AtomicCell<DownstreamState>) -> bool {
    s.compare_exchange(DownstreamState::Initializing, DownstreamState::Normal)
        .is_ok()
}

impl DownstreamState {
    pub fn ready_for_change_events(&self) -> bool {
        match *self {
            DownstreamState::Uninitialized | DownstreamState::Stopped => false,
            DownstreamState::Initializing | DownstreamState::Normal => true,
        }
    }

    pub fn ready_for_advancing_ts(&self) -> bool {
        match *self {
            DownstreamState::Normal => true,

            DownstreamState::Uninitialized
            | DownstreamState::Stopped
            | DownstreamState::Initializing => false,
        }
    }
}

pub struct Downstream {
    /// A unique identifier of the Downstream.
    pub id: DownstreamId,
    /// The request ID set by CDC to identify events corresponding different
    /// requests.
    pub req_id: RequestId,
    pub conn_id: ConnId,

    /// The IP address of downstream.
    pub peer: String,
    pub region_epoch: RegionEpoch,

    pub kv_api: ChangeDataRequestKvApi,
    pub filter_loop: bool,
    pub observed_range: ObservedRange,

    pub sink: DownstreamSink,
    state: Arc<AtomicCell<DownstreamState>>,

    // Fields to handle ResolvedTs advancing. If `lock_heap` is none it means
    // the downstream hasn't finished the incremental scanning.
    lock_heap: Option<BTreeMap<TimeStamp, isize>>,
    pub advanced_to: Arc<AtomicU64>,
}

impl fmt::Debug for Downstream {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Downstream")
            .field("id", &self.id)
            .field("req_id", &self.req_id)
            .field("conn_id", &self.conn_id)
            .finish()
    }
}

impl Downstream {
    /// Create a Downstream.
    ///
    /// peer is the address of the downstream.
    /// sink sends data to the downstream.
    pub fn new(
        req_id: RequestId,
        conn_id: ConnId,
        peer: String,
        region_epoch: RegionEpoch,
        kv_api: ChangeDataRequestKvApi,
        filter_loop: bool,
        observed_range: ObservedRange,
        sink: DownstreamSink,
    ) -> Downstream {
        Downstream {
            id: DownstreamId::new(),
            req_id,
            conn_id,

            peer,
            region_epoch,

            kv_api,
            filter_loop,
            observed_range,

            sink,
            state: Arc::new(AtomicCell::new(DownstreamState::default())),

            lock_heap: None,
            advanced_to: Arc::new(AtomicU64::new(0)),
        }
    }

    pub fn get_state(&self) -> Arc<AtomicCell<DownstreamState>> {
        self.state.clone()
    }
}

// In `PendingLock`,  `key` is encoded.
pub enum PendingLock {
    Track { key: Key, start_ts: MiniLock },
    Untrack { key: Key, start_ts: TimeStamp },
}

impl HeapSize for PendingLock {
    fn approximate_heap_size(&self) -> usize {
        match self {
            PendingLock::Track { key, .. } | PendingLock::Untrack { key, .. } => {
                key.approximate_heap_size()
            }
        }
    }
}

pub enum LockTracker {
    Pending,
    Preparing(Vec<PendingLock>),
    Prepared {
        region: Region,
        locks: BTreeMap<Key, MiniLock>,
    },
}

impl fmt::Debug for LockTracker {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LockTracker::Pending => write!(f, "LockTracker::Pending"),
            LockTracker::Preparing(ref locks) => {
                write!(f, "LockTracker::Preparing({})", locks.len())
            }
            LockTracker::Prepared { locks, .. } => {
                write!(f, "LockTracker::Prepared({})", locks.len())
            }
        }
    }
}

/// `MiniLock` is like `Lock`, but only contains fields that CDC cares about.
#[derive(Eq, PartialEq, Debug)]
pub struct MiniLock {
    pub ts: TimeStamp,
    pub txn_source: u64,
    pub generation: u64,
}

impl MiniLock {
    pub fn new<T>(ts: T, txn_source: u64, generation: u64) -> Self
    where
        TimeStamp: From<T>,
    {
        MiniLock {
            ts: TimeStamp::from(ts),
            txn_source,
            generation,
        }
    }

    #[cfg(test)]
    pub fn from_ts<T>(ts: T) -> Self
    where
        TimeStamp: From<T>,
    {
        MiniLock {
            ts: TimeStamp::from(ts),
            txn_source: 0,
            generation: 0,
        }
    }
}

/// A CDC delegate of a raftstore region peer.
///
/// It converts raft commands into CDC events and broadcast to downstreams.
/// It also tracks transactions on the fly in order to compute resolved ts.
pub struct Delegate {
    pub region_id: u64,
    pub handle: ObserveHandle,
    pub sched: UnboundedSender<DelegateTask>,

    tasks: UnboundedReceiver<DelegateTask>,
    feedbacks: Scheduler<Task>,
    memory_quota: Arc<MemoryQuota>,
    old_value_cache: Arc<Mutex<OldValueCache>>,
    txn_extra_op: Arc<AtomicCell<TxnExtraOp>>,

    lock_tracker: LockTracker,
    downstreams: Vec<Downstream>,

    old_value_stats: Statistics,
    created: Instant,
    last_lag_warn: Instant,
}

impl Drop for Delegate {
    fn drop(&mut self) {
        match &self.lock_tracker {
            LockTracker::Pending => {}
            LockTracker::Preparing(locks) => {
                let mut free_bytes = 0;
                for lock in locks {
                    free_bytes += lock.approximate_heap_size();
                }
                self.memory_quota.free(free_bytes);
                CDC_PENDING_BYTES_GAUGE.sub(free_bytes as _);
            }
            LockTracker::Prepared { locks, .. } => {
                let mut free_bytes = 0;
                for lock in locks.keys() {
                    free_bytes += lock.approximate_heap_size();
                }
                self.memory_quota.free(free_bytes);
                CDC_PENDING_BYTES_GAUGE.sub(free_bytes as _);
            }
        }
    }
}

impl Delegate {
    /// Create a Delegate the given region.
    pub fn new(
        region_id: u64,
        feedbacks: Scheduler<Task>,
        memory_quota: Arc<MemoryQuota>,
        old_value_cache: Arc<Mutex<OldValueCache>>,
        txn_extra_op: Arc<AtomicCell<TxnExtraOp>>,
    ) -> Delegate {
        let (tx, rx) = mpsc::unbounded();
        Delegate {
            region_id,
            handle: ObserveHandle::new(),
            sched: tx,

            tasks: rx,
            feedbacks,
            memory_quota,
            old_value_cache,
            txn_extra_op,

            lock_tracker: LockTracker::Pending,
            downstreams: Vec::new(),

            old_value_stats: Statistics::default(),
            created: Instant::now_coarse(),
            last_lag_warn: Instant::now_coarse(),
        }
    }

    fn push_lock(&mut self, key: Key, start_ts: MiniLock) -> Result<isize> {
        let bytes = key.approximate_heap_size();
        let mut lock_count_modify = 0;
        match &mut self.lock_tracker {
            LockTracker::Pending => unreachable!(),
            LockTracker::Preparing(locks) => {
                self.memory_quota.alloc(bytes)?;
                CDC_PENDING_BYTES_GAUGE.add(bytes as _);
                locks.push(PendingLock::Track { key, start_ts });
            }
            LockTracker::Prepared { locks, .. } => match locks.entry(key) {
                BTreeMapEntry::Occupied(mut x) => {
                    assert_eq!(x.get().ts, start_ts.ts);
                    assert!(x.get().generation <= start_ts.generation);
                    x.get_mut().generation = start_ts.generation;
                }
                BTreeMapEntry::Vacant(x) => {
                    x.insert(start_ts);
                    self.memory_quota.alloc(bytes)?;
                    CDC_PENDING_BYTES_GAUGE.add(bytes as _);
                    lock_count_modify = 1;
                }
            },
        }
        Ok(lock_count_modify)
    }

    fn pop_lock(&mut self, key: Key, start_ts: TimeStamp) -> Result<isize> {
        let mut lock_count_modify = 0;
        match &mut self.lock_tracker {
            LockTracker::Pending => unreachable!(),
            LockTracker::Preparing(locks) => {
                let bytes = key.approximate_heap_size();
                self.memory_quota.alloc(bytes)?;
                CDC_PENDING_BYTES_GAUGE.add(bytes as _);
                locks.push(PendingLock::Untrack { key, start_ts });
            }
            LockTracker::Prepared { locks, .. } => {
                if let BTreeMapEntry::Occupied(x) = locks.entry(key) {
                    if x.get().ts == start_ts {
                        let (key, _) = x.remove_entry();
                        let bytes = key.approximate_heap_size();
                        self.memory_quota.free(bytes);
                        CDC_PENDING_BYTES_GAUGE.sub(bytes as _);
                        lock_count_modify = -1;
                    }
                }
            }
        }
        Ok(lock_count_modify)
    }

    fn init_lock_tracker(&mut self) -> bool {
        if matches!(self.lock_tracker, LockTracker::Pending) {
            self.lock_tracker = LockTracker::Preparing(vec![]);
            return true;
        }
        false
    }

    fn finish_prepare_lock_tracker(
        &mut self,
        region: Region,
        mut locks: BTreeMap<Key, MiniLock>,
    ) -> Result<()> {
        let delta_locks = match std::mem::replace(&mut self.lock_tracker, LockTracker::Pending) {
            LockTracker::Preparing(locks) => locks,
            _ => unreachable!(),
        };

        let mut free_bytes = 0usize;
        for delta_lock in delta_locks {
            free_bytes += delta_lock.approximate_heap_size();
            match delta_lock {
                PendingLock::Track { key, start_ts } => match locks.entry(key) {
                    BTreeMapEntry::Vacant(x) => {
                        x.insert(start_ts);
                    }
                    BTreeMapEntry::Occupied(x) => {
                        assert_eq!(x.get().ts, start_ts.ts);
                        assert!(x.get().generation <= start_ts.generation);
                    }
                },
                PendingLock::Untrack { key, start_ts } => {
                    if let BTreeMapEntry::Occupied(x) = locks.entry(key) {
                        if x.get().ts == start_ts {
                            x.remove();
                        }
                    }
                }
            }
        }
        self.memory_quota.free(free_bytes);
        CDC_PENDING_BYTES_GAUGE.sub(free_bytes as _);

        let mut alloc_bytes = 0usize;
        for key in locks.keys() {
            alloc_bytes += key.approximate_heap_size();
        }
        self.memory_quota.alloc(alloc_bytes)?;
        CDC_PENDING_BYTES_GAUGE.add(alloc_bytes as _);

        self.lock_tracker = LockTracker::Prepared { region, locks };
        Ok(())
    }

    fn finish_scan_locks(
        &mut self,
        region: Region,
        locks: BTreeMap<Key, MiniLock>,
    ) -> Result<Vec<(Downstream, Error)>> {
        fail::fail_point!("cdc_finish_scan_locks_memory_quota_exceed", |_| Err(
            Error::MemoryQuotaExceeded(tikv_util::memory::MemoryQuotaExceeded)
        ));

        info!("cdc region is ready"; "region_id" => self.region_id);
        self.finish_prepare_lock_tracker(region, locks)?;

        let region = match &self.lock_tracker {
            LockTracker::Prepared { region, .. } => region,
            _ => unreachable!(),
        };

        // Check observed key range in region.
        let mut failed_downstreams = Vec::new();
        for mut downstream in std::mem::take(&mut self.downstreams) {
            downstream.observed_range.update_region_key_range(region);
            if let Err(e) = Self::check_epoch_on_ready(&downstream, region) {
                failed_downstreams.push((downstream, e));
            } else {
                self.downstreams.push(downstream);
            }
        }
        Ok(failed_downstreams)
    }

    fn subscribe(&mut self, downstream: Downstream) -> StdResult<(), (Error, Downstream)> {
        if let LockTracker::Prepared { ref region, .. } = &self.lock_tracker {
            // Check if the downstream is outdated.
            if let Err(e) = Self::check_epoch_on_ready(&downstream, region) {
                return Err((e, downstream));
            }
        }
        self.add_downstream(downstream);
        Ok(())
    }

    fn on_min_ts(&mut self, min_ts: TimeStamp, current_ts: TimeStamp) {
        let locks = match &self.lock_tracker {
            LockTracker::Prepared { locks, .. } => locks,
            _ => {
                let now = Instant::now_coarse();
                let elapsed = now.duration_since(self.created);
                if elapsed > WARN_LAG_THRESHOLD
                    && now.duration_since(self.last_lag_warn) > WARN_LAG_INTERVAL
                {
                    warn!(
                        "cdc region scan locks too slow";
                        "region_id" => self.region_id,
                        "elapsed" => ?elapsed,
                        "stage" => ?self.lock_tracker,
                    );
                    self.last_lag_warn = now;
                }
                return;
            }
        };

        let handle_downstream = |downstream: &mut Downstream| {
            if !downstream.state.load().ready_for_advancing_ts() {
                return;
            }

            if downstream.lock_heap.is_none() {
                let mut lock_heap = BTreeMap::<TimeStamp, isize>::new();
                for (_, lock) in locks.range(downstream.observed_range.to_range()) {
                    let lock_count = lock_heap.entry(lock.ts).or_default();
                    *lock_count += 1;
                }
                downstream.lock_heap = Some(lock_heap);
            }

            let lock_heap = downstream.lock_heap.as_ref().unwrap();
            let min_lock = lock_heap.keys().next().cloned().unwrap_or(min_ts);
            let advanced_to = std::cmp::min(min_lock, min_ts).into_inner();
            if advanced_to > downstream.advanced_to.load(Ordering::Acquire) {
                downstream.advanced_to.store(advanced_to, Ordering::Release);
            }
        };

        let mut slow_downstreams = Vec::new();
        for d in &mut self.downstreams {
            handle_downstream(d);
            let advanced_to = d.advanced_to.load(Ordering::Relaxed);
            if advanced_to > 0 {
                let lag = current_ts
                    .physical()
                    .saturating_sub(TimeStamp::from(advanced_to).physical());
                if Duration::from_millis(lag) > WARN_LAG_THRESHOLD {
                    slow_downstreams.push(d.id);
                }
            }
        }

        if !slow_downstreams.is_empty() {
            let now = Instant::now_coarse();
            if now.duration_since(self.last_lag_warn) > WARN_LAG_INTERVAL {
                warn!(
                    "cdc region downstreams are too slow";
                    "region_id" => self.region_id,
                    "downstreams" => ?slow_downstreams,
                );
                self.last_lag_warn = now;
            }
        }
    }

    async fn on_batch(&mut self, batch: CmdBatch, old_value_cb: OldValueCallback) -> Result<()> {
        if batch.cdc_id != self.handle.id {
            return Ok(());
        }
        for cmd in batch.into_iter(self.region_id) {
            let Cmd {
                index,
                term: _,
                mut request,
                mut response,
            } = cmd;
            if response.get_header().has_error() {
                let err_header = response.mut_header().take_error();
                return Err(Error::request(err_header));
            }
            if !request.has_admin_request() {
                let flags = WriteBatchFlags::from_bits_truncate(request.get_header().get_flags());
                self.sink_data(index, request.requests.into(), flags, &old_value_cb)
                    .await?;
            } else {
                self.sink_admin(request.take_admin_request(), response.take_admin_response())?;
            }
        }
        Ok(())
    }

    async fn sink_data(
        &mut self,
        index: u64,
        requests: Vec<Request>,
        flags: WriteBatchFlags,
        old_value_cb: &OldValueCallback,
    ) -> Result<()> {
        debug_assert_eq!(self.txn_extra_op.load(), TxnExtraOp::ReadOldValue);

        let mut rows_builder = RowsBuilder::default();
        rows_builder.is_one_pc = flags.contains(WriteBatchFlags::ONE_PC);
        for mut req in requests {
            match req.get_cmd_type() {
                CmdType::Put => self.sink_put(req.take_put(), &mut rows_builder).await?,
                _ => debug!("cdc skip other command";
                    "region_id" => self.region_id,
                    "command" => ?req),
            };
        }

        let (raws, txns) = rows_builder.finish_build();
        self.sink_downstream_raw(raws, index).await?;
        self.sink_downstream_tidb(txns, old_value_cb).await?;
        Ok(())
    }

    async fn sink_downstream_raw(&mut self, entries: Vec<EventRow>, index: u64) -> Result<()> {
        let mut downstreams = Vec::with_capacity(self.downstreams.len());
        for d in &mut self.downstreams {
            if d.kv_api == ChangeDataRequestKvApi::RawKv && d.state.load().ready_for_change_events()
            {
                downstreams.push(d);
            }
        }
        if downstreams.is_empty() {
            return Ok(());
        }

        for downstream in downstreams {
            let filtered_entries: Vec<_> = entries
                .iter()
                .filter(|x| downstream.observed_range.contains_raw_key(&x.key))
                .cloned()
                .collect();
            if filtered_entries.is_empty() {
                continue;
            }
            downstream
                .sink
                .send_observed_raw(index, filtered_entries)
                .await?;
        }
        Ok(())
    }

    async fn sink_downstream_tidb(
        &mut self,
        mut entries: Vec<RowInBuilding>,
        old_value_cb: &OldValueCallback,
    ) -> Result<()> {
        let mut downstreams = Vec::with_capacity(self.downstreams.len());
        for d in &mut self.downstreams {
            if d.kv_api == ChangeDataRequestKvApi::TiDb && d.state.load().ready_for_change_events()
            {
                downstreams.push(d);
            }
        }

        for downstream in downstreams {
            let mut filtered_entries = Vec::with_capacity(entries.len());
            for RowInBuilding {
                v,
                lock_count_modify,
                needs_old_value,
                ..
            } in &mut entries
            {
                if !downstream.observed_range.contains_raw_key(&v.key) {
                    continue;
                }
                if let Some(ts) = needs_old_value {
                    let key = Key::from_raw(&v.key).append_ts(v.start_ts.into());
                    let mut cache = self.old_value_cache.lock().await;
                    let old_value = old_value_cb(key, *ts, &mut *cache, &mut self.old_value_stats)?;
                    v.old_value = old_value.unwrap_or_default();
                    *needs_old_value = None;
                }

                if *lock_count_modify != 0 && downstream.lock_heap.is_some() {
                    let lock_heap = downstream.lock_heap.as_mut().unwrap();
                    match lock_heap.entry(v.start_ts.into()) {
                        BTreeMapEntry::Vacant(x) => {
                            x.insert(*lock_count_modify);
                        }
                        BTreeMapEntry::Occupied(mut x) => {
                            *x.get_mut() += *lock_count_modify;
                            assert!(
                                *x.get() >= 0,
                                "lock_count_modify should never be negative, start_ts: {}",
                                v.start_ts
                            );
                            if *x.get() == 0 {
                                x.remove();
                            }
                        }
                    }
                }

                if TxnSource::is_lightning_physical_import(v.txn_source)
                    || TxnSource::is_lossy_ddl_reorg_source_set(v.txn_source)
                    || downstream.filter_loop && TxnSource::is_cdc_write_source_set(v.txn_source)
                {
                    continue;
                }

                filtered_entries.push(v.clone());
            }
            if filtered_entries.is_empty() {
                continue;
            }
            downstream.sink.send_observed_tidb(filtered_entries).await?;
        }
        Ok(())
    }

    async fn sink_put(&mut self, put: PutRequest, rows_builder: &mut RowsBuilder) -> Result<()> {
        let key_mode = ApiV2::parse_key_mode(put.get_key());
        if key_mode == KeyMode::Raw {
            self.sink_raw_put(put, rows_builder)
        } else {
            self.sink_txn_put(put, rows_builder).await
        }
    }

    fn sink_raw_put(&mut self, mut put: PutRequest, rows: &mut RowsBuilder) -> Result<()> {
        let mut row = EventRow::default();
        decode_rawkv(put.take_key(), put.take_value(), &mut row)?;
        rows.raws.push(row);
        Ok(())
    }

    async fn sink_txn_put(&mut self, mut put: PutRequest, rows: &mut RowsBuilder) -> Result<()> {
        match put.cf.as_str() {
            "write" => {
                let key = Key::from_encoded_slice(&put.key).truncate_ts().unwrap();
                let row = rows.txns_by_key.entry(key.clone()).or_default();
                if decode_write(
                    put.take_key(),
                    &put.value,
                    &mut row.v,
                    &mut row.has_value,
                    true,
                ) {
                    return Ok(());
                }

                if rows.is_one_pc {
                    assert_eq!(row.v.r_type, EventLogType::Commit);
                    set_event_row_type(&mut row.v, EventLogType::Committed);
                    let read_old_ts = TimeStamp::from(row.v.commit_ts).prev();
                    row.needs_old_value = Some(read_old_ts);
                } else {
                    assert_eq!(row.lock_count_modify, 0);
                    let start_ts = TimeStamp::from(row.v.start_ts);
                    row.lock_count_modify = self.pop_lock(key, start_ts)?;
                }
            }
            "lock" => {
                let lock = Lock::parse(put.get_value()).unwrap();
                let for_update_ts = lock.for_update_ts;
                let txn_source = lock.txn_source;
                let generation = lock.generation;

                let key = Key::from_encoded_slice(&put.key);
                let row = rows.txns_by_key.entry(key.clone()).or_default();
                if decode_lock(put.take_key(), lock, &mut row.v, &mut row.has_value) {
                    return Ok(());
                }

                assert_eq!(row.lock_count_modify, 0);
                let mini_lock = MiniLock::new(row.v.start_ts, txn_source, generation);
                row.lock_count_modify = self.push_lock(key, mini_lock)?;
                let read_old_ts = std::cmp::max(for_update_ts, row.v.start_ts.into());
                row.needs_old_value = Some(read_old_ts);
            }
            "" | "default" => {
                let key = Key::from_encoded(put.take_key()).truncate_ts().unwrap();
                let row = rows.txns_by_key.entry(key).or_default();
                decode_default(put.take_value(), &mut row.v, &mut row.has_value);
            }
            other => panic!("invalid cf {}", other),
        }
        Ok(())
    }

    fn sink_admin(&mut self, request: AdminRequest, mut response: AdminResponse) -> Result<()> {
        let store_err = match request.get_cmd_type() {
            AdminCmdType::Split => RaftStoreError::EpochNotMatch(
                "split".to_owned(),
                vec![
                    response.mut_split().take_left(),
                    response.mut_split().take_right(),
                ],
            ),
            AdminCmdType::BatchSplit => RaftStoreError::EpochNotMatch(
                "batchsplit".to_owned(),
                response.mut_splits().take_regions().into(),
            ),
            AdminCmdType::PrepareMerge
            | AdminCmdType::CommitMerge
            | AdminCmdType::RollbackMerge => {
                RaftStoreError::EpochNotMatch("merge".to_owned(), vec![])
            }
            _ => return Ok(()),
        };
        Err(Error::request(store_err.into()))
    }

    fn add_downstream(&mut self, downstream: Downstream) {
        self.downstreams.push(downstream);
        self.txn_extra_op.store(TxnExtraOp::ReadOldValue);
    }

    fn check_epoch_on_ready(downstream: &Downstream, region: &Region) -> Result<()> {
        if let Err(e) = compare_region_epoch(
            &downstream.region_epoch,
            region,
            false, // check_conf_ver
            true,  // check_ver
            true,  // include_region
        ) {
            info!(
                "cdc fail to subscribe downstream";
                "region_id" => region.id,
                "downstream_id" => ?downstream.id,
                "conn_id" => ?downstream.conn_id,
                "req_id" => ?downstream.req_id,
                "err" => ?e
            );
            // Downstream is outdated, mark stop.
            downstream.state.store(DownstreamState::Stopped);
            return Err(Error::request(e.into()));
        }
        Ok(())
    }

    fn stop_observing(&self) {
        // Stop observe further events.
        self.handle.stop_observing();
        // To inform transaction layer no more old values are required for the region.
        self.txn_extra_op.store(TxnExtraOp::Noop);
    }

    pub fn meta(&self) -> DelegateMeta {
        DelegateMeta {
            region_id: self.region_id,
            handle: self.handle.clone(),
            sched: self.sched.clone(),
        }
    }

    pub async fn handle_tasks(&mut self) {
        while let Some(task) = self.tasks.next().await {
            match task {
                DelegateTask::Subscribe { downstream } => {
                    self.on_subscribe_downstream(downstream).await;
                }
                DelegateTask::ObservedEvent { cmds, old_value_cb } => {
                    fail_point!("cdc_before_handle_multi_batch", |_| {});
                    self.memory_quota.free(cmds.size());
                    if let Err(e) = self.on_batch(cmds, old_value_cb).await {
                        self.on_stop(Some(e)).await;
                    }
                    // flush_oldvalue_stats(&statistics, TAG_DELTA_CHANGE);
                }
                DelegateTask::Stop { err } => {
                    self.on_stop(err).await;
                }
                DelegateTask::StopDownstream { err, downstream_id } => {
                    self.on_stop_downstream(err, downstream_id).await;
                }
                DelegateTask::MinTs { min_ts, current_ts } => {
                    self.on_min_ts(min_ts, current_ts);
                }
                DelegateTask::FinishScanLocks {
                    observe_id,
                    region,
                    locks,
                } => self.on_finish_scan_locks(observe_id, region, locks).await,
                DelegateTask::InitDownstream {
                    observe_id,
                    downstream_id,
                    build_resolver,
                    cb,
                } => {
                    self.on_init_downstream(observe_id, downstream_id, build_resolver, cb)
                        .await;
                }
                DelegateTask::Validate(validate) => {
                    validate(Some(&self));
                }
            }
        }
    }

    async fn on_subscribe_downstream(&mut self, downstream: Downstream) {
        if let Err((err, downstream)) = self.subscribe(downstream) {
            let err_event = Some(err.into_error_event(self.region_id));
            self.deregister_downstream(err_event, downstream).await;
        }
    }

    async fn deregister_downstream_inner(
        &mut self,
        err_event: Option<ErrorEvent>,
        downstream: Downstream,
    ) -> bool {
        if let Some(err_event) = err_event {
            if let Err(err) = downstream.sink.cancel_by_error(err_event).await {
                warn!("cdc send region error failed";
                    "region_id" => self.region_id, "error" => ?err,
                    "downstream_id" => ?downstream.id, "downstream" => ?downstream.peer,
                    "request_id" => ?downstream.req_id, "conn_id" => ?downstream.conn_id);
            } else {
                info!("cdc send region error success";
                    "region_id" => self.region_id,
                    "downstream_id" => ?downstream.id, "downstream" => ?downstream.peer,
                    "request_id" => ?downstream.req_id, "conn_id" => ?downstream.conn_id);
            }
        }
        downstream.state.store(DownstreamState::Stopped);
        let _ = self
            .feedbacks
            .schedule_force(Task::Deregister(Deregister::Downstream {
                conn_id: downstream.conn_id,
                request_id: downstream.req_id,
                region_id: self.region_id,
                downstream_id: downstream.id,
            }));
        self.downstreams.is_empty()
    }

    async fn deregister_downstream(
        &mut self,
        err_event: Option<ErrorEvent>,
        downstream: Downstream,
    ) {
        if self
            .deregister_downstream_inner(err_event, downstream)
            .await
        {
            self.on_stop(None).await;
        }
    }

    async fn on_stop(&mut self, err: Option<Error>) {
        info!("cdc stop delegate"; "region_id" => self.region_id, "error" => ?err);
        self.stop_observing();
        let err_event = err.map(|x| x.into_error_event(self.region_id));
        while !self.downstreams.is_empty() {
            let downstream = self.downstreams.swap_remove(0);
            self.deregister_downstream_inner(err_event.clone(), downstream)
                .await;
        }
        let _ = self
            .feedbacks
            .schedule_force(Task::Deregister(Deregister::Delegate {
                region_id: self.region_id,
                observe_id: self.handle.id.clone(),
            }));
    }

    async fn on_stop_downstream(&mut self, err: Option<Error>, downstream_id: DownstreamId) {
        info!("cdc stop downstream"; "region_id" => self.region_id, "downstream_id" => ?downstream_id, "error" => ?err);
        if let Some(x) = self.downstreams.iter().position(|d| d.id == downstream_id) {
            let downstream = self.downstreams.swap_remove(x);
            let err_event = err.map(|x| x.into_error_event(self.region_id));
            self.deregister_downstream(err_event, downstream).await;
        }
    }

    async fn on_finish_scan_locks(
        &mut self,
        observe_id: ObserveId,
        region: Region,
        locks: BTreeMap<Key, MiniLock>,
    ) {
        if self.handle.id != observe_id {
            debug!("cdc stale region finish scan locks";
                "region_id" => region.get_id(),
                "observe_id" => ?observe_id,
                "current_id" => ?self.handle.id);
            return;
        }
        match self.finish_scan_locks(region, locks) {
            Ok(fails) => {
                for (downstream, err) in fails {
                    let err_event = Some(err.into_error_event(self.region_id));
                    self.deregister_downstream(err_event, downstream).await;
                }
            }
            Err(err) => self.on_stop(Some(err)).await,
        }
    }

    async fn on_init_downstream(
        &mut self,
        observe_id: ObserveId,
        downstream_id: DownstreamId,
        build_resolver: Arc<AtomicBool>,
        cb: Box<dyn FnOnce() + Send>,
    ) {
        if self.handle.id != observe_id {
            debug!("cdc stale region init downstream";
                "region_id" => self.region_id,
                "observe_id" => ?observe_id,
                "current_id" => ?self.handle.id);
            return;
        }
        if self.init_lock_tracker() {
            build_resolver.store(true, Ordering::Release);
        }
        if let Some(d) = self.downstreams.iter().find(|d| d.id == downstream_id) {
            let downstream_state = d.get_state();
            if on_init_downstream(&downstream_state) {
                info!("cdc downstream starts to initialize";
                    "region_id" => self.region_id,
                    "observe_id" => ?observe_id,
                    "downstream_id" => ?downstream_id);
            } else {
                warn!("cdc downstream fails to initialize: canceled";
                    "region_id" => self.region_id,
                    "observe_id" => ?observe_id,
                    "downstream_id" => ?downstream_id);
            }
        }
        cb();
    }

    /// Only used in tests.
    pub fn downstream_count(&self) -> usize {
        self.downstreams.len()
    }
}

#[derive(Default)]
struct RowsBuilder {
    txns_by_key: HashMap<Key, RowInBuilding>,

    raws: Vec<EventRow>,

    is_one_pc: bool,
}

#[derive(Default)]
struct RowInBuilding {
    v: EventRow,
    has_value: bool,
    lock_count_modify: isize,
    needs_old_value: Option<TimeStamp>,
}

impl RowsBuilder {
    fn finish_build(self) -> (Vec<EventRow>, Vec<RowInBuilding>) {
        let mut txns = Vec::with_capacity(self.txns_by_key.len());
        for row in self.txns_by_key.into_values() {
            if row.v.r_type == EventLogType::Prewrite
                && row.v.op_type == EventRowOpType::Put
                && !row.has_value
            {
                // It's possible that a prewrite command only contains lock but without
                // default. It's not documented by classic Percolator but introduced with
                // Large-Transaction. Those prewrites are not complete, we must skip them.
                continue;
            }
            txns.push(row);
        }
        (self.raws, txns)
    }
}

fn set_event_row_type(row: &mut EventRow, ty: EventLogType) {
    row.r_type = ty;
}

fn make_overlapped_rollback(key: Key, row: &mut EventRow) {
    // The current record's commit_ts is the rolled-back transaction's start_ts.
    row.start_ts = key.decode_ts().unwrap().into_inner();
    row.commit_ts = 0;
    row.key = key.truncate_ts().unwrap().into_raw().unwrap();
    row.op_type = EventRowOpType::Unknown as _;
    set_event_row_type(row, EventLogType::Rollback);
}

/// Decodes the write record and store its information in `row`. This may be
/// called both when doing incremental scan of observing apply events. There's
/// different behavior for the two case, distinguished by the `is_apply`
/// parameter.
fn decode_write(
    key: Vec<u8>,
    value: &[u8],
    row: &mut EventRow,
    has_value: &mut bool,
    is_apply: bool,
) -> bool {
    let key = Key::from_encoded(key);
    let write = WriteRef::parse(value).unwrap().to_owned();

    // For scanning, ignore the GC fence and read the old data;
    // For observed apply, drop the record it self but keep only the overlapped
    // rollback information if gc_fence exists.
    if is_apply && write.gc_fence.is_some() {
        // `gc_fence` is set means the write record has been rewritten.
        // Currently the only case is writing overlapped_rollback. And in this case
        assert!(write.has_overlapped_rollback);
        assert_ne!(write.write_type, WriteType::Rollback);
        make_overlapped_rollback(key, row);
        return false;
    }

    let (op_type, r_type) = match write.write_type {
        WriteType::Put => (EventRowOpType::Put, EventLogType::Commit),
        WriteType::Delete => (EventRowOpType::Delete, EventLogType::Commit),
        WriteType::Rollback => (EventRowOpType::Unknown, EventLogType::Rollback),
        other => {
            debug!("cdc skip write record"; "write" => ?other, "key" => %key);
            return true;
        }
    };
    let commit_ts = if write.write_type == WriteType::Rollback {
        assert_eq!(write.txn_source, 0);
        0
    } else {
        key.decode_ts().unwrap().into_inner()
    };
    row.start_ts = write.start_ts.into_inner();
    row.commit_ts = commit_ts;
    row.key = key.truncate_ts().unwrap().into_raw().unwrap();
    row.op_type = op_type as _;
    // used for filter out the event. see `txn_source` field for more detail.
    row.txn_source = write.txn_source;
    set_event_row_type(row, r_type);
    if let Some(value) = write.short_value {
        row.value = value;
        *has_value = true;
    }

    false
}

fn decode_lock(key: Vec<u8>, mut lock: Lock, row: &mut EventRow, has_value: &mut bool) -> bool {
    let key = Key::from_encoded(key);
    let op_type = match lock.lock_type {
        LockType::Put => EventRowOpType::Put,
        LockType::Delete => EventRowOpType::Delete,
        other => {
            debug!("cdc skip lock record"; "lock" => ?other, "key" => %key);
            return true;
        }
    };

    row.start_ts = lock.ts.into_inner();
    row.generation = lock.generation;
    row.key = key.into_raw().unwrap();
    row.op_type = op_type as _;
    row.txn_source = lock.txn_source;
    set_event_row_type(row, EventLogType::Prewrite);
    if let Some(value) = lock.short_value.take() {
        assert!(!*has_value, "unexpected lock with value: {:?}", lock);
        *has_value = true;
        row.value = value;
    }

    false
}

fn decode_rawkv(key: Vec<u8>, value: Vec<u8>, row: &mut EventRow) -> Result<()> {
    let (decoded_key, ts) = ApiV2::decode_raw_key_owned(Key::from_encoded(key), true)?;
    let decoded_value = ApiV2::decode_raw_value_owned(value)?;

    row.start_ts = ts.unwrap().into_inner();
    row.commit_ts = row.start_ts;
    row.key = decoded_key;
    row.value = decoded_value.user_value;

    if let Some(expire_ts) = decoded_value.expire_ts {
        row.expire_ts_unix_secs = expire_ts;
    }

    if decoded_value.is_delete {
        row.op_type = EventRowOpType::Delete;
    } else {
        row.op_type = EventRowOpType::Put;
    }
    set_event_row_type(row, EventLogType::Committed);
    Ok(())
}

fn decode_default(value: Vec<u8>, row: &mut EventRow, has_value: &mut bool) {
    if !value.is_empty() {
        row.value = value.to_vec();
    }
    // If default CF is given in a command it means the command always has a value.
    *has_value = true;
}

/// Observed key range.
#[derive(Clone)]
pub struct ObservedRange {
    pub start_key_encoded: Key,
    pub end_key_encoded: Key,
    pub start_key_raw: Vec<u8>,
    pub end_key_raw: Vec<u8>,
    pub all_key_covered: bool,
}

impl Default for ObservedRange {
    fn default() -> Self {
        ObservedRange {
            start_key_encoded: Key::from_encoded(vec![]),
            end_key_encoded: Key::from_encoded(vec![]),
            start_key_raw: vec![],
            end_key_raw: vec![],
            all_key_covered: false,
        }
    }
}

impl ObservedRange {
    pub fn new(start_key_encoded: Vec<u8>, end_key_encoded: Vec<u8>) -> Result<ObservedRange> {
        let start_key_encoded = Key::from_encoded(start_key_encoded);
        let end_key_encoded = Key::from_encoded(end_key_encoded);
        let start_key_raw = start_key_encoded
            .clone()
            .into_raw()
            .map_err(|e| Error::Other(e.into()))?;
        let end_key_raw = end_key_encoded
            .clone()
            .into_raw()
            .map_err(|e| Error::Other(e.into()))?;
        Ok(ObservedRange {
            start_key_encoded,
            end_key_encoded,
            start_key_raw,
            end_key_raw,
            all_key_covered: false,
        })
    }

    #[allow(clippy::collapsible_if)]
    pub fn update_region_key_range(&mut self, region: &Region) {
        // Check observed key range in region.
        if self.start_key_encoded.as_encoded() <= &region.start_key {
            if self.end_key_encoded.is_empty()
                || (&region.end_key <= self.end_key_encoded.as_encoded()
                    && !region.end_key.is_empty())
            {
                // Observed range covers the region.
                self.all_key_covered = true;
            }
        }
    }

    fn is_key_in_range(&self, start_key: &[u8], end_key: &[u8], key: &[u8]) -> bool {
        if self.all_key_covered {
            return true;
        }
        if start_key <= key && (key < end_key || end_key.is_empty()) {
            return true;
        }
        false
    }

    pub fn contains_encoded_key(&self, key: &[u8]) -> bool {
        self.is_key_in_range(
            self.start_key_encoded.as_encoded(),
            self.end_key_encoded.as_encoded(),
            key,
        )
    }

    pub fn contains_raw_key(&self, key: &[u8]) -> bool {
        self.is_key_in_range(&self.start_key_raw, &self.end_key_raw, key)
    }

    pub fn filter_entries(&self, mut entries: Vec<EventRow>) -> Vec<EventRow> {
        if self.all_key_covered {
            return entries;
        }
        // Entry's key is in raw key format.
        entries.retain(|e| self.is_key_in_range(&self.start_key_raw, &self.end_key_raw, &e.key));
        entries
    }

    fn to_range(&self) -> (Bound<&Key>, Bound<&Key>) {
        let start = Bound::Included(&self.start_key_encoded);
        let end = if self.end_key_encoded.is_empty() {
            Bound::Unbounded
        } else {
            Bound::Excluded(&self.end_key_encoded)
        };
        (start, end)
    }
}

pub enum DelegateTask {
    Subscribe {
        downstream: Downstream,
    },
    ObservedEvent {
        cmds: CmdBatch,
        old_value_cb: OldValueCallback,
    },
    Stop {
        err: Option<Error>,
    },
    StopDownstream {
        err: Option<Error>,
        downstream_id: DownstreamId,
    },
    MinTs {
        min_ts: TimeStamp,
        current_ts: TimeStamp,
    },
    FinishScanLocks {
        observe_id: ObserveId,
        region: Region,
        locks: BTreeMap<Key, MiniLock>,
    },
    InitDownstream {
        observe_id: ObserveId,
        downstream_id: DownstreamId,
        build_resolver: Arc<AtomicBool>,
        cb: Box<dyn FnOnce() + Send>,
    },
    Validate(Box<dyn FnOnce(Option<&Delegate>) + Send>),
}

#[derive(Clone)]
pub struct DelegateMeta {
    pub region_id: u64,
    pub handle: ObserveHandle,
    pub sched: UnboundedSender<DelegateTask>,
}

const WARN_LAG_THRESHOLD: Duration = Duration::from_secs(600);
const WARN_LAG_INTERVAL: Duration = Duration::from_secs(60);

pub(crate) fn convert_to_grpc_events(
    entries: Vec<Option<KvEntry>>,
    filter_loop: bool,
) -> Result<Vec<Vec<EventRow>>> {
    let entries_len = entries.len();
    let mut rows = vec![Vec::with_capacity(entries_len)];
    let mut current_rows_size: usize = 0;
    for entry in entries {
        let (mut row, mut _has_value) = (EventRow::default(), false);
        let row_size: usize;
        match entry {
            Some(KvEntry::RawKvEntry(kv_pair)) => {
                decode_rawkv(kv_pair.0, kv_pair.1, &mut row)?;
                row_size = row.key.len() + row.value.len();
            }
            Some(KvEntry::TxnEntry(TxnEntry::Prewrite {
                default,
                lock,
                old_value,
            })) => {
                let l = Lock::parse(&lock.1).unwrap();
                if decode_lock(lock.0, l, &mut row, &mut _has_value) {
                    continue;
                }
                decode_default(default.1, &mut row, &mut _has_value);
                row.old_value = old_value.finalized().unwrap_or_default();
                row_size = row.key.len() + row.value.len() + row.old_value.len();
            }
            Some(KvEntry::TxnEntry(TxnEntry::Commit {
                default,
                write,
                old_value,
            })) => {
                if decode_write(write.0, &write.1, &mut row, &mut _has_value, false) {
                    continue;
                }
                decode_default(default.1, &mut row, &mut _has_value);

                // This type means the row is self-contained, it has,
                //   1. start_ts
                //   2. commit_ts
                //   3. key
                //   4. value
                if row.get_type() == EventLogType::Rollback {
                    // We dont need to send rollbacks to downstream,
                    // because downstream does not needs rollback to clean
                    // prewrite as it drops all previous stashed data.
                    continue;
                }
                set_event_row_type(&mut row, EventLogType::Committed);
                row.old_value = old_value.finalized().unwrap_or_default();
                row_size = row.key.len() + row.value.len() + row.old_value.len();
            }
            None => {
                // This type means scan has finished.
                set_event_row_type(&mut row, EventLogType::Initialized);
                row_size = 0;
            }
        }
        if TxnSource::is_lightning_physical_import(row.txn_source)
            || TxnSource::is_lossy_ddl_reorg_source_set(row.txn_source)
            || filter_loop && TxnSource::is_cdc_write_source_set(row.txn_source)
        {
            continue;
        }

        current_rows_size += row_size;
        if current_rows_size >= CDC_EVENT_MAX_BYTES {
            rows.push(Vec::with_capacity(entries_len));
            current_rows_size = row_size;
        }
        rows.last_mut().unwrap().push(row);
    }

    Ok(rows)
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;

    use api_version::RawValue;
    use futures::{executor::block_on, stream::StreamExt};
    use kvproto::{cdcpb::Event_oneof_event, errorpb::Error as ErrorHeader, metapb::Region};
    use tikv_util::memory::MemoryQuota;

    use super::*;
    use crate::channel::{channel, recv_timeout, CdcEvent, SendError};

    #[test]
    fn test_error() {
        let region_id = 1;
        let mut region = Region::default();
        region.set_id(region_id);
        region.mut_peers().push(Default::default());
        region.mut_region_epoch().set_version(2);
        region.mut_region_epoch().set_conf_ver(2);
        let region_epoch = region.get_region_epoch().clone();

        let quota = Arc::new(MemoryQuota::new(usize::MAX));
        let (sink, mut drain) = channel(ConnId::new(), quota.clone());
        let rx = drain.drain();
        let request_id = RequestId(123);
        let mut downstream = Downstream::new(
            String::new(),
            region_epoch,
            request_id,
            ConnId::new(),
            ChangeDataRequestKvApi::TiDb,
            false,
            ObservedRange::default(),
        );
        downstream.set_sink(sink);

        let mut delegate = Delegate::new(region_id, quota, Default::default());
        delegate.subscribe(downstream).unwrap();
        assert!(delegate.handle.is_observing());

        assert!(delegate.init_lock_tracker());
        let fails = delegate
            .finish_scan_locks(region, Default::default())
            .unwrap();
        assert!(fails.is_empty());
        assert!(delegate.downstreams[0].observed_range.all_key_covered);

        let rx_wrap = Cell::new(Some(rx));
        let receive_error = || {
            let (event, rx) = block_on(rx_wrap.replace(None).unwrap().into_future());
            rx_wrap.set(Some(rx));
            if let CdcEvent::Event(mut e) = event.unwrap().0 {
                assert_eq!(e.get_request_id(), request_id.0);
                let event = e.event.take().unwrap();
                match event {
                    Event_oneof_event::Error(err) => err,
                    other => panic!("unknown event {:?}", other),
                }
            } else {
                panic!("unknown event")
            }
        };

        let mut err_header = ErrorHeader::default();
        err_header.set_not_leader(Default::default());
        delegate.stop(Error::request(err_header));
        let err = receive_error();
        assert!(err.has_not_leader());
        // Observing is disabled by any error.
        assert!(!delegate.handle.is_observing());

        let mut err_header = ErrorHeader::default();
        err_header.set_region_not_found(Default::default());
        delegate.stop(Error::request(err_header));
        let err = receive_error();
        assert!(err.has_region_not_found());

        delegate.stop(Error::Sink(SendError::Congested));
        let err = receive_error();
        assert!(err.has_congested());

        let mut err_header = ErrorHeader::default();
        err_header.set_epoch_not_match(Default::default());
        delegate.stop(Error::request(err_header));
        let err = receive_error();
        assert!(err.has_epoch_not_match());

        // Split
        let mut region = Region::default();
        region.set_id(1);
        let mut request = AdminRequest::default();
        request.set_cmd_type(AdminCmdType::Split);
        let mut response = AdminResponse::default();
        response.mut_split().set_left(region.clone());
        let err = delegate.sink_admin(request, response).err().unwrap();
        delegate.stop(err);
        let mut err = receive_error();
        assert!(err.has_epoch_not_match());
        err.take_epoch_not_match()
            .current_regions
            .into_iter()
            .find(|r| r.get_id() == 1)
            .unwrap();

        let mut request = AdminRequest::default();
        request.set_cmd_type(AdminCmdType::BatchSplit);
        let mut response = AdminResponse::default();
        response.mut_splits().set_regions(vec![region].into());
        let err = delegate.sink_admin(request, response).err().unwrap();
        delegate.stop(err);
        let mut err = receive_error();
        assert!(err.has_epoch_not_match());
        err.take_epoch_not_match()
            .current_regions
            .into_iter()
            .find(|r| r.get_id() == 1)
            .unwrap();

        // Merge
        let mut request = AdminRequest::default();
        request.set_cmd_type(AdminCmdType::PrepareMerge);
        let response = AdminResponse::default();
        let err = delegate.sink_admin(request, response).err().unwrap();
        delegate.stop(err);
        let mut err = receive_error();
        assert!(err.has_epoch_not_match());
        assert!(err.take_epoch_not_match().current_regions.is_empty());

        let mut request = AdminRequest::default();
        request.set_cmd_type(AdminCmdType::CommitMerge);
        let response = AdminResponse::default();
        let err = delegate.sink_admin(request, response).err().unwrap();
        delegate.stop(err);
        let mut err = receive_error();
        assert!(err.has_epoch_not_match());
        assert!(err.take_epoch_not_match().current_regions.is_empty());

        let mut request = AdminRequest::default();
        request.set_cmd_type(AdminCmdType::RollbackMerge);
        let response = AdminResponse::default();
        let err = delegate.sink_admin(request, response).err().unwrap();
        delegate.stop(err);
        let mut err = receive_error();
        assert!(err.has_epoch_not_match());
        assert!(err.take_epoch_not_match().current_regions.is_empty());
    }

    #[test]
    fn test_delegate_subscribe_unsubscribe() {
        let new_downstream = |id: RequestId, region_version: u64| {
            let peer = format!("{:?}", id);
            let mut epoch = RegionEpoch::default();
            epoch.set_conf_ver(region_version);
            epoch.set_version(region_version);
            Downstream::new(
                peer,
                epoch,
                id,
                ConnId::new(),
                ChangeDataRequestKvApi::TiDb,
                false,
                ObservedRange::default(),
            )
        };

        // Create a new delegate.
        let memory_quota = Arc::new(MemoryQuota::new(usize::MAX));
        let txn_extra_op = Arc::new(AtomicCell::new(TxnExtraOp::Noop));
        let mut delegate = Delegate::new(1, memory_quota, txn_extra_op.clone());
        assert_eq!(txn_extra_op.load(), TxnExtraOp::Noop);
        assert!(delegate.handle.is_observing());

        // Subscribe once.
        let downstream1 = new_downstream(RequestId(1), 1);
        let downstream1_id = downstream1.id;
        delegate.subscribe(downstream1).unwrap();
        assert_eq!(txn_extra_op.load(), TxnExtraOp::ReadOldValue);
        assert!(delegate.handle.is_observing());

        // Subscribe twice and then unsubscribe the second downstream.
        let downstream2 = new_downstream(RequestId(2), 1);
        let downstream2_id = downstream2.id;
        delegate.subscribe(downstream2).unwrap();
        assert!(!delegate.unsubscribe(downstream2_id, None));
        assert_eq!(txn_extra_op.load(), TxnExtraOp::ReadOldValue);
        assert!(delegate.handle.is_observing());

        // `on_region_ready` when the delegate isn't resolved.
        delegate.subscribe(new_downstream(RequestId(1), 2)).unwrap();
        let mut region = Region::default();
        region.mut_region_epoch().set_conf_ver(1);
        region.mut_region_epoch().set_version(1);
        {
            assert!(delegate.init_lock_tracker());
            let failures = delegate
                .finish_scan_locks(region, Default::default())
                .unwrap();
            assert_eq!(failures.len(), 1);
            let id = failures[0].0.id;
            delegate.unsubscribe(id, None);
            assert_eq!(delegate.downstreams().len(), 1);
        }
        assert_eq!(txn_extra_op.load(), TxnExtraOp::ReadOldValue);
        assert!(delegate.handle.is_observing());

        // Subscribe with an invalid epoch.
        delegate
            .subscribe(new_downstream(RequestId(1), 2))
            .unwrap_err();
        assert_eq!(delegate.downstreams().len(), 1);

        // Unsubscribe all downstreams.
        assert!(delegate.unsubscribe(downstream1_id, None));
        assert!(delegate.downstreams().is_empty());
        assert_eq!(txn_extra_op.load(), TxnExtraOp::Noop);
        assert!(!delegate.handle.is_observing());
    }

    #[test]
    fn test_observed_range() {
        for case in vec![
            (b"".as_slice(), b"".as_slice(), false),
            (b"a", b"", false),
            (b"", b"b", false),
            (b"a", b"b", true),
            (b"a", b"bb", false),
            (b"a", b"aa", true),
            (b"aa", b"aaa", true),
        ] {
            let start_key = if !case.0.is_empty() {
                Key::from_raw(case.0).into_encoded()
            } else {
                case.0.to_owned()
            };
            let end_key = if !case.1.is_empty() {
                Key::from_raw(case.1).into_encoded()
            } else {
                case.1.to_owned()
            };
            let mut region = Region::default();
            region.start_key = start_key.to_owned();
            region.end_key = end_key.to_owned();

            for k in 0..=0xff {
                let mut observed_range = ObservedRange::default();
                observed_range.update_region_key_range(&region);
                assert!(observed_range.contains_encoded_key(&Key::from_raw(&[k]).into_encoded()));
            }
            let mut observed_range = ObservedRange::new(
                Key::from_raw(b"a").into_encoded(),
                Key::from_raw(b"b").into_encoded(),
            )
            .unwrap();
            observed_range.update_region_key_range(&region);
            assert_eq!(observed_range.all_key_covered, case.2, "{:?}", case);
            assert!(
                observed_range.contains_encoded_key(&Key::from_raw(b"a").into_encoded()),
                "{:?}",
                case
            );
            assert!(
                observed_range.contains_encoded_key(&Key::from_raw(b"ab").into_encoded()),
                "{:?}",
                case
            );
            if observed_range.all_key_covered {
                assert!(
                    observed_range.contains_encoded_key(&Key::from_raw(b"b").into_encoded()),
                    "{:?}",
                    case
                );
            } else {
                assert!(
                    !observed_range.contains_encoded_key(&Key::from_raw(b"b").into_encoded()),
                    "{:?}",
                    case
                );
            }
        }
    }

    #[test]
    fn test_downstream_filter_entires() {
        // Create a new delegate that observes [b, d).
        let observed_range = ObservedRange::new(
            Key::from_raw(b"b").into_encoded(),
            Key::from_raw(b"d").into_encoded(),
        )
        .unwrap();
        let memory_quota = Arc::new(MemoryQuota::new(usize::MAX));
        let txn_extra_op = Arc::new(AtomicCell::new(TxnExtraOp::Noop));
        let mut delegate = Delegate::new(1, memory_quota, txn_extra_op);
        assert!(delegate.handle.is_observing());
        assert!(delegate.init_lock_tracker());

        let mut rows_builder = RowsBuilder::default();
        for k in b'a'..=b'e' {
            let mut put = PutRequest::default();
            put.key = Key::from_raw(&[k]).into_encoded();
            put.cf = "lock".to_owned();
            put.value = Lock::new(
                LockType::Put,
                put.key.clone(),
                1.into(),
                10,
                Some(b"test".to_vec()),
                TimeStamp::zero(),
                0,
                TimeStamp::zero(),
                false,
            )
            .to_bytes();
            delegate.sink_txn_put(put, &mut rows_builder).unwrap();
        }
        assert_eq!(rows_builder.txns_by_key.len(), 5);

        let (sink, mut drain) = channel(ConnId::new(), Arc::new(MemoryQuota::new(1024)));
        let mut downstream = Downstream::new(
            "peer".to_owned(),
            RegionEpoch::default(),
            RequestId(1),
            ConnId::new(),
            ChangeDataRequestKvApi::TiDb,
            false,
            observed_range,
        );
        downstream.set_sink(sink);
        downstream.get_state().store(DownstreamState::Normal);
        delegate.add_downstream(downstream);
        let (_, entries) = rows_builder.finish_build();
        delegate
            .sink_downstream_tidb(entries, |_, _| Ok(()))
            .unwrap();

        let (mut tx, mut rx) = futures::channel::mpsc::unbounded();
        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.spawn(async move {
            drain.forward(&mut tx).await.unwrap();
        });
        let (e, _) = recv_timeout(&mut rx, std::time::Duration::from_secs(5))
            .unwrap()
            .unwrap();
        assert_eq!(e.events[0].get_entries().get_entries().len(), 2, "{:?}", e);
    }

    fn test_downstream_txn_source_filter(txn_source: TxnSource, filter_loop: bool) {
        // Create a new delegate that observes [a, f).
        let observed_range = ObservedRange::new(
            Key::from_raw(b"a").into_encoded(),
            Key::from_raw(b"f").into_encoded(),
        )
        .unwrap();
        let memory_quota = Arc::new(MemoryQuota::new(usize::MAX));
        let txn_extra_op = Arc::new(AtomicCell::new(TxnExtraOp::Noop));
        let mut delegate = Delegate::new(1, memory_quota, txn_extra_op);
        assert!(delegate.handle.is_observing());
        assert!(delegate.init_lock_tracker());

        let mut rows_builder = RowsBuilder::default();
        for k in b'a'..=b'e' {
            let mut put = PutRequest::default();
            put.key = Key::from_raw(&[k]).into_encoded();
            put.cf = "lock".to_owned();
            let mut lock = Lock::new(
                LockType::Put,
                put.key.clone(),
                1.into(),
                10,
                Some(b"test".to_vec()),
                TimeStamp::zero(),
                0,
                TimeStamp::zero(),
                false,
            );
            // Only the key `a` is a normal write.
            if k != b'a' {
                lock = lock.set_txn_source(txn_source.into());
            }
            put.value = lock.to_bytes();
            delegate.sink_txn_put(put, &mut rows_builder).unwrap();
        }
        assert_eq!(rows_builder.txns_by_key.len(), 5);

        let (sink, mut drain) = channel(ConnId::new(), Arc::new(MemoryQuota::new(1024)));
        let mut downstream = Downstream::new(
            "peer".to_owned(),
            RegionEpoch::default(),
            RequestId(1),
            ConnId::new(),
            ChangeDataRequestKvApi::TiDb,
            filter_loop,
            observed_range,
        );
        downstream.set_sink(sink);
        downstream.get_state().store(DownstreamState::Normal);
        delegate.add_downstream(downstream);
        let (_, entries) = rows_builder.finish_build();
        delegate
            .sink_downstream_tidb(entries, |_, _| Ok(()))
            .unwrap();

        let (mut tx, mut rx) = futures::channel::mpsc::unbounded();
        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.spawn(async move {
            drain.forward(&mut tx).await.unwrap();
        });
        let (e, _) = recv_timeout(&mut rx, std::time::Duration::from_secs(5))
            .unwrap()
            .unwrap();
        assert_eq!(e.events[0].get_entries().get_entries().len(), 1, "{:?}", e);
    }

    #[test]
    fn test_downstream_filter_cdc_write_entires() {
        let mut txn_source = TxnSource::default();
        txn_source.set_cdc_write_source(1);

        test_downstream_txn_source_filter(txn_source, true);
    }

    #[test]
    fn test_downstream_filter_lightning_physical_import() {
        let mut txn_source = TxnSource::default();
        txn_source.set_lightning_physical_import();
        test_downstream_txn_source_filter(txn_source, false);
    }

    #[test]
    fn test_downstream_filter_lossy_ddl_entires() {
        let mut txn_source = TxnSource::default();
        txn_source.set_lossy_ddl_reorg_source(1);
        test_downstream_txn_source_filter(txn_source, false);

        // With cdr write source and filter loop is false, we should still ignore lossy
        // ddl changes.
        let mut txn_source = TxnSource::default();
        txn_source.set_cdc_write_source(1);
        txn_source.set_lossy_ddl_reorg_source(1);
        test_downstream_txn_source_filter(txn_source, false);

        // With cdr write source and filter loop is true, we should still ignore some
        // events.
        let mut txn_source = TxnSource::default();
        txn_source.set_cdc_write_source(1);
        txn_source.set_lossy_ddl_reorg_source(1);
        test_downstream_txn_source_filter(txn_source, true);
    }

    #[test]
    fn test_decode_rawkv() {
        let cases = vec![
            (vec![b'r', 2, 3, 4], b"world1".to_vec(), 1, Some(10), false),
            (vec![b'r', 3, 4, 5], b"world2".to_vec(), 2, None, true),
        ];

        for (key, value, start_ts, expire_ts, is_delete) in cases.into_iter() {
            let mut row = EventRow::default();
            let key_with_ts = ApiV2::encode_raw_key(&key, Some(start_ts.into())).into_encoded();
            let raw_value = RawValue {
                user_value: value.to_vec(),
                expire_ts,
                is_delete,
            };
            let encoded_value = ApiV2::encode_raw_value_owned(raw_value);
            decode_rawkv(key_with_ts, encoded_value, &mut row).unwrap();

            assert_eq!(row.start_ts, start_ts);
            assert_eq!(row.commit_ts, start_ts);
            assert_eq!(row.key, key);
            assert_eq!(row.value, value);

            if is_delete {
                assert_eq!(row.op_type, EventRowOpType::Delete);
            } else {
                assert_eq!(row.op_type, EventRowOpType::Put);
            }

            if let Some(expire_ts) = expire_ts {
                assert_eq!(row.expire_ts_unix_secs, expire_ts);
            } else {
                assert_eq!(row.expire_ts_unix_secs, 0);
            }
        }
    }

    #[test]
    fn test_lock_tracker() {
        let quota = Arc::new(MemoryQuota::new(usize::MAX));
        let mut delegate = Delegate::new(1, quota.clone(), Default::default());
        assert!(delegate.init_lock_tracker());
        assert!(!delegate.init_lock_tracker());

        let mut k1 = Vec::with_capacity(100);
        k1.extend_from_slice(Key::from_raw(b"key1").as_encoded());
        let k1 = Key::from_encoded(k1);
        assert_eq!(delegate.push_lock(k1, MiniLock::from_ts(100)).unwrap(), 0);
        assert_eq!(quota.in_use(), 100);

        delegate
            .pop_lock(Key::from_raw(b"key1"), TimeStamp::from(99))
            .unwrap();
        assert_eq!(quota.in_use(), 117);

        delegate
            .pop_lock(Key::from_raw(b"key1"), TimeStamp::from(100))
            .unwrap();
        assert_eq!(quota.in_use(), 134);

        let mut k2 = Vec::with_capacity(200);
        k2.extend_from_slice(Key::from_raw(b"key2").as_encoded());
        let k2 = Key::from_encoded(k2);
        assert_eq!(delegate.push_lock(k2, MiniLock::from_ts(100)).unwrap(), 0);
        assert_eq!(quota.in_use(), 334);

        let mut scaned_locks = BTreeMap::default();
        scaned_locks.insert(Key::from_raw(b"key1"), MiniLock::from_ts(100));
        scaned_locks.insert(Key::from_raw(b"key2"), MiniLock::from_ts(100));
        scaned_locks.insert(Key::from_raw(b"key3"), MiniLock::from_ts(100));
        delegate
            .finish_prepare_lock_tracker(Default::default(), scaned_locks)
            .unwrap();
        assert_eq!(quota.in_use(), 34);

        delegate
            .pop_lock(Key::from_raw(b"key2"), TimeStamp::from(100))
            .unwrap();
        delegate
            .pop_lock(Key::from_raw(b"key3"), TimeStamp::from(100))
            .unwrap();
        assert_eq!(quota.in_use(), 0);

        let v = delegate
            .push_lock(Key::from_raw(b"key1"), MiniLock::from_ts(300))
            .unwrap();
        assert_eq!(v, 1);
        assert_eq!(quota.in_use(), 17);
        let v = delegate
            .push_lock(Key::from_raw(b"key1"), MiniLock::from_ts(300))
            .unwrap();
        assert_eq!(v, 0);
        assert_eq!(quota.in_use(), 17);
    }

    #[test]
    fn test_lock_tracker_untrack_vacant() {
        let quota = Arc::new(MemoryQuota::new(usize::MAX));
        let mut delegate = Delegate::new(1, quota.clone(), Default::default());
        assert!(delegate.init_lock_tracker());
        assert!(!delegate.init_lock_tracker());

        delegate
            .pop_lock(Key::from_raw(b"key1"), TimeStamp::zero())
            .unwrap();
        let mut scaned_locks = BTreeMap::default();
        scaned_locks.insert(Key::from_raw(b"key2"), MiniLock::from_ts(100));
        delegate
            .finish_prepare_lock_tracker(Default::default(), scaned_locks)
            .unwrap();
    }
}
