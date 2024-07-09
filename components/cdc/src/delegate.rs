// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    mem,
    string::String,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
};

use api_version::{ApiV2, KeyMode, KvFormat};
use collections::{HashMap, HashMapEntry};
use crossbeam::atomic::AtomicCell;
use kvproto::{
    cdcpb::{
        ChangeDataRequestKvApi, Error as EventError, Event, EventEntries, EventLogType, EventRow,
        EventRowOpType, Event_oneof_event,
    },
    kvrpcpb::ExtraOp as TxnExtraOp,
    metapb::{Region, RegionEpoch},
    raft_cmdpb::{
        AdminCmdType, AdminRequest, AdminResponse, CmdType, DeleteRequest, PutRequest, Request,
    },
};
use raftstore::{
    coprocessor::{Cmd, CmdBatch, ObserveHandle},
    store::util::compare_region_epoch,
    Error as RaftStoreError,
};
use resolved_ts::{Resolver, TsSource, ON_DROP_WARN_HEAP_SIZE};
use tikv::storage::{txn::TxnEntry, Statistics};
use tikv_util::{
    debug, info,
    memory::{HeapSize, MemoryQuota},
    warn,
};
use txn_types::{Key, Lock, LockType, TimeStamp, WriteBatchFlags, WriteRef, WriteType};

use crate::{
    channel::{CdcEvent, SendError, Sink, CDC_EVENT_MAX_BYTES},
    initializer::KvEntry,
    metrics::*,
    old_value::{OldValueCache, OldValueCallback},
    service::ConnId,
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
    /// accepted. However it still rejects resolved timestamps.
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

#[derive(Clone)]
pub struct Downstream {
    // TODO: include cdc request.
    /// A unique identifier of the Downstream.
    id: DownstreamId,
    // The request ID set by CDC to identify events corresponding different requests.
    req_id: u64,
    conn_id: ConnId,
    // The IP address of downstream.
    peer: String,
    region_epoch: RegionEpoch,
    sink: Option<Sink>,
    state: Arc<AtomicCell<DownstreamState>>,
    kv_api: ChangeDataRequestKvApi,
    filter_loop: bool,
    pub(crate) observed_range: ObservedRange,

    // When meet region errors like split or merge, we can cancel incremental scan draining
    // by `scan_truncated`.
    pub(crate) scan_truncated: Arc<AtomicBool>,
}

impl Downstream {
    /// Create a Downstream.
    ///
    /// peer is the address of the downstream.
    /// sink sends data to the downstream.
    pub fn new(
        peer: String,
        region_epoch: RegionEpoch,
        req_id: u64,
        conn_id: ConnId,
        kv_api: ChangeDataRequestKvApi,
        filter_loop: bool,
        observed_range: ObservedRange,
    ) -> Downstream {
        Downstream {
            id: DownstreamId::new(),
            req_id,
            conn_id,
            peer,
            region_epoch,
            sink: None,
            state: Arc::new(AtomicCell::new(DownstreamState::default())),
            kv_api,
            filter_loop,
            observed_range,

            scan_truncated: Arc::new(AtomicBool::new(false)),
        }
    }

    // NOTE: it's not allowed to sink `EventError` directly by this function,
    // because the sink can be also used by an incremental scan. We must ensure
    // no more events can be pushed to the sink after an `EventError` is sent.
    pub fn sink_event(&self, mut event: Event, force: bool) -> Result<()> {
        event.set_request_id(self.req_id);
        if self.sink.is_none() {
            info!("cdc drop event, no sink";
                "conn_id" => ?self.conn_id, "downstream_id" => ?self.id, "req_id" => self.req_id);
            return Err(Error::Sink(SendError::Disconnected));
        }
        let sink = self.sink.as_ref().unwrap();
        match sink.unbounded_send(CdcEvent::Event(event), force) {
            Ok(_) => Ok(()),
            Err(SendError::Disconnected) => {
                debug!("cdc send event failed, disconnected";
                    "conn_id" => ?self.conn_id, "downstream_id" => ?self.id, "req_id" => self.req_id);
                Err(Error::Sink(SendError::Disconnected))
            }
            // TODO handle errors.
            Err(e @ SendError::Full) | Err(e @ SendError::Congested) => {
                info!("cdc send event failed, full";
                    "conn_id" => ?self.conn_id, "downstream_id" => ?self.id, "req_id" => self.req_id);
                Err(Error::Sink(e))
            }
        }
    }

    /// EventErrors must be sent by this function. And we must ensure no more
    /// events or ResolvedTs will be sent to the downstream after
    /// `sink_error_event` is called.
    pub fn sink_error_event(&self, region_id: u64, err_event: EventError) -> Result<()> {
        info!("cdc downstream meets region error";
            "conn_id" => ?self.conn_id, "downstream_id" => ?self.id, "req_id" => self.req_id);

        self.scan_truncated.store(true, Ordering::Release);
        let mut change_data_event = Event::default();
        change_data_event.event = Some(Event_oneof_event::Error(err_event));
        change_data_event.region_id = region_id;
        // Try it's best to send error events.
        let force_send = true;
        self.sink_event(change_data_event, force_send)
    }

    pub fn set_sink(&mut self, sink: Sink) {
        self.sink = Some(sink);
    }

    pub fn get_id(&self) -> DownstreamId {
        self.id
    }

    pub fn get_filter_loop(&self) -> bool {
        self.filter_loop
    }

    pub fn get_state(&self) -> Arc<AtomicCell<DownstreamState>> {
        self.state.clone()
    }

    pub fn get_conn_id(&self) -> ConnId {
        self.conn_id
    }
    pub fn get_req_id(&self) -> u64 {
        self.req_id
    }
}

struct Pending {
    downstreams: Vec<Downstream>,
    locks: Vec<PendingLock>,
    pending_bytes: usize,
    memory_quota: Arc<MemoryQuota>,
}

impl Pending {
    fn new(memory_quota: Arc<MemoryQuota>) -> Pending {
        Pending {
            downstreams: vec![],
            locks: vec![],
            pending_bytes: 0,
            memory_quota,
        }
    }

    fn push_pending_lock(&mut self, lock: PendingLock) -> Result<()> {
        let bytes = lock.approximate_heap_size();
        self.memory_quota.alloc(bytes)?;
        self.locks.push(lock);
        self.pending_bytes += bytes;
        CDC_PENDING_BYTES_GAUGE.add(bytes as i64);
        Ok(())
    }

    fn on_region_ready(&mut self, resolver: &mut Resolver) -> Result<()> {
        fail::fail_point!("cdc_pending_on_region_ready", |_| Err(
            Error::MemoryQuotaExceeded(tikv_util::memory::MemoryQuotaExceeded)
        ));
        // Must take locks, otherwise it may double free memory quota on drop.
        for lock in mem::take(&mut self.locks) {
            self.memory_quota.free(lock.approximate_heap_size());
            match lock {
                PendingLock::Track { key, start_ts } => {
                    resolver.track_lock(start_ts, key, None)?;
                }
                PendingLock::Untrack { key } => resolver.untrack_lock(&key, None),
            }
        }
        Ok(())
    }
}

impl Drop for Pending {
    fn drop(&mut self) {
        CDC_PENDING_BYTES_GAUGE.sub(self.pending_bytes as i64);
        let locks = mem::take(&mut self.locks);
        if locks.is_empty() {
            return;
        }

        // Free memory quota used by pending locks and unlocks.
        let mut bytes = 0;
        let num_locks = locks.len();
        for lock in locks {
            bytes += lock.approximate_heap_size();
        }
        if bytes > ON_DROP_WARN_HEAP_SIZE {
            warn!("cdc drop huge Pending";
                "bytes" => bytes,
                "num_locks" => num_locks,
                "memory_quota_in_use" => self.memory_quota.in_use(),
                "memory_quota_capacity" => self.memory_quota.capacity(),
            );
        }
        self.memory_quota.free(bytes);
    }
}

enum PendingLock {
    Track { key: Vec<u8>, start_ts: TimeStamp },
    Untrack { key: Vec<u8> },
}

impl HeapSize for PendingLock {
    fn approximate_heap_size(&self) -> usize {
        match self {
            PendingLock::Track { key, .. } | PendingLock::Untrack { key } => {
                key.approximate_heap_size()
            }
        }
    }
}

/// A CDC delegate of a raftstore region peer.
///
/// It converts raft commands into CDC events and broadcast to downstreams.
/// It also track trancation on the fly in order to compute resolved ts.
pub struct Delegate {
    pub handle: ObserveHandle,
    pub region_id: u64,

    // None if the delegate is not initialized.
    region: Option<Region>,
    pub resolver: Option<Resolver>,

    // Downstreams after the delegate has been resolved.
    resolved_downstreams: Vec<Downstream>,
    pending: Option<Pending>,
    txn_extra_op: Arc<AtomicCell<TxnExtraOp>>,
    failed: bool,
}

impl Delegate {
    /// Create a Delegate the given region.
    pub fn new(
        region_id: u64,
        txn_extra_op: Arc<AtomicCell<TxnExtraOp>>,
        memory_quota: Arc<MemoryQuota>,
    ) -> Delegate {
        Delegate {
            region_id,
            handle: ObserveHandle::new(),
            resolver: None,
            region: None,
            resolved_downstreams: Vec::new(),
            pending: Some(Pending::new(memory_quota)),
            txn_extra_op,
            failed: false,
        }
    }

    /// Let downstream subscribe the delegate.
    /// Return error if subscribe fails and the `Delegate` won't be changed.
    pub fn subscribe(&mut self, downstream: Downstream) -> Result<()> {
        if self.region.is_some() {
            // Check if the downstream is out dated.
            self.check_epoch_on_ready(&downstream)?;
        }
        self.add_downstream(downstream);
        Ok(())
    }

    pub fn downstream(&self, downstream_id: DownstreamId) -> Option<&Downstream> {
        self.downstreams().iter().find(|d| d.id == downstream_id)
    }

    pub fn downstreams(&self) -> &Vec<Downstream> {
        self.pending
            .as_ref()
            .map(|p| &p.downstreams)
            .unwrap_or(&self.resolved_downstreams)
    }

    pub fn downstreams_mut(&mut self) -> &mut Vec<Downstream> {
        self.pending
            .as_mut()
            .map(|p| &mut p.downstreams)
            .unwrap_or(&mut self.resolved_downstreams)
    }

    /// Let downstream unsubscribe the delegate.
    /// Return whether the delegate is empty or not.
    pub fn unsubscribe(&mut self, id: DownstreamId, err: Option<Error>) -> bool {
        let error_event = err.map(|err| err.into_error_event(self.region_id));
        let region_id = self.region_id;
        if let Some(d) = self.remove_downstream(id) {
            if let Some(error_event) = error_event {
                if let Err(err) = d.sink_error_event(region_id, error_event.clone()) {
                    warn!("cdc send unsubscribe failed";
                        "region_id" => region_id, "error" => ?err, "origin_error" => ?error_event,
                        "downstream_id" => ?d.id, "downstream" => ?d.peer,
                        "request_id" => d.req_id, "conn_id" => ?d.conn_id);
                }
            }
            d.state.store(DownstreamState::Stopped);
        }
        self.downstreams().is_empty()
    }

    pub fn mark_failed(&mut self) {
        self.failed = true;
    }

    pub fn has_failed(&self) -> bool {
        self.failed
    }

    /// Stop the delegate
    ///
    /// This means the region has met an unrecoverable error for CDC.
    /// It broadcasts errors to all downstream and stops.
    pub fn stop(&mut self, err: Error) {
        self.mark_failed();
        self.stop_observing();

        info!("cdc met region error";
            "region_id" => self.region_id, "error" => ?err);
        let region_id = self.region_id;
        let error = err.into_error_event(self.region_id);
        let send = move |downstream: &Downstream| {
            downstream.state.store(DownstreamState::Stopped);
            let error_event = error.clone();
            if let Err(err) = downstream.sink_error_event(region_id, error_event) {
                warn!("cdc send region error failed";
                    "region_id" => region_id, "error" => ?err, "origin_error" => ?error,
                    "downstream_id" => ?downstream.id, "downstream" => ?downstream.peer,
                    "request_id" => downstream.req_id, "conn_id" => ?downstream.conn_id);
            } else {
                info!("cdc send region error success";
                    "region_id" => region_id, "origin_error" => ?error,
                    "downstream_id" => ?downstream.id, "downstream" => ?downstream.peer,
                    "request_id" => downstream.req_id, "conn_id" => ?downstream.conn_id);
            }
            Ok(())
        };

        // TODO: In case we drop error messages, maybe we need a heartbeat mechanism
        //       to allow TiCDC detect region status.
        let _ = self.broadcast(send);
    }

    /// `txn_extra_op` returns a shared flag which is accessed in TiKV's
    /// transaction layer to determine whether to capture modifications' old
    /// value or not. Unsubscribing all downstreams or calling
    /// `Delegate::stop` will store it with `TxnExtraOp::Noop`.
    ///
    /// NOTE: Dropping a `Delegate` won't update this flag.
    pub fn txn_extra_op(&self) -> &AtomicCell<TxnExtraOp> {
        self.txn_extra_op.as_ref()
    }

    fn broadcast<F>(&self, send: F) -> Result<()>
    where
        F: Fn(&Downstream) -> Result<()>,
    {
        let downstreams = self.downstreams();
        assert!(
            !downstreams.is_empty(),
            "region {} miss downstream",
            self.region_id
        );
        for downstream in downstreams {
            send(downstream)?;
        }
        Ok(())
    }

    /// Install a resolver. Return downstreams which fail because of the
    /// region's internal changes.
    pub fn on_region_ready(
        &mut self,
        mut resolver: Resolver,
        region: Region,
    ) -> Result<Vec<(&Downstream, Error)>> {
        assert!(
            self.resolver.is_none(),
            "region {} resolver should not be ready",
            self.region_id,
        );

        // Check observed key range in region.
        for downstream in self.downstreams_mut() {
            downstream.observed_range.update_region_key_range(&region);
        }

        // Mark the delegate as initialized.
        info!("cdc region is ready"; "region_id" => self.region_id);
        // Downstreams in pending must be moved to resolved_downstreams
        // immediately and must not return in the middle, otherwise the delegate
        // loses downstreams.
        let mut pending = self.pending.take().unwrap();
        self.resolved_downstreams = mem::take(&mut pending.downstreams);

        pending.on_region_ready(&mut resolver)?;
        self.resolver = Some(resolver);
        self.region = Some(region);

        let mut failed_downstreams = Vec::new();
        for downstream in self.downstreams() {
            if let Err(e) = self.check_epoch_on_ready(downstream) {
                failed_downstreams.push((downstream, e));
            }
        }
        Ok(failed_downstreams)
    }

    /// Try advance and broadcast resolved ts.
    pub fn on_min_ts(&mut self, min_ts: TimeStamp) -> Option<TimeStamp> {
        if self.resolver.is_none() {
            debug!("cdc region resolver not ready";
                "region_id" => self.region_id, "min_ts" => min_ts);
            return None;
        }
        debug!("cdc try to advance ts"; "region_id" => self.region_id, "min_ts" => min_ts);
        let resolver = self.resolver.as_mut().unwrap();
        let resolved_ts = resolver.resolve(min_ts, None, TsSource::Cdc);
        debug!("cdc resolved ts updated";
            "region_id" => self.region_id, "resolved_ts" => resolved_ts);
        Some(resolved_ts)
    }

    pub fn on_batch(
        &mut self,
        batch: CmdBatch,
        old_value_cb: &OldValueCallback,
        old_value_cache: &mut OldValueCache,
        statistics: &mut Statistics,
    ) -> Result<()> {
        // Stale CmdBatch, drop it silently.
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
                self.mark_failed();
                return Err(Error::request(err_header));
            }
            if !request.has_admin_request() {
                let flags = WriteBatchFlags::from_bits_truncate(request.get_header().get_flags());
                let is_one_pc = flags.contains(WriteBatchFlags::ONE_PC);
                self.sink_data(
                    index,
                    request.requests.into(),
                    old_value_cb,
                    old_value_cache,
                    statistics,
                    is_one_pc,
                )?;
            } else {
                self.sink_admin(request.take_admin_request(), response.take_admin_response())?;
            }
        }
        Ok(())
    }

    pub(crate) fn convert_to_grpc_events(
        region_id: u64,
        request_id: u64,
        entries: Vec<Option<KvEntry>>,
        filter_loop: bool,
        observed_range: &ObservedRange,
    ) -> Result<Vec<CdcEvent>> {
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
                    if !observed_range.contains_encoded_key(&lock.0) {
                        continue;
                    }
                    let l = Lock::parse(&lock.1).unwrap();
                    if decode_lock(lock.0, l, &mut row, &mut _has_value) {
                        continue;
                    }
                    decode_default(default.1, &mut row, &mut _has_value);
                    row.old_value = old_value.finalized().unwrap_or_default();
                    row_size = row.key.len() + row.value.len();
                }
                Some(KvEntry::TxnEntry(TxnEntry::Commit {
                    default,
                    write,
                    old_value,
                })) => {
                    if !observed_range.contains_encoded_key(&write.0) {
                        continue;
                    }
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
                    row_size = row.key.len() + row.value.len();
                }
                None => {
                    // This type means scan has finished.
                    set_event_row_type(&mut row, EventLogType::Initialized);
                    row_size = 0;
                }
            }
            let lossy_ddl_filter = TxnSource::is_lossy_ddl_reorg_source_set(row.txn_source);
            let cdc_write_filter =
                TxnSource::is_cdc_write_source_set(row.txn_source) && filter_loop;
            if lossy_ddl_filter || cdc_write_filter {
                continue;
            }
            if current_rows_size + row_size >= CDC_EVENT_MAX_BYTES {
                rows.push(Vec::with_capacity(entries_len));
                current_rows_size = 0;
            }
            current_rows_size += row_size;
            rows.last_mut().unwrap().push(row);
        }

        let rows = rows
            .into_iter()
            .filter(|rs| !rs.is_empty())
            .map(|rs| {
                let event_entries = EventEntries {
                    entries: rs.into(),
                    ..Default::default()
                };
                CdcEvent::Event(Event {
                    region_id,
                    request_id,
                    event: Some(Event_oneof_event::Entries(event_entries)),
                    ..Default::default()
                })
            })
            .collect();
        Ok(rows)
    }

    fn sink_data(
        &mut self,
        index: u64,
        requests: Vec<Request>,
        old_value_cb: &OldValueCallback,
        old_value_cache: &mut OldValueCache,
        statistics: &mut Statistics,
        is_one_pc: bool,
    ) -> Result<()> {
        debug_assert_eq!(self.txn_extra_op.load(), TxnExtraOp::ReadOldValue);
        let mut read_old_value = |row: &mut EventRow, read_old_ts| -> Result<()> {
            let key = Key::from_raw(&row.key).append_ts(row.start_ts.into());
            let old_value = old_value_cb(key, read_old_ts, old_value_cache, statistics)?;
            row.old_value = old_value.unwrap_or_default();
            Ok(())
        };

        // map[key] -> (event, has_value).
        let mut txn_rows: HashMap<Vec<u8>, (EventRow, bool)> = HashMap::default();
        let mut raw_rows: Vec<EventRow> = Vec::new();
        for mut req in requests {
            let res = match req.get_cmd_type() {
                CmdType::Put => self.sink_put(
                    req.take_put(),
                    is_one_pc,
                    &mut txn_rows,
                    &mut raw_rows,
                    &mut read_old_value,
                ),
                CmdType::Delete => self.sink_delete(req.take_delete()),
                _ => {
                    debug!(
                        "skip other command";
                        "region_id" => self.region_id,
                        "command" => ?req,
                    );
                    Ok(())
                }
            };
            if res.is_err() {
                self.mark_failed();
                return res;
            }
        }

        let mut rows = Vec::with_capacity(txn_rows.len());
        for (_, (v, has_value)) in txn_rows {
            if v.r_type == EventLogType::Prewrite && v.op_type == EventRowOpType::Put && !has_value
            {
                // It's possible that a prewrite command only contains lock but without
                // default. It's not documented by classic Percolator but introduced with
                // Large-Transaction. Those prewrites are not complete, we must skip them.
                continue;
            }
            rows.push(v);
        }
        self.sink_downstream(rows, index, ChangeDataRequestKvApi::TiDb)?;
        self.sink_downstream(raw_rows, index, ChangeDataRequestKvApi::RawKv)
    }

    fn sink_downstream(
        &mut self,
        entries: Vec<EventRow>,
        index: u64,
        kv_api: ChangeDataRequestKvApi,
    ) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }

        // Filter the entries which are lossy DDL events.
        // We don't need to send them to downstream.
        let entries = entries
            .iter()
            .filter(|x| !TxnSource::is_lossy_ddl_reorg_source_set(x.txn_source))
            .cloned()
            .collect::<Vec<EventRow>>();

        let downstreams = self.downstreams();
        assert!(
            !downstreams.is_empty(),
            "region {} miss downstream",
            self.region_id
        );

        // Collect the change event cause by user write, which cdc write source is not
        // set. For changefeed which only need the user write,
        // send the `filtered_entries`, or else, send them all.
        let mut filtered_entries = None;
        for downstream in downstreams {
            if downstream.filter_loop {
                let filtered = entries
                    .iter()
                    .filter(|x| !TxnSource::is_cdc_write_source_set(x.txn_source))
                    .cloned()
                    .collect::<Vec<EventRow>>();
                if !filtered.is_empty() {
                    filtered_entries = Some(filtered);
                }
                break;
            }
        }

        let region_id = self.region_id;
        let send = move |downstream: &Downstream| {
            // No ready downstream or a downstream that does not match the kv_api type, will
            // be ignored. There will be one region that contains both Txn & Raw entries.
            // The judgement here is for sending entries to downstreams with correct kv_api.
            if !downstream.state.load().ready_for_change_events() || downstream.kv_api != kv_api {
                return Ok(());
            }
            if downstream.filter_loop && filtered_entries.is_none() {
                return Ok(());
            }

            let entries_clone = if downstream.filter_loop {
                downstream
                    .observed_range
                    .filter_entries(filtered_entries.clone().unwrap())
            } else {
                downstream.observed_range.filter_entries(entries.clone())
            };

            if entries_clone.is_empty() {
                return Ok(());
            }

            let event = Event {
                region_id,
                request_id: downstream.get_req_id(),
                index,
                event: Some(Event_oneof_event::Entries(EventEntries {
                    entries: entries_clone.into(),
                    ..Default::default()
                })),
                ..Default::default()
            };

            // Do not force send for real time change data events.
            let force_send = false;
            downstream.sink_event(event, force_send)
        };
        match self.broadcast(send) {
            Ok(()) => Ok(()),
            Err(e) => {
                self.mark_failed();
                Err(e)
            }
        }
    }

    fn sink_put(
        &mut self,
        put: PutRequest,
        is_one_pc: bool,
        txn_rows: &mut HashMap<Vec<u8>, (EventRow, bool)>,
        raw_rows: &mut Vec<EventRow>,
        read_old_value: impl FnMut(&mut EventRow, TimeStamp) -> Result<()>,
    ) -> Result<()> {
        let key_mode = ApiV2::parse_key_mode(put.get_key());
        if key_mode == KeyMode::Raw {
            self.sink_raw_put(put, raw_rows)
        } else {
            self.sink_txn_put(put, is_one_pc, txn_rows, read_old_value)
        }
    }

    fn sink_raw_put(&mut self, mut put: PutRequest, rows: &mut Vec<EventRow>) -> Result<()> {
        let mut row = EventRow::default();
        decode_rawkv(put.take_key(), put.take_value(), &mut row)?;
        rows.push(row);
        Ok(())
    }

    fn sink_txn_put(
        &mut self,
        mut put: PutRequest,
        is_one_pc: bool,
        rows: &mut HashMap<Vec<u8>, (EventRow, bool)>,
        mut read_old_value: impl FnMut(&mut EventRow, TimeStamp) -> Result<()>,
    ) -> Result<()> {
        match put.cf.as_str() {
            "write" => {
                let (mut row, mut has_value) = (EventRow::default(), false);
                if decode_write(put.take_key(), &put.value, &mut row, &mut has_value, true) {
                    return Ok(());
                }

                let commit_ts = if is_one_pc {
                    set_event_row_type(&mut row, EventLogType::Committed);
                    let commit_ts = TimeStamp::from(row.commit_ts);
                    read_old_value(&mut row, commit_ts.prev())?;
                    Some(commit_ts)
                } else {
                    // 2PC
                    if row.commit_ts == 0 {
                        None
                    } else {
                        Some(TimeStamp::from(row.commit_ts))
                    }
                };
                // validate commit_ts must be greater than the current resolved_ts
                if let (Some(resolver), Some(commit_ts)) = (&self.resolver, commit_ts) {
                    let resolved_ts = resolver.resolved_ts();
                    assert!(
                        commit_ts > resolved_ts,
                        "region {} commit_ts: {:?}, resolved_ts: {:?}",
                        self.region_id,
                        commit_ts,
                        resolved_ts
                    );
                }

                match rows.entry(row.key.clone()) {
                    HashMapEntry::Occupied(o) => {
                        let o = o.into_mut();
                        mem::swap(&mut o.0.value, &mut row.value);
                        o.0 = row;
                    }
                    HashMapEntry::Vacant(v) => {
                        v.insert((row, has_value));
                    }
                }
            }
            "lock" => {
                let (mut row, mut has_value) = (EventRow::default(), false);
                let lock = Lock::parse(put.get_value()).unwrap();
                let for_update_ts = lock.for_update_ts;
                if decode_lock(put.take_key(), lock, &mut row, &mut has_value) {
                    return Ok(());
                }

                let read_old_ts = std::cmp::max(for_update_ts, row.start_ts.into());
                read_old_value(&mut row, read_old_ts)?;

                // In order to compute resolved ts, we must track inflight txns.
                match self.resolver {
                    Some(ref mut resolver) => {
                        resolver.track_lock(row.start_ts.into(), row.key.clone(), None)?;
                    }
                    None => {
                        assert!(self.pending.is_some(), "region resolver not ready");
                        let pending = self.pending.as_mut().unwrap();
                        pending.push_pending_lock(PendingLock::Track {
                            key: row.key.clone(),
                            start_ts: row.start_ts.into(),
                        })?;
                    }
                }

                let occupied = rows.entry(row.key.clone()).or_default();
                if occupied.1 {
                    assert!(!has_value);
                    has_value = true;
                    mem::swap(&mut occupied.0.value, &mut row.value);
                }
                *occupied = (row, has_value);
            }
            "" | "default" => {
                let key = Key::from_encoded(put.take_key()).truncate_ts().unwrap();
                let row = rows.entry(key.into_raw().unwrap()).or_default();
                decode_default(put.take_value(), &mut row.0, &mut row.1);
            }
            other => panic!("invalid cf {}", other),
        }
        Ok(())
    }

    fn sink_delete(&mut self, mut delete: DeleteRequest) -> Result<()> {
        match delete.cf.as_str() {
            "lock" => {
                let raw_key = Key::from_encoded(delete.take_key()).into_raw().unwrap();
                match self.resolver {
                    Some(ref mut resolver) => resolver.untrack_lock(&raw_key, None),
                    None => {
                        assert!(self.pending.is_some(), "region resolver not ready");
                        let pending = self.pending.as_mut().unwrap();
                        pending.push_pending_lock(PendingLock::Untrack { key: raw_key })?;
                    }
                }
            }
            "" | "default" | "write" => {}
            other => {
                panic!("invalid cf {}", other);
            }
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
        self.mark_failed();
        Err(Error::request(store_err.into()))
    }

    fn add_downstream(&mut self, downstream: Downstream) {
        self.downstreams_mut().push(downstream);
        self.txn_extra_op.store(TxnExtraOp::ReadOldValue);
    }

    fn remove_downstream(&mut self, id: DownstreamId) -> Option<Downstream> {
        let downstreams = self.downstreams_mut();
        if let Some(index) = downstreams.iter().position(|x| x.id == id) {
            let downstream = downstreams.swap_remove(index);
            if downstreams.is_empty() {
                // Stop observing when the last downstream is removed. Otherwise the observer
                // will keep pushing events to the delegate.
                self.stop_observing();
            }
            return Some(downstream);
        }
        None
    }

    fn check_epoch_on_ready(&self, downstream: &Downstream) -> Result<()> {
        let region = self.region.as_ref().unwrap();
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
                "req_id" => downstream.req_id,
                "err" => ?e
            );
            // Downstream is outdated, mark stop.
            downstream.state.store(DownstreamState::Stopped);
            return Err(Error::request(e.into()));
        }
        Ok(())
    }

    fn stop_observing(&self) {
        info!("cdc stop observing"; "region_id" => self.region_id, "failed" => self.failed);
        // Stop observe further events.
        self.handle.stop_observing();
        // To inform transaction layer no more old values are required for the region.
        self.txn_extra_op.store(TxnExtraOp::Noop);
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

fn decode_lock(key: Vec<u8>, lock: Lock, row: &mut EventRow, has_value: &mut bool) -> bool {
    let op_type = match lock.lock_type {
        LockType::Put => EventRowOpType::Put,
        LockType::Delete => EventRowOpType::Delete,
        other => {
            debug!("cdc skip lock record";
                "type" => ?other,
                "start_ts" => ?lock.ts,
                "key" => &log_wrappers::Value::key(&key),
                "for_update_ts" => ?lock.for_update_ts);
            return true;
        }
    };
    let key = Key::from_encoded(key);
    row.start_ts = lock.ts.into_inner();
    row.key = key.into_raw().unwrap();
    row.op_type = op_type as _;
    // used for filter out the event. see `txn_source` field for more detail.
    row.txn_source = lock.txn_source;
    set_event_row_type(row, EventLogType::Prewrite);
    if let Some(value) = lock.short_value {
        row.value = value;
        *has_value = true;
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
#[derive(Clone, Default)]
pub struct ObservedRange {
    start_key_encoded: Vec<u8>,
    end_key_encoded: Vec<u8>,
    start_key_raw: Vec<u8>,
    end_key_raw: Vec<u8>,
    pub(crate) all_key_covered: bool,
}

impl ObservedRange {
    pub fn new(start_key_encoded: Vec<u8>, end_key_encoded: Vec<u8>) -> Result<ObservedRange> {
        let start_key_raw = Key::from_encoded(start_key_encoded.clone())
            .into_raw()
            .map_err(|e| Error::Other(e.into()))?;
        let end_key_raw = Key::from_encoded(end_key_encoded.clone())
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
        if self.start_key_encoded <= region.start_key {
            if self.end_key_encoded.is_empty()
                || (region.end_key <= self.end_key_encoded && !region.end_key.is_empty())
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
        self.is_key_in_range(&self.start_key_encoded, &self.end_key_encoded, key)
    }

    pub fn filter_entries(&self, mut entries: Vec<EventRow>) -> Vec<EventRow> {
        if self.all_key_covered {
            return entries;
        }
        // Entry's key is in raw key format.
        entries.retain(|e| self.is_key_in_range(&self.start_key_raw, &self.end_key_raw, &e.key));
        entries
    }
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;

    use api_version::RawValue;
    use futures::{executor::block_on, stream::StreamExt};
    use kvproto::{errorpb::Error as ErrorHeader, metapb::Region};
    use tikv_util::memory::MemoryQuota;

    use super::*;
    use crate::channel::{channel, recv_timeout};

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
        let (sink, mut drain) = crate::channel::channel(1, quota);
        let rx = drain.drain();
        let request_id = 123;
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
        let memory_quota = Arc::new(MemoryQuota::new(usize::MAX));
        let mut delegate = Delegate::new(region_id, Default::default(), memory_quota);
        delegate.subscribe(downstream).unwrap();
        assert!(delegate.handle.is_observing());
        let memory_quota = Arc::new(MemoryQuota::new(std::usize::MAX));
        let resolver = Resolver::new(region_id, memory_quota);
        assert!(
            delegate
                .on_region_ready(resolver, region)
                .unwrap()
                .is_empty()
        );
        assert!(delegate.downstreams()[0].observed_range.all_key_covered);

        let rx_wrap = Cell::new(Some(rx));
        let receive_error = || {
            let (event, rx) = block_on(rx_wrap.replace(None).unwrap().into_future());
            rx_wrap.set(Some(rx));
            let event = event.unwrap();
            assert!(
                matches!(event.0, CdcEvent::Event(_)),
                "unknown event {:?}",
                event
            );
            if let CdcEvent::Event(mut e) = event.0 {
                assert_eq!(e.get_request_id(), request_id);
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
        let new_downstream = |id: u64, region_version: u64| {
            let peer = format!("{}", id);
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
        let mut delegate = Delegate::new(1, txn_extra_op.clone(), memory_quota);
        assert_eq!(txn_extra_op.load(), TxnExtraOp::Noop);
        assert!(delegate.handle.is_observing());

        // Subscribe once.
        let downstream1 = new_downstream(1, 1);
        let downstream1_id = downstream1.id;
        delegate.subscribe(downstream1).unwrap();
        assert_eq!(txn_extra_op.load(), TxnExtraOp::ReadOldValue);
        assert!(delegate.handle.is_observing());

        // Subscribe twice and then unsubscribe the second downstream.
        let downstream2 = new_downstream(2, 1);
        let downstream2_id = downstream2.id;
        delegate.subscribe(downstream2).unwrap();
        assert!(!delegate.unsubscribe(downstream2_id, None));
        assert_eq!(txn_extra_op.load(), TxnExtraOp::ReadOldValue);
        assert!(delegate.handle.is_observing());

        // `on_region_ready` when the delegate isn't resolved.
        delegate.subscribe(new_downstream(1, 2)).unwrap();
        let mut region = Region::default();
        region.mut_region_epoch().set_conf_ver(1);
        region.mut_region_epoch().set_version(1);
        {
            let memory_quota = Arc::new(MemoryQuota::new(std::usize::MAX));
            let failures = delegate
                .on_region_ready(Resolver::new(1, memory_quota), region)
                .unwrap();
            assert_eq!(failures.len(), 1);
            let id = failures[0].0.id;
            delegate.unsubscribe(id, None);
            assert_eq!(delegate.downstreams().len(), 1);
        }
        assert_eq!(txn_extra_op.load(), TxnExtraOp::ReadOldValue);
        assert!(delegate.handle.is_observing());

        // Subscribe with an invalid epoch.
        delegate.subscribe(new_downstream(1, 2)).unwrap_err();
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
        let mut delegate = Delegate::new(1, txn_extra_op, memory_quota);
        assert!(delegate.handle.is_observing());

        let mut map = HashMap::default();
        for k in b'a'..=b'e' {
            let mut put = PutRequest::default();
            put.key = Key::from_raw(&[k]).into_encoded();
            put.cf = "lock".to_owned();
            put.value = Lock::new(
                LockType::Put,
                put.key.clone(),
                1.into(),
                10,
                None,
                TimeStamp::zero(),
                0,
                TimeStamp::zero(),
                false,
            )
            .to_bytes();
            delegate
                .sink_txn_put(
                    put,
                    false,
                    &mut map,
                    |_: &mut EventRow, _: TimeStamp| Ok(()),
                )
                .unwrap();
        }
        assert_eq!(map.len(), 5);

        let (sink, mut drain) = channel(1, Arc::new(MemoryQuota::new(1024)));
        let downstream = Downstream {
            id: DownstreamId::new(),
            req_id: 1,
            conn_id: ConnId::new(),
            peer: String::new(),
            region_epoch: RegionEpoch::default(),
            sink: Some(sink),
            state: Arc::new(AtomicCell::new(DownstreamState::Normal)),
            scan_truncated: Arc::new(Default::default()),
            kv_api: ChangeDataRequestKvApi::TiDb,
            filter_loop: false,
            observed_range,
        };
        delegate.add_downstream(downstream);
        let entries = map.values().map(|(r, _)| r).cloned().collect();
        delegate
            .sink_downstream(entries, 1, ChangeDataRequestKvApi::TiDb)
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
        let mut delegate = Delegate::new(1, txn_extra_op, memory_quota);
        assert!(delegate.handle.is_observing());

        let mut map = HashMap::default();
        for k in b'a'..=b'e' {
            let mut put = PutRequest::default();
            put.key = Key::from_raw(&[k]).into_encoded();
            put.cf = "lock".to_owned();
            let mut lock = Lock::new(
                LockType::Put,
                put.key.clone(),
                1.into(),
                10,
                None,
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
            delegate
                .sink_txn_put(
                    put,
                    false,
                    &mut map,
                    |_: &mut EventRow, _: TimeStamp| Ok(()),
                )
                .unwrap();
        }
        assert_eq!(map.len(), 5);

        let (sink, mut drain) = channel(1, Arc::new(MemoryQuota::new(1024)));
        let downstream = Downstream {
            id: DownstreamId::new(),
            req_id: 1,
            conn_id: ConnId::new(),
            peer: String::new(),
            region_epoch: RegionEpoch::default(),
            sink: Some(sink),
            state: Arc::new(AtomicCell::new(DownstreamState::Normal)),
            scan_truncated: Arc::new(Default::default()),
            kv_api: ChangeDataRequestKvApi::TiDb,
            filter_loop,
            observed_range,
        };
        delegate.add_downstream(downstream);
        let entries = map.values().map(|(r, _)| r).cloned().collect();
        delegate
            .sink_downstream(entries, 1, ChangeDataRequestKvApi::TiDb)
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
}
