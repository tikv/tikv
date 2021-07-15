// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::cell::RefCell;
use std::mem;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

use crossbeam::atomic::AtomicCell;
#[cfg(feature = "prost-codec")]
use kvproto::cdcpb::{
    event::{
        row::OpType as EventRowOpType, Entries as EventEntries, Event as Event_oneof_event,
        LogType as EventLogType, Row as EventRow,
    },
    Error as EventError, Event,
};
#[cfg(not(feature = "prost-codec"))]
use kvproto::cdcpb::{
    Error as EventError, Event, EventEntries, EventLogType, EventRow, EventRowOpType,
    Event_oneof_event,
};
use kvproto::errorpb;
use kvproto::kvrpcpb::ExtraOp as TxnExtraOp;
use kvproto::metapb::{Region, RegionEpoch};
use kvproto::raft_cmdpb::{AdminCmdType, AdminRequest, AdminResponse, CmdType, Request};
use raftstore::coprocessor::{Cmd, CmdBatch};
use raftstore::store::fsm::ObserveID;
use raftstore::store::util::compare_region_epoch;
use raftstore::Error as RaftStoreError;
use resolved_ts::Resolver;
use tikv::storage::txn::TxnEntry;
use tikv_util::collections::HashMap;
use tikv_util::time::Instant;
use txn_types::{Key, Lock, LockType, TimeStamp, WriteRef, WriteType};

use crate::channel::{SendError, Sink};
use crate::endpoint::OldValueCallback;
use crate::endpoint::OldValueStats;
use crate::metrics::*;
use crate::service::{CdcEvent, ConnID};
use crate::{Error, Result};

const EVENT_MAX_SIZE: usize = 6 * 1024 * 1024; // 6MB
static DOWNSTREAM_ID_ALLOC: AtomicUsize = AtomicUsize::new(0);

/// A unique identifier of a Downstream.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct DownstreamID(usize);

impl DownstreamID {
    pub fn new() -> DownstreamID {
        DownstreamID(DOWNSTREAM_ID_ALLOC.fetch_add(1, Ordering::SeqCst))
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum DownstreamState {
    Uninitialized,
    Normal,
    Stopped,
}

impl Default for DownstreamState {
    fn default() -> Self {
        Self::Uninitialized
    }
}

#[derive(Clone)]
pub struct Downstream {
    // TODO: include cdc request.
    /// A unique identifier of the Downstream.
    id: DownstreamID,
    // The reqeust ID set by CDC to identify events corresponding different requests.
    req_id: u64,
    conn_id: ConnID,
    // The IP address of downstream.
    peer: String,
    region_epoch: RegionEpoch,
    sink: Option<Sink>,
    state: Arc<AtomicCell<DownstreamState>>,
    enable_old_value: bool,
}

impl Downstream {
    /// Create a Downsteam.
    ///
    /// peer is the address of the downstream.
    /// sink sends data to the downstream.
    pub fn new(
        peer: String,
        region_epoch: RegionEpoch,
        req_id: u64,
        conn_id: ConnID,
        enable_old_value: bool,
    ) -> Downstream {
        Downstream {
            id: DownstreamID::new(),
            req_id,
            conn_id,
            peer,
            region_epoch,
            sink: None,
            state: Arc::new(AtomicCell::new(DownstreamState::default())),
            enable_old_value,
        }
    }

    /// Sink events to the downstream.
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

    pub fn sink_error_event(&self, region_id: u64, err_event: EventError) -> Result<()> {
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

    pub fn get_id(&self) -> DownstreamID {
        self.id
    }

    pub fn get_state(&self) -> Arc<AtomicCell<DownstreamState>> {
        self.state.clone()
    }

    pub fn get_conn_id(&self) -> ConnID {
        self.conn_id
    }
}

#[derive(Default)]
struct Pending {
    pub downstreams: Vec<Downstream>,
    pub locks: Vec<PendingLock>,
    pub pending_bytes: usize,
}

impl Drop for Pending {
    fn drop(&mut self) {
        CDC_PENDING_BYTES_GAUGE.sub(self.pending_bytes as i64);
    }
}

impl Pending {
    fn take_downstreams(&mut self) -> Vec<Downstream> {
        mem::take(&mut self.downstreams)
    }

    fn take_locks(&mut self) -> Vec<PendingLock> {
        mem::take(&mut self.locks)
    }
}

enum PendingLock {
    Track {
        key: Vec<u8>,
        start_ts: TimeStamp,
    },
    Untrack {
        key: Vec<u8>,
        start_ts: TimeStamp,
        commit_ts: Option<TimeStamp>,
    },
}

/// A CDC delegate of a raftstore region peer.
///
/// It converts raft commands into CDC events and broadcast to downstreams.
/// It also track trancation on the fly in order to compute resolved ts.
pub struct Delegate {
    pub id: ObserveID,
    pub region_id: u64,
    region: Option<Region>,
    pub downstreams: Vec<Downstream>,
    pub resolver: Option<Resolver>,
    pending: Option<Pending>,
    enabled: Arc<AtomicBool>,
    failed: bool,
    pub txn_extra_op: TxnExtraOp,
    pub old_value_cache_miss_cnt: usize,
}

impl Delegate {
    /// Create a Delegate the given region.
    pub fn new(region_id: u64) -> Delegate {
        Delegate {
            region_id,
            id: ObserveID::new(),
            downstreams: Vec::new(),
            resolver: None,
            region: None,
            pending: Some(Pending::default()),
            enabled: Arc::new(AtomicBool::new(true)),
            failed: false,
            txn_extra_op: TxnExtraOp::default(),
            old_value_cache_miss_cnt: 0,
        }
    }

    /// Returns a shared flag.
    /// True if there are some active downstreams subscribe the region.
    /// False if all downstreams has unsubscribed.
    pub fn enabled(&self) -> Arc<AtomicBool> {
        self.enabled.clone()
    }

    /// Return false if subscribe failed.
    pub fn subscribe(&mut self, downstream: Downstream) -> bool {
        if let Some(region) = self.region.as_ref() {
            if let Err(e) = compare_region_epoch(
                &downstream.region_epoch,
                region,
                false, /* check_conf_ver */
                true,  /* check_ver */
                true,  /* include_region */
            ) {
                info!("cdc fail to subscribe downstream";
                    "region_id" => region.get_id(),
                    "downstream_id" => ?downstream.get_id(),
                    "conn_id" => ?downstream.get_conn_id(),
                    "req_id" => downstream.req_id,
                    "err" => ?e);
                let err = Error::Request(e.into());
                let error_event = self.error_event(err);
                if let Err(err) = downstream.sink_error_event(self.region_id, error_event.clone()) {
                    warn!("cdc send subscribe error failed";
                        "region_id" => self.region_id, "error" => ?err, "origin_error" => ?error_event,
                        "downstream_id" => ?downstream.id, "downstream" => ?downstream.peer,
                        "request_id" => downstream.req_id, "conn_id" => ?downstream.conn_id);
                }
                return false;
            }
            self.downstreams.push(downstream);
        } else {
            self.pending.as_mut().unwrap().downstreams.push(downstream);
        }
        true
    }

    pub fn downstream(&self, downstream_id: DownstreamID) -> Option<&Downstream> {
        self.downstreams.iter().find(|d| d.id == downstream_id)
    }

    pub fn downstreams(&self) -> &Vec<Downstream> {
        if self.pending.is_some() {
            &self.pending.as_ref().unwrap().downstreams
        } else {
            &self.downstreams
        }
    }

    pub fn downstreams_mut(&mut self) -> &mut Vec<Downstream> {
        if self.pending.is_some() {
            &mut self.pending.as_mut().unwrap().downstreams
        } else {
            &mut self.downstreams
        }
    }

    pub fn unsubscribe(&mut self, id: DownstreamID, err: Option<Error>) -> bool {
        let error_event = err.map(|err| self.error_event(err));
        let region_id = self.region_id;
        let downstreams = self.downstreams_mut();
        downstreams.retain(|d| {
            if d.id == id {
                if let Some(error_event) = error_event.clone() {
                    if let Err(err) = d.sink_error_event(region_id, error_event.clone()) {
                        warn!("cdc send unsubscribe failed";
                            "region_id" => region_id, "error" => ?err, "origin_error" => ?error_event,
                            "downstream_id" => ?d.id, "downstream" => ?d.peer,
                            "request_id" => d.req_id, "conn_id" => ?d.conn_id);
                    }
                }
                d.state.store(DownstreamState::Stopped);
            }
            d.id != id
        });
        let is_last = downstreams.is_empty();
        if is_last {
            self.enabled.store(false, Ordering::SeqCst);
        }
        is_last
    }

    fn error_event(&self, err: Error) -> EventError {
        let mut err_event = EventError::default();
        let mut err = err.extract_region_error();
        if err.has_not_leader() {
            let not_leader = err.take_not_leader();
            err_event.set_not_leader(not_leader);
        } else if err.has_epoch_not_match() {
            let epoch_not_match = err.take_epoch_not_match();
            err_event.set_epoch_not_match(epoch_not_match);
        } else {
            // TODO: Add more errors to the cdc protocol
            let mut region_not_found = errorpb::RegionNotFound::default();
            region_not_found.set_region_id(self.region_id);
            err_event.set_region_not_found(region_not_found);
        }
        err_event
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
        // Stop observe further events.
        self.enabled.store(false, Ordering::SeqCst);

        info!("cdc met region error";
            "region_id" => self.region_id, "error" => ?err);
        let region_id = self.region_id;
        let error = self.error_event(err);
        let send = move |downstream: &Downstream| {
            downstream.state.store(DownstreamState::Stopped);
            let error_event = error.clone();
            if let Err(err) = downstream.sink_error_event(region_id, error_event) {
                warn!("cdc broadcast error failed";
                    "region_id" => region_id, "error" => ?err, "origin_error" => ?error,
                    "downstream_id" => ?downstream.id, "downstream" => ?downstream.peer,
                    "request_id" => downstream.req_id, "conn_id" => ?downstream.conn_id);
            }
            Ok(())
        };

        // TODO: In case we drop error messages, maybe we need a heartbeat mechanism
        //       to allow TiCDC detect region status.
        let _ = self.broadcast(send);
    }

    fn broadcast<F>(&self, send: F) -> Result<()>
    where
        F: Fn(&Downstream) -> Result<()>,
    {
        let downstreams = self.downstreams();
        assert!(
            !downstreams.is_empty(),
            "region {} miss downstream",
            self.region_id,
        );
        for downstream in downstreams {
            send(downstream)?;
        }
        Ok(())
    }

    /// Install a resolver and return pending downstreams.
    pub fn on_region_ready(&mut self, mut resolver: Resolver, region: Region) -> Vec<Downstream> {
        assert!(
            self.resolver.is_none(),
            "region {} resolver should not be ready",
            self.region_id,
        );
        // Mark the delegate as initialized.
        self.region = Some(region);
        let mut pending = self.pending.take().unwrap();
        for lock in pending.take_locks() {
            match lock {
                PendingLock::Track { key, start_ts } => resolver.track_lock(start_ts, key),
                PendingLock::Untrack {
                    key,
                    start_ts,
                    commit_ts,
                } => resolver.untrack_lock(start_ts, commit_ts, key),
            }
        }
        self.resolver = Some(resolver);
        info!("cdc region is ready"; "region_id" => self.region_id);
        pending.take_downstreams()
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
        let resolved_ts = match resolver.resolve(min_ts) {
            Some(rts) => rts,
            None => return None,
        };
        debug!("cdc resolved ts updated";
            "region_id" => self.region_id, "resolved_ts" => resolved_ts);
        CDC_RESOLVED_TS_GAP_HISTOGRAM
            .observe((min_ts.physical() - resolved_ts.physical()) as f64 / 1000f64);
        Some(resolved_ts)
    }

    pub fn on_batch(
        &mut self,
        batch: CmdBatch,
        old_value_cb: Rc<RefCell<OldValueCallback>>,
        old_value_stats: &mut OldValueStats,
    ) -> Result<()> {
        // Stale CmdBatch, drop it sliently.
        if batch.observe_id != self.id {
            return Ok(());
        }
        for cmd in batch.into_iter(self.region_id) {
            let Cmd {
                index,
                mut request,
                mut response,
            } = cmd;
            if response.get_header().has_error() {
                let err_header = response.mut_header().take_error();
                self.mark_failed();
                return Err(Error::Request(err_header));
            }
            if !request.has_admin_request() {
                self.sink_data(
                    index,
                    request.requests.into(),
                    old_value_cb.clone(),
                    old_value_stats,
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
        entries: Vec<Option<TxnEntry>>,
    ) -> Vec<CdcEvent> {
        let entries_len = entries.len();
        let mut rows = vec![Vec::with_capacity(entries_len)];
        let mut current_rows_size: usize = 0;
        for entry in entries {
            match entry {
                Some(TxnEntry::Prewrite {
                    default,
                    lock,
                    old_value,
                }) => {
                    let mut row = EventRow::default();
                    let skip = decode_lock(lock.0, Lock::parse(&lock.1).unwrap(), &mut row);
                    if skip {
                        continue;
                    }
                    decode_default(default.1, &mut row);
                    let row_size = row.key.len() + row.value.len();
                    if current_rows_size + row_size >= EVENT_MAX_SIZE {
                        rows.push(Vec::with_capacity(entries_len));
                        current_rows_size = 0;
                    }
                    current_rows_size += row_size;
                    row.old_value = old_value.unwrap_or_default();
                    rows.last_mut().unwrap().push(row);
                }
                Some(TxnEntry::Commit {
                    default,
                    write,
                    old_value,
                }) => {
                    let mut row = EventRow::default();
                    let skip = decode_write(write.0, &write.1, &mut row);
                    if skip {
                        continue;
                    }
                    decode_default(default.1, &mut row);

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
                    row.old_value = old_value.unwrap_or_default();
                    let row_size = row.key.len() + row.value.len();
                    if current_rows_size + row_size >= EVENT_MAX_SIZE {
                        rows.push(Vec::with_capacity(entries_len));
                        current_rows_size = 0;
                    }
                    current_rows_size += row_size;
                    rows.last_mut().unwrap().push(row);
                }
                None => {
                    let mut row = EventRow::default();

                    // This type means scan has finised.
                    set_event_row_type(&mut row, EventLogType::Initialized);
                    rows.last_mut().unwrap().push(row);
                }
            }
        }

        rows.into_iter()
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
            .collect()
    }

    fn sink_data(
        &mut self,
        index: u64,
        requests: Vec<Request>,
        old_value_cb: Rc<RefCell<OldValueCallback>>,
        old_value_stats: &mut OldValueStats,
    ) -> Result<()> {
        let mut rows = HashMap::default();
        for mut req in requests {
            // CDC cares about put requests only.
            if req.get_cmd_type() != CmdType::Put {
                // Do not log delete requests because they are issued by GC
                // frequently.
                if req.get_cmd_type() != CmdType::Delete {
                    debug!(
                        "skip other command";
                        "region_id" => self.region_id,
                        "command" => ?req,
                    );
                }
                continue;
            }
            let mut put = req.take_put();
            match put.cf.as_str() {
                "write" => {
                    let mut row = EventRow::default();
                    let skip = decode_write(put.take_key(), put.get_value(), &mut row);
                    if skip {
                        continue;
                    }

                    // In order to advance resolved ts,
                    // we must untrack inflight txns if they are committed.
                    let commit_ts = if row.commit_ts == 0 {
                        None
                    } else {
                        Some(row.commit_ts)
                    };
                    match self.resolver {
                        Some(ref mut resolver) => resolver.untrack_lock(
                            row.start_ts.into(),
                            commit_ts.map(Into::into),
                            row.key.clone(),
                        ),
                        None => {
                            assert!(self.pending.is_some(), "region resolver not ready");
                            let pending = self.pending.as_mut().unwrap();
                            pending.locks.push(PendingLock::Untrack {
                                key: row.key.clone(),
                                start_ts: row.start_ts.into(),
                                commit_ts: commit_ts.map(Into::into),
                            });
                            pending.pending_bytes += row.key.len();
                            CDC_PENDING_BYTES_GAUGE.add(row.key.len() as i64);
                        }
                    }

                    let r = rows.insert(row.key.clone(), row);
                    assert!(r.is_none());
                }
                "lock" => {
                    let mut row = EventRow::default();
                    let lock = Lock::parse(put.get_value()).unwrap();
                    let for_update_ts = lock.for_update_ts;
                    let skip = decode_lock(put.take_key(), lock, &mut row);
                    if skip {
                        continue;
                    }

                    if self.txn_extra_op == TxnExtraOp::ReadOldValue {
                        let key = Key::from_raw(&row.key).append_ts(row.start_ts.into());
                        let start = Instant::now();
                        let (old_value, statistics) = old_value_cb.borrow_mut().as_mut()(
                            key,
                            std::cmp::max(for_update_ts, row.start_ts.into()),
                            old_value_stats,
                        );
                        row.old_value = old_value.unwrap_or_default();
                        CDC_OLD_VALUE_DURATION_HISTOGRAM
                            .with_label_values(&["all"])
                            .observe(start.saturating_elapsed().as_secs_f64());
                        if let Some(statistics) = statistics {
                            for (cf, cf_details) in statistics.details().iter() {
                                for (tag, count) in cf_details.iter() {
                                    CDC_OLD_VALUE_SCAN_DETAILS
                                        .with_label_values(&[*cf, *tag])
                                        .inc_by(*count as i64);
                                }
                            }
                        }
                    }

                    let occupied = rows.entry(row.key.clone()).or_default();
                    if !occupied.value.is_empty() {
                        assert!(row.value.is_empty());
                        let mut value = vec![];
                        mem::swap(&mut occupied.value, &mut value);
                        row.value = value;
                    }

                    // In order to compute resolved ts,
                    // we must track inflight txns.
                    match self.resolver {
                        Some(ref mut resolver) => {
                            resolver.track_lock(row.start_ts.into(), row.key.clone())
                        }
                        None => {
                            assert!(self.pending.is_some(), "region resolver not ready");
                            let pending = self.pending.as_mut().unwrap();
                            pending.locks.push(PendingLock::Track {
                                key: row.key.clone(),
                                start_ts: row.start_ts.into(),
                            });
                            pending.pending_bytes += row.key.len();
                            CDC_PENDING_BYTES_GAUGE.add(row.key.len() as i64);
                        }
                    }

                    *occupied = row;
                }
                "" | "default" => {
                    let key = Key::from_encoded(put.take_key()).truncate_ts().unwrap();
                    let row = rows.entry(key.into_raw().unwrap()).or_default();
                    decode_default(put.take_value(), row);
                }
                other => {
                    panic!("invalid cf {}", other);
                }
            }
        }

        // Skip broadcast if there is no Put or Delete.
        if rows.is_empty() {
            return Ok(());
        }

        let mut entries = Vec::with_capacity(rows.len());
        for (_, v) in rows {
            entries.push(v);
        }
        let mut event_entries = EventEntries::default();
        event_entries.entries = entries.into();
        let mut change_data_event = Event::default();
        change_data_event.region_id = self.region_id;
        change_data_event.index = index;
        change_data_event.event = Some(Event_oneof_event::Entries(event_entries));

        let txn_extra_op = self.txn_extra_op;
        let send = move |downstream: &Downstream| {
            if downstream.state.load() != DownstreamState::Normal {
                return Ok(());
            }
            let mut event = change_data_event.clone();
            if !downstream.enable_old_value && txn_extra_op == TxnExtraOp::ReadOldValue {
                if let Some(Event_oneof_event::Entries(ref mut entries)) = event.event {
                    for entry in entries.mut_entries().iter_mut() {
                        entry.mut_old_value().clear();
                    }
                }
            }
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
        Err(Error::Request(store_err.into()))
    }
}

fn set_event_row_type(row: &mut EventRow, ty: EventLogType) {
    #[cfg(feature = "prost-codec")]
    {
        row.r#type = ty.into();
    }
    #[cfg(not(feature = "prost-codec"))]
    {
        row.r_type = ty;
    }
}

fn decode_write(key: Vec<u8>, value: &[u8], row: &mut EventRow) -> bool {
    let write = WriteRef::parse(value).unwrap().to_owned();
    let (op_type, r_type) = match write.write_type {
        WriteType::Put => (EventRowOpType::Put, EventLogType::Commit),
        WriteType::Delete => (EventRowOpType::Delete, EventLogType::Commit),
        WriteType::Rollback => (EventRowOpType::Unknown, EventLogType::Rollback),
        other => {
            debug!("cdc skip write record"; "write" => ?other, "key" => &log_wrappers::Value::key(&key));
            return true;
        }
    };
    let key = Key::from_encoded(key);
    let commit_ts = if write.write_type == WriteType::Rollback {
        0
    } else {
        key.decode_ts().unwrap().into_inner()
    };
    row.start_ts = write.start_ts.into_inner();
    row.commit_ts = commit_ts;
    row.key = key.truncate_ts().unwrap().into_raw().unwrap();
    row.op_type = op_type.into();
    set_event_row_type(row, r_type);
    if let Some(value) = write.short_value {
        row.value = value;
    }

    false
}

fn decode_lock(key: Vec<u8>, lock: Lock, row: &mut EventRow) -> bool {
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
    row.op_type = op_type.into();
    set_event_row_type(row, EventLogType::Prewrite);
    if let Some(value) = lock.short_value {
        row.value = value;
    }

    false
}

fn decode_default(value: Vec<u8>, row: &mut EventRow) {
    if !value.is_empty() {
        row.value = value.to_vec();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures03::{executor::block_on, StreamExt};
    use kvproto::errorpb::Error as ErrorHeader;
    use kvproto::metapb::Region;
    use std::cell::Cell;

    #[test]
    fn test_error() {
        let region_id = 1;
        let mut region = Region::default();
        region.set_id(region_id);
        region.mut_peers().push(Default::default());
        region.mut_region_epoch().set_version(2);
        region.mut_region_epoch().set_conf_ver(2);
        let region_epoch = region.get_region_epoch().clone();

        let quota = crate::channel::MemoryQuota::new(std::usize::MAX);
        let (sink, mut drain) = crate::channel::channel(1, quota);
        let rx = drain.drain();
        let request_id = 123;
        let mut downstream =
            Downstream::new(String::new(), region_epoch, request_id, ConnID::new(), true);
        downstream.set_sink(sink);
        let mut delegate = Delegate::new(region_id);
        delegate.subscribe(downstream);
        let enabled = delegate.enabled();
        assert!(enabled.load(Ordering::SeqCst));
        let mut resolver = Resolver::new(region_id);
        resolver.init();
        for downstream in delegate.on_region_ready(resolver, region) {
            delegate.subscribe(downstream);
        }

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
        delegate.stop(Error::Request(err_header));
        let err = receive_error();
        assert!(err.has_not_leader());
        // Enable is disabled by any error.
        assert!(!enabled.load(Ordering::SeqCst));

        let mut err_header = ErrorHeader::default();
        err_header.set_region_not_found(Default::default());
        delegate.stop(Error::Request(err_header));
        let err = receive_error();
        assert!(err.has_region_not_found());

        let mut err_header = ErrorHeader::default();
        err_header.set_epoch_not_match(Default::default());
        delegate.stop(Error::Request(err_header));
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
}
