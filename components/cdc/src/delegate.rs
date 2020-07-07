// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::collections::BTreeSet;
use std::mem;
use std::ops::Deref;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

use crossbeam::atomic::AtomicCell;
use engine_rocks::{RocksEngine, RocksSnapshot};
use engine_traits::{IterOptions, KvEngine, ReadOptions, CF_DEFAULT, CF_LOCK, CF_WRITE};
#[cfg(feature = "prost-codec")]
use kvproto::cdcpb::{
    error::DuplicateRequest as ErrorDuplicateRequest,
    event::{
        row::OpType as EventRowOpType, Entries as EventEntries, Event as Event_oneof_event,
        LogType as EventLogType, Row as EventRow,
    },
    Error as EventError, Event,
};
#[cfg(not(feature = "prost-codec"))]
use kvproto::cdcpb::{
    Error as EventError, ErrorDuplicateRequest, Event, EventEntries, EventLogType, EventRow,
    EventRowOpType, Event_oneof_event,
};
use kvproto::errorpb;
use kvproto::kvrpcpb::ExtraOp as TxnExtraOp;
use kvproto::metapb::{Peer, Region, RegionEpoch};
use kvproto::raft_cmdpb::{AdminCmdType, AdminRequest, AdminResponse, CmdType, Request};
use raftstore::coprocessor::{Cmd, CmdBatch};
use raftstore::store::fsm::ObserveID;
use raftstore::store::util::compare_region_epoch;
use raftstore::store::RegionSnapshot;
use raftstore::Error as RaftStoreError;
use resolved_ts::Resolver;
use tikv::storage::txn::TxnEntry;
use tikv::storage::{Cursor, ScanMode, Snapshot as EngineSnapshot, Statistics};
use tikv_util::collections::HashMap;
use tikv_util::mpsc::batch::Sender as BatchSender;
use tikv_util::Either;
use txn_types::{
    Key, Lock, LockType, MutationType, TimeStamp, TxnExtra, Value, WriteRef, WriteType,
};

use crate::metrics::*;
use crate::service::ConnID;
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
    sink: Option<BatchSender<(usize, Event)>>,
    state: Arc<AtomicCell<DownstreamState>>,
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
    ) -> Downstream {
        Downstream {
            id: DownstreamID::new(),
            req_id,
            conn_id,
            peer,
            region_epoch,
            sink: None,
            state: Arc::new(AtomicCell::new(DownstreamState::default())),
        }
    }

    /// Sink events to the downstream.
    /// The size of `Error` and `ResolvedTS` are considered zero.
    pub fn sink_event(&self, mut change_data_event: Event, size: usize) {
        change_data_event.set_request_id(self.req_id);
        if self
            .sink
            .as_ref()
            .unwrap()
            .send((size, change_data_event))
            .is_err()
        {
            error!("send event failed"; "downstream" => %self.peer);
        }
    }

    pub fn set_sink(&mut self, sink: BatchSender<(usize, Event)>) {
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

    pub fn sink_duplicate_error(&self, region_id: u64) {
        let mut change_data_event = Event::default();
        let mut cdc_err = EventError::default();
        let mut err = ErrorDuplicateRequest::default();
        err.set_region_id(region_id);
        cdc_err.set_duplicate_request(err);
        change_data_event.event = Some(Event_oneof_event::Error(cdc_err));
        change_data_event.region_id = region_id;
        self.sink_event(change_data_event, 0);
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
                info!("fail to subscribe downstream";
                    "region_id" => region.get_id(),
                    "downstream_id" => ?downstream.get_id(),
                    "conn_id" => ?downstream.get_conn_id(),
                    "req_id" => downstream.req_id,
                    "err" => ?e);
                let err = Error::Request(e.into());
                let change_data_error = self.error_event(err);
                downstream.sink_event(change_data_error, 0);
                return false;
            }
            self.downstreams.push(downstream);
        } else {
            self.pending.as_mut().unwrap().downstreams.push(downstream);
        }
        true
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
        let change_data_error = err.map(|err| self.error_event(err));
        let downstreams = self.downstreams_mut();
        downstreams.retain(|d| {
            if d.id == id {
                if let Some(change_data_error) = change_data_error.clone() {
                    d.sink_event(change_data_error, 0);
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

    fn error_event(&self, err: Error) -> Event {
        let mut change_data_event = Event::default();
        let mut cdc_err = EventError::default();
        let mut err = err.extract_error_header();
        if err.has_not_leader() {
            let not_leader = err.take_not_leader();
            cdc_err.set_not_leader(not_leader);
        } else if err.has_epoch_not_match() {
            let epoch_not_match = err.take_epoch_not_match();
            cdc_err.set_epoch_not_match(epoch_not_match);
        } else {
            // TODO: Add more errors to the cdc protocol
            let mut region_not_found = errorpb::RegionNotFound::default();
            region_not_found.set_region_id(self.region_id);
            cdc_err.set_region_not_found(region_not_found);
        }
        change_data_event.event = Some(Event_oneof_event::Error(cdc_err));
        change_data_event.region_id = self.region_id;
        change_data_event
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

        info!("region met error";
            "region_id" => self.region_id, "error" => ?err);
        let change_data_err = self.error_event(err);
        for d in &self.downstreams {
            d.state.store(DownstreamState::Stopped);
        }
        self.broadcast(change_data_err, 0, false);
    }

    fn broadcast(&self, change_data_event: Event, size: usize, normal_only: bool) {
        let downstreams = self.downstreams();
        assert!(
            !downstreams.is_empty(),
            "region {} miss downstream, event: {:?}",
            self.region_id,
            change_data_event,
        );
        for i in 0..downstreams.len() - 1 {
            if normal_only && downstreams[i].state.load() != DownstreamState::Normal {
                continue;
            }
            downstreams[i].sink_event(change_data_event.clone(), size);
        }
        downstreams
            .last()
            .unwrap()
            .sink_event(change_data_event, size);
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
        info!("region is ready"; "region_id" => self.region_id);
        pending.take_downstreams()
    }

    /// Try advance and broadcast resolved ts.
    pub fn on_min_ts(&mut self, min_ts: TimeStamp) -> Option<TimeStamp> {
        if self.resolver.is_none() {
            debug!("region resolver not ready";
                "region_id" => self.region_id, "min_ts" => min_ts);
            return None;
        }
        debug!("try to advance ts"; "region_id" => self.region_id, "min_ts" => min_ts);
        let resolver = self.resolver.as_mut().unwrap();
        let resolved_ts = match resolver.resolve(min_ts) {
            Some(rts) => rts,
            None => return None,
        };
        debug!("resolved ts updated";
            "region_id" => self.region_id, "resolved_ts" => resolved_ts);
        let mut change_data_event = Event::default();
        change_data_event.region_id = self.region_id;
        change_data_event.event = Some(Event_oneof_event::ResolvedTs(resolved_ts.into_inner()));
        self.broadcast(change_data_event, 0, true);
        CDC_RESOLVED_TS_GAP_HISTOGRAM
            .observe((min_ts.physical() - resolved_ts.physical()) as f64 / 1000f64);
        Some(resolved_ts)
    }

    pub fn on_batch(
        &mut self,
        batch: CmdBatch,
        txn_extra: &mut TxnExtra,
        kv_engine: &RocksEngine,
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
            if !response.get_header().has_error() {
                if !request.has_admin_request() {
                    self.sink_data(index, request.requests.into(), txn_extra, kv_engine)?;
                } else {
                    self.sink_admin(request.take_admin_request(), response.take_admin_response())?;
                }
            } else {
                let err_header = response.mut_header().take_error();
                self.mark_failed();
                return Err(Error::Request(err_header));
            }
        }
        Ok(())
    }

    pub fn on_scan(&mut self, downstream_id: DownstreamID, entries: Vec<Option<TxnEntry>>) {
        let downstreams = if let Some(pending) = self.pending.as_mut() {
            &pending.downstreams
        } else {
            &self.downstreams
        };
        let downstream = if let Some(d) = downstreams.iter().find(|d| d.id == downstream_id) {
            d
        } else {
            warn!("downstream not found"; "downstream_id" => ?downstream_id, "region_id" => self.region_id);
            return;
        };

        let entries_len = entries.len();
        let mut rows = vec![(0, Vec::with_capacity(entries_len))];
        let mut current_rows_size: usize = 0;
        for entry in entries {
            match entry {
                Some(TxnEntry::Prewrite {
                    default,
                    lock,
                    old_value,
                }) => {
                    let mut row = EventRow::default();
                    let skip = decode_lock(lock.0, &lock.1, &mut row);
                    if skip {
                        continue;
                    }
                    decode_default(default.1, &mut row);
                    let row_size = row.key.len() + row.value.len();
                    if current_rows_size + row_size >= EVENT_MAX_SIZE {
                        rows.last_mut().unwrap().0 = current_rows_size;
                        rows.push((0, Vec::with_capacity(entries_len)));
                        current_rows_size = 0;
                    }
                    current_rows_size += row_size;
                    row.old_value = old_value.unwrap_or_default();
                    rows.last_mut().unwrap().1.push(row);
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
                        rows.last_mut().unwrap().0 = current_rows_size;
                        rows.push((0, Vec::with_capacity(entries_len)));
                        current_rows_size = 0;
                    }
                    current_rows_size += row_size;
                    rows.last_mut().unwrap().1.push(row);
                }
                None => {
                    let mut row = EventRow::default();

                    // This type means scan has finised.
                    set_event_row_type(&mut row, EventLogType::Initialized);
                    rows.last_mut().unwrap().1.push(row);
                }
            }
        }

        for (s, rs) in rows {
            if !rs.is_empty() {
                let mut event_entries = EventEntries::default();
                event_entries.entries = rs.into();
                let mut change_data_event = Event::default();
                change_data_event.region_id = self.region_id;
                change_data_event.event = Some(Event_oneof_event::Entries(event_entries));
                downstream.sink_event(change_data_event, s);
            }
        }
    }

    fn sink_data(
        &mut self,
        index: u64,
        requests: Vec<Request>,
        txn_extra: &mut TxnExtra,
        kv_engine: &RocksEngine,
    ) -> Result<()> {
        let mut rows = HashMap::default();
        let mut total_size = 0;
        let mut to_seek_old = BTreeSet::new();
        let mut old_value_reader = None;
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
                    let skip = decode_lock(put.take_key(), put.get_value(), &mut row);
                    if skip {
                        continue;
                    }

                    if self.txn_extra_op == TxnExtraOp::ReadOldValue {
                        old_value_reader = old_value_reader.or_else(|| {
                            // Hack: init a empty region to pass the check
                            let mut region = Region::default();
                            region.mut_peers().push(Peer::default());
                            let snapshot = RegionSnapshot::from_snapshot(
                                Arc::new(kv_engine.snapshot()),
                                region,
                            );
                            Some(OldValueReader::new(snapshot))
                        });
                        match self.old_value_from_extra(
                            &row,
                            txn_extra,
                            old_value_reader.as_mut().unwrap(),
                        ) {
                            Either::Left(key) => {
                                to_seek_old.insert(key);
                            }
                            Either::Right(Some(value)) => row.old_value = value,
                            Either::Right(None) => {
                                // TODO(5kbpers): may add a log here.
                                self.mark_failed();
                                let store_err = RaftStoreError::RegionNotFound(self.region_id);
                                return Err(Error::Request(store_err.into()));
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
                    total_size += row.value.len();
                }
                other => {
                    panic!("invalid cf {}", other);
                }
            }
        }
        if !to_seek_old.is_empty() {
            self.seek_for_old_value(to_seek_old, &mut rows, old_value_reader.as_mut().unwrap())?;
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
        self.broadcast(change_data_event, total_size, true);
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
        Err(Error::Request(store_err.into()))
    }

    // Return Either::Right(Some(...)) if key was contained in the extra and can get it from short_value or default cf.
    // Return Either::Right(None) if key was contained in the extra and cannot get it from short_value and default cf.
    // Return Either::Left(key) if key wasn't contained in the extra, needs to be seeked from the engine.
    fn old_value_from_extra(
        &mut self,
        row: &EventRow,
        txn_extra: &mut TxnExtra,
        reader: &mut OldValueReader<RegionSnapshot<RocksSnapshot>>,
    ) -> Either<Key, Option<Value>> {
        let old_values = txn_extra.mut_old_values();
        let key = Key::from_raw(row.key.as_slice()).append_ts(row.start_ts.into());
        if let Some((old_value, mutation_type)) = old_values.remove(&key) {
            match mutation_type {
                MutationType::Insert => assert!(old_value.is_none()),
                MutationType::Put | MutationType::Delete => {
                    if let Some(old_value) = old_value {
                        let start_ts = old_value.start_ts;
                        return Either::Right(old_value.short_value.or_else(|| {
                            let prev_key = key.truncate_ts().unwrap().append_ts(start_ts);
                            let mut opts = ReadOptions::new();
                            opts.set_fill_cache(false);
                            reader.get_value_default(&prev_key)
                        }));
                    }
                }
                _ => unreachable!(),
            }
        }
        Either::Left(key)
    }

    // to_seek_old contains encoded keys.
    fn seek_for_old_value(
        &mut self,
        to_seek_old: BTreeSet<Key>,
        rows: &mut HashMap<Vec<u8>, EventRow>,
        reader: &mut OldValueReader<RegionSnapshot<RocksSnapshot>>,
    ) -> Result<()> {
        for key in to_seek_old {
            match reader.near_seek_old_value(&key)? {
                Some(value) => {
                    let raw_key = key.truncate_ts().unwrap().into_raw().unwrap();
                    rows.get_mut(&raw_key).unwrap().old_value = value;
                }
                None => {
                    self.mark_failed();
                    let store_err = RaftStoreError::RegionNotFound(self.region_id);
                    return Err(Error::Request(store_err.into()));
                }
            }
        }
        Ok(())
    }
}

struct OldValueReader<S: EngineSnapshot> {
    snapshot: S,
    write_cursor: Cursor<S::Iter>,
    // TODO(5kbpers): add a metric here.
    statistics: Statistics,
}

impl<S: EngineSnapshot> OldValueReader<S> {
    fn new(snapshot: S) -> Self {
        let mut iter_opts = IterOptions::default()
            .use_prefix_seek()
            .set_prefix_same_as_start(true);
        iter_opts.set_fill_cache(false);
        let write_cursor = snapshot
            .iter_cf(CF_WRITE, iter_opts, ScanMode::Mixed)
            .unwrap();
        Self {
            snapshot,
            write_cursor,
            statistics: Statistics::default(),
        }
    }

    // return Some(vec![]) if value is empty.
    // return None if key not exist.
    fn get_value_default(&mut self, key: &Key) -> Option<Value> {
        self.statistics.data.get += 1;
        let mut opts = ReadOptions::new();
        opts.set_fill_cache(false);
        self.snapshot
            .get_cf_opt(opts, CF_DEFAULT, &key)
            .unwrap()
            .map(|v| v.deref().to_vec())
    }

    fn check_lock(&mut self, key: &Key) -> bool {
        self.statistics.lock.get += 1;
        let mut opts = ReadOptions::new();
        opts.set_fill_cache(false);
        let key_slice = key.as_encoded();
        let user_key = Key::from_encoded_slice(Key::truncate_ts_for(key_slice).unwrap());

        match self.snapshot.get_cf_opt(opts, CF_LOCK, &user_key).unwrap() {
            Some(v) => {
                let lock = Lock::parse(v.deref()).unwrap();
                lock.ts == Key::decode_ts_from(key_slice).unwrap()
            }
            None => false,
        }
    }

    // return Some(vec![]) if value is empty.
    // return None if key not exist.
    fn near_seek_old_value(&mut self, key: &Key) -> Result<Option<Value>> {
        let user_key = Key::truncate_ts_for(key.as_encoded()).unwrap();
        if self
            .write_cursor
            .near_seek(key, &mut self.statistics.write)?
            && Key::is_user_key_eq(self.write_cursor.key(&mut self.statistics.write), user_key)
        {
            if self.write_cursor.key(&mut self.statistics.write) == key.as_encoded().as_slice() {
                // Key was committed, move cursor to the next key to seek for old value.
                if !self.write_cursor.next(&mut self.statistics.write) {
                    // Do not has any next key, return empty value.
                    return Ok(Some(Vec::default()));
                }
            } else if !self.check_lock(key) {
                return Ok(None);
            }

            // Key was not committed, check if the lock is corresponding to the key.
            let mut old_value = Some(Vec::default());
            while Key::is_user_key_eq(self.write_cursor.key(&mut self.statistics.write), user_key) {
                let write =
                    WriteRef::parse(self.write_cursor.value(&mut self.statistics.write)).unwrap();
                old_value = match write.write_type {
                    WriteType::Put => match write.short_value {
                        Some(short_value) => Some(short_value.to_vec()),
                        None => {
                            let key = key.clone().truncate_ts().unwrap().append_ts(write.start_ts);
                            self.get_value_default(&key)
                        }
                    },
                    WriteType::Delete => Some(Vec::default()),
                    WriteType::Rollback | WriteType::Lock => {
                        if !self.write_cursor.next(&mut self.statistics.write) {
                            Some(Vec::default())
                        } else {
                            continue;
                        }
                    }
                };
                break;
            }
            Ok(old_value)
        } else if self.check_lock(key) {
            Ok(Some(Vec::default()))
        } else {
            Ok(None)
        }
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
            debug!("skip write record"; "write" => ?other, "key" => hex::encode_upper(key));
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

fn decode_lock(key: Vec<u8>, value: &[u8], row: &mut EventRow) -> bool {
    let lock = Lock::parse(value).unwrap();
    let op_type = match lock.lock_type {
        LockType::Put => EventRowOpType::Put,
        LockType::Delete => EventRowOpType::Delete,
        other => {
            debug!("skip lock record";
                "type" => ?other,
                "start_ts" => ?lock.ts,
                "key" => hex::encode_upper(key),
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
    use futures::{Future, Stream};
    use kvproto::errorpb::Error as ErrorHeader;
    use kvproto::metapb::Region;
    use std::cell::Cell;
    use tikv::storage::kv::TestEngineBuilder;
    use tikv::storage::mvcc::test_util::*;
    use tikv::storage::mvcc::tests::*;
    use tikv_util::mpsc::batch::{self, BatchReceiver, VecCollector};

    #[test]
    fn test_error() {
        let region_id = 1;
        let mut region = Region::default();
        region.set_id(region_id);
        region.mut_peers().push(Default::default());
        region.mut_region_epoch().set_version(2);
        region.mut_region_epoch().set_conf_ver(2);
        let region_epoch = region.get_region_epoch().clone();

        let (sink, rx) = batch::unbounded(1);
        let rx = BatchReceiver::new(rx, 1, Vec::new, VecCollector);
        let request_id = 123;
        let mut downstream =
            Downstream::new(String::new(), region_epoch, request_id, ConnID::new());
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
            let (events, rx) = match rx_wrap.replace(None).unwrap().into_future().wait() {
                Ok((events, rx)) => (events, rx),
                Err(e) => panic!("unexpected recv error: {:?}", e.0),
            };
            rx_wrap.set(Some(rx));
            let mut events = events.unwrap();
            assert_eq!(events.len(), 1);
            for e in &events {
                assert_eq!(e.1.get_request_id(), request_id);
            }
            let (_, change_data_event) = &mut events[0];
            let event = change_data_event.event.take().unwrap();
            match event {
                Event_oneof_event::Error(err) => err,
                _ => panic!("unknown event"),
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

    #[test]
    fn test_scan() {
        let region_id = 1;
        let mut region = Region::default();
        region.set_id(region_id);
        region.mut_peers().push(Default::default());
        region.mut_region_epoch().set_version(2);
        region.mut_region_epoch().set_conf_ver(2);
        let region_epoch = region.get_region_epoch().clone();

        let (sink, rx) = batch::unbounded(1);
        let rx = BatchReceiver::new(rx, 1, Vec::new, VecCollector);
        let request_id = 123;
        let mut downstream =
            Downstream::new(String::new(), region_epoch, request_id, ConnID::new());
        let downstream_id = downstream.get_id();
        downstream.set_sink(sink);
        let mut delegate = Delegate::new(region_id);
        delegate.subscribe(downstream);
        let enabled = delegate.enabled();
        assert!(enabled.load(Ordering::SeqCst));

        let rx_wrap = Cell::new(Some(rx));
        let check_event = |event_rows: Vec<EventRow>| {
            let (events, rx) = match rx_wrap.replace(None).unwrap().into_future().wait() {
                Ok((events, rx)) => (events, rx),
                Err(e) => panic!("unexpected recv error: {:?}", e.0),
            };
            rx_wrap.set(Some(rx));
            let mut events = events.unwrap();
            assert_eq!(events.len(), 1);
            for e in &events {
                assert_eq!(e.1.get_request_id(), request_id);
            }
            let (_, change_data_event) = &mut events[0];
            assert_eq!(change_data_event.region_id, region_id);
            assert_eq!(change_data_event.index, 0);
            let event = change_data_event.event.take().unwrap();
            match event {
                Event_oneof_event::Entries(entries) => {
                    assert_eq!(entries.entries.as_slice(), event_rows.as_slice());
                }
                _ => panic!("unknown event"),
            }
        };

        // Stashed in pending before region ready.
        let entries = vec![
            Some(
                EntryBuilder::default()
                    .key(b"a")
                    .value(b"b")
                    .start_ts(1.into())
                    .commit_ts(0.into())
                    .primary(&[])
                    .for_update_ts(0.into())
                    .build_prewrite(LockType::Put, false),
            ),
            Some(
                EntryBuilder::default()
                    .key(b"a")
                    .value(b"b")
                    .start_ts(1.into())
                    .commit_ts(2.into())
                    .primary(&[])
                    .for_update_ts(0.into())
                    .build_commit(WriteType::Put, false),
            ),
            Some(
                EntryBuilder::default()
                    .key(b"a")
                    .value(b"b")
                    .start_ts(3.into())
                    .commit_ts(0.into())
                    .primary(&[])
                    .for_update_ts(0.into())
                    .build_rollback(),
            ),
            None,
        ];
        delegate.on_scan(downstream_id, entries);
        // Flush all pending entries.
        let mut row1 = EventRow::default();
        row1.start_ts = 1;
        row1.commit_ts = 0;
        row1.key = b"a".to_vec();
        row1.op_type = EventRowOpType::Put.into();
        set_event_row_type(&mut row1, EventLogType::Prewrite);
        row1.value = b"b".to_vec();
        let mut row2 = EventRow::default();
        row2.start_ts = 1;
        row2.commit_ts = 2;
        row2.key = b"a".to_vec();
        row2.op_type = EventRowOpType::Put.into();
        set_event_row_type(&mut row2, EventLogType::Committed);
        row2.value = b"b".to_vec();
        let mut row3 = EventRow::default();
        set_event_row_type(&mut row3, EventLogType::Initialized);
        check_event(vec![row1, row2, row3]);

        let mut resolver = Resolver::new(region_id);
        resolver.init();
        delegate.on_region_ready(resolver, region);
    }

    #[test]
    fn test_old_value_reader() {
        let engine = TestEngineBuilder::new().build().unwrap();
        let kv_engine = engine.get_rocksdb();
        let k = b"k";
        let key = Key::from_raw(k);

        let must_get_eq = |ts: u64, value| {
            let mut old_value_reader = OldValueReader::new(Arc::new(kv_engine.snapshot()));
            assert_eq!(
                old_value_reader
                    .near_seek_old_value(&key.clone().append_ts(ts.into()))
                    .unwrap(),
                value
            );
            let mut opts = ReadOptions::new();
            opts.set_fill_cache(false);
            // engine
            //     .get_rocksdb()
            //     .snapshot()
            //     .get_value_cf_opt(&opts, CF_LOCK, key.as_encoded())
            //     .unwrap()
        };

        must_prewrite_put(&engine, k, b"v1", k, 1);
        must_get_eq(2, None);
        must_get_eq(1, Some(vec![]));
        must_commit(&engine, k, 1, 1);
        must_get_eq(1, Some(vec![]));

        must_prewrite_put(&engine, k, b"v2", k, 2);
        must_get_eq(2, Some(b"v1".to_vec()));
        must_rollback(&engine, k, 2);

        must_prewrite_put(&engine, k, b"v3", k, 3);
        must_get_eq(3, Some(b"v1".to_vec()));
        must_commit(&engine, k, 3, 3);

        must_prewrite_delete(&engine, k, k, 4);
        must_get_eq(4, Some(b"v3".to_vec()));
        must_commit(&engine, k, 4, 4);

        must_prewrite_put(&engine, k, vec![b'v'; 5120].as_slice(), k, 5);
        must_get_eq(5, Some(vec![]));
        must_commit(&engine, k, 5, 5);

        must_prewrite_delete(&engine, k, k, 6);
        must_get_eq(6, Some(vec![b'v'; 5120]));
    }
}
