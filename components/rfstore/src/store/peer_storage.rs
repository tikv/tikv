// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use crate::errors::*;
use byteorder::{ByteOrder, LittleEndian};
use kvproto::*;
use std::cell::RefCell;
use std::error;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::mpsc::Receiver;
use std::sync::Arc;

use super::keys::raft_state_key;
use crate::store::{
    region_state_key, Engines, RaftApplyState, RaftState, RegionTask, KV_ENGINE_META_KEY, TERM_KEY,
};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use engine_traits::{Mutable, RaftEngineReadOnly, RaftLogBatch};
use futures::channel::mpsc::UnboundedSender;
use kvengine::ShardMeta;
use kvenginepb::Snapshot;
use kvproto::metapb::Region;
use kvproto::raft_serverpb::RaftLocalState;
use protobuf::Message;
use raft::StorageError::Unavailable;
use raft::{is_empty_snap, StorageError};
use raft_proto::eraftpb;
use raft_proto::eraftpb::{ConfState, HardState};
use raftstore::store::util;
use raftstore::store::util::conf_state_from_region;
use rfengine;
use tikv_util::mpsc::Sender;
use tikv_util::worker::Scheduler;
use tikv_util::{box_err, info};
use tokio::sync::mpsc;

// When we create a region peer, we should initialize its log term/index > 0,
// so that we can force the follower peer to sync the snapshot first.
pub const RAFT_INIT_LOG_TERM: u64 = 5;
pub const RAFT_INIT_LOG_INDEX: u64 = 5;
const MAX_SNAP_TRY_CNT: usize = 5;

/// The initial region epoch version.
pub const INIT_EPOCH_VER: u64 = 1;
/// The initial region epoch conf_version.
pub const INIT_EPOCH_CONF_VER: u64 = 1;

pub const JOB_STATUS_PENDING: usize = 0;
pub const JOB_STATUS_RUNNING: usize = 1;
pub const JOB_STATUS_CANCELLING: usize = 2;
pub const JOB_STATUS_CANCELLED: usize = 3;
pub const JOB_STATUS_FINISHED: usize = 4;
pub const JOB_STATUS_FAILED: usize = 5;

/// Possible status returned by `check_applying_snap`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum CheckApplyingSnapStatus {
    /// A snapshot is just applied.
    Success,
    /// A snapshot is being applied.
    Applying,
    /// No snapshot is being applied at all or the snapshot is canceled
    Idle,
}

#[derive(Debug)]
pub enum SnapState {
    Relax,
    Generating {
        canceled: Arc<AtomicBool>,
        index: Arc<AtomicU64>,
        receiver: Receiver<Snapshot>,
    },
    Applying(Arc<AtomicUsize>),
    ApplyAborted,
}

impl PartialEq for SnapState {
    fn eq(&self, other: &SnapState) -> bool {
        match (self, other) {
            (&SnapState::Relax, &SnapState::Relax)
            | (&SnapState::ApplyAborted, &SnapState::ApplyAborted)
            | (&SnapState::Generating { .. }, &SnapState::Generating { .. }) => true,
            (&SnapState::Applying(ref b1), &SnapState::Applying(ref b2)) => {
                b1.load(Ordering::Relaxed) == b2.load(Ordering::Relaxed)
            }
            _ => false,
        }
    }
}

pub struct ApplySnapResult {
    // prev_region is the region before snapshot applied.
    pub prev_region: metapb::Region,
    pub region: metapb::Region,
    pub destroyed_regions: Vec<metapb::Region>,
}

pub(crate) struct PeerStorage {
    pub(crate) engines: Engines,

    pub(crate) peer_id: u64,
    region: metapb::Region,
    raft_state: RaftState,
    apply_state: RaftApplyState,
    last_term: u64,

    snap_state: RefCell<SnapState>,
    snap_tried_cnt: usize,

    split_stage: kvenginepb::SplitStage,
    initial_flushed: bool,
    shard_meta: Option<kvengine::ShardMeta>,

    pub tag: String,
}

impl raft::Storage for PeerStorage {
    fn initial_state(&self) -> raft::Result<raft::RaftState> {
        let hard_state = self.raft_state.get_hard_state();
        if hard_state == HardState::default() {
            assert!(
                !self.is_initialized(),
                "peer for region {:?} is initialized but local state {:?} has empty hard \
                 state",
                self.region,
                self.raft_state
            );

            return Ok(raft::RaftState::new(hard_state, ConfState::default()));
        }
        Ok(raft::RaftState::new(
            hard_state,
            util::conf_state_from_region(self.region()),
        ))
    }

    fn entries(
        &self,
        low: u64,
        high: u64,
        max_size: impl Into<Option<u64>>,
    ) -> raft::Result<Vec<eraftpb::Entry>> {
        self.check_range(low, high)?;
        let mut ents = Vec::with_capacity((high - low) as usize);
        if low == high {
            return Ok(ents);
        }
        let region_id = self.get_region_id();
        self.engines.raft.fetch_entries_to(
            region_id,
            low,
            high,
            max_size.into().map(|x| x as usize),
            &mut ents,
        )?;
        Ok(ents)
    }

    fn term(&self, idx: u64) -> raft::Result<u64> {
        if self.shard_meta.is_none() {
            return Ok(0);
        }
        if idx == self.truncated_index() {
            return Ok(RAFT_INIT_LOG_TERM);
        }
        self.check_range(idx, idx + 1)?;
        Ok(self
            .engines
            .raft
            .get_term(self.get_region_id(), idx)
            .unwrap())
    }

    fn first_index(&self) -> raft::Result<u64> {
        Ok(self.shard_meta.as_ref().map_or(0, |m| m.seq + 1))
    }

    fn last_index(&self) -> raft::Result<u64> {
        Ok(self.raft_state.last_index)
    }

    fn snapshot(&self, request_index: u64) -> raft::Result<eraftpb::Snapshot> {
        if !self.initial_flushed || self.shard_meta.is_none() {
            info!("shard has not flushed for generating snapshot"; "region" => &self.tag);
            return Err(raft::Error::Store(StorageError::Unavailable));
        }
        let shard_meta = self.shard_meta.as_ref().unwrap();
        let snap_index = shard_meta.seq;
        if snap_index < request_index {
            info!("requesting index is too high"; "region" => &self.tag,
                "request_index" => request_index, "snap_index" => snap_index);
            return Err(raft::Error::Store(StorageError::Unavailable));
        }
        let snap_term = self.truncated_term();

        let mut snap = eraftpb::Snapshot::default();
        let change_set = self.shard_meta.as_ref().unwrap().to_change_set();
        let snap_data = encode_snap_data(self.region(), &change_set);
        snap.set_data(snap_data);
        let mut snap_meta = eraftpb::SnapshotMetadata::default();
        snap_meta.set_index(snap_index);
        snap_meta.set_term(snap_term);
        let conf_state = conf_state_from_region(self.region());
        snap_meta.set_conf_state(conf_state);
        snap.set_metadata(snap_meta);
        Ok(snap)
    }
}

impl PeerStorage {
    pub(crate) fn new(
        engines: Engines,
        region: metapb::Region,
        peer_id: u64,
        tag: String,
    ) -> Result<PeerStorage> {
        let raft_state = init_raft_state(&engines.raft, &region)?;
        let apply_state = init_apply_state(&engines.kv, &region);
        let mut shard_meta: Option<ShardMeta> = None;
        let (mut meta_index, mut meta_term) = (0u64, 0u64);
        if apply_state.applied_index > 0 {
            let shard_meta_bin = engines
                .raft
                .get_state(region.get_id(), KV_ENGINE_META_KEY)
                .unwrap();
            let mut change_set = kvenginepb::ChangeSet::default();
            change_set.merge_from_bytes(&shard_meta_bin).unwrap();
            let meta = kvengine::ShardMeta::new(change_set);
            meta_index = meta.seq;
            meta_term = meta.properties.get(TERM_KEY).unwrap().get_u64_le();
            shard_meta = Some(meta);
        }
        let last_term = init_last_term(&engines.raft, &region, raft_state, meta_index, meta_term)?;
        let mut initial_flushed = false;
        let mut split_stage = kvenginepb::SplitStage::Initial;
        if let Some(shard) = engines.kv.get_shard(region.get_id()) {
            initial_flushed = shard.get_initial_flushed();
            split_stage = shard.get_split_stage();
        }
        Ok(PeerStorage {
            engines,
            peer_id,
            region,
            raft_state,
            apply_state,
            last_term,
            snap_state: RefCell::new(SnapState::Relax),
            snap_tried_cnt: 0,
            split_stage,
            initial_flushed,
            shard_meta,
            tag,
        })
    }

    pub(crate) fn clear_meta(&self, rwb: &mut rfengine::WriteBatch) {
        clear_meta(&self.engines.raft, rwb, &self.region);
    }

    pub(crate) fn get_region_id(&self) -> u64 {
        self.region.get_id()
    }

    pub(crate) fn is_initialized(&self) -> bool {
        self.shard_meta.is_some()
    }

    pub(crate) fn clear_data(&self) -> Result<()> {
        // Todo: currently it is a place holder
        Ok(())
    }

    #[inline]
    pub fn first_index(&self) -> u64 {
        self.shard_meta.as_ref().map_or(0, |m| m.seq + 1)
    }

    #[inline]
    pub fn last_index(&self) -> u64 {
        self.raft_state.last_index
    }

    #[inline]
    pub fn last_term(&self) -> u64 {
        self.last_term
    }

    #[inline]
    pub fn hard_state_term(&self) -> u64 {
        self.raft_state.term
    }

    #[inline]
    pub fn set_applied_state(&mut self, apply_state: RaftApplyState) {
        self.apply_state = apply_state;
    }

    #[inline]
    pub fn apply_state(&self) -> RaftApplyState {
        self.apply_state
    }

    #[inline]
    pub fn applied_index(&self) -> u64 {
        self.apply_state.applied_index
    }

    #[inline]
    pub fn applied_index_term(&self) -> u64 {
        self.apply_state.applied_index_term
    }

    #[inline]
    pub fn commit_index(&self) -> u64 {
        self.raft_state.commit
    }

    #[inline]
    pub fn set_commit_index(&mut self, commit: u64) {
        assert!(commit >= self.commit_index());
        self.raft_state.commit = commit;
    }

    #[inline]
    pub fn truncated_index(&self) -> u64 {
        self.shard_meta.as_ref().map_or(0, |m| m.seq)
    }

    #[inline]
    pub fn truncated_term(&self) -> u64 {
        self.shard_meta
            .as_ref()
            .map_or(0, |m| m.properties.get(TERM_KEY).unwrap().get_u64_le())
    }

    pub fn region(&self) -> &metapb::Region {
        &self.region
    }

    pub fn set_region(&mut self, region: metapb::Region) {
        self.region = region;
    }

    #[inline]
    pub fn is_applying_snapshot(&self) -> bool {
        matches!(*self.snap_state.borrow(), SnapState::Applying(_))
    }

    pub fn check_range(&self, low: u64, high: u64) -> raft::Result<()> {
        if low > high {
            return Err(raft::Error::Store(raft::StorageError::Other(box_err!(
                "low: {} is greater that high: {}",
                low,
                high
            ))));
        } else if low <= self.truncated_index() {
            return Err(raft::Error::Store(StorageError::Compacted));
        } else if high > self.last_index() + 1 {
            return Err(raft::Error::Store(raft::StorageError::Other(box_err!(
                "entries' high {} is out of bound lastindex {}",
                high,
                self.last_index()
            ))));
        }
        Ok(())
    }

    /// Check if the storage is applying a snapshot.
    #[inline]
    pub fn check_applying_snap(&mut self) -> CheckApplyingSnapStatus {
        let mut res = CheckApplyingSnapStatus::Idle;
        let new_state = match *self.snap_state.borrow() {
            SnapState::Applying(ref status) => {
                let s = status.load(Ordering::Relaxed);
                if s == JOB_STATUS_FINISHED {
                    res = CheckApplyingSnapStatus::Success;
                    SnapState::Relax
                } else if s == JOB_STATUS_CANCELLED {
                    SnapState::ApplyAborted
                } else if s == JOB_STATUS_FAILED {
                    // TODO: cleanup region and treat it as tombstone.
                    panic!("{} applying snapshot failed", self.get_region_id());
                } else {
                    return CheckApplyingSnapStatus::Applying;
                }
            }
            _ => return res,
        };
        *self.snap_state.borrow_mut() = new_state;
        res
    }

    /// Cancel applying snapshot, return true if the job can be considered not be run again.
    pub fn cancel_applying_snap(&mut self) -> bool {
        let is_canceled = match *self.snap_state.borrow() {
            SnapState::Applying(ref status) => {
                if status
                    .compare_exchange(
                        JOB_STATUS_PENDING,
                        JOB_STATUS_CANCELLING,
                        Ordering::SeqCst,
                        Ordering::SeqCst,
                    )
                    .is_ok()
                {
                    true
                } else if status
                    .compare_exchange(
                        JOB_STATUS_RUNNING,
                        JOB_STATUS_CANCELLING,
                        Ordering::SeqCst,
                        Ordering::SeqCst,
                    )
                    .is_ok()
                {
                    return false;
                } else {
                    false
                }
            }
            _ => return false,
        };
        if is_canceled {
            *self.snap_state.borrow_mut() = SnapState::ApplyAborted;
            return true;
        }
        // now status can only be JOB_STATUS_CANCELLING, JOB_STATUS_CANCELLED,
        // JOB_STATUS_FAILED and JOB_STATUS_FINISHED.
        self.check_applying_snap() != CheckApplyingSnapStatus::Applying
    }

    pub fn flush_cache_metrics(&mut self) {
        // TODO(x)
    }

    pub fn handle_raft_ready(
        &mut self,
        raft_wb: &mut rfengine::WriteBatch,
        ready: &mut raft::Ready,
    ) -> Option<ApplySnapResult> {
        let mut res = None;
        let prev_raft_state = self.raft_state;
        if !ready.snapshot().is_empty() {
            let prev_region = self.region().clone();
            self.apply_snapshot(ready.snapshot(), raft_wb);
            let region = self.region.clone();
            res = Some(ApplySnapResult {
                prev_region,
                region,
                destroyed_regions: vec![],
            })
        }
        if !ready.entries().is_empty() {
            self.append(ready.take_entries());
        }

        // Last index is 0 means the peer is created from raft message
        // and has not applied snapshot yet, so skip persistent hard state.
        if self.raft_state.last_index > 0 {
            if let Some(hs) = ready.hs() {
                self.raft_state.set_hard_state(hs);
            }
        }
        if prev_raft_state != self.raft_state || !ready.snapshot().is_empty() {
            let key = raft_state_key(self.region.get_region_epoch().get_version());
            raft_wb.set_state(self.get_region_id(), &key, &self.raft_state.marshal());
        }
        res
    }

    fn apply_snapshot(&mut self, snap: &eraftpb::Snapshot, raft_wb: &mut rfengine::WriteBatch) {
        info!("peer storage begin to apply snapshot"; "region" => &self.tag);

        // schedule apply snapshot.
        todo!()
    }

    fn append(&mut self, entries: Vec<eraftpb::Entry>) {
        todo!()
    }
}

fn init_raft_state(raft_engine: &rfengine::RFEngine, region: &metapb::Region) -> Result<RaftState> {
    let mut rs = RaftState::default();
    let rs_key = raft_state_key(region.id);
    let rs_val = raft_engine.get_state(region.id, rs_key.chunk());
    if let Some(val) = rs_val {
        if region.peers.len() > 0 {
            // new split region.
            rs.last_index = RAFT_INIT_LOG_INDEX;
            rs.term = RAFT_INIT_LOG_TERM;
            rs.commit = RAFT_INIT_LOG_INDEX;
            let mut wb = rfengine::WriteBatch::new();
            wb.set_state(region.id, rs_key.chunk(), rs.marshal().chunk());
            raft_engine.write(&wb)?;
        }
    }
    Ok(rs)
}

fn init_apply_state(kv_engine: &kvengine::Engine, region: &metapb::Region) -> RaftApplyState {
    if let Some(shard) = kv_engine.get_shard(region.get_id()) {
        let mut term_bin = shard.get_property(TERM_KEY).unwrap();
        return RaftApplyState::new(shard.get_write_sequence(), term_bin.get_u64_le());
    }
    if region.get_peers().len() > 0 {
        return RaftApplyState::new(RAFT_INIT_LOG_INDEX, RAFT_INIT_LOG_TERM);
    }
    return RaftApplyState::new(0, 0);
}

fn init_last_term(
    rf: &rfengine::RFEngine,
    region: &metapb::Region,
    raft_state: RaftState,
    meta_index: u64,
    meta_term: u64,
) -> Result<u64> {
    let last_index = raft_state.last_index;
    if last_index == 0 {
        return Ok(0);
    } else if last_index == RAFT_INIT_LOG_INDEX {
        return Ok(RAFT_INIT_LOG_TERM);
    } else if last_index == meta_index {
        return Ok(meta_term);
    } else {
        assert!(last_index > RAFT_INIT_LOG_INDEX)
    }
    let term = rf.get_term(region.get_id(), last_index);
    if term.is_none() {
        return Err(box_err!(
            "region {} at index {} doesn't exists, may lost data",
            region.get_id(),
            last_index
        ));
    }
    Ok(term.unwrap())
}

/// Delete all meta belong to the region. Results are stored in `wb`.
pub fn clear_meta(
    raft: &rfengine::RFEngine,
    raft_wb: &mut rfengine::WriteBatch,
    region: &metapb::Region,
) {
    let region_id = region.get_id();
    raft.iterate_region_states(region_id, false, |k, v| {
        raft_wb.set_state(region_id, k, &[]);
        Ok(())
    })
    .unwrap();
    if let Some((_, end_idx)) = raft.get_range(region_id) {
        raft_wb.truncate_raft_log(region_id, end_idx);
    }
}

// When we bootstrap the region we must call this to initialize region local state first.
pub fn write_initial_raft_state(raft_wb: &mut rfengine::WriteBatch, region_id: u64) -> Result<()> {
    let mut raft_state = RaftState {
        last_index: RAFT_INIT_LOG_INDEX,
        vote: 0,
        term: RAFT_INIT_LOG_TERM,
        commit: RAFT_INIT_LOG_INDEX,
    };
    raft_wb.set_state(region_id, &raft_state_key(1), &raft_state.marshal());
    Ok(())
}

// When we bootstrap the region or handling split new region, we must
// call this to initialize region apply state first.
pub fn write_initial_apply_state(kv_wb: &mut kvengine::WriteBatch) {
    kv_wb.set_sequence(RAFT_INIT_LOG_INDEX);
}

pub fn write_peer_state(
    raft_wb: &mut rfengine::WriteBatch,
    region: &metapb::Region,
    state: raft_serverpb::PeerState,
    merge_state: Option<raft_serverpb::MergeState>,
) {
    let mut region_state = raft_serverpb::RegionLocalState::default();
    region_state.set_state(state);
    region_state.set_region(region.clone());
    if let Some(ms) = merge_state {
        region_state.set_merge_state(ms);
    }
    let state_bin = region_state.write_to_bytes().unwrap();
    let epoch = region.get_region_epoch();
    let key = region_state_key(epoch.get_version(), epoch.get_conf_ver());
    raft_wb.set_state(region.get_id(), &key, &state_bin);
}

pub fn encode_snap_data(region: &metapb::Region, change_set: &kvenginepb::ChangeSet) -> Bytes {
    let size1 = region.compute_size() as usize;
    let size2 = change_set.compute_size() as usize;
    let mut buf = BytesMut::with_capacity(4 + size1 + 4 + size2);
    buf.put_u32_le(size1 as u32);
    buf.extend_from_slice(&region.write_to_bytes().unwrap());
    buf.put_u32_le(size2 as u32);
    buf.extend_from_slice(&change_set.write_to_bytes().unwrap());
    buf.freeze()
}

pub fn decode_snap_data(data: Bytes) -> Result<(metapb::Region, kvenginepb::ChangeSet)> {
    let mut offset = 0;
    let size1 = LittleEndian::read_u32(&data) as usize;
    offset += 4;
    let mut region = metapb::Region::default();
    region.merge_from_bytes(&data[offset..(offset + size1)])?;
    offset += size1;
    let size2 = LittleEndian::read_u32(&data[offset..]) as usize;
    let mut change_set = kvenginepb::ChangeSet::default();
    change_set.merge_from_bytes(&data[offset..(offset + size2)])?;
    Ok((region, change_set))
}
