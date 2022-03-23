// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use crate::errors::*;
use byteorder::{ByteOrder, LittleEndian};
use kvproto::*;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
use std::time::{Duration, Instant};

use super::keys::raft_state_key;
use crate::store::{
    region_state_key, Engines, RaftApplyState, RaftContext, RaftState, RegionIDVer, RegionTask,
    KV_ENGINE_META_KEY, TERM_KEY,
};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use engine_traits::RaftEngineReadOnly;
use kvengine::ShardMeta;
use kvproto::raft_serverpb::PeerState;
use protobuf::Message;
use raft::StorageError;
use raft_proto::eraftpb;
use raft_proto::eraftpb::{ConfState, HardState};
use raftstore::store::util;
use raftstore::store::util::conf_state_from_region;
use rfengine;
use tikv_util::{box_err, debug, info};

// When we create a region peer, we should initialize its log term/index > 0,
// so that we can force the follower peer to sync the snapshot first.
pub const RAFT_INIT_LOG_TERM: u64 = 5;
pub const RAFT_INIT_LOG_INDEX: u64 = 5;

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

#[derive(Debug, PartialEq)]
pub enum SnapState {
    Relax,
    Applying,
    ApplyAborted,
}

pub struct ApplySnapResult {
    // prev_region is the region before snapshot applied.
    pub prev_region: metapb::Region,
    pub region: metapb::Region,
    pub destroyed_regions: Vec<metapb::Region>,
}

#[derive(Default)]
pub(crate) struct SplitJobStates {
    pub(crate) split_file_job_status: Option<Arc<AtomicUsize>>,
    pub(crate) scheduled_split_file_time: Option<Instant>,
    pub(crate) split_file_duration: Option<Duration>,
    pub(crate) finish_split_job_state: Option<Arc<AtomicUsize>>,
}

impl SplitJobStates {
    pub(crate) fn reset(&mut self) {
        self.split_file_job_status = None;
        self.scheduled_split_file_time = None;
        self.split_file_duration = None;
        self.finish_split_job_state = None;
    }
}

pub(crate) struct PeerStorage {
    pub(crate) engines: Engines,

    pub(crate) peer_id: u64,
    region: metapb::Region,
    pub(crate) raft_state: RaftState,
    apply_state: RaftApplyState,
    last_term: u64,

    pub(crate) snap_state: SnapState,

    pub(crate) initial_flushed: bool,
    pub(crate) shard_meta: Option<kvengine::ShardMeta>,
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
        if idx == self.last_index() {
            return Ok(self.last_term());
        }
        if idx == self.truncated_index() {
            return Ok(self.truncated_term());
        }
        self.check_range(idx, idx + 1)?;
        if self.truncated_term() == self.last_term {
            return Ok(self.last_term);
        }
        Ok(self
            .engines
            .raft
            .get_term(self.get_region_id(), idx)
            .unwrap())
    }

    fn first_index(&self) -> raft::Result<u64> {
        Ok(self.shard_meta.as_ref().map_or(1, |m| m.data_sequence + 1))
    }

    fn last_index(&self) -> raft::Result<u64> {
        Ok(self.raft_state.last_index)
    }

    fn snapshot(&self, request_index: u64) -> raft::Result<eraftpb::Snapshot> {
        if !self.initial_flushed || self.shard_meta.is_none() {
            info!("shard has not flushed for generating snapshot"; "region" => self.tag());
            return Err(raft::Error::Store(
                StorageError::SnapshotTemporarilyUnavailable,
            ));
        }
        let shard_meta = self.shard_meta.as_ref().unwrap();
        let snap_index = shard_meta.data_sequence;
        if snap_index < request_index {
            info!("requesting index is too high"; "region" => self.tag(),
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
        debug!("peer storage generate snapshot {}", self.tag());
        Ok(snap)
    }
}

impl PeerStorage {
    pub(crate) fn new(
        engines: Engines,
        region: metapb::Region,
        peer_id: u64,
    ) -> Result<PeerStorage> {
        let raft_state = init_raft_state(&engines.raft, &region)?;
        let apply_state = init_apply_state(&engines.kv, &region);
        let mut shard_meta: Option<ShardMeta> = None;
        if apply_state.applied_index > 0 {
            let res = engines.raft.get_state(region.get_id(), KV_ENGINE_META_KEY);
            let shard_meta_bin = res.unwrap();
            let mut change_set = kvenginepb::ChangeSet::default();
            change_set.merge_from_bytes(&shard_meta_bin).unwrap();
            let meta = kvengine::ShardMeta::new(&change_set);
            shard_meta = Some(meta);
        }
        let last_term = init_last_term(&engines.raft, &region, raft_state)?;
        let mut initial_flushed = false;
        if let Some(shard) = engines.kv.get_shard(region.get_id()) {
            initial_flushed = shard.get_initial_flushed();
            if !initial_flushed {
                engines.raft.add_dependent(shard.parent_id, shard.id);
            }
        }
        Ok(PeerStorage {
            engines,
            peer_id,
            region,
            raft_state,
            apply_state,
            last_term,
            snap_state: SnapState::Relax,
            initial_flushed,
            shard_meta,
        })
    }

    pub(crate) fn tag(&self) -> RegionIDVer {
        RegionIDVer::from_region(&self.region)
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
        self.shard_meta.as_ref().map_or(0, |m| m.data_sequence)
    }

    #[inline]
    pub fn truncated_term(&self) -> u64 {
        self.shard_meta.as_ref().map_or(0, |m| {
            debug!("get property term key");
            m.get_property(TERM_KEY).unwrap().get_u64_le()
        })
    }

    pub fn region(&self) -> &metapb::Region {
        &self.region
    }

    pub fn set_region(&mut self, region: metapb::Region) {
        self.region = region;
    }

    #[inline]
    pub fn is_applying_snapshot(&self) -> bool {
        self.snap_state == SnapState::Applying
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

    pub fn flush_cache_metrics(&mut self) {
        // TODO(x)
    }

    pub fn handle_raft_ready(
        &mut self,
        ctx: &mut RaftContext,
        ready: &mut raft::Ready,
    ) -> Option<ApplySnapResult> {
        let mut res = None;
        let prev_raft_state = self.raft_state;
        if !ready.snapshot().is_empty() {
            let region_id = self.get_region_id();
            let store_meta = ctx.global.store_meta.lock().unwrap();
            let pending_split = store_meta.pending_new_regions.contains_key(&region_id);
            let replaced_by_split = if let Some(meta_region) = store_meta.regions.get(&region_id) {
                !self.region.is_initialized() && meta_region.is_initialized()
            } else {
                false
            };
            drop(store_meta);
            if !pending_split && !replaced_by_split {
                let prev_region = self.region().clone();
                self.apply_snapshot(ready.snapshot(), ctx).unwrap();
                let region = self.region.clone();
                res = Some(ApplySnapResult {
                    prev_region,
                    region,
                    destroyed_regions: vec![],
                })
            }
        }
        if !ready.entries().is_empty() {
            self.append(ready.take_entries(), &mut ctx.raft_wb);
        }

        // Last index is 0 means the peer is created from raft message
        // and has not applied snapshot yet, so skip persistent hard state.
        if self.is_initialized() {
            if let Some(hs) = ready.hs() {
                self.raft_state.set_hard_state(hs);
            }
        }
        if prev_raft_state != self.raft_state || !ready.snapshot().is_empty() {
            self.write_raft_state(ctx);
        }
        res
    }

    pub fn update_commit_index(&mut self, ctx: &mut RaftContext, commit_idx: u64) {
        if commit_idx == self.raft_state.commit {
            return;
        }
        assert!(commit_idx > self.raft_state.commit);
        self.raft_state.commit = commit_idx;
        self.write_raft_state(ctx);
    }

    pub fn write_raft_state(&mut self, ctx: &mut RaftContext) {
        let key = raft_state_key(self.region.get_region_epoch().get_version());
        ctx.raft_wb
            .set_state(self.get_region_id(), &key, &self.raft_state.marshal());
    }

    fn apply_snapshot(&mut self, snap: &eraftpb::Snapshot, ctx: &mut RaftContext) -> Result<()> {
        info!("peer storage begin to apply snapshot"; "region" => self.tag());
        let (region, change_set) = decode_snap_data(snap.get_data())?;
        if region.get_id() != self.get_region_id() {
            return Err(box_err!(
                "mismatch region id {} != {}",
                region.get_id(),
                self.get_region_id()
            ));
        }
        if self.is_initialized() {
            // we can only delete the old data when the peer is initialized.
            self.clear_meta(&mut ctx.raft_wb);
        }
        write_peer_state(&mut ctx.raft_wb, &region);
        let last_index = snap.get_metadata().get_index();
        let last_term = snap.get_metadata().get_term();
        self.raft_state.last_index = last_index;
        self.last_term = last_term;
        self.apply_state.applied_index = last_index;
        self.apply_state.applied_index_term = last_term;
        ctx.raft_wb.set_state(
            region.get_id(),
            KV_ENGINE_META_KEY,
            &change_set.write_to_bytes().unwrap(),
        );
        self.shard_meta = Some(kvengine::ShardMeta::new(&change_set));
        self.region = region;

        let snap_task = RegionTask::ApplyChangeSet { change: change_set };
        ctx.global.region_scheduler.schedule(snap_task).unwrap();
        self.snap_state = SnapState::Applying;
        Ok(())
    }

    fn append(&mut self, entries: Vec<eraftpb::Entry>, raft_wb: &mut rfengine::WriteBatch) {
        if entries.is_empty() {
            return;
        }
        for e in &entries {
            raft_wb.append_raft_log(self.get_region_id(), e);
        }
        let last_entry = entries.last().unwrap();
        self.raft_state.last_index = last_entry.get_index();
        self.last_term = last_entry.get_term();
    }

    pub(crate) fn mut_engine_meta(&mut self) -> &mut ShardMeta {
        self.shard_meta.as_mut().unwrap()
    }

    pub(crate) fn parent_id(&self) -> Option<u64> {
        if let Some(meta) = &self.shard_meta {
            if let Ok(shard) = self.engines.kv.get_shard_with_ver(meta.id, meta.ver) {
                return Some(shard.parent_id);
            }
        }
        None
    }
}

fn init_raft_state(raft_engine: &rfengine::RFEngine, region: &metapb::Region) -> Result<RaftState> {
    let mut rs = RaftState::default();
    let rs_key = raft_state_key(region.get_region_epoch().get_version());
    let rs_val = raft_engine.get_state(region.id, rs_key.chunk());
    if rs_val.is_none() {
        if region.peers.len() > 0 {
            // new split region.
            rs.last_index = RAFT_INIT_LOG_INDEX;
            rs.term = RAFT_INIT_LOG_TERM;
            rs.commit = RAFT_INIT_LOG_INDEX;
            let mut wb = rfengine::WriteBatch::new();
            wb.set_state(region.id, rs_key.chunk(), rs.marshal().chunk());
            raft_engine.write(wb)?;
        }
    } else {
        rs.unmarshal(&rs_val.unwrap())
    }
    Ok(rs)
}

fn init_apply_state(kv_engine: &kvengine::Engine, region: &metapb::Region) -> RaftApplyState {
    if let Some(shard) = kv_engine.get_shard(region.get_id()) {
        let mut term_bin = shard.get_property(TERM_KEY).unwrap();
        let mut applied_index = shard.get_write_sequence();
        if applied_index < shard.get_meta_sequence() {
            // When a shard's last raft log is meta change,
            // the write sequence is less than meta sequence.
            applied_index = shard.get_meta_sequence();
        }
        return RaftApplyState::new(applied_index, term_bin.get_u64_le());
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
) -> Result<u64> {
    let last_index = raft_state.last_index;
    if last_index == 0 {
        return Ok(0);
    } else if last_index == RAFT_INIT_LOG_INDEX {
        return Ok(RAFT_INIT_LOG_TERM);
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
    raft.iterate_region_states(region_id, false, |k, _| {
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
    let raft_state = RaftState {
        last_index: RAFT_INIT_LOG_INDEX,
        vote: 0,
        term: RAFT_INIT_LOG_TERM,
        commit: RAFT_INIT_LOG_INDEX,
    };
    raft_wb.set_state(region_id, &raft_state_key(1), &raft_state.marshal());
    Ok(())
}

pub fn write_peer_state(raft_wb: &mut rfengine::WriteBatch, region: &metapb::Region) {
    let mut region_state = raft_serverpb::RegionLocalState::default();
    region_state.set_state(PeerState::Normal);
    region_state.set_region(region.clone());
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

pub fn decode_snap_data(data: &[u8]) -> Result<(metapb::Region, kvenginepb::ChangeSet)> {
    let mut offset = 0;
    let size1 = LittleEndian::read_u32(data) as usize;
    offset += 4;
    let mut region = metapb::Region::default();
    region.merge_from_bytes(&data[offset..(offset + size1)])?;
    offset += size1;
    let size2 = LittleEndian::read_u32(&data[offset..]) as usize;
    offset += 4;
    let mut change_set = kvenginepb::ChangeSet::default();
    change_set.merge_from_bytes(&data[offset..(offset + size2)])?;
    Ok((region, change_set))
}
