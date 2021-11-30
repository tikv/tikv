// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use crate::{Error, Result};
use core::cmp;
use kvproto::kvrpcpb::{self, KeyRange, LeaderInfo};
use kvproto::metapb::{self, Peer, PeerRole, Region, RegionEpoch};
use kvproto::raft_cmdpb::{AdminCmdType, RaftCmdRequest};
use raft_proto::eraftpb::{self, MessageType};
use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering as AtomicOrdering;
use std::sync::{Arc, Mutex};
use tikv_util::box_err;
use tikv_util::time::monotonic_raw_now;
use tikv_util::Either;
use tikv_util::{debug, info};
use time::{Duration, Timespec};
use crate::store::rlog;

#[derive(Debug, Copy, Clone)]
pub struct AdminCmdEpochState {
    pub check_ver: bool,
    pub check_conf_ver: bool,
    pub change_ver: bool,
    pub change_conf_ver: bool,
}

impl AdminCmdEpochState {
    fn new(
        check_ver: bool,
        check_conf_ver: bool,
        change_ver: bool,
        change_conf_ver: bool,
    ) -> AdminCmdEpochState {
        AdminCmdEpochState {
            check_ver,
            check_conf_ver,
            change_ver,
            change_conf_ver,
        }
    }
}

/// WARNING: the existing settings below **MUST NOT** be changed!!!
/// Changing any admin cmd's `AdminCmdEpochState` or the epoch-change behavior during applying
/// will break upgrade compatibility and correctness dependency of `CmdEpochChecker`.
/// Please remember it is very difficult to fix the issues arising from not following this rule.
///
/// If you really want to change an admin cmd behavior, please add a new admin cmd and **DO NOT**
/// delete the old one.
pub fn admin_cmd_epoch_lookup(admin_cmp_type: AdminCmdType) -> AdminCmdEpochState {
    match admin_cmp_type {
        AdminCmdType::InvalidAdmin => AdminCmdEpochState::new(false, false, false, false),
        AdminCmdType::CompactLog => AdminCmdEpochState::new(false, false, false, false),
        AdminCmdType::ComputeHash => AdminCmdEpochState::new(false, false, false, false),
        AdminCmdType::VerifyHash => AdminCmdEpochState::new(false, false, false, false),
        // Change peer
        AdminCmdType::ChangePeer => AdminCmdEpochState::new(false, true, false, true),
        AdminCmdType::ChangePeerV2 => AdminCmdEpochState::new(false, true, false, true),
        // Split
        AdminCmdType::Split => AdminCmdEpochState::new(true, true, true, false),
        AdminCmdType::BatchSplit => AdminCmdEpochState::new(true, true, true, false),
        // Merge
        AdminCmdType::PrepareMerge => AdminCmdEpochState::new(true, true, true, true),
        AdminCmdType::CommitMerge => AdminCmdEpochState::new(true, true, true, false),
        AdminCmdType::RollbackMerge => AdminCmdEpochState::new(true, true, true, false),
        // Transfer leader
        AdminCmdType::TransferLeader => AdminCmdEpochState::new(true, true, false, false),
    }
}

/// WARNING: `NORMAL_REQ_CHECK_VER` and `NORMAL_REQ_CHECK_CONF_VER` **MUST NOT** be changed.
/// The reason is the same as `admin_cmd_epoch_lookup`.
pub static NORMAL_REQ_CHECK_VER: bool = true;
pub static NORMAL_REQ_CHECK_CONF_VER: bool = false;

pub fn check_region_epoch(
    rlog: &rlog::RaftLog,
    region: &metapb::Region,
    include_region: bool,
) -> Result<()> {
    let (version, conf_ver) = rlog.get_epoch();
    let (mut check_ver, mut check_conf_ver) = (NORMAL_REQ_CHECK_VER, NORMAL_REQ_CHECK_CONF_VER);
    if let rlog::RaftLog::Request(req) =  &rlog {
        if req.has_admin_request() {
            let epoch_state = admin_cmd_epoch_lookup(req.get_admin_request().get_cmd_type());
            check_ver = epoch_state.check_ver;
            check_conf_ver = epoch_state.check_conf_ver;
        }
    };
    if !check_ver && !check_conf_ver {
        return Ok(());
    }
    compare_region_epoch(
        version,
        conf_ver,
        region,
        check_conf_ver,
        check_ver,
        include_region,
    )
}

pub fn compare_region_epoch(
    version: u64,
    conf_ver: u64,
    region: &metapb::Region,
    check_conf_ver: bool,
    check_ver: bool,
    include_region: bool,
) -> Result<()> {
    // We must check epochs strictly to avoid key not in region error.
    //
    // A 3 nodes TiKV cluster with merge enabled, after commit merge, TiKV A
    // tells TiDB with a epoch not match error contains the latest target Region
    // info, TiDB updates its region cache and sends requests to TiKV B,
    // and TiKV B has not applied commit merge yet, since the region epoch in
    // request is higher than TiKV B, the request must be denied due to epoch
    // not match, so it does not read on a stale snapshot, thus avoid the
    // KeyNotInRegion error.
    let current_epoch = region.get_region_epoch();
    if (check_conf_ver && conf_ver != current_epoch.get_conf_ver())
        || (check_ver && version != current_epoch.get_version())
    {
        debug!(
            "epoch not match";
            "region_id" => region.get_id(),
            "from_epoch_ver" => version,
            "from_epoch_conf_ver" => conf_ver,
            "current_epoch" => ?current_epoch,
        );
        let regions = if include_region {
            vec![region.to_owned()]
        } else {
            vec![]
        };
        return Err(Error::EpochNotMatch(
            format!(
                "current epoch of region {} is {:?}, but you \
                 sent {}:{}",
                region.get_id(),
                current_epoch,
                version,
                conf_ver,
            ),
            regions,
        ));
    }

    Ok(())
}

pub fn is_region_epoch_equal(
    from_epoch: &metapb::RegionEpoch,
    current_epoch: &metapb::RegionEpoch,
) -> bool {
    from_epoch.get_conf_ver() == current_epoch.get_conf_ver()
        && from_epoch.get_version() == current_epoch.get_version()
}

#[inline]
pub fn check_store_id(req: &rlog::RaftLog, store_id: u64) -> Result<()> {
    let to_store_id = req.get_store_id();
    if to_store_id == store_id {
        Ok(())
    } else {
        Err(Error::StoreNotMatch {
            to_store_id,
            my_store_id: store_id,
        })
    }
}

#[inline]
pub fn check_term(req: &rlog::RaftLog, term: u64) -> Result<()> {
    let to_term = req.get_term();
    if to_term == 0 || term <= to_term + 1 {
        Ok(())
    } else {
        // If header's term is 2 verions behind current term,
        // leadership may have been changed away.
        Err(Error::StaleCommand)
    }
}

#[inline]
pub fn check_peer_id(req: &rlog::RaftLog, peer_id: u64) -> Result<()> {
    let to_peer_id = req.get_peer_id();
    if to_peer_id == peer_id {
        Ok(())
    } else {
        Err(box_err!(
            "mismatch peer id {} != {}",
            to_peer_id,
            peer_id
        ))
    }
}

/// Check if key in region range (`start_key`, `end_key`).
pub fn check_key_in_region_exclusive(key: &[u8], region: &metapb::Region) -> Result<()> {
    let end_key = region.get_end_key();
    let start_key = region.get_start_key();
    if start_key < key && (key < end_key || end_key.is_empty()) {
        Ok(())
    } else {
        Err(Error::KeyNotInRegion(key.to_vec(), region.clone()))
    }
}

/// Check if key in region range [`start_key`, `end_key`].
pub fn check_key_in_region_inclusive(key: &[u8], region: &metapb::Region) -> Result<()> {
    let end_key = region.get_end_key();
    let start_key = region.get_start_key();
    if key >= start_key && (end_key.is_empty() || key <= end_key) {
        Ok(())
    } else {
        Err(Error::KeyNotInRegion(key.to_vec(), region.clone()))
    }
}

/// Check if key in region range [`start_key`, `end_key`).
pub fn check_key_in_region(key: &[u8], region: &metapb::Region) -> Result<()> {
    let end_key = region.get_end_key();
    let start_key = region.get_start_key();
    if key >= start_key && (end_key.is_empty() || key < end_key) {
        Ok(())
    } else {
        Err(Error::KeyNotInRegion(key.to_vec(), region.clone()))
    }
}

pub fn cf_name_to_num(cf_name: &str) -> usize {
    match cf_name {
        "write" => 0,
        "lock" => 1,
        "extra" => 2,
        _ => 0,
    }
}

#[derive(Clone)]
pub struct RegionReadProgressRegistry {
    registry: Arc<Mutex<HashMap<u64, Arc<RegionReadProgress>>>>,
}

impl RegionReadProgressRegistry {
    pub fn new() -> RegionReadProgressRegistry {
        RegionReadProgressRegistry {
            registry: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn insert(
        &self,
        region_id: u64,
        read_progress: Arc<RegionReadProgress>,
    ) -> Option<Arc<RegionReadProgress>> {
        self.registry
            .lock()
            .unwrap()
            .insert(region_id, read_progress)
    }

    pub fn remove(&self, region_id: &u64) -> Option<Arc<RegionReadProgress>> {
        self.registry.lock().unwrap().remove(region_id)
    }

    pub fn get(&self, region_id: &u64) -> Option<Arc<RegionReadProgress>> {
        self.registry.lock().unwrap().get(region_id).cloned()
    }

    pub fn get_safe_ts(&self, region_id: &u64) -> Option<u64> {
        self.registry
            .lock()
            .unwrap()
            .get(region_id)
            .map(|rp| rp.safe_ts())
    }

    // Update `safe_ts` with the provided `LeaderInfo` and return the regions that have the
    // same `LeaderInfo`
    pub fn handle_check_leaders(&self, leaders: Vec<LeaderInfo>) -> Vec<u64> {
        let mut regions = Vec::with_capacity(leaders.len());
        let registry = self.registry.lock().unwrap();
        for leader_info in leaders {
            let region_id = leader_info.get_region_id();
            if let Some(rp) = registry.get(&region_id) {
                if rp.consume_leader_info(leader_info) {
                    regions.push(region_id);
                }
            }
        }
        regions
    }

    // Get the `LeaderInfo` of the requested regions
    pub fn dump_leader_infos(&self, regions: &[u64]) -> HashMap<u64, (Vec<Peer>, LeaderInfo)> {
        let registry = self.registry.lock().unwrap();
        let mut info_map = HashMap::with_capacity(regions.len());
        for region_id in regions {
            if let Some(rrp) = registry.get(region_id) {
                info_map.insert(*region_id, rrp.dump_leader_info());
            }
        }
        info_map
    }

    /// Invoke the provided callback with the registry, an internal lock will hold
    /// while invoking the callback so it is important that *not* try to acquiring any
    /// lock inside the callback to avoid dead lock
    pub fn with<F, T>(&self, f: F) -> T
    where
        F: FnOnce(&HashMap<u64, Arc<RegionReadProgress>>) -> T,
    {
        let registry = self.registry.lock().unwrap();
        f(&registry)
    }
}

/// `RegionReadProgress` is used to keep track of the replica's `safe_ts`, the replica can handle a read
/// request directly without requiring leader lease or read index iff `safe_ts` >= `read_ts` (the `read_ts`
/// is usually stale i.e seconds ago).
///
/// `safe_ts` is updated by the `(apply index, safe ts)` item:
/// ```
/// if self.applied_index >= item.apply_index {
///     self.safe_ts = max(self.safe_ts, item.safe_ts)
/// }
/// ```
///
/// For the leader, the `(apply index, safe ts)` item is publish by the `resolved-ts` worker periodically.
/// For the followers, the item is sync periodically from the leader through the `CheckLeader` rpc.
///
/// The intend is to make the item's `safe ts` larger (more up to date) and `apply index` smaller (require less data)
//
/// TODO: the name `RegionReadProgress` is conflict with the leader lease's `ReadProgress`, shoule change it to another
/// more proper name
#[derive(Debug)]
pub struct RegionReadProgress {
    // `core` used to keep track and update `safe_ts`, it should
    // not be accessed outside to avoid dead lock
    core: Mutex<RegionReadProgressCore>,
    // The fast path to read `safe_ts` without acquiring the mutex
    // on `core`
    safe_ts: AtomicU64,
}

impl RegionReadProgress {
    pub fn new(region: &Region, applied_index: u64, cap: usize, tag: String) -> RegionReadProgress {
        RegionReadProgress {
            core: Mutex::new(RegionReadProgressCore::new(region, applied_index, cap, tag)),
            safe_ts: AtomicU64::from(0),
        }
    }

    pub fn update_applied(&self, applied: u64) {
        let mut core = self.core.lock().unwrap();
        if let Some(ts) = core.update_applied(applied) {
            if !core.pause {
                self.safe_ts.store(ts, AtomicOrdering::Release);
            }
        }
    }

    pub fn update_safe_ts(&self, apply_index: u64, ts: u64) {
        if apply_index == 0 || ts == 0 {
            return;
        }
        let mut core = self.core.lock().unwrap();
        if core.discard {
            return;
        }
        if let Some(ts) = core.update_safe_ts(apply_index, ts) {
            if !core.pause {
                self.safe_ts.store(ts, AtomicOrdering::Release);
            }
        }
    }

    pub fn merge_safe_ts(&self, source_safe_ts: u64, merge_index: u64) {
        let mut core = self.core.lock().unwrap();
        if let Some(ts) = core.merge_safe_ts(source_safe_ts, merge_index) {
            if !core.pause {
                self.safe_ts.store(ts, AtomicOrdering::Release);
            }
        }
    }

    // Consume the provided `LeaderInfo` to update `safe_ts` and return whether the provided
    // `LeaderInfo` is same as ours
    pub fn consume_leader_info(&self, mut leader_info: LeaderInfo) -> bool {
        let mut core = self.core.lock().unwrap();
        if leader_info.has_read_state() {
            // It is okay to update `safe_ts` without checking the `LeaderInfo`, the `read_state`
            // is guaranteed to be valid when it is published by the leader
            let rs = leader_info.take_read_state();
            let (apply_index, ts) = (rs.get_applied_index(), rs.get_safe_ts());
            if apply_index != 0 && ts != 0 && !core.discard {
                if let Some(ts) = core.update_safe_ts(apply_index, ts) {
                    if !core.pause {
                        self.safe_ts.store(ts, AtomicOrdering::Release);
                    }
                }
            }
        }
        // whether the provided `LeaderInfo` is same as ours
        core.leader_info.leader_term == leader_info.term
            && core.leader_info.leader_id == leader_info.peer_id
            && is_region_epoch_equal(&core.leader_info.epoch, leader_info.get_region_epoch())
    }

    // Dump the `LeaderInfo` and the peer list
    fn dump_leader_info(&self) -> (Vec<Peer>, LeaderInfo) {
        let mut leader_info = LeaderInfo::default();
        let core = self.core.lock().unwrap();
        let read_state = {
            // Get the latest `read_state`
            let ReadState { idx, ts } = core.pending_items.back().unwrap_or(&core.read_state);
            let mut rs = kvrpcpb::ReadState::default();
            rs.set_applied_index(*idx);
            rs.set_safe_ts(*ts);
            rs
        };
        let li = &core.leader_info;
        leader_info.set_peer_id(li.leader_id);
        leader_info.set_term(li.leader_term);
        leader_info.set_region_id(core.region_id);
        leader_info.set_region_epoch(li.epoch.clone());
        leader_info.set_read_state(read_state);
        (li.peers.clone(), leader_info)
    }

    pub fn update_leader_info(&self, peer_id: u64, term: u64, region: &Region) {
        let mut core = self.core.lock().unwrap();
        core.leader_info.leader_id = peer_id;
        core.leader_info.leader_term = term;
        if !is_region_epoch_equal(region.get_region_epoch(), &core.leader_info.epoch) {
            core.leader_info.epoch = region.get_region_epoch().clone();
            core.leader_info.peers = region.get_peers().to_vec();
        }
    }

    /// Reset `safe_ts` to 0 and stop updating it
    pub fn pause(&self) {
        let mut core = self.core.lock().unwrap();
        core.pause = true;
        self.safe_ts.store(0, AtomicOrdering::Release);
    }

    /// Discard incoming `read_state` item and stop updating `safe_ts`
    pub fn discard(&self) {
        let mut core = self.core.lock().unwrap();
        core.pause = true;
        core.discard = true;
    }

    /// Reset `safe_ts` and resume updating it
    pub fn resume(&self) {
        let mut core = self.core.lock().unwrap();
        core.pause = false;
        core.discard = false;
        self.safe_ts
            .store(core.read_state.ts, AtomicOrdering::Release);
    }

    pub fn safe_ts(&self) -> u64 {
        self.safe_ts.load(AtomicOrdering::Acquire)
    }
}

#[derive(Debug)]
struct RegionReadProgressCore {
    tag: String,
    region_id: u64,
    applied_index: u64,
    // A wraper of `(apply_index, safe_ts)` item, where the `read_state.ts` is the peer's current `safe_ts`
    // and the `read_state.idx` is the smallest `apply_index` required for that `safe_ts`
    read_state: ReadState,
    // The local peer's acknowledge about the leader
    leader_info: LocalLeaderInfo,
    // `pending_items` is a *sorted* list of `(apply_index, safe_ts)` item
    pending_items: VecDeque<ReadState>,
    // After the region commit merged, the region's key range is extended and the region's `safe_ts`
    // should reset to `min(source_safe_ts, target_safe_ts)`, and start reject stale `read_state` item
    // with index smaller than `last_merge_index` to avoid `safe_ts` undo the decrease
    last_merge_index: u64,
    // Stop update `safe_ts`
    pause: bool,
    // Discard incoming `(idx, ts)`
    discard: bool,
}

// A helpful wraper of `(apply_index, safe_ts)` item
#[derive(Clone, Debug, Default)]
pub struct ReadState {
    pub idx: u64,
    pub ts: u64,
}

/// The local peer's acknowledge about the leader
#[derive(Debug)]
pub struct LocalLeaderInfo {
    leader_id: u64,
    leader_term: u64,
    epoch: RegionEpoch,
    peers: Vec<Peer>,
}

impl LocalLeaderInfo {
    fn new(region: &Region) -> LocalLeaderInfo {
        LocalLeaderInfo {
            leader_id: raft::INVALID_ID,
            leader_term: 0,
            epoch: region.get_region_epoch().clone(),
            peers: region.get_peers().to_vec(),
        }
    }
}

impl RegionReadProgressCore {
    fn new(region: &Region, applied_index: u64, cap: usize, tag: String) -> RegionReadProgressCore {
        RegionReadProgressCore {
            tag,
            region_id: region.get_id(),
            applied_index,
            read_state: ReadState::default(),
            leader_info: LocalLeaderInfo::new(region),
            pending_items: VecDeque::with_capacity(cap),
            last_merge_index: 0,
            pause: false,
            discard: false,
        }
    }

    // Reset target region's `safe_ts` to min(`source_safe_ts`, `safe_ts`)
    fn merge_safe_ts(&mut self, source_safe_ts: u64, merge_index: u64) -> Option<u64> {
        // Consume all pending items before `merge_index`
        self.update_applied(merge_index);
        // Update `last_merge_index`
        self.last_merge_index = merge_index;
        // Reset target region's `safe_ts`
        let target_safe_ts = self.read_state.ts;
        self.read_state.ts = cmp::min(source_safe_ts, target_safe_ts);
        info!(
            "reset safe_ts due to merge";
            "tag" => &self.tag,
            "source_safe_ts" => source_safe_ts,
            "target_safe_ts" => target_safe_ts,
            "safe_ts" => self.read_state.ts,
        );
        if self.read_state.ts != target_safe_ts {
            Some(self.read_state.ts)
        } else {
            None
        }
    }

    // Return the `safe_ts` if it is updated
    fn update_applied(&mut self, applied: u64) -> Option<u64> {
        // The apply index should not decrease
        assert!(applied >= self.applied_index);
        self.applied_index = applied;
        // Consume pending items with `apply_index` less or equal to `self.applied_index`
        let mut to_update = self.read_state.clone();
        while let Some(item) = self.pending_items.pop_front() {
            if self.applied_index < item.idx {
                self.pending_items.push_front(item);
                break;
            }
            if to_update.ts < item.ts {
                to_update = item;
            }
        }
        if self.read_state.ts < to_update.ts {
            self.read_state = to_update;
            Some(self.read_state.ts)
        } else {
            None
        }
    }

    // Return the `safe_ts` if it is updated
    fn update_safe_ts(&mut self, idx: u64, ts: u64) -> Option<u64> {
        // Discard stale item with `apply_index` before `last_merge_index`
        // in order to prevent the stale item makes the `safe_ts` larger again
        if idx < self.last_merge_index {
            return None;
        }
        // The peer has enough data, try update `safe_ts` directly
        if self.applied_index >= idx {
            let mut updated_ts = None;
            if self.read_state.ts < ts {
                self.read_state = ReadState { idx, ts };
                updated_ts = Some(ts);
            }
            return updated_ts;
        }
        // The peer doesn't has enough data, keep the item for future use
        if let Some(prev_item) = self.pending_items.back_mut() {
            // Discard the item if it has a smaller `safe_ts`
            if prev_item.ts >= ts {
                return None;
            }
            // Discard the item if it has a smaller `apply_index`
            if prev_item.idx >= idx {
                // Instead of dropping the incoming item, try use it to update
                // the last one, update the last item's `safe_ts` if the incoming
                // item require a less or equal `apply_index` with a larger `safe_ts`
                prev_item.ts = ts;
                return None;
            }
        }
        self.push_back(ReadState { idx, ts });
        None
    }

    fn push_back(&mut self, item: ReadState) {
        if self.pending_items.len() >= self.pending_items.capacity() {
            // Stepping by one to evently remove pending items, so the follower can keep
            // the old items and use them to update the `safe_ts` even when the follower
            // not catch up new enough data
            let mut keep = false;
            self.pending_items.retain(|_| {
                keep = !keep;
                keep
            });
        }
        self.pending_items.push_back(item);
    }
}

#[derive(PartialEq, Eq, Debug)]
pub enum ConfChangeKind {
    // Only contains one configuration change
    Simple,
    // Enter joint state
    EnterJoint,
    // Leave joint state
    LeaveJoint,
}

impl ConfChangeKind {
    pub fn confchange_kind(change_num: usize) -> ConfChangeKind {
        match change_num {
            0 => ConfChangeKind::LeaveJoint,
            1 => ConfChangeKind::Simple,
            _ => ConfChangeKind::EnterJoint,
        }
    }
}
