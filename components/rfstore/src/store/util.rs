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

pub fn find_peer(region: &metapb::Region, store_id: u64) -> Option<&metapb::Peer> {
    region
        .get_peers()
        .iter()
        .find(|&p| p.get_store_id() == store_id)
}

pub fn find_peer_mut(region: &mut metapb::Region, store_id: u64) -> Option<&mut metapb::Peer> {
    region
        .mut_peers()
        .iter_mut()
        .find(|p| p.get_store_id() == store_id)
}

pub fn remove_peer(region: &mut metapb::Region, store_id: u64) -> Option<metapb::Peer> {
    region
        .get_peers()
        .iter()
        .position(|x| x.get_store_id() == store_id)
        .map(|i| region.mut_peers().remove(i))
}

// a helper function to create peer easily.
pub fn new_peer(store_id: u64, peer_id: u64) -> metapb::Peer {
    let mut peer = metapb::Peer::default();
    peer.set_store_id(store_id);
    peer.set_id(peer_id);
    peer.set_role(PeerRole::Voter);
    peer
}

// a helper function to create learner peer easily.
pub fn new_learner_peer(store_id: u64, peer_id: u64) -> metapb::Peer {
    let mut peer = metapb::Peer::default();
    peer.set_store_id(store_id);
    peer.set_id(peer_id);
    peer.set_role(PeerRole::Learner);
    peer
}

/// `is_initial_msg` checks whether the `msg` can be used to initialize a new peer or not.
// There could be two cases:
// 1. Target peer already exists but has not established communication with leader yet
// 2. Target peer is added newly due to member change or region split, but it's not
//    created yet
// For both cases the region start key and end key are attached in RequestVote and
// Heartbeat message for the store of that peer to check whether to create a new peer
// when receiving these messages, or just to wait for a pending region split to perform
// later.
#[inline]
pub fn is_initial_msg(msg: &eraftpb::Message) -> bool {
    let msg_type = msg.get_msg_type();
    msg_type == MessageType::MsgRequestVote
        || msg_type == MessageType::MsgRequestPreVote
        // the peer has not been known to this leader, it may exist or not.
        || (msg_type == MessageType::MsgHeartbeat && msg.get_commit() == raft::INVALID_INDEX)
}

// check whether epoch is staler than check_epoch.
pub fn is_epoch_stale(epoch: &metapb::RegionEpoch, check_epoch: &metapb::RegionEpoch) -> bool {
    epoch.get_version() < check_epoch.get_version()
        || epoch.get_conf_ver() < check_epoch.get_conf_ver()
}

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
    req: &RaftCmdRequest,
    region: &metapb::Region,
    include_region: bool,
) -> Result<()> {
    let (check_ver, check_conf_ver) = if !req.has_admin_request() {
        // for get/set/delete, we don't care conf_version.
        (NORMAL_REQ_CHECK_VER, NORMAL_REQ_CHECK_CONF_VER)
    } else {
        let epoch_state = admin_cmd_epoch_lookup(req.get_admin_request().get_cmd_type());
        (epoch_state.check_ver, epoch_state.check_conf_ver)
    };

    if !check_ver && !check_conf_ver {
        return Ok(());
    }

    if !req.get_header().has_region_epoch() {
        return Err(box_err!("missing epoch!"));
    }

    let from_epoch = req.get_header().get_region_epoch();
    compare_region_epoch(
        from_epoch,
        region,
        check_conf_ver,
        check_ver,
        include_region,
    )
}

pub fn compare_region_epoch(
    from_epoch: &metapb::RegionEpoch,
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
    if (check_conf_ver && from_epoch.get_conf_ver() != current_epoch.get_conf_ver())
        || (check_ver && from_epoch.get_version() != current_epoch.get_version())
    {
        debug!(
            "epoch not match";
            "region_id" => region.get_id(),
            "from_epoch" => ?from_epoch,
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
                 sent {:?}",
                region.get_id(),
                current_epoch,
                from_epoch
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
pub fn check_store_id(req: &RaftCmdRequest, store_id: u64) -> Result<()> {
    let peer = req.get_header().get_peer();
    if peer.get_store_id() == store_id {
        Ok(())
    } else {
        Err(Error::StoreNotMatch {
            to_store_id: peer.get_store_id(),
            my_store_id: store_id,
        })
    }
}

#[inline]
pub fn check_term(req: &RaftCmdRequest, term: u64) -> Result<()> {
    let header = req.get_header();
    if header.get_term() == 0 || term <= header.get_term() + 1 {
        Ok(())
    } else {
        // If header's term is 2 verions behind current term,
        // leadership may have been changed away.
        Err(Error::StaleCommand)
    }
}

#[inline]
pub fn check_peer_id(req: &RaftCmdRequest, peer_id: u64) -> Result<()> {
    let header = req.get_header();
    if header.get_peer().get_id() == peer_id {
        Ok(())
    } else {
        Err(box_err!(
            "mismatch peer id {} != {}",
            header.get_peer().get_id(),
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

/// Lease records an expired time, for examining the current moment is in lease or not.
/// It's dedicated to the Raft leader lease mechanism, contains either state of
///   1. Suspect Timestamp
///      A suspicious leader lease timestamp, which marks the leader may still hold or lose
///      its lease until the clock time goes over this timestamp.
///   2. Valid Timestamp
///      A valid leader lease timestamp, which marks the leader holds the lease for now.
///      The lease is valid until the clock time goes over this timestamp.
///
/// ```text
/// Time
/// |---------------------------------->
///         ^               ^
///        Now           Suspect TS
/// State:  |    Suspect    |   Suspect
///
/// |---------------------------------->
///         ^               ^
///        Now           Valid TS
/// State:  |     Valid     |   Expired
/// ```
///
/// Note:
///   - Valid timestamp would increase when raft log entries are applied in current term.
///   - Suspect timestamp would be set after the message `MsgTimeoutNow` is sent by current peer.
///     The message `MsgTimeoutNow` starts a leader transfer procedure. During this procedure,
///     current peer as an old leader may still hold its lease or lose it.
///     It's possible there is a new leader elected and current peer as an old leader
///     doesn't step down due to network partition from the new leader. In that case,
///     current peer lose its leader lease.
///     Within this suspect leader lease expire time, read requests could not be performed
///     locally.
///   - The valid leader lease should be `lease = max_lease - (commit_ts - send_ts)`
///     And the expired timestamp for that leader lease is `commit_ts + lease`,
///     which is `send_ts + max_lease` in short.
pub struct Lease {
    // A suspect timestamp is in the Either::Left(_),
    // a valid timestamp is in the Either::Right(_).
    bound: Option<Either<Timespec, Timespec>>,
    max_lease: Duration,

    max_drift: Duration,
    last_update: Timespec,
    remote: Option<RemoteLease>,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum LeaseState {
    /// The lease is suspicious, may be invalid.
    Suspect,
    /// The lease is valid.
    Valid,
    /// The lease is expired.
    Expired,
}

impl Lease {
    pub fn new(max_lease: Duration) -> Lease {
        Lease {
            bound: None,
            max_lease,

            max_drift: max_lease / 3,
            last_update: Timespec::new(0, 0),
            remote: None,
        }
    }

    /// The valid leader lease should be `lease = max_lease - (commit_ts - send_ts)`
    /// And the expired timestamp for that leader lease is `commit_ts + lease`,
    /// which is `send_ts + max_lease` in short.
    fn next_expired_time(&self, send_ts: Timespec) -> Timespec {
        send_ts + self.max_lease
    }

    /// Renew the lease to the bound.
    pub fn renew(&mut self, send_ts: Timespec) {
        let bound = self.next_expired_time(send_ts);
        match self.bound {
            // Longer than suspect ts or longer than valid ts.
            Some(Either::Left(ts)) | Some(Either::Right(ts)) => {
                if ts <= bound {
                    self.bound = Some(Either::Right(bound));
                }
            }
            // Or an empty lease
            None => {
                self.bound = Some(Either::Right(bound));
            }
        }
        // Renew remote if it's valid.
        if let Some(Either::Right(bound)) = self.bound {
            if bound - self.last_update > self.max_drift {
                self.last_update = bound;
                if let Some(ref r) = self.remote {
                    r.renew(bound);
                }
            }
        }
    }

    /// Suspect the lease to the bound.
    pub fn suspect(&mut self, send_ts: Timespec) {
        self.expire_remote_lease();
        let bound = self.next_expired_time(send_ts);
        self.bound = Some(Either::Left(bound));
    }

    /// Inspect the lease state for the ts or now.
    pub fn inspect(&self, ts: Option<Timespec>) -> LeaseState {
        match self.bound {
            Some(Either::Left(_)) => LeaseState::Suspect,
            Some(Either::Right(bound)) => {
                if ts.unwrap_or_else(monotonic_raw_now) < bound {
                    LeaseState::Valid
                } else {
                    LeaseState::Expired
                }
            }
            None => LeaseState::Expired,
        }
    }

    pub fn expire(&mut self) {
        self.expire_remote_lease();
        self.bound = None;
    }

    pub fn expire_remote_lease(&mut self) {
        // Expire remote lease if there is any.
        if let Some(r) = self.remote.take() {
            r.expire();
        }
    }

    /// Return a new `RemoteLease` if there is none.
    pub fn maybe_new_remote_lease(&mut self, term: u64) -> Option<RemoteLease> {
        if let Some(ref remote) = self.remote {
            if remote.term() == term {
                // At most one connected RemoteLease in the same term.
                return None;
            } else {
                // Term has changed. It is unreachable in the current implementation,
                // because we expire remote lease when leaders step down.
                unreachable!("Must expire the old remote lease first!");
            }
        }
        let expired_time = match self.bound {
            Some(Either::Right(ts)) => timespec_to_u64(ts),
            _ => 0,
        };
        let remote = RemoteLease {
            expired_time: Arc::new(AtomicU64::new(expired_time)),
            term,
        };
        // Clone the remote.
        let remote_clone = remote.clone();
        self.remote = Some(remote);
        Some(remote_clone)
    }
}

impl fmt::Debug for Lease {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut fmter = fmt.debug_struct("Lease");
        match self.bound {
            Some(Either::Left(ts)) => fmter.field("suspect", &ts).finish(),
            Some(Either::Right(ts)) => fmter.field("valid", &ts).finish(),
            None => fmter.finish(),
        }
    }
}

/// A remote lease, it can only be derived by `Lease`. It will be sent
/// to the local read thread, so name it remote. If Lease expires, the remote must
/// expire too.
#[derive(Clone)]
pub struct RemoteLease {
    expired_time: Arc<AtomicU64>,
    term: u64,
}

impl RemoteLease {
    pub fn inspect(&self, ts: Option<Timespec>) -> LeaseState {
        let expired_time = self.expired_time.load(AtomicOrdering::Acquire);
        if ts.unwrap_or_else(monotonic_raw_now) < u64_to_timespec(expired_time) {
            LeaseState::Valid
        } else {
            LeaseState::Expired
        }
    }

    fn renew(&self, bound: Timespec) {
        self.expired_time
            .store(timespec_to_u64(bound), AtomicOrdering::Release);
    }

    fn expire(&self) {
        self.expired_time.store(0, AtomicOrdering::Release);
    }

    pub fn term(&self) -> u64 {
        self.term
    }
}

impl fmt::Debug for RemoteLease {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt.debug_struct("RemoteLease")
            .field(
                "expired_time",
                &u64_to_timespec(self.expired_time.load(AtomicOrdering::Relaxed)),
            )
            .field("term", &self.term)
            .finish()
    }
}

// Contants used in `timespec_to_u64` and `u64_to_timespec`.
const NSEC_PER_MSEC: i32 = 1_000_000;
const TIMESPEC_NSEC_SHIFT: usize = 32 - NSEC_PER_MSEC.leading_zeros() as usize;

const MSEC_PER_SEC: i64 = 1_000;
const TIMESPEC_SEC_SHIFT: usize = 64 - MSEC_PER_SEC.leading_zeros() as usize;

const TIMESPEC_NSEC_MASK: u64 = (1 << TIMESPEC_SEC_SHIFT) - 1;

/// Convert Timespec to u64. It's millisecond precision and
/// covers a range of about 571232829 years in total.
///
/// # Panics
///
/// If Timespecs have negative sec.
#[inline]
fn timespec_to_u64(ts: Timespec) -> u64 {
    // > Darwin's and Linux's struct timespec functions handle pre-
    // > epoch timestamps using a "two steps back, one step forward" representation,
    // > though the man pages do not actually document this. For example, the time
    // > -1.2 seconds before the epoch is represented by `Timespec { sec: -2_i64,
    // > nsec: 800_000_000 }`.
    //
    // Quote from crate time,
    //   https://github.com/rust-lang-deprecated/time/blob/
    //   e313afbd9aad2ba7035a23754b5d47105988789d/src/lib.rs#L77
    assert!(ts.sec >= 0 && ts.sec < (1i64 << (64 - TIMESPEC_SEC_SHIFT)));
    assert!(ts.nsec >= 0);

    // Round down to millisecond precision.
    let ms = ts.nsec >> TIMESPEC_NSEC_SHIFT;
    let sec = ts.sec << TIMESPEC_SEC_SHIFT;
    sec as u64 | ms as u64
}

/// Convert Timespec to u64.
///
/// # Panics
///
/// If nsec is negative or GE than 1_000_000_000(nano seconds pre second).
#[inline]
pub(crate) fn u64_to_timespec(u: u64) -> Timespec {
    let sec = u >> TIMESPEC_SEC_SHIFT;
    let nsec = (u & TIMESPEC_NSEC_MASK) << TIMESPEC_NSEC_SHIFT;
    Timespec::new(sec as i64, nsec as i32)
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

pub struct KeysInfoFormatter<
    'a,
    I: std::iter::DoubleEndedIterator<Item = &'a Vec<u8>>
        + std::iter::ExactSizeIterator<Item = &'a Vec<u8>>
        + Clone,
>(pub I);

impl<
    'a,
    I: std::iter::DoubleEndedIterator<Item = &'a Vec<u8>>
        + std::iter::ExactSizeIterator<Item = &'a Vec<u8>>
        + Clone,
> fmt::Display for KeysInfoFormatter<'a, I>
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut it = self.0.clone();
        match it.len() {
            0 => write!(f, "(no key)"),
            1 => write!(f, "key {}", log_wrappers::Value::key(it.next().unwrap())),
            _ => write!(
                f,
                "{} keys range from {} to {}",
                it.len(),
                log_wrappers::Value::key(it.next().unwrap()),
                log_wrappers::Value::key(it.next_back().unwrap())
            ),
        }
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
