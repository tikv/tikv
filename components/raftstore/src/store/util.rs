// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

// #[PerformanceCriticalPath]
use std::{
    cmp,
    collections::{HashMap, VecDeque},
    fmt,
    fmt::Display,
    option::Option,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering as AtomicOrdering},
        Arc, Mutex,
    },
    u64,
};

use kvproto::{
    kvrpcpb::{self, KeyRange, LeaderInfo},
    metapb::{self, Peer, PeerRole, Region, RegionEpoch},
    raft_cmdpb::{AdminCmdType, ChangePeerRequest, ChangePeerV2Request, RaftCmdRequest},
    raft_serverpb::RaftMessage,
};
use protobuf::{self, Message};
use raft::{
    eraftpb::{self, ConfChangeType, ConfState, MessageType},
    INVALID_INDEX,
};
use raft_proto::ConfChangeI;
use tikv_util::{box_err, debug, info, time::monotonic_raw_now, Either};
use time::{Duration, Timespec};

use super::peer_storage;
use crate::{Error, Result};

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

/// `is_first_vote_msg` checks `msg` is the first vote (or prevote) message or not. It's used for
/// when the message is received but there is no such region in `Store::region_peers` and the
/// region overlaps with others. In this case we should put `msg` into `pending_msg` instead of
/// create the peer.
#[inline]
fn is_first_vote_msg(msg: &eraftpb::Message) -> bool {
    match msg.get_msg_type() {
        MessageType::MsgRequestVote | MessageType::MsgRequestPreVote => {
            msg.get_term() == peer_storage::RAFT_INIT_LOG_TERM + 1
        }
        _ => false,
    }
}

/// `is_first_append_entry` checks `msg` is the first append message or not. This meassge is the first
/// message that the learner peers of the new split region will receive from the leader. It's used for
/// when the message is received but there is no such region in `Store::region_peers`. In this case we
/// should put `msg` into `pending_msg` instead of create the peer.
#[inline]
fn is_first_append_entry(msg: &eraftpb::Message) -> bool {
    match msg.get_msg_type() {
        MessageType::MsgAppend => {
            let ent = msg.get_entries();
            ent.len() == 1
                && ent[0].data.is_empty()
                && ent[0].index == peer_storage::RAFT_INIT_LOG_INDEX + 1
        }
        _ => false,
    }
}

pub fn is_first_message(msg: &eraftpb::Message) -> bool {
    is_first_vote_msg(msg) || is_first_append_entry(msg)
}

#[inline]
pub fn is_vote_msg(msg: &eraftpb::Message) -> bool {
    let msg_type = msg.get_msg_type();
    msg_type == MessageType::MsgRequestVote || msg_type == MessageType::MsgRequestPreVote
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
        || (msg_type == MessageType::MsgHeartbeat && msg.get_commit() == INVALID_INDEX)
}

const STR_CONF_CHANGE_ADD_NODE: &str = "AddNode";
const STR_CONF_CHANGE_REMOVE_NODE: &str = "RemoveNode";
const STR_CONF_CHANGE_ADDLEARNER_NODE: &str = "AddLearner";

pub fn conf_change_type_str(conf_type: eraftpb::ConfChangeType) -> &'static str {
    match conf_type {
        ConfChangeType::AddNode => STR_CONF_CHANGE_ADD_NODE,
        ConfChangeType::RemoveNode => STR_CONF_CHANGE_REMOVE_NODE,
        ConfChangeType::AddLearnerNode => STR_CONF_CHANGE_ADDLEARNER_NODE,
    }
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

#[inline]
pub fn build_key_range(start_key: &[u8], end_key: &[u8], reverse_scan: bool) -> KeyRange {
    let mut range = KeyRange::default();
    if reverse_scan {
        range.set_start_key(end_key.to_vec());
        range.set_end_key(start_key.to_vec());
    } else {
        range.set_start_key(start_key.to_vec());
        range.set_end_key(end_key.to_vec());
    }
    range
}

/// Check if replicas of two regions are on the same stores.
pub fn region_on_same_stores(lhs: &metapb::Region, rhs: &metapb::Region) -> bool {
    if lhs.get_peers().len() != rhs.get_peers().len() {
        return false;
    }

    // Because every store can only have one replica for the same region,
    // so just one round check is enough.
    lhs.get_peers().iter().all(|lp| {
        rhs.get_peers()
            .iter()
            .any(|rp| rp.get_store_id() == lp.get_store_id() && rp.get_role() == lp.get_role())
    })
}

#[inline]
pub fn is_region_initialized(r: &metapb::Region) -> bool {
    !r.get_peers().is_empty()
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
    advance_renew_lease: Duration,
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
    pub fn new(max_lease: Duration, advance_renew_lease: Duration) -> Lease {
        Lease {
            bound: None,
            max_lease,

            max_drift: max_lease / 3,
            advance_renew_lease,
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

    pub fn is_suspect(&self) -> bool {
        matches!(self.bound, Some(Either::Left(_)))
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
            renewing: Arc::new(AtomicBool::new(false)),
            term,
            advance_renew_lease: self.advance_renew_lease,
        };
        // Clone the remote.
        let remote_clone = remote.clone();
        self.remote = Some(remote);
        Some(remote_clone)
    }

    /// Check if the lease will be expired in near future, if so return a
    /// future timestamp in which the lease will be expired, if not `None`
    /// will return
    pub fn need_renew(&self, ts: Timespec) -> Option<Timespec> {
        let future_ts = ts + self.advance_renew_lease;
        match self.bound {
            Some(Either::Right(bound)) => {
                if future_ts < bound {
                    None
                } else {
                    Some(future_ts)
                }
            }
            None | Some(Either::Left(_)) => Some(future_ts),
        }
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
    renewing: Arc<AtomicBool>,
    advance_renew_lease: Duration,
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
        self.renewing.store(false, AtomicOrdering::Release);
    }

    pub fn need_renew(&self, ts: Timespec) -> bool {
        self.inspect(Some(ts + self.advance_renew_lease)) == LeaseState::Expired
            && !self.renewing.swap(true, AtomicOrdering::Relaxed)
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

/// Parse data of entry `index`.
///
/// # Panics
///
/// If `data` is corrupted, this function will panic.
// TODO: make sure received entries are not corrupted
#[inline]
pub fn parse_data_at<T: Message + Default>(data: &[u8], index: u64, tag: &str) -> T {
    let mut result = T::default();
    result.merge_from_bytes(data).unwrap_or_else(|e| {
        panic!("{} data is corrupted at {}: {:?}", tag, index, e);
    });
    result
}

/// Check if two regions are sibling.
///
/// They are sibling only when they share borders and don't overlap.
pub fn is_sibling_regions(lhs: &metapb::Region, rhs: &metapb::Region) -> bool {
    if lhs.get_id() == rhs.get_id() {
        return false;
    }
    if lhs.get_start_key() == rhs.get_end_key() && !rhs.get_end_key().is_empty() {
        return true;
    }
    if lhs.get_end_key() == rhs.get_start_key() && !lhs.get_end_key().is_empty() {
        return true;
    }
    false
}

pub fn conf_state_from_region(region: &metapb::Region) -> ConfState {
    let mut conf_state = ConfState::default();
    let mut in_joint = false;
    for p in region.get_peers() {
        match p.get_role() {
            PeerRole::Voter => {
                conf_state.mut_voters().push(p.get_id());
                conf_state.mut_voters_outgoing().push(p.get_id());
            }
            PeerRole::Learner => conf_state.mut_learners().push(p.get_id()),
            role => {
                in_joint = true;
                match role {
                    PeerRole::IncomingVoter => conf_state.mut_voters().push(p.get_id()),
                    PeerRole::DemotingVoter => {
                        conf_state.mut_voters_outgoing().push(p.get_id());
                        conf_state.mut_learners_next().push(p.get_id());
                    }
                    _ => unreachable!(),
                }
            }
        }
    }
    if !in_joint {
        conf_state.mut_voters_outgoing().clear();
    }
    conf_state
}

pub fn is_learner(peer: &metapb::Peer) -> bool {
    peer.get_role() == PeerRole::Learner
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

/// Abstracts over ChangePeerV2Request and (legacy) ChangePeerRequest to allow
/// treating them in a unified manner.
pub trait ChangePeerI {
    type CC: ConfChangeI;
    type CP: AsRef<[ChangePeerRequest]>;

    fn get_change_peers(&self) -> Self::CP;

    fn to_confchange(&self, _: Vec<u8>) -> Self::CC;
}

impl<'a> ChangePeerI for &'a ChangePeerRequest {
    type CC = eraftpb::ConfChange;
    type CP = Vec<ChangePeerRequest>;

    fn get_change_peers(&self) -> Vec<ChangePeerRequest> {
        vec![ChangePeerRequest::clone(self)]
    }

    fn to_confchange(&self, ctx: Vec<u8>) -> eraftpb::ConfChange {
        let mut cc = eraftpb::ConfChange::default();
        cc.set_change_type(self.get_change_type());
        cc.set_node_id(self.get_peer().get_id());
        cc.set_context(ctx.into());
        cc
    }
}

impl<'a> ChangePeerI for &'a ChangePeerV2Request {
    type CC = eraftpb::ConfChangeV2;
    type CP = &'a [ChangePeerRequest];

    fn get_change_peers(&self) -> &'a [ChangePeerRequest] {
        self.get_changes()
    }

    fn to_confchange(&self, ctx: Vec<u8>) -> eraftpb::ConfChangeV2 {
        let mut cc = eraftpb::ConfChangeV2::default();
        let changes: Vec<_> = self
            .get_changes()
            .iter()
            .map(|c| {
                let mut ccs = eraftpb::ConfChangeSingle::default();
                ccs.set_change_type(c.get_change_type());
                ccs.set_node_id(c.get_peer().get_id());
                ccs
            })
            .collect();

        if changes.len() <= 1 {
            // Leave joint or simple confchange
            cc.set_transition(eraftpb::ConfChangeTransition::Auto);
        } else {
            // Enter joint
            cc.set_transition(eraftpb::ConfChangeTransition::Explicit);
        }
        cc.set_changes(changes.into());
        cc.set_context(ctx.into());
        cc
    }
}

pub struct MsgType<'a>(pub &'a RaftMessage);

impl Display for MsgType<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if !self.0.has_extra_msg() {
            write!(f, "{:?}", self.0.get_message().get_msg_type())
        } else {
            write!(f, "{:?}", self.0.get_extra_msg().get_type())
        }
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

impl Default for RegionReadProgressRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// `RegionReadProgress` is used to keep track of the replica's `safe_ts`, the replica can handle a read
/// request directly without requiring leader lease or read index iff `safe_ts` >= `read_ts` (the `read_ts`
/// is usually stale i.e seconds ago).
///
/// `safe_ts` is updated by the `(apply index, safe ts)` item:
/// ```ignore
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
    pub fn dump_leader_info(&self) -> (Vec<Peer>, LeaderInfo) {
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

/// Represent the duration of all stages of raftstore recorded by one inspecting.
#[derive(Default, Debug)]
pub struct RaftstoreDuration {
    pub store_wait_duration: Option<std::time::Duration>,
    pub store_process_duration: Option<std::time::Duration>,
    pub store_write_duration: Option<std::time::Duration>,
    pub apply_wait_duration: Option<std::time::Duration>,
    pub apply_process_duration: Option<std::time::Duration>,
}

impl RaftstoreDuration {
    pub fn sum(&self) -> std::time::Duration {
        self.store_wait_duration.unwrap_or_default()
            + self.store_process_duration.unwrap_or_default()
            + self.store_write_duration.unwrap_or_default()
            + self.apply_wait_duration.unwrap_or_default()
            + self.apply_process_duration.unwrap_or_default()
    }
}

/// Used to inspect the latency of all stages of raftstore.
pub struct LatencyInspector {
    id: u64,
    duration: RaftstoreDuration,
    cb: Box<dyn FnOnce(u64, RaftstoreDuration) + Send>,
}

impl LatencyInspector {
    pub fn new(id: u64, cb: Box<dyn FnOnce(u64, RaftstoreDuration) + Send>) -> Self {
        Self {
            id,
            cb,
            duration: RaftstoreDuration::default(),
        }
    }

    pub fn record_store_wait(&mut self, duration: std::time::Duration) {
        self.duration.store_wait_duration = Some(duration);
    }

    pub fn record_store_process(&mut self, duration: std::time::Duration) {
        self.duration.store_process_duration = Some(duration);
    }

    pub fn record_store_write(&mut self, duration: std::time::Duration) {
        self.duration.store_write_duration = Some(duration);
    }

    pub fn record_apply_wait(&mut self, duration: std::time::Duration) {
        self.duration.apply_wait_duration = Some(duration);
    }

    pub fn record_apply_process(&mut self, duration: std::time::Duration) {
        self.duration.apply_process_duration = Some(duration);
    }

    /// Call the callback.
    pub fn finish(self) {
        (self.cb)(self.id, self.duration);
    }
}

#[cfg(test)]
mod tests {
    use std::thread;

    use kvproto::{
        metapb::{self, RegionEpoch},
        raft_cmdpb::AdminRequest,
    };
    use raft::eraftpb::{ConfChangeType, Entry, Message, MessageType};
    use tikv_util::time::monotonic_raw_now;
    use time::Duration as TimeDuration;

    use super::*;
    use crate::store::peer_storage;

    #[test]
    fn test_lease() {
        #[inline]
        fn sleep_test(duration: TimeDuration, lease: &Lease, state: LeaseState) {
            // In linux, lease uses CLOCK_MONOTONIC_RAW, while sleep uses CLOCK_MONOTONIC
            let monotonic_raw_start = monotonic_raw_now();
            thread::sleep(duration.to_std().unwrap());
            let mut monotonic_raw_end = monotonic_raw_now();
            // spin wait to make sure pace is aligned with MONOTONIC_RAW clock
            while monotonic_raw_end - monotonic_raw_start < duration {
                thread::yield_now();
                monotonic_raw_end = monotonic_raw_now();
            }
            assert_eq!(lease.inspect(Some(monotonic_raw_end)), state);
            assert_eq!(lease.inspect(None), state);
        }

        let duration = TimeDuration::milliseconds(1500);

        // Empty lease.
        let mut lease = Lease::new(duration, duration / 4);
        let remote = lease.maybe_new_remote_lease(1).unwrap();
        let inspect_test = |lease: &Lease, ts: Option<Timespec>, state: LeaseState| {
            assert_eq!(lease.inspect(ts), state);
            if state == LeaseState::Expired || state == LeaseState::Suspect {
                assert_eq!(remote.inspect(ts), LeaseState::Expired);
            }
        };

        inspect_test(&lease, Some(monotonic_raw_now()), LeaseState::Expired);

        let now = monotonic_raw_now();
        let next_expired_time = lease.next_expired_time(now);
        assert_eq!(now + duration, next_expired_time);

        // Transit to the Valid state.
        lease.renew(now);
        inspect_test(&lease, Some(monotonic_raw_now()), LeaseState::Valid);
        inspect_test(&lease, None, LeaseState::Valid);

        // After lease expired time.
        sleep_test(duration, &lease, LeaseState::Expired);
        inspect_test(&lease, Some(monotonic_raw_now()), LeaseState::Expired);
        inspect_test(&lease, None, LeaseState::Expired);

        // Transit to the Suspect state.
        lease.suspect(monotonic_raw_now());
        inspect_test(&lease, Some(monotonic_raw_now()), LeaseState::Suspect);
        inspect_test(&lease, None, LeaseState::Suspect);

        // After lease expired time, still suspect.
        sleep_test(duration, &lease, LeaseState::Suspect);
        inspect_test(&lease, Some(monotonic_raw_now()), LeaseState::Suspect);

        // Clear lease.
        lease.expire();
        inspect_test(&lease, Some(monotonic_raw_now()), LeaseState::Expired);
        inspect_test(&lease, None, LeaseState::Expired);

        // An expired remote lease can never renew.
        lease.renew(monotonic_raw_now() + TimeDuration::minutes(1));
        assert_eq!(
            remote.inspect(Some(monotonic_raw_now())),
            LeaseState::Expired
        );

        // A new remote lease.
        let m1 = lease.maybe_new_remote_lease(1).unwrap();
        assert_eq!(m1.inspect(Some(monotonic_raw_now())), LeaseState::Valid);
    }

    #[test]
    fn test_timespec_u64() {
        let cases = vec![
            (Timespec::new(0, 0), 0x0000_0000_0000_0000u64),
            (Timespec::new(0, 1), 0x0000_0000_0000_0000u64), // 1ns is round down to 0ms.
            (Timespec::new(0, 999_999), 0x0000_0000_0000_0000u64), // 999_999ns is round down to 0ms.
            (
                // 1_048_575ns is round down to 0ms.
                Timespec::new(0, 1_048_575 /* 0x0FFFFF */),
                0x0000_0000_0000_0000u64,
            ),
            (
                // 1_048_576ns is round down to 1ms.
                Timespec::new(0, 1_048_576 /* 0x100000 */),
                0x0000_0000_0000_0001u64,
            ),
            (Timespec::new(1, 0), 1 << TIMESPEC_SEC_SHIFT),
            (Timespec::new(1, 0x100000), 1 << TIMESPEC_SEC_SHIFT | 1),
            (
                // Max seconds.
                Timespec::new((1i64 << (64 - TIMESPEC_SEC_SHIFT)) - 1, 0),
                (-1i64 as u64) << TIMESPEC_SEC_SHIFT,
            ),
            (
                // Max nano seconds.
                Timespec::new(
                    0,
                    (999_999_999 >> TIMESPEC_NSEC_SHIFT) << TIMESPEC_NSEC_SHIFT,
                ),
                999_999_999 >> TIMESPEC_NSEC_SHIFT,
            ),
            (
                Timespec::new(
                    (1i64 << (64 - TIMESPEC_SEC_SHIFT)) - 1,
                    (999_999_999 >> TIMESPEC_NSEC_SHIFT) << TIMESPEC_NSEC_SHIFT,
                ),
                (-1i64 as u64) << TIMESPEC_SEC_SHIFT | (999_999_999 >> TIMESPEC_NSEC_SHIFT),
            ),
        ];

        for (ts, u) in cases {
            assert!(u64_to_timespec(timespec_to_u64(ts)) <= ts);
            assert!(u64_to_timespec(u) <= ts);
            assert_eq!(timespec_to_u64(u64_to_timespec(u)), u);
            assert_eq!(timespec_to_u64(ts), u);
        }

        let start = monotonic_raw_now();
        let mut now = monotonic_raw_now();
        while now - start < Duration::seconds(1) {
            let u = timespec_to_u64(now);
            let round = u64_to_timespec(u);
            assert!(round <= now, "{:064b} = {:?} > {:?}", u, round, now);
            now = monotonic_raw_now();
        }
    }

    // Tests the util function `check_key_in_region`.
    #[test]
    fn test_check_key_in_region() {
        let test_cases = vec![
            ("", "", "", true, true, false),
            ("", "", "6", true, true, false),
            ("", "3", "6", false, false, false),
            ("4", "3", "6", true, true, true),
            ("4", "3", "", true, true, true),
            ("3", "3", "", true, true, false),
            ("2", "3", "6", false, false, false),
            ("", "3", "6", false, false, false),
            ("", "3", "", false, false, false),
            ("6", "3", "6", false, true, false),
        ];
        for (key, start_key, end_key, is_in_region, inclusive, exclusive) in test_cases {
            let mut region = metapb::Region::default();
            region.set_start_key(start_key.as_bytes().to_vec());
            region.set_end_key(end_key.as_bytes().to_vec());
            let mut result = check_key_in_region(key.as_bytes(), &region);
            assert_eq!(result.is_ok(), is_in_region);
            result = check_key_in_region_inclusive(key.as_bytes(), &region);
            assert_eq!(result.is_ok(), inclusive);
            result = check_key_in_region_exclusive(key.as_bytes(), &region);
            assert_eq!(result.is_ok(), exclusive);
        }
    }

    fn gen_region(
        voters: &[u64],
        learners: &[u64],
        incomming_v: &[u64],
        demoting_v: &[u64],
    ) -> metapb::Region {
        let mut region = metapb::Region::default();
        macro_rules! push_peer {
            ($ids: ident, $role: expr) => {
                for id in $ids {
                    let mut peer = metapb::Peer::default();
                    peer.set_id(*id);
                    peer.set_role($role);
                    region.mut_peers().push(peer);
                }
            };
        }
        push_peer!(voters, metapb::PeerRole::Voter);
        push_peer!(learners, metapb::PeerRole::Learner);
        push_peer!(incomming_v, metapb::PeerRole::IncomingVoter);
        push_peer!(demoting_v, metapb::PeerRole::DemotingVoter);
        region
    }

    #[test]
    fn test_conf_state_from_region() {
        let cases = vec![
            (vec![1], vec![2], vec![], vec![]),
            (vec![], vec![], vec![1], vec![2]),
            (vec![1, 2], vec![], vec![], vec![]),
            (vec![1, 2], vec![], vec![3], vec![]),
            (vec![1], vec![2], vec![3, 4], vec![5, 6]),
        ];

        for (voter, learner, incomming, demoting) in cases {
            let region = gen_region(
                voter.as_slice(),
                learner.as_slice(),
                incomming.as_slice(),
                demoting.as_slice(),
            );
            let cs = conf_state_from_region(&region);
            if incomming.is_empty() && demoting.is_empty() {
                // Not in joint
                assert!(cs.get_voters_outgoing().is_empty());
                assert!(cs.get_learners_next().is_empty());
                assert!(voter.iter().all(|id| cs.get_voters().contains(id)));
                assert!(learner.iter().all(|id| cs.get_learners().contains(id)));
            } else {
                // In joint
                assert!(voter.iter().all(
                    |id| cs.get_voters().contains(id) && cs.get_voters_outgoing().contains(id)
                ));
                assert!(learner.iter().all(|id| cs.get_learners().contains(id)));
                assert!(incomming.iter().all(|id| cs.get_voters().contains(id)));
                assert!(
                    demoting
                        .iter()
                        .all(|id| cs.get_voters_outgoing().contains(id)
                            && cs.get_learners_next().contains(id))
                );
            }
        }
    }

    #[test]
    fn test_changepeer_v2_to_confchange() {
        let mut req = ChangePeerV2Request::default();

        // Zore change for leave joint
        assert_eq!(
            (&req).to_confchange(vec![]).get_transition(),
            eraftpb::ConfChangeTransition::Auto
        );

        // One change for simple confchange
        req.mut_changes().push(ChangePeerRequest::default());
        assert_eq!(
            (&req).to_confchange(vec![]).get_transition(),
            eraftpb::ConfChangeTransition::Auto
        );

        // More than one change for enter joint
        req.mut_changes().push(ChangePeerRequest::default());
        assert_eq!(
            (&req).to_confchange(vec![]).get_transition(),
            eraftpb::ConfChangeTransition::Explicit
        );
        req.mut_changes().push(ChangePeerRequest::default());
        assert_eq!(
            (&req).to_confchange(vec![]).get_transition(),
            eraftpb::ConfChangeTransition::Explicit
        );
    }

    #[test]
    fn test_peer() {
        let mut region = metapb::Region::default();
        region.set_id(1);
        region.mut_peers().push(new_peer(1, 1));
        region.mut_peers().push(new_learner_peer(2, 2));

        assert!(!is_learner(find_peer(&region, 1).unwrap()));
        assert!(is_learner(find_peer(&region, 2).unwrap()));

        assert!(remove_peer(&mut region, 1).is_some());
        assert!(remove_peer(&mut region, 1).is_none());
        assert!(find_peer(&region, 1).is_none());
    }

    #[test]
    fn test_first_vote_msg() {
        let tbl = vec![
            (
                MessageType::MsgRequestVote,
                peer_storage::RAFT_INIT_LOG_TERM + 1,
                true,
            ),
            (
                MessageType::MsgRequestPreVote,
                peer_storage::RAFT_INIT_LOG_TERM + 1,
                true,
            ),
            (
                MessageType::MsgRequestVote,
                peer_storage::RAFT_INIT_LOG_TERM,
                false,
            ),
            (
                MessageType::MsgRequestPreVote,
                peer_storage::RAFT_INIT_LOG_TERM,
                false,
            ),
            (
                MessageType::MsgHup,
                peer_storage::RAFT_INIT_LOG_TERM + 1,
                false,
            ),
        ];

        for (msg_type, term, is_vote) in tbl {
            let mut msg = Message::default();
            msg.set_msg_type(msg_type);
            msg.set_term(term);
            assert_eq!(is_first_vote_msg(&msg), is_vote);
        }
    }

    #[test]
    fn test_first_append_entry() {
        let tbl = vec![
            (
                MessageType::MsgAppend,
                peer_storage::RAFT_INIT_LOG_INDEX + 1,
                true,
            ),
            (
                MessageType::MsgAppend,
                peer_storage::RAFT_INIT_LOG_INDEX,
                false,
            ),
            (
                MessageType::MsgHup,
                peer_storage::RAFT_INIT_LOG_INDEX + 1,
                false,
            ),
        ];

        for (msg_type, index, is_append) in tbl {
            let mut msg = Message::default();
            msg.set_msg_type(msg_type);
            let ent = {
                let mut e = Entry::default();
                e.set_index(index);
                e
            };
            msg.set_entries(vec![ent].into());
            assert_eq!(is_first_append_entry(&msg), is_append);
        }
    }

    #[test]
    fn test_is_initial_msg() {
        let tbl = vec![
            (MessageType::MsgRequestVote, INVALID_INDEX, true),
            (MessageType::MsgRequestPreVote, INVALID_INDEX, true),
            (MessageType::MsgHeartbeat, INVALID_INDEX, true),
            (MessageType::MsgHeartbeat, 100, false),
            (MessageType::MsgAppend, 100, false),
        ];

        for (msg_type, commit, can_create) in tbl {
            let mut msg = Message::default();
            msg.set_msg_type(msg_type);
            msg.set_commit(commit);
            assert_eq!(is_initial_msg(&msg), can_create);
        }
    }

    #[test]
    fn test_conf_change_type_str() {
        assert_eq!(
            conf_change_type_str(ConfChangeType::AddNode),
            STR_CONF_CHANGE_ADD_NODE
        );
        assert_eq!(
            conf_change_type_str(ConfChangeType::RemoveNode),
            STR_CONF_CHANGE_REMOVE_NODE
        );
    }

    #[test]
    fn test_epoch_stale() {
        let mut epoch = metapb::RegionEpoch::default();
        epoch.set_version(10);
        epoch.set_conf_ver(10);

        let tbl = vec![
            (11, 10, true),
            (10, 11, true),
            (10, 10, false),
            (10, 9, false),
        ];

        for (version, conf_version, is_stale) in tbl {
            let mut check_epoch = metapb::RegionEpoch::default();
            check_epoch.set_version(version);
            check_epoch.set_conf_ver(conf_version);
            assert_eq!(is_epoch_stale(&epoch, &check_epoch), is_stale);
        }
    }

    #[test]
    fn test_on_same_store() {
        let cases = vec![
            (vec![2, 3, 4], vec![], vec![1, 2, 3], vec![], false),
            (vec![2, 3, 1], vec![], vec![1, 2, 3], vec![], true),
            (vec![2, 3, 4], vec![], vec![1, 2], vec![], false),
            (vec![1, 2, 3], vec![], vec![1, 2, 3], vec![], true),
            (vec![1, 3], vec![2, 4], vec![1, 2], vec![3, 4], false),
            (vec![1, 3], vec![2, 4], vec![1, 3], vec![], false),
            (vec![1, 3], vec![2, 4], vec![], vec![2, 4], false),
            (vec![1, 3], vec![2, 4], vec![3, 1], vec![4, 2], true),
        ];

        for (s1, s2, s3, s4, exp) in cases {
            let mut r1 = metapb::Region::default();
            for (store_id, peer_id) in s1.into_iter().zip(0..) {
                r1.mut_peers().push(new_peer(store_id, peer_id));
            }
            for (store_id, peer_id) in s2.into_iter().zip(0..) {
                r1.mut_peers().push(new_learner_peer(store_id, peer_id));
            }

            let mut r2 = metapb::Region::default();
            for (store_id, peer_id) in s3.into_iter().zip(10..) {
                r2.mut_peers().push(new_peer(store_id, peer_id));
            }
            for (store_id, peer_id) in s4.into_iter().zip(10..) {
                r2.mut_peers().push(new_learner_peer(store_id, peer_id));
            }
            let res = super::region_on_same_stores(&r1, &r2);
            assert_eq!(res, exp, "{:?} vs {:?}", r1, r2);
        }
    }

    fn split(mut r: metapb::Region, key: &[u8]) -> (metapb::Region, metapb::Region) {
        let mut r2 = r.clone();
        r.set_end_key(key.to_owned());
        r2.set_id(r.get_id() + 1);
        r2.set_start_key(key.to_owned());
        (r, r2)
    }

    fn check_sibling(r1: &metapb::Region, r2: &metapb::Region, is_sibling: bool) {
        assert_eq!(is_sibling_regions(r1, r2), is_sibling);
        assert_eq!(is_sibling_regions(r2, r1), is_sibling);
    }

    #[test]
    fn test_region_sibling() {
        let r1 = metapb::Region::default();
        check_sibling(&r1, &r1, false);

        let (r1, r2) = split(r1, b"k1");
        check_sibling(&r1, &r2, true);

        let (r2, r3) = split(r2, b"k2");
        check_sibling(&r2, &r3, true);

        let (r3, r4) = split(r3, b"k3");
        check_sibling(&r3, &r4, true);
        check_sibling(&r1, &r2, true);
        check_sibling(&r2, &r3, true);
        check_sibling(&r1, &r3, false);
        check_sibling(&r2, &r4, false);
        check_sibling(&r1, &r4, false);
    }

    #[test]
    fn test_check_store_id() {
        let mut req = RaftCmdRequest::default();
        req.mut_header().mut_peer().set_store_id(1);
        check_store_id(&req, 1).unwrap();
        check_store_id(&req, 2).unwrap_err();
    }

    #[test]
    fn test_check_peer_id() {
        let mut req = RaftCmdRequest::default();
        req.mut_header().mut_peer().set_id(1);
        check_peer_id(&req, 1).unwrap();
        check_peer_id(&req, 2).unwrap_err();
    }

    #[test]
    fn test_check_term() {
        let mut req = RaftCmdRequest::default();
        req.mut_header().set_term(7);
        check_term(&req, 7).unwrap();
        check_term(&req, 8).unwrap();
        // If header's term is 2 verions behind current term,
        // leadership may have been changed away.
        check_term(&req, 9).unwrap_err();
        check_term(&req, 10).unwrap_err();
    }

    #[test]
    fn test_check_region_epoch() {
        let mut epoch = RegionEpoch::default();
        epoch.set_conf_ver(2);
        epoch.set_version(2);
        let mut region = metapb::Region::default();
        region.set_region_epoch(epoch.clone());

        // Epoch is required for most requests even if it's empty.
        check_region_epoch(&RaftCmdRequest::default(), &region, false).unwrap_err();

        // These admin commands do not require epoch.
        for ty in &[
            AdminCmdType::CompactLog,
            AdminCmdType::InvalidAdmin,
            AdminCmdType::ComputeHash,
            AdminCmdType::VerifyHash,
        ] {
            let mut admin = AdminRequest::default();
            admin.set_cmd_type(*ty);
            let mut req = RaftCmdRequest::default();
            req.set_admin_request(admin);

            // It is Okay if req does not have region epoch.
            check_region_epoch(&req, &region, false).unwrap();

            req.mut_header().set_region_epoch(epoch.clone());
            check_region_epoch(&req, &region, true).unwrap();
            check_region_epoch(&req, &region, false).unwrap();
        }

        // These admin commands requires epoch.version.
        for ty in &[
            AdminCmdType::Split,
            AdminCmdType::BatchSplit,
            AdminCmdType::PrepareMerge,
            AdminCmdType::CommitMerge,
            AdminCmdType::RollbackMerge,
            AdminCmdType::TransferLeader,
        ] {
            let mut admin = AdminRequest::default();
            admin.set_cmd_type(*ty);
            let mut req = RaftCmdRequest::default();
            req.set_admin_request(admin);

            // Error if req does not have region epoch.
            check_region_epoch(&req, &region, false).unwrap_err();

            let mut stale_version_epoch = epoch.clone();
            stale_version_epoch.set_version(1);
            let mut stale_region = metapb::Region::default();
            stale_region.set_region_epoch(stale_version_epoch.clone());
            req.mut_header()
                .set_region_epoch(stale_version_epoch.clone());
            check_region_epoch(&req, &stale_region, false).unwrap();

            let mut latest_version_epoch = epoch.clone();
            latest_version_epoch.set_version(3);
            for epoch in &[stale_version_epoch, latest_version_epoch] {
                req.mut_header().set_region_epoch(epoch.clone());
                check_region_epoch(&req, &region, false).unwrap_err();
                check_region_epoch(&req, &region, true).unwrap_err();
            }
        }

        // These admin commands requires epoch.conf_version.
        for ty in &[
            AdminCmdType::Split,
            AdminCmdType::BatchSplit,
            AdminCmdType::ChangePeer,
            AdminCmdType::ChangePeerV2,
            AdminCmdType::PrepareMerge,
            AdminCmdType::CommitMerge,
            AdminCmdType::RollbackMerge,
            AdminCmdType::TransferLeader,
        ] {
            let mut admin = AdminRequest::default();
            admin.set_cmd_type(*ty);
            let mut req = RaftCmdRequest::default();
            req.set_admin_request(admin);

            // Error if req does not have region epoch.
            check_region_epoch(&req, &region, false).unwrap_err();

            let mut stale_conf_epoch = epoch.clone();
            stale_conf_epoch.set_conf_ver(1);
            let mut stale_region = metapb::Region::default();
            stale_region.set_region_epoch(stale_conf_epoch.clone());
            req.mut_header().set_region_epoch(stale_conf_epoch.clone());
            check_region_epoch(&req, &stale_region, false).unwrap();

            let mut latest_conf_epoch = epoch.clone();
            latest_conf_epoch.set_conf_ver(3);
            for epoch in &[stale_conf_epoch, latest_conf_epoch] {
                req.mut_header().set_region_epoch(epoch.clone());
                check_region_epoch(&req, &region, false).unwrap_err();
                check_region_epoch(&req, &region, true).unwrap_err();
            }
        }
    }

    #[test]
    fn test_is_region_initialized() {
        let mut region = metapb::Region::default();
        assert!(!is_region_initialized(&region));
        let peers = vec![new_peer(1, 2)];
        region.set_peers(peers.into());
        assert!(is_region_initialized(&region));
    }

    #[test]
    fn test_region_read_progress() {
        // Return the number of the pending (index, ts) item
        fn pending_items_num(rrp: &RegionReadProgress) -> usize {
            rrp.core.lock().unwrap().pending_items.len()
        }

        let cap = 10;
        let rrp = RegionReadProgress::new(&Default::default(), 10, cap, "".to_owned());
        for i in 1..=20 {
            rrp.update_safe_ts(i, i);
        }
        // `safe_ts` update according to its `applied_index`
        assert_eq!(rrp.safe_ts(), 10);
        assert_eq!(pending_items_num(&rrp), 10);

        rrp.update_applied(20);
        assert_eq!(rrp.safe_ts(), 20);
        assert_eq!(pending_items_num(&rrp), 0);

        for i in 100..200 {
            rrp.update_safe_ts(i, i);
        }
        assert_eq!(rrp.safe_ts(), 20);
        // the number of pending item should not exceed `cap`
        assert!(pending_items_num(&rrp) <= cap);

        // `applied_index` large than all pending items will clear all pending items
        rrp.update_applied(200);
        assert_eq!(rrp.safe_ts(), 199);
        assert_eq!(pending_items_num(&rrp), 0);

        // pending item can be updated instead of adding a new one
        rrp.update_safe_ts(300, 300);
        assert_eq!(pending_items_num(&rrp), 1);
        rrp.update_safe_ts(200, 400);
        assert_eq!(pending_items_num(&rrp), 1);
        rrp.update_safe_ts(300, 500);
        assert_eq!(pending_items_num(&rrp), 1);
        rrp.update_safe_ts(301, 600);
        assert_eq!(pending_items_num(&rrp), 2);
        // `safe_ts` will update to 500 instead of 300
        rrp.update_applied(300);
        assert_eq!(rrp.safe_ts(), 500);
        rrp.update_applied(301);
        assert_eq!(rrp.safe_ts(), 600);
        assert_eq!(pending_items_num(&rrp), 0);

        // stale item will be ignored
        rrp.update_safe_ts(300, 500);
        rrp.update_safe_ts(301, 600);
        rrp.update_safe_ts(400, 0);
        rrp.update_safe_ts(0, 700);
        assert_eq!(pending_items_num(&rrp), 0);
    }
}
