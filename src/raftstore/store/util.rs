// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::Bound::Excluded;
use std::option::Option;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::Arc;
use std::{fmt, u64};

use kvproto::metapb;
use kvproto::raft_cmdpb::{AdminCmdType, RaftCmdRequest};
use protobuf::{self, Message};
use raft::eraftpb::{self, ConfChangeType, ConfState, MessageType};
use raft::INVALID_INDEX;
use raftstore::store::keys;
use raftstore::{Error, Result};
use rocksdb::{Range, TablePropertiesCollection, Writable, WriteBatch, DB};
use time::{Duration, Timespec};

use storage::{Key, CF_DEFAULT, CF_LOCK, CF_RAFT, CF_WRITE, LARGE_CFS};
use util::escape;
use util::properties::RangeProperties;
use util::rocksdb::stats::get_range_entries_and_versions;
use util::time::monotonic_raw_now;
use util::{rocksdb as rocksdb_util, Either};

use super::engine::{IterOption, Iterable};
use super::peer_storage;

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
    let mut peer = metapb::Peer::new();
    peer.set_store_id(store_id);
    peer.set_id(peer_id);
    peer
}

// a helper function to create learner peer easily.
pub fn new_learner_peer(store_id: u64, peer_id: u64) -> metapb::Peer {
    let mut peer = new_peer(store_id, peer_id);
    peer.set_is_learner(true);
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
/// region overlaps with others. In this case we should put `msg` into `pending_votes` instead of
/// create the peer.
#[inline]
pub fn is_first_vote_msg(msg: &eraftpb::Message) -> bool {
    match msg.get_msg_type() {
        MessageType::MsgRequestVote | MessageType::MsgRequestPreVote => {
            msg.get_term() == peer_storage::RAFT_INIT_LOG_TERM + 1
        }
        _ => false,
    }
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

// In our tests, we found that if the batch size is too large, running delete_all_in_range will
// reduce OLTP QPS by 30% ~ 60%. We found that 32K is a proper choice.
pub const MAX_DELETE_BATCH_SIZE: usize = 32 * 1024;

pub fn delete_all_in_range(
    db: &DB,
    start_key: &[u8],
    end_key: &[u8],
    use_delete_range: bool,
) -> Result<()> {
    if start_key >= end_key {
        return Ok(());
    }

    for cf in db.cf_names() {
        delete_all_in_range_cf(db, cf, start_key, end_key, use_delete_range)?;
    }

    Ok(())
}

pub fn delete_all_in_range_cf(
    db: &DB,
    cf: &str,
    start_key: &[u8],
    end_key: &[u8],
    use_delete_range: bool,
) -> Result<()> {
    let handle = rocksdb_util::get_cf_handle(db, cf)?;
    let mut wb = WriteBatch::new();
    // Since CF_RAFT and CF_LOCK is usually small, so using
    // traditional way to cleanup.
    if use_delete_range && cf != CF_RAFT && cf != CF_LOCK {
        if cf == CF_WRITE {
            let start = Key::from_encoded_slice(start_key).append_ts(u64::MAX);
            wb.delete_range_cf(handle, start.as_encoded(), end_key)?;
        } else {
            wb.delete_range_cf(handle, start_key, end_key)?;
        }
    } else {
        let iter_opt = IterOption::new(Some(start_key.to_vec()), Some(end_key.to_vec()), false);
        let mut it = db.new_iterator_cf(cf, iter_opt)?;
        it.seek(start_key.into());
        while it.valid() {
            wb.delete_cf(handle, it.key())?;
            if wb.data_size() >= MAX_DELETE_BATCH_SIZE {
                // Can't use write_without_wal here.
                // Otherwise it may cause dirty data when applying snapshot.
                db.write(wb)?;
                wb = WriteBatch::new();
            }

            if !it.next() {
                break;
            }
        }
    }

    if wb.count() > 0 {
        db.write(wb)?;
    }

    Ok(())
}

pub fn delete_all_files_in_range(db: &DB, start_key: &[u8], end_key: &[u8]) -> Result<()> {
    if start_key >= end_key {
        return Ok(());
    }

    for cf in db.cf_names() {
        let handle = rocksdb_util::get_cf_handle(db, cf)?;
        db.delete_files_in_range_cf(handle, start_key, end_key, false)?;
    }

    Ok(())
}

// check whether epoch is staler than check_epoch.
pub fn is_epoch_stale(epoch: &metapb::RegionEpoch, check_epoch: &metapb::RegionEpoch) -> bool {
    epoch.get_version() < check_epoch.get_version()
        || epoch.get_conf_ver() < check_epoch.get_conf_ver()
}

pub fn check_region_epoch(
    req: &RaftCmdRequest,
    region: &metapb::Region,
    include_region: bool,
) -> Result<()> {
    let (mut check_ver, mut check_conf_ver) = (false, false);
    if !req.has_admin_request() {
        // for get/set/delete, we don't care conf_version.
        check_ver = true;
    } else {
        match req.get_admin_request().get_cmd_type() {
            AdminCmdType::CompactLog
            | AdminCmdType::InvalidAdmin
            | AdminCmdType::ComputeHash
            | AdminCmdType::VerifyHash => {}
            AdminCmdType::Split | AdminCmdType::BatchSplit => check_ver = true,
            AdminCmdType::ChangePeer => check_conf_ver = true,
            AdminCmdType::PrepareMerge
            | AdminCmdType::CommitMerge
            | AdminCmdType::RollbackMerge
            | AdminCmdType::TransferLeader => {
                check_ver = true;
                check_conf_ver = true;
            }
        };
    }

    if !check_ver && !check_conf_ver {
        return Ok(());
    }

    if !req.get_header().has_region_epoch() {
        return Err(box_err!("missing epoch!"));
    }

    let from_epoch = req.get_header().get_region_epoch();
    let latest_epoch = region.get_region_epoch();

    // should we use not equal here?
    if (check_conf_ver && from_epoch.get_conf_ver() < latest_epoch.get_conf_ver())
        || (check_ver && from_epoch.get_version() < latest_epoch.get_version())
    {
        debug!(
            "[region {}] received stale epoch {:?}, mine: {:?}",
            region.get_id(),
            from_epoch,
            latest_epoch
        );
        let regions = if include_region {
            vec![region.to_owned()]
        } else {
            vec![]
        };
        return Err(Error::StaleEpoch(
            format!(
                "latest_epoch of region {} is {:?}, but you \
                 sent {:?}",
                region.get_id(),
                latest_epoch,
                from_epoch
            ),
            regions,
        ));
    }

    Ok(())
}

#[inline]
pub fn check_store_id(req: &RaftCmdRequest, store_id: u64) -> Result<()> {
    let peer = req.get_header().get_peer();
    if peer.get_store_id() == store_id {
        Ok(())
    } else {
        error!(
            "[store {}] mismatch store id {}",
            store_id,
            peer.get_store_id()
        );
        Err(Error::StoreNotMatch(peer.get_store_id(), store_id))
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

pub fn get_region_properties_cf(
    db: &DB,
    cfname: &str,
    region: &metapb::Region,
) -> Result<TablePropertiesCollection> {
    let cf = rocksdb_util::get_cf_handle(db, cfname)?;
    let start = keys::enc_start_key(region);
    let end = keys::enc_end_key(region);
    let range = Range::new(&start, &end);
    db.get_properties_of_tables_in_range(cf, &[range])
        .map_err(|e| e.into())
}

/// Get the approximate size of the region.
pub fn get_region_approximate_size(db: &DB, region: &metapb::Region) -> Result<u64> {
    let mut size = 0;
    for cfname in LARGE_CFS {
        size += get_region_approximate_size_cf(db, cfname, region)?
    }
    Ok(size)
}

pub fn get_region_approximate_size_cf(
    db: &DB,
    cfname: &str,
    region: &metapb::Region,
) -> Result<u64> {
    let cf = rocksdb_util::get_cf_handle(db, cfname)?;
    let start = keys::enc_start_key(region);
    let end = keys::enc_end_key(region);
    let range = Range::new(&start, &end);
    let (_, mut size) = db.get_approximate_memtable_stats_cf(cf, &range);

    let collection = db.get_properties_of_tables_in_range(cf, &[range])?;
    for (_, v) in &*collection {
        let props = RangeProperties::decode(v.user_collected_properties())?;
        size += props.get_approximate_size_in_range(&start, &end);
    }
    Ok(size)
}

/// Get the approximate number of keys in the region.
pub fn get_region_approximate_keys(db: &DB, region: &metapb::Region) -> Result<u64> {
    // try to get from RangeProperties first.
    match get_region_approximate_keys_cf(db, CF_WRITE, region) {
        Ok(v) => if v > 0 {
            return Ok(v);
        },
        Err(e) => debug!(
            "old_version:get keys from RangeProperties failed with err:{:?}",
            e
        ),
    }

    let cf = rocksdb_util::get_cf_handle(db, CF_WRITE)?;
    let start = keys::enc_start_key(region);
    let end = keys::enc_end_key(region);
    let (_, keys) = get_range_entries_and_versions(db, cf, &start, &end).unwrap_or_default();
    Ok(keys)
}

pub fn get_region_approximate_keys_cf(
    db: &DB,
    cfname: &str,
    region: &metapb::Region,
) -> Result<u64> {
    let cf = rocksdb_util::get_cf_handle(db, cfname)?;
    let start = keys::enc_start_key(region);
    let end = keys::enc_end_key(region);
    let range = Range::new(&start, &end);
    let (mut keys, _) = db.get_approximate_memtable_stats_cf(cf, &range);

    let collection = db.get_properties_of_tables_in_range(cf, &[range])?;
    for (_, v) in &*collection {
        let props = RangeProperties::decode(v.user_collected_properties())?;
        keys += props.get_approximate_keys_in_range(&start, &end);
    }
    Ok(keys)
}

/// Get region approximate middle key based on default and write cf size.
pub fn get_region_approximate_middle(db: &DB, region: &metapb::Region) -> Result<Option<Vec<u8>>> {
    let get_cf_size = |cf: &str| get_region_approximate_size_cf(db, cf, &region);

    let default_cf_size = box_try!(get_cf_size(CF_DEFAULT));
    let write_cf_size = box_try!(get_cf_size(CF_WRITE));

    let middle_by_cf = if default_cf_size >= write_cf_size {
        CF_DEFAULT
    } else {
        CF_WRITE
    };

    get_region_approximate_middle_cf(db, middle_by_cf, &region)
}

/// Get the approxmiate middle key of the region. If we suppose the region
/// is stored on disk as a plain file, "middle key" means the key whose
/// position is in the middle of the file.
///
/// The returned key maybe is timestamped if transaction KV is used,
/// and must start with "z".
pub fn get_region_approximate_middle_cf(
    db: &DB,
    cfname: &str,
    region: &metapb::Region,
) -> Result<Option<Vec<u8>>> {
    let cf = rocksdb_util::get_cf_handle(db, cfname)?;
    let start = keys::enc_start_key(region);
    let end = keys::enc_end_key(region);
    let range = Range::new(&start, &end);
    let collection = db.get_properties_of_tables_in_range(cf, &[range])?;

    let mut keys = Vec::new();
    for (_, v) in &*collection {
        let props = RangeProperties::decode(v.user_collected_properties())?;
        keys.extend(
            props
                .offsets
                .range::<[u8], _>((Excluded(start.as_slice()), Excluded(end.as_slice())))
                .map(|(k, _)| k.to_owned()),
        );
    }
    if keys.is_empty() {
        return Ok(None);
    }
    keys.sort();
    // Calculate the position by (len-1)/2. So it's the left one
    // of two middle positions if the number of keys is even.
    let middle = (keys.len() - 1) / 2;
    Ok(Some(keys.swap_remove(middle)))
}

/// Get region approximate split keys based on default and write cf.
pub fn get_region_approximate_split_keys(
    db: &DB,
    region: &metapb::Region,
    split_size: u64,
    max_size: u64,
    batch_split_limit: u64,
) -> Result<Vec<Vec<u8>>> {
    let get_cf_size = |cf: &str| get_region_approximate_size_cf(db, cf, &region);

    let default_cf_size = box_try!(get_cf_size(CF_DEFAULT));
    let write_cf_size = box_try!(get_cf_size(CF_WRITE));
    if default_cf_size + write_cf_size == 0 {
        return Err(box_err!("default cf and write cf is empty"));
    }

    // assume the size of keys is uniform distribution in both cfs.
    let (cf, cf_split_size) = if default_cf_size >= write_cf_size {
        (
            CF_DEFAULT,
            split_size * default_cf_size / (default_cf_size + write_cf_size),
        )
    } else {
        (
            CF_WRITE,
            split_size * write_cf_size / (default_cf_size + write_cf_size),
        )
    };

    get_region_approximate_split_keys_cf(
        db,
        cf,
        &region,
        cf_split_size,
        max_size,
        batch_split_limit,
    )
}

pub fn get_region_approximate_split_keys_cf(
    db: &DB,
    cfname: &str,
    region: &metapb::Region,
    split_size: u64,
    max_size: u64,
    batch_split_limit: u64,
) -> Result<Vec<Vec<u8>>> {
    let cf = rocksdb_util::get_cf_handle(db, cfname)?;
    let start = keys::enc_start_key(region);
    let end = keys::enc_end_key(region);
    let range = Range::new(&start, &end);
    let collection = db.get_properties_of_tables_in_range(cf, &[range])?;

    let mut keys = vec![];
    let mut total_size = 0;
    for (_, v) in &*collection {
        let props = RangeProperties::decode(v.user_collected_properties())?;
        total_size += props.get_approximate_size_in_range(&start, &end);
        keys.extend(
            props
                .offsets
                .range::<[u8], _>((Excluded(start.as_slice()), Excluded(end.as_slice())))
                .map(|(k, _)| k.to_owned()),
        );
    }
    if keys.len() == 1 {
        return Ok(vec![]);
    }
    if keys.is_empty() || total_size == 0 || split_size == 0 {
        return Err(box_err!(
            "unexpected key len {} or total_size {} or split size {}, len of collection {}, cf {}, start {}, end {}",
            keys.len(),
            total_size,
            split_size,
            collection.len(),
            cfname,
            escape(&start),
            escape(&end)
        ));
    }
    keys.sort();

    // use total size of this range and the number of keys in this range to
    // calculate the average distance between two keys, and we produce a
    // split_key every `split_size / distance` keys.
    let len = keys.len();
    let distance = total_size as f64 / len as f64;
    let n = (split_size as f64 / distance).ceil() as usize;
    if n == 0 {
        return Err(box_err!(
            "unexpected n == 0, total_size: {}, split_size: {}, len: {}, distance: {}",
            total_size,
            split_size,
            keys.len(),
            distance
        ));
    }

    // cause first element of the iterator will always be returned by step_by(),
    // so the first key returned may not the desired split key. Note that, the
    // start key of region is not included, so we we drop first n - 1 keys.
    //
    // For example, the split size is `3 * distance`. And the numbers stand for the
    // key in `RangeProperties`, `^` stands for produced split key.
    //
    // skip:
    // start___1___2___3___4___5___6___7....
    //                 ^           ^
    //
    // not skip:
    // start___1___2___3___4___5___6___7....
    //         ^           ^           ^
    let mut split_keys = keys
        .into_iter()
        .skip(n - 1)
        .step_by(n)
        .collect::<Vec<Vec<u8>>>();

    if split_keys.len() as u64 > batch_split_limit {
        split_keys.truncate(batch_split_limit as usize);
    } else {
        // make sure not to split when less than max_size for last part
        let rest = (len % n) as u64;
        if rest * distance as u64 + split_size < max_size {
            split_keys.pop();
        }
    }
    Ok(split_keys)
}

/// Check if replicas of two regions are on the same stores.
pub fn region_on_same_stores(lhs: &metapb::Region, rhs: &metapb::Region) -> bool {
    if lhs.get_peers().len() != rhs.get_peers().len() {
        return false;
    }

    // Because every store can only have one replica for the same region,
    // so just one round check is enough.
    lhs.get_peers().iter().all(|lp| {
        rhs.get_peers().iter().any(|rp| {
            rp.get_store_id() == lp.get_store_id() && rp.get_is_learner() == lp.get_is_learner()
        })
    })
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
            Some(Either::Left(ts)) | Some(Either::Right(ts)) => if ts <= bound {
                self.bound = Some(Either::Right(bound));
            },
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
            Some(Either::Right(bound)) => if ts.unwrap_or_else(monotonic_raw_now) < bound {
                LeaseState::Valid
            } else {
                LeaseState::Expired
            },
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
        let remote_clone = RemoteLease {
            expired_time: Arc::clone(&remote.expired_time),
            term,
        };
        self.remote = Some(remote);
        Some(remote_clone)
    }
}

impl fmt::Debug for Lease {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
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
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
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
fn u64_to_timespec(u: u64) -> Timespec {
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
pub fn parse_data_at<T: Message>(data: &[u8], index: u64, tag: &str) -> T {
    protobuf::parse_from_bytes::<T>(data).unwrap_or_else(|e| {
        panic!("{} data is corrupted at {}: {:?}", tag, index, e);
    })
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
    // Here `learners` means learner peers, and `nodes` means voter peers.
    let mut conf_state = ConfState::new();
    for p in region.get_peers() {
        if p.get_is_learner() {
            conf_state.mut_learners().push(p.get_id());
        } else {
            conf_state.mut_nodes().push(p.get_id());
        }
    }
    conf_state
}

#[derive(Clone, Debug)]
pub struct Engines {
    pub kv: Arc<DB>,
    pub raft: Arc<DB>,
}

impl Engines {
    pub fn new(kv_engine: Arc<DB>, raft_engine: Arc<DB>) -> Engines {
        Engines {
            kv: kv_engine,
            raft: raft_engine,
        }
    }
}

pub struct KeysInfoFormatter<'a>(pub &'a [Vec<u8>]);

impl<'a> fmt::Display for KeysInfoFormatter<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self.0.len() {
            0 => write!(f, "no key"),
            1 => write!(f, "key \"{}\"", escape(self.0.first().unwrap())),
            _ => write!(
                f,
                "{} keys range from \"{}\" to \"{}\"",
                self.0.len(),
                escape(self.0.first().unwrap()),
                escape(self.0.last().unwrap())
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{iter, process, thread};

    use kvproto::metapb::{self, RegionEpoch};
    use kvproto::raft_cmdpb::AdminRequest;
    use raft::eraftpb::{ConfChangeType, Message, MessageType};
    use rocksdb::{ColumnFamilyOptions, DBOptions, SeekKey, Writable, WriteBatch, DB};
    use tempdir::TempDir;
    use time::Duration as TimeDuration;

    use raftstore::store::peer_storage;
    use storage::mvcc::{Write, WriteType};
    use storage::{Key, ALL_CFS, CF_DEFAULT};
    use util::escape;
    use util::properties::{MvccPropertiesCollectorFactory, RangePropertiesCollectorFactory};
    use util::rocksdb::{get_cf_handle, new_engine_opt, CFOptions};
    use util::time::{monotonic_now, monotonic_raw_now};

    use super::*;

    #[test]
    fn test_lease() {
        #[inline]
        fn sleep_test(duration: TimeDuration, lease: &Lease, state: LeaseState) {
            // In linux, sleep uses CLOCK_MONOTONIC.
            let monotonic_start = monotonic_now();
            // In linux, lease uses CLOCK_MONOTONIC_RAW.
            let monotonic_raw_start = monotonic_raw_now();
            thread::sleep(duration.to_std().unwrap());
            let monotonic_end = monotonic_now();
            let monotonic_raw_end = monotonic_raw_now();
            assert_eq!(
                lease.inspect(Some(monotonic_raw_end)),
                state,
                "elapsed monotonic_raw: {:?}, monotonic: {:?}",
                monotonic_raw_end - monotonic_raw_start,
                monotonic_end - monotonic_start
            );
            assert_eq!(lease.inspect(None), state);
        }

        let duration = TimeDuration::milliseconds(1500);

        // Empty lease.
        let mut lease = Lease::new(duration);
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
            let mut region = metapb::Region::new();
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

    #[test]
    fn test_conf_state_from_region() {
        let mut region = metapb::Region::new();

        let mut peer = metapb::Peer::new();
        peer.set_id(1);
        region.mut_peers().push(peer);

        let mut peer = metapb::Peer::new();
        peer.set_id(2);
        peer.set_is_learner(true);
        region.mut_peers().push(peer);

        let cs = conf_state_from_region(&region);
        assert!(cs.get_nodes().contains(&1));
        assert!(cs.get_learners().contains(&2));
    }

    #[test]
    fn test_peer() {
        let mut region = metapb::Region::new();
        region.set_id(1);
        region.mut_peers().push(new_peer(1, 1));
        region.mut_peers().push(new_learner_peer(2, 2));

        assert!(!find_peer(&region, 1).unwrap().get_is_learner());
        assert!(find_peer(&region, 2).unwrap().get_is_learner());

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
            let mut msg = Message::new();
            msg.set_msg_type(msg_type);
            msg.set_term(term);
            assert_eq!(is_first_vote_msg(&msg), is_vote);
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
            let mut msg = Message::new();
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
        let mut epoch = metapb::RegionEpoch::new();
        epoch.set_version(10);
        epoch.set_conf_ver(10);

        let tbl = vec![
            (11, 10, true),
            (10, 11, true),
            (10, 10, false),
            (10, 9, false),
        ];

        for (version, conf_version, is_stale) in tbl {
            let mut check_epoch = metapb::RegionEpoch::new();
            check_epoch.set_version(version);
            check_epoch.set_conf_ver(conf_version);
            assert_eq!(is_epoch_stale(&epoch, &check_epoch), is_stale);
        }
    }

    fn make_region(id: u64, start_key: Vec<u8>, end_key: Vec<u8>) -> metapb::Region {
        let mut peer = metapb::Peer::new();
        peer.set_id(id);
        peer.set_store_id(id);
        let mut region = metapb::Region::new();
        region.set_id(id);
        region.set_start_key(start_key);
        region.set_end_key(end_key);
        region.mut_peers().push(peer);
        region
    }

    #[test]
    fn test_region_approximate_keys() {
        let path = TempDir::new("_test_region_approximate_keys").expect("");
        let path_str = path.path().to_str().unwrap();
        let db_opts = DBOptions::new();
        let mut cf_opts = ColumnFamilyOptions::new();
        cf_opts.set_level_zero_file_num_compaction_trigger(10);
        let f = Box::new(MvccPropertiesCollectorFactory::default());
        cf_opts.add_table_properties_collector_factory("tikv.mvcc-properties-collector", f);
        let cfs_opts = LARGE_CFS
            .iter()
            .map(|cf| CFOptions::new(cf, cf_opts.clone()))
            .collect();
        let db = rocksdb_util::new_engine_opt(path_str, db_opts, cfs_opts).unwrap();

        let cases = [("a", 1024), ("b", 2048), ("c", 4096)];
        for &(key, vlen) in &cases {
            let key = keys::data_key(Key::from_raw(key.as_bytes()).append_ts(2).as_encoded());
            let write_v = Write::new(WriteType::Put, 0, None).to_bytes();
            let write_cf = db.cf_handle(CF_WRITE).unwrap();
            db.put_cf(write_cf, &key, &write_v).unwrap();
            db.flush_cf(write_cf, true).unwrap();

            let default_v = vec![0; vlen as usize];
            let default_cf = db.cf_handle(CF_DEFAULT).unwrap();
            db.put_cf(default_cf, &key, &default_v).unwrap();
            db.flush_cf(default_cf, true).unwrap();
        }

        let region = make_region(1, vec![], vec![]);
        let region_keys = get_region_approximate_keys(&db, &region).unwrap();
        assert_eq!(region_keys, cases.len() as u64);
    }

    #[test]
    fn test_region_approximate_size() {
        let path = TempDir::new("_test_raftstore_region_approximate_size").expect("");
        let path_str = path.path().to_str().unwrap();
        let db_opts = DBOptions::new();
        let mut cf_opts = ColumnFamilyOptions::new();
        cf_opts.set_level_zero_file_num_compaction_trigger(10);
        let f = Box::new(RangePropertiesCollectorFactory::default());
        cf_opts.add_table_properties_collector_factory("tikv.range-collector", f);
        let cfs_opts = LARGE_CFS
            .iter()
            .map(|cf| CFOptions::new(cf, cf_opts.clone()))
            .collect();
        let db = rocksdb_util::new_engine_opt(path_str, db_opts, cfs_opts).unwrap();

        let cases = [("a", 1024), ("b", 2048), ("c", 4096)];
        let cf_size = 2 + 1024 + 2 + 2048 + 2 + 4096;
        for &(key, vlen) in &cases {
            for cfname in LARGE_CFS {
                let k1 = keys::data_key(key.as_bytes());
                let v1 = vec![0; vlen as usize];
                assert_eq!(k1.len(), 2);
                let cf = db.cf_handle(cfname).unwrap();
                db.put_cf(cf, &k1, &v1).unwrap();
                db.flush_cf(cf, true).unwrap();
            }
        }

        let region = make_region(1, vec![], vec![]);
        let size = get_region_approximate_size(&db, &region).unwrap();
        assert_eq!(size, cf_size * LARGE_CFS.len() as u64);
        for cfname in LARGE_CFS {
            let size = get_region_approximate_size_cf(&db, cfname, &region).unwrap();
            assert_eq!(size, cf_size);
        }
    }

    fn check_data(db: &DB, cfs: &[&str], expected: &[(&[u8], &[u8])]) {
        for cf in cfs {
            let handle = get_cf_handle(db, cf).unwrap();
            let mut iter = db.iter_cf(handle);
            iter.seek(SeekKey::Start);
            for &(k, v) in expected {
                assert_eq!(k, iter.key());
                assert_eq!(v, iter.value());
                iter.next();
            }
            assert!(!iter.valid());
        }
    }

    fn test_delete_all_in_range(use_delete_range: bool) {
        let path = TempDir::new("_raftstore_util_delete_all_in_range").expect("");
        let path_str = path.path().to_str().unwrap();

        let cfs_opts = ALL_CFS
            .into_iter()
            .map(|cf| CFOptions::new(cf, ColumnFamilyOptions::new()))
            .collect();
        let db = new_engine_opt(path_str, DBOptions::new(), cfs_opts).unwrap();

        let wb = WriteBatch::new();
        let ts: u64 = 12345;
        let keys = vec![
            Key::from_raw(b"k1").append_ts(ts),
            Key::from_raw(b"k2").append_ts(ts),
            Key::from_raw(b"k3").append_ts(ts),
            Key::from_raw(b"k4").append_ts(ts),
        ];

        let mut kvs: Vec<(&[u8], &[u8])> = vec![];
        for (_, key) in keys.iter().enumerate() {
            kvs.push((key.as_encoded().as_slice(), b"value"));
        }
        let kvs_left: Vec<(&[u8], &[u8])> = vec![(kvs[0].0, kvs[0].1), (kvs[3].0, kvs[3].1)];
        for &(k, v) in kvs.as_slice() {
            for cf in ALL_CFS {
                let handle = get_cf_handle(&db, cf).unwrap();
                wb.put_cf(handle, k, v).unwrap();
            }
        }
        db.write(wb).unwrap();
        check_data(&db, ALL_CFS, kvs.as_slice());

        // Delete all in ["k2", "k4").
        let start = Key::from_raw(b"k2");
        let end = Key::from_raw(b"k4");
        delete_all_in_range(
            &db,
            start.as_encoded().as_slice(),
            end.as_encoded().as_slice(),
            use_delete_range,
        ).unwrap();
        check_data(&db, ALL_CFS, kvs_left.as_slice());
    }

    #[test]
    fn test_delete_all_in_range_use_delete_range() {
        test_delete_all_in_range(true);
    }

    #[test]
    fn test_delete_all_in_range_not_use_delete_range() {
        test_delete_all_in_range(false);
    }

    #[test]
    fn test_delete_all_files_in_range() {
        let path = TempDir::new("_raftstore_util_delete_all_files_in_range").expect("");
        let path_str = path.path().to_str().unwrap();

        let cfs_opts = ALL_CFS
            .into_iter()
            .map(|cf| {
                let mut cf_opts = ColumnFamilyOptions::new();
                cf_opts.set_level_zero_file_num_compaction_trigger(1);
                CFOptions::new(cf, cf_opts)
            })
            .collect();
        let db = new_engine_opt(path_str, DBOptions::new(), cfs_opts).unwrap();

        let keys = vec![b"k1", b"k2", b"k3", b"k4"];

        let mut kvs: Vec<(&[u8], &[u8])> = vec![];
        for key in keys {
            kvs.push((key, b"value"));
        }
        let kvs_left: Vec<(&[u8], &[u8])> = vec![(kvs[0].0, kvs[0].1), (kvs[3].0, kvs[3].1)];
        for cf in ALL_CFS {
            let handle = get_cf_handle(&db, cf).unwrap();
            for &(k, v) in kvs.as_slice() {
                db.put_cf(handle, k, v).unwrap();
                db.flush_cf(handle, true).unwrap();
            }
        }
        check_data(&db, ALL_CFS, kvs.as_slice());

        delete_all_files_in_range(&db, b"k2", b"k4").unwrap();
        check_data(&db, ALL_CFS, kvs_left.as_slice());
    }

    fn exit_with_err(msg: String) -> ! {
        error!("{}", msg);
        process::exit(1)
    }

    #[test]
    fn test_delete_range_prefix_bloom_case() {
        let path = TempDir::new("_raftstore_util_delete_range_prefix_bloom").expect("");
        let path_str = path.path().to_str().unwrap();

        let mut opts = DBOptions::new();
        opts.create_if_missing(true);

        let mut cf_opts = ColumnFamilyOptions::new();
        // Prefix extractor(trim the timestamp at tail) for write cf.
        cf_opts
            .set_prefix_extractor(
                "FixedSuffixSliceTransform",
                Box::new(rocksdb_util::FixedSuffixSliceTransform::new(8)),
            )
            .unwrap_or_else(|err| exit_with_err(format!("{:?}", err)));
        // Create prefix bloom filter for memtable.
        cf_opts.set_memtable_prefix_bloom_size_ratio(0.1 as f64);
        let cf = "default";
        let db = DB::open_cf(opts, path_str, vec![(cf, cf_opts)]).unwrap();
        let wb = WriteBatch::new();
        let kvs: Vec<(&[u8], &[u8])> = vec![
            (b"kabcdefg1", b"v1"),
            (b"kabcdefg2", b"v2"),
            (b"kabcdefg3", b"v3"),
            (b"kabcdefg4", b"v4"),
        ];
        let kvs_left: Vec<(&[u8], &[u8])> = vec![(b"kabcdefg1", b"v1"), (b"kabcdefg4", b"v4")];

        for &(k, v) in kvs.as_slice() {
            let handle = get_cf_handle(&db, cf).unwrap();
            wb.put_cf(handle, k, v).unwrap();
        }
        db.write(wb).unwrap();
        check_data(&db, &[cf], kvs.as_slice());

        // Delete all in ["k2", "k4").
        delete_all_in_range(&db, b"kabcdefg2", b"kabcdefg4", true).unwrap();
        check_data(&db, &[cf], kvs_left.as_slice());
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
            let mut r1 = metapb::Region::new();
            for (store_id, peer_id) in s1.into_iter().zip(0..) {
                r1.mut_peers().push(new_peer(store_id, peer_id));
            }
            for (store_id, peer_id) in s2.into_iter().zip(0..) {
                r1.mut_peers().push(new_learner_peer(store_id, peer_id));
            }

            let mut r2 = metapb::Region::new();
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
        let r1 = metapb::Region::new();
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
        let mut req = RaftCmdRequest::new();
        req.mut_header().mut_peer().set_store_id(1);
        check_store_id(&req, 1).unwrap();
        check_store_id(&req, 2).unwrap_err();
    }

    #[test]
    fn test_check_peer_id() {
        let mut req = RaftCmdRequest::new();
        req.mut_header().mut_peer().set_id(1);
        check_peer_id(&req, 1).unwrap();
        check_peer_id(&req, 2).unwrap_err();
    }

    #[test]
    fn test_check_term() {
        let mut req = RaftCmdRequest::new();
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
        let mut epoch = RegionEpoch::new();
        epoch.set_conf_ver(2);
        epoch.set_version(2);
        let mut region = metapb::Region::new();
        region.set_region_epoch(epoch.clone());

        // Epoch is required for most requests even if it's empty.
        check_region_epoch(&RaftCmdRequest::new(), &region, false).unwrap_err();

        // These admin commands do not require epoch.
        for ty in &[
            AdminCmdType::CompactLog,
            AdminCmdType::InvalidAdmin,
            AdminCmdType::ComputeHash,
            AdminCmdType::VerifyHash,
        ] {
            let mut admin = AdminRequest::new();
            admin.set_cmd_type(*ty);
            let mut req = RaftCmdRequest::new();
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
            AdminCmdType::PrepareMerge,
            AdminCmdType::CommitMerge,
            AdminCmdType::RollbackMerge,
            AdminCmdType::TransferLeader,
        ] {
            let mut admin = AdminRequest::new();
            admin.set_cmd_type(*ty);
            let mut req = RaftCmdRequest::new();
            req.set_admin_request(admin);

            // Error if req does not have region epoch.
            check_region_epoch(&req, &region, false).unwrap_err();

            let mut stale_version_epoch = epoch.clone();
            stale_version_epoch.set_version(1);
            let mut stale_region = metapb::Region::new();
            stale_region.set_region_epoch(stale_version_epoch.clone());
            req.mut_header()
                .set_region_epoch(stale_version_epoch.clone());
            check_region_epoch(&req, &stale_region, false).unwrap();
            check_region_epoch(&req, &region, false).unwrap_err();
            check_region_epoch(&req, &region, true).unwrap_err();
        }

        // These admin commands requires epoch.conf_version.
        for ty in &[
            AdminCmdType::ChangePeer,
            AdminCmdType::PrepareMerge,
            AdminCmdType::CommitMerge,
            AdminCmdType::RollbackMerge,
            AdminCmdType::TransferLeader,
        ] {
            let mut admin = AdminRequest::new();
            admin.set_cmd_type(*ty);
            let mut req = RaftCmdRequest::new();
            req.set_admin_request(admin);

            // Error if req does not have region epoch.
            check_region_epoch(&req, &region, false).unwrap_err();

            let mut stale_conf_epoch = epoch.clone();
            stale_conf_epoch.set_conf_ver(1);
            let mut stale_region = metapb::Region::new();
            stale_region.set_region_epoch(stale_conf_epoch.clone());
            req.mut_header().set_region_epoch(stale_conf_epoch.clone());
            check_region_epoch(&req, &stale_region, false).unwrap();
            check_region_epoch(&req, &region, false).unwrap_err();
            check_region_epoch(&req, &region, true).unwrap_err();
        }
    }

    #[test]
    fn test_get_region_approximate_middle_cf() {
        let tmp = TempDir::new("test_raftstore_util").unwrap();
        let path = tmp.path().to_str().unwrap();

        let db_opts = DBOptions::new();
        let mut cf_opts = ColumnFamilyOptions::new();
        cf_opts.set_level_zero_file_num_compaction_trigger(10);
        let f = Box::new(RangePropertiesCollectorFactory::default());
        cf_opts.add_table_properties_collector_factory("tikv.size-collector", f);
        let cfs_opts = LARGE_CFS
            .iter()
            .map(|cf| CFOptions::new(cf, cf_opts.clone()))
            .collect();
        let engine = rocksdb_util::new_engine_opt(path, db_opts, cfs_opts).unwrap();

        let cf_handle = engine.cf_handle(CF_DEFAULT).unwrap();
        let mut big_value = Vec::with_capacity(256);
        big_value.extend(iter::repeat(b'v').take(256));
        for i in 0..100 {
            let k = format!("key_{:03}", i).into_bytes();
            let k = keys::data_key(Key::from_raw(&k).as_encoded());
            engine.put_cf(cf_handle, &k, &big_value).unwrap();
            // Flush for every key so that we can know the exact middle key.
            engine.flush_cf(cf_handle, true).unwrap();
        }

        let region = make_region(1, vec![], vec![]);
        let middle_key = get_region_approximate_middle_cf(&engine, CF_DEFAULT, &region)
            .unwrap()
            .unwrap();

        let middle_key = Key::from_encoded_slice(keys::origin_key(&middle_key))
            .into_raw()
            .unwrap();
        assert_eq!(escape(&middle_key), "key_049");
    }

    #[test]
    fn test_get_region_approximate_split_keys_error() {
        let tmp = TempDir::new("test_raftstore_util").unwrap();
        let path = tmp.path().to_str().unwrap();

        let db_opts = DBOptions::new();
        let mut cf_opts = ColumnFamilyOptions::new();
        cf_opts.set_level_zero_file_num_compaction_trigger(10);

        let cfs_opts = LARGE_CFS
            .iter()
            .map(|cf| CFOptions::new(cf, cf_opts.clone()))
            .collect();
        let engine = rocksdb_util::new_engine_opt(path, db_opts, cfs_opts).unwrap();

        let region = make_region(1, vec![], vec![]);
        assert_eq!(
            get_region_approximate_split_keys(&engine, &region, 3, 5, 1).is_err(),
            true
        );

        let cf_handle = engine.cf_handle(CF_DEFAULT).unwrap();
        let mut big_value = Vec::with_capacity(256);
        big_value.extend(iter::repeat(b'v').take(256));
        for i in 0..100 {
            let k = format!("key_{:03}", i).into_bytes();
            let k = keys::data_key(Key::from_raw(&k).as_encoded());
            engine.put_cf(cf_handle, &k, &big_value).unwrap();
            engine.flush_cf(cf_handle, true).unwrap();
        }
        assert_eq!(
            get_region_approximate_split_keys(&engine, &region, 3, 5, 1).is_err(),
            true
        );
    }

    #[test]
    fn test_get_region_approximate_split_keys() {
        let tmp = TempDir::new("test_raftstore_util").unwrap();
        let path = tmp.path().to_str().unwrap();

        let db_opts = DBOptions::new();
        let mut cf_opts = ColumnFamilyOptions::new();
        cf_opts.set_level_zero_file_num_compaction_trigger(10);
        let f = Box::new(RangePropertiesCollectorFactory::default());
        cf_opts.add_table_properties_collector_factory("tikv.size-collector", f);
        let cfs_opts = LARGE_CFS
            .iter()
            .map(|cf| CFOptions::new(cf, cf_opts.clone()))
            .collect();
        let engine = rocksdb_util::new_engine_opt(path, db_opts, cfs_opts).unwrap();

        let cf_handle = engine.cf_handle(CF_DEFAULT).unwrap();
        let mut big_value = Vec::with_capacity(256);
        big_value.extend(iter::repeat(b'v').take(256));

        // total size for one key and value
        const ENTRY_SIZE: u64 = 256 + 9;

        for i in 0..4 {
            let k = format!("key_{:03}", i).into_bytes();
            let k = keys::data_key(Key::from_raw(&k).as_encoded());
            engine.put_cf(cf_handle, &k, &big_value).unwrap();
            // Flush for every key so that we can know the exact middle key.
            engine.flush_cf(cf_handle, true).unwrap();
        }
        let region = make_region(1, vec![], vec![]);
        let split_keys =
            get_region_approximate_split_keys(&engine, &region, 3 * ENTRY_SIZE, 5 * ENTRY_SIZE, 1)
                .unwrap()
                .into_iter()
                .map(|k| {
                    Key::from_encoded_slice(keys::origin_key(&k))
                        .into_raw()
                        .unwrap()
                })
                .collect::<Vec<Vec<u8>>>();

        assert_eq!(split_keys.is_empty(), true);

        for i in 4..5 {
            let k = format!("key_{:03}", i).into_bytes();
            let k = keys::data_key(Key::from_raw(&k).as_encoded());
            engine.put_cf(cf_handle, &k, &big_value).unwrap();
            // Flush for every key so that we can know the exact middle key.
            engine.flush_cf(cf_handle, true).unwrap();
        }
        let split_keys =
            get_region_approximate_split_keys(&engine, &region, 3 * ENTRY_SIZE, 5 * ENTRY_SIZE, 5)
                .unwrap()
                .into_iter()
                .map(|k| {
                    Key::from_encoded_slice(keys::origin_key(&k))
                        .into_raw()
                        .unwrap()
                })
                .collect::<Vec<Vec<u8>>>();

        assert_eq!(split_keys, vec![b"key_002".to_vec()]);

        for i in 5..10 {
            let k = format!("key_{:03}", i).into_bytes();
            let k = keys::data_key(Key::from_raw(&k).as_encoded());
            engine.put_cf(cf_handle, &k, &big_value).unwrap();
            // Flush for every key so that we can know the exact middle key.
            engine.flush_cf(cf_handle, true).unwrap();
        }
        let split_keys =
            get_region_approximate_split_keys(&engine, &region, 3 * ENTRY_SIZE, 5 * ENTRY_SIZE, 5)
                .unwrap()
                .into_iter()
                .map(|k| {
                    Key::from_encoded_slice(keys::origin_key(&k))
                        .into_raw()
                        .unwrap()
                })
                .collect::<Vec<Vec<u8>>>();

        assert_eq!(split_keys, vec![b"key_002".to_vec(), b"key_005".to_vec()]);

        for i in 10..20 {
            let k = format!("key_{:03}", i).into_bytes();
            let k = keys::data_key(Key::from_raw(&k).as_encoded());
            engine.put_cf(cf_handle, &k, &big_value).unwrap();
            // Flush for every key so that we can know the exact middle key.
            engine.flush_cf(cf_handle, true).unwrap();
        }
        let split_keys =
            get_region_approximate_split_keys(&engine, &region, 3 * ENTRY_SIZE, 5 * ENTRY_SIZE, 5)
                .unwrap()
                .into_iter()
                .map(|k| {
                    Key::from_encoded_slice(keys::origin_key(&k))
                        .into_raw()
                        .unwrap()
                })
                .collect::<Vec<Vec<u8>>>();

        assert_eq!(
            split_keys,
            vec![
                b"key_002".to_vec(),
                b"key_005".to_vec(),
                b"key_008".to_vec(),
                b"key_011".to_vec(),
                b"key_014".to_vec(),
            ]
        );
    }
}
