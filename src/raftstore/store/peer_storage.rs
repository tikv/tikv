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

use std::sync::{self, Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::fs::File;
use std::{error, mem};
use std::time::Instant;

use rocksdb::{DB, WriteBatch, Writable};
use protobuf::Message;

use kvproto::metapb;
use kvproto::raftpb::{Entry, Snapshot, ConfState};
use kvproto::raft_serverpb::{RaftSnapshotData, RaftLocalState, RegionLocalState};
use util::HandyRwLock;
use util::codec::bytes::{BytesEncoder, CompactBytesDecoder};
use raft::{self, Storage, RaftState, StorageError, Error as RaftError, Ready};
use raftstore::{Result, Error};
use super::keys::{self, enc_start_key, enc_end_key};
use super::engine::{Snapshot as DbSnapshot, Peekable, Iterable, Mutable};
use super::{SnapFile, SnapKey, SnapEntry, SnapManager};

// When we create a region peer, we should initialize its log term/index > 0,
// so that we can force the follower peer to sync the snapshot first.
pub const RAFT_INIT_LOG_TERM: u64 = 5;
pub const RAFT_INIT_LOG_INDEX: u64 = 5;
const MAX_SNAP_TRY_CNT: u8 = 5;

pub type Ranges = Vec<(Vec<u8>, Vec<u8>)>;

#[derive(PartialEq, Debug)]
pub enum SnapState {
    Relax,
    Pending,
    Generating,
    Snap(Snapshot),
    Failed,
}

pub struct PeerStorage {
    engine: Arc<DB>,

    pub region: metapb::Region,
    pub local_state: RaftLocalState,

    pub snap_state: SnapState,
    snap_mgr: SnapManager,
    snap_tried_cnt: u8,
}

fn storage_error<E>(error: E) -> raft::Error
    where E: Into<Box<error::Error + Send + Sync>>
{
    raft::Error::Store(StorageError::Other(error.into()))
}

impl From<Error> for RaftError {
    fn from(err: Error) -> RaftError {
        storage_error(err)
    }
}

impl<T> From<sync::PoisonError<T>> for RaftError {
    fn from(_: sync::PoisonError<T>) -> RaftError {
        storage_error("lock failed")
    }
}

pub struct ApplySnapResult {
    // prev_region is the region before snapshot applied.
    pub prev_region: metapb::Region,
    pub region: metapb::Region,
}

pub struct InvokeContext {
    pub local_state: RaftLocalState,
    pub wb: WriteBatch,
}

impl InvokeContext {
    pub fn new(store: &PeerStorage) -> InvokeContext {
        InvokeContext {
            local_state: store.local_state.clone(),
            wb: WriteBatch::new(),
        }
    }

    pub fn save(&self, region_id: u64) -> Result<()> {
        try!(self.wb.put_msg(&keys::raft_state_key(region_id), &self.local_state));
        Ok(())
    }
}

impl PeerStorage {
    pub fn new(engine: Arc<DB>,
               region: &metapb::Region,
               snap_mgr: SnapManager)
               -> Result<PeerStorage> {
        let state = match try!(engine.get_msg(&keys::raft_state_key(region.get_id()))) {
            Some(s) => s,
            None => {
                let mut s = RaftLocalState::new();
                if !region.get_peers().is_empty() {
                    s.set_applied_index(RAFT_INIT_LOG_INDEX);
                    s.set_last_index(RAFT_INIT_LOG_INDEX);
                    let state = s.mut_truncated_state();
                    state.set_index(RAFT_INIT_LOG_INDEX);
                    state.set_term(RAFT_INIT_LOG_TERM);
                }
                s
            }
        };

        Ok(PeerStorage {
            engine: engine,
            region: region.clone(),
            local_state: state,
            snap_state: SnapState::Relax,
            snap_tried_cnt: 0,
            snap_mgr: snap_mgr,
        })
    }

    pub fn is_initialized(&self) -> bool {
        !self.region.get_peers().is_empty()
    }

    pub fn initial_state(&mut self) -> raft::Result<RaftState> {
        let initialized = self.is_initialized();

        let found = self.local_state.has_hard_state();
        let mut hard_state = self.local_state.get_hard_state().clone();

        if !found {
            if initialized {
                hard_state.set_term(RAFT_INIT_LOG_TERM);
                hard_state.set_commit(RAFT_INIT_LOG_INDEX);
                self.local_state.set_last_index(RAFT_INIT_LOG_INDEX);
            } else {
                // This is a new region created from another node.
                // Initialize to 0 so that we can receive a snapshot.
                self.local_state.set_last_index(0);
            }
        } else if initialized && hard_state.get_commit() == 0 {
            // How can we enter this condition? Log first and try to find later.
            warn!("peer initialized but hard state commit is 0");
            hard_state.set_commit(RAFT_INIT_LOG_INDEX);
        }

        let mut conf_state = ConfState::new();
        if found || initialized {
            for p in self.region.get_peers() {
                conf_state.mut_nodes().push(p.get_id());
            }
        }

        Ok(RaftState {
            hard_state: hard_state,
            conf_state: conf_state,
        })
    }

    fn check_range(&self, low: u64, high: u64) -> raft::Result<()> {
        if low > high {
            return Err(storage_error(format!("low: {} is greater that high: {}", low, high)));
        } else if low <= self.truncated_index() {
            return Err(RaftError::Store(StorageError::Compacted));
        } else if high > self.last_index() + 1 {
            return Err(storage_error(format!("entries' high {} is out of bound lastindex {}",
                                             high,
                                             self.last_index())));
        }
        Ok(())
    }

    pub fn entries(&self, low: u64, high: u64, max_size: u64) -> raft::Result<Vec<Entry>> {
        try!(self.check_range(low, high));
        let mut ents = vec![];
        let mut total_size: u64 = 0;
        let mut next_index = low;
        let mut exceeded_max_size = false;

        let start_key = keys::raft_log_key(self.get_region_id(), low);
        let end_key = keys::raft_log_key(self.get_region_id(), high);

        try!(self.engine.scan(&start_key,
                              &end_key,
                              &mut |_, value| {
            let mut entry = Entry::new();
            try!(entry.merge_from_bytes(value));

            // May meet gap or has been compacted.
            if entry.get_index() != next_index {
                return Ok(false);
            }

            next_index += 1;

            total_size += entry.compute_size() as u64;
            exceeded_max_size = total_size > max_size;

            if !exceeded_max_size || ents.is_empty() {
                ents.push(entry);
            }

            Ok(!exceeded_max_size)
        }));

        // If we get the correct number of entries the total size exceeds max_size, returns.
        if ents.len() == (high - low) as usize || exceeded_max_size {
            return Ok(ents);
        }

        // Here means we don't fetch enough entries.
        Err(RaftError::Store(StorageError::Unavailable))
    }

    pub fn term(&self, idx: u64) -> raft::Result<u64> {
        if idx == self.truncated_index() {
            return Ok(self.truncated_term());
        }
        try!(self.check_range(idx, idx + 1));
        let key = keys::raft_log_key(self.get_region_id(), idx);
        match try!(self.engine.get_msg::<Entry>(&key)) {
            Some(entry) => Ok(entry.get_term()),
            None => Err(RaftError::Store(StorageError::Unavailable)),
        }
    }

    #[inline]
    pub fn first_index(&self) -> u64 {
        self.local_state.get_truncated_state().get_index() + 1
    }

    #[inline]
    pub fn last_index(&self) -> u64 {
        self.local_state.get_last_index()
    }

    #[inline]
    pub fn applied_index(&self) -> u64 {
        self.local_state.get_applied_index()
    }

    #[inline]
    pub fn truncated_index(&self) -> u64 {
        self.local_state.get_truncated_state().get_index()
    }

    #[inline]
    pub fn truncated_term(&self) -> u64 {
        self.local_state.get_truncated_state().get_term()
    }

    pub fn get_region(&self) -> &metapb::Region {
        &self.region
    }

    pub fn raw_snapshot(&self) -> DbSnapshot {
        DbSnapshot::new(self.engine.clone())
    }

    pub fn snapshot(&mut self) -> raft::Result<Snapshot> {
        if let SnapState::Relax = self.snap_state {
            info!("requesting snapshot on {}...", self.get_region_id());
            self.snap_tried_cnt = 0;
            self.snap_state = SnapState::Pending;
        } else if let SnapState::Snap(_) = self.snap_state {
            match mem::replace(&mut self.snap_state, SnapState::Relax) {
                SnapState::Snap(s) => return Ok(s),
                _ => unreachable!(),
            }
        } else if let SnapState::Failed = self.snap_state {
            if self.snap_tried_cnt >= MAX_SNAP_TRY_CNT {
                return Err(raft::Error::Store(box_err!("failed to get snapshot after {} times",
                                                       self.snap_tried_cnt)));
            }
            self.snap_tried_cnt += 1;
            warn!("snapshot generating failed, retry {} time",
                  self.snap_tried_cnt);
            self.snap_state = SnapState::Pending;
        }
        Err(raft::Error::Store(raft::StorageError::SnapshotTemporarilyUnavailable))
    }

    // Append the given entries to the raft log using previous last index or self.last_index.
    // Return the new last index for later update. After we commit in engine, we can set last_index
    // to the return one.
    pub fn append(&self, ctx: &mut InvokeContext, entries: &[Entry]) -> Result<u64> {
        debug!("append {} entries for region {}",
               entries.len(),
               self.get_region_id());
        let prev_last_index = ctx.local_state.get_last_index();
        if entries.len() == 0 {
            return Ok(prev_last_index);
        }

        for entry in entries {
            try!(ctx.wb.put_msg(&keys::raft_log_key(self.get_region_id(), entry.get_index()),
                                entry));
        }

        let last_index = entries[entries.len() - 1].get_index();

        // Delete any previously appended log entries which never committed.
        for i in (last_index + 1)..(prev_last_index + 1) {
            try!(ctx.wb.delete(&keys::raft_log_key(self.get_region_id(), i)));
        }

        ctx.local_state.set_last_index(last_index);

        Ok(last_index)
    }

    // Apply the peer with given snapshot.
    pub fn apply_snapshot(&self,
                          ctx: &mut InvokeContext,
                          snap: &Snapshot)
                          -> Result<ApplySnapResult> {
        info!("begin to apply snapshot for region {}",
              self.get_region_id());

        let key = try!(SnapKey::from_snap(snap));
        let snap_file = try!(self.snap_mgr.rl().get_snap_file(&key, false));
        self.snap_mgr.wl().register(key.clone(), SnapEntry::Applying);
        defer!({
            self.snap_mgr.wl().deregister(&key, &SnapEntry::Applying);
            snap_file.delete();
        });
        if !snap_file.exists() {
            return Err(box_err!("missing snap file {}", snap_file.path().display()));
        }
        try!(snap_file.validate());
        let mut reader = try!(File::open(snap_file.path()));

        let mut snap_data = RaftSnapshotData::new();
        try!(snap_data.merge_from_bytes(snap.get_data()));

        let region_id = self.get_region_id();

        let region = snap_data.get_region();
        if region.get_id() != region_id {
            return Err(box_err!("mismatch region id {} != {}", region_id, region.get_id()));
        }

        if self.is_initialized() {
            // we can only delete the old data when the peer is initialized.
            let timer = Instant::now();
            // Delete everything in the region for this peer.
            try!(self.scan_region(self.engine.as_ref(),
                                  &mut |key, _| {
                                      try!(ctx.wb.delete(key));
                                      Ok(true)
                                  }));
            info!("clean old region takes {:?}", timer.elapsed());
        }

        let timer = Instant::now();
        // Write the snapshot into the region.
        loop {
            // TODO: avoid too many allocation
            let key = try!(reader.decode_compact_bytes());
            if key.is_empty() {
                break;
            }
            let value = try!(reader.decode_compact_bytes());
            try!(ctx.wb.put(&key, &value));
        }
        info!("apply new data takes {:?}", timer.elapsed());

        let mut region_state = RegionLocalState::new();
        // TODO: state.set_state(PeerState::Apply);
        region_state.set_region(region.clone());
        try!(ctx.wb.put_msg(&keys::region_state_key(region_id), &region_state));

        let last_index = snap.get_metadata().get_index();

        ctx.local_state.set_last_index(last_index);
        ctx.local_state.set_applied_index(last_index);

        // The snapshot only contains log which index > applied index, so
        // here the truncate state's (index, term) is in snapshot metadata.
        ctx.local_state.mut_truncated_state().set_index(last_index);
        ctx.local_state.mut_truncated_state().set_term(snap.get_metadata().get_term());

        info!("apply snapshot ok for region {}", self.get_region_id());

        Ok(ApplySnapResult {
            prev_region: self.region.clone(),
            region: region.clone(),
        })
    }

    // Discard all log entries prior to compact_index. We must guarantee
    // that the compact_index is not greater than applied index.
    pub fn compact(&self, ctx: &mut InvokeContext, compact_index: u64) -> Result<()> {
        debug!("compact log entries to prior to {} for region {}",
               compact_index,
               self.get_region_id());

        if compact_index <= self.truncated_index() {
            return Err(box_err!("try to truncate compacted entries"));
        } else if compact_index > self.applied_index() {
            return Err(box_err!("compact index {} > applied index {}",
                                compact_index,
                                self.applied_index()));
        }

        let term = try!(self.term(compact_index - 1));
        // we don't actually compact the log now, we add an async task to do it.

        ctx.local_state.mut_truncated_state().set_index(compact_index - 1);
        ctx.local_state.mut_truncated_state().set_term(term);

        Ok(())
    }

    pub fn get_engine(&self) -> Arc<DB> {
        self.engine.clone()
    }

    // For region snapshot, we return three key ranges in database for this region.
    // [region raft start, region raft end) -> saving raft entries, applied index, etc.
    // [region meta start, region meta end) -> saving region meta information except raft.
    // [region data start, region data end) -> saving region data.
    pub fn region_key_ranges(&self) -> Ranges {
        let (start_key, end_key) = (enc_start_key(self.get_region()),
                                    enc_end_key(self.get_region()));

        let region_id = self.get_region_id();
        vec![(keys::region_raft_prefix(region_id), keys::region_raft_prefix(region_id + 1)),
             (keys::region_meta_prefix(region_id), keys::region_meta_prefix(region_id + 1)),
             (start_key, end_key)]

    }

    /// scan all region related kv
    ///
    /// Note: all keys will be iterated with prefix untouched.
    pub fn scan_region<T, F>(&self, db: &T, f: &mut F) -> Result<()>
        where T: Iterable,
              F: FnMut(&[u8], &[u8]) -> Result<bool>
    {
        let ranges = self.region_key_ranges();
        for r in ranges {
            try!(db.scan(&r.0, &r.1, f));
        }

        Ok(())
    }

    pub fn get_region_id(&self) -> u64 {
        self.region.get_id()
    }

    pub fn handle_raft_ready(&mut self, ready: &Ready) -> Result<Option<ApplySnapResult>> {
        let mut ctx = InvokeContext::new(self);
        let mut apply_snap_res = None;
        let region_id = self.get_region_id();
        if !raft::is_empty_snap(&ready.snapshot) {
            let res = try!(self.apply_snapshot(&mut ctx, &ready.snapshot));
            apply_snap_res = Some(res);
        }
        if !ready.entries.is_empty() {
            try!(self.append(&mut ctx, &ready.entries));
        }

        // Last index is 0 means the peer is created from raft message
        // and has not applied snapshot yet, so skip persistent hard state.
        if ctx.local_state.get_last_index() > 0 {
            if let Some(ref hs) = ready.hs {
                ctx.local_state.set_hard_state(hs.clone());
            }
        }

        if ctx.local_state != self.local_state {
            try!(ctx.save(region_id));
        }

        if !ctx.wb.is_empty() {
            try!(self.engine.write(ctx.wb));
        }

        self.local_state = ctx.local_state;
        // If we apply snapshot ok, we should update some infos like applied index too.
        if let Some(res) = apply_snap_res {
            self.region = res.region.clone();
            return Ok(Some(res));
        }

        Ok(None)
    }
}

fn build_snap_file(f: &mut SnapFile,
                   snap: &DbSnapshot,
                   region: &metapb::Region)
                   -> raft::Result<()> {
    let mut snap_size = 0;
    let mut snap_key_cnt = 0;
    let (begin_key, end_key) = (enc_start_key(region), enc_end_key(region));
    try!(snap.scan(&begin_key,
                   &end_key,
                   &mut |key, value| {
        snap_size += key.len();
        snap_size += value.len();
        snap_key_cnt += 1;

        try!(f.encode_compact_bytes(key));
        try!(f.encode_compact_bytes(value));
        Ok(true)
    }));
    // use an empty byte array to indicate that kvpair reaches an end.
    box_try!(f.encode_compact_bytes(b""));
    try!(f.save());

    info!("scan snapshot for region {}, size {}, key count {}",
          region.get_id(),
          snap_size,
          snap_key_cnt);
    Ok(())
}

pub fn do_snapshot(mgr: SnapManager, snap: &DbSnapshot, key: SnapKey) -> raft::Result<Snapshot> {
    debug!("begin to generate a snapshot for region {}", key.region_id);
    mgr.wl().register(key.clone(), SnapEntry::Generating);
    defer!(mgr.wl().deregister(&key, &SnapEntry::Generating));

    let mut state: RegionLocalState = try!(snap.get_msg(&keys::region_state_key(key.region_id))
        .and_then(|res| {
            match res {
                None => Err(box_err!("could not find region info")),
                Some(state) => Ok(state),
            }
        }));

    let mut snapshot = Snapshot::new();

    // Set snapshot metadata.
    snapshot.mut_metadata().set_index(key.idx);
    snapshot.mut_metadata().set_term(key.term);

    let mut conf_state = ConfState::new();
    for p in state.get_region().get_peers() {
        conf_state.mut_nodes().push(p.get_id());
    }

    snapshot.mut_metadata().set_conf_state(conf_state);

    let mut snap_file = try!(mgr.rl().get_snap_file(&key, true));
    if snap_file.exists() {
        if let Err(e) = snap_file.validate() {
            error!("file {} is invalid, will regenerate: {:?}",
                   snap_file.path().display(),
                   e);
            try!(snap_file.try_delete());
            try!(snap_file.init());
            try!(build_snap_file(&mut snap_file, snap, state.get_region()));
        }
    } else {
        try!(build_snap_file(&mut snap_file, snap, state.get_region()));
    }

    // Set snapshot data.
    let mut snap_data = RaftSnapshotData::new();
    snap_data.set_region(state.take_region());

    let len = try!(snap_file.meta()).len();
    snap_data.set_file_size(len);

    let mut v = vec![];
    box_try!(snap_data.write_to_vec(&mut v));
    snapshot.set_data(v);

    Ok(snapshot)
}

#[derive(Clone)]
pub struct RaftStorage {
    store: Arc<RwLock<PeerStorage>>,
}

impl RaftStorage {
    pub fn new(store: PeerStorage) -> RaftStorage {
        RaftStorage { store: Arc::new(RwLock::new(store)) }
    }

    #[inline]
    pub fn rl(&self) -> RwLockReadGuard<PeerStorage> {
        self.store.rl()
    }

    #[inline]
    pub fn wl(&self) -> RwLockWriteGuard<PeerStorage> {
        self.store.wl()
    }
}

impl Storage for RaftStorage {
    fn initial_state(&self) -> raft::Result<RaftState> {
        self.wl().initial_state()
    }

    fn entries(&self, low: u64, high: u64, max_size: u64) -> raft::Result<Vec<Entry>> {
        self.rl().entries(low, high, max_size)
    }

    fn term(&self, idx: u64) -> raft::Result<u64> {
        self.rl().term(idx)
    }

    fn first_index(&self) -> raft::Result<u64> {
        Ok(self.rl().first_index())
    }

    fn last_index(&self) -> raft::Result<u64> {
        Ok(self.rl().last_index())
    }

    fn snapshot(&self) -> raft::Result<Snapshot> {
        self.wl().snapshot()
    }
}

#[cfg(test)]
mod test {
    use std::sync::*;
    use std::io;
    use std::fs::File;
    use rocksdb::*;
    use kvproto::raftpb::{Entry, ConfState, Snapshot};
    use kvproto::raft_serverpb::RaftSnapshotData;
    use raft::{StorageError, Error as RaftError};
    use tempdir::*;
    use protobuf;
    use raftstore::store::*;
    use util::codec::number::NumberEncoder;
    use util::HandyRwLock;

    use super::InvokeContext;

    fn new_storage(mgr: SnapManager, path: &TempDir) -> RaftStorage {
        let db = DB::open_default(path.path().to_str().unwrap()).unwrap();
        let db = Arc::new(db);
        bootstrap::bootstrap_store(&db, 1, 1).expect("");
        let region = bootstrap::bootstrap_region(&db, 1, 1, 1).expect("");
        RaftStorage::new(PeerStorage::new(db, &region, mgr).unwrap())
    }

    fn new_storage_from_ents(mgr: SnapManager, path: &TempDir, ents: &[Entry]) -> RaftStorage {
        let store = new_storage(mgr, path);
        let mut ctx = InvokeContext::new(&store.rl());
        store.rl().append(&mut ctx, &ents[1..]).expect("");
        store.rl().engine.write(ctx.wb).expect("");
        ctx.local_state.mut_truncated_state().set_index(ents[0].get_index());
        ctx.local_state.mut_truncated_state().set_term(ents[0].get_term());
        ctx.local_state.set_applied_index(ents.last().unwrap().get_index());
        store.wl().local_state = ctx.local_state;
        store
    }

    fn new_entry(index: u64, term: u64) -> Entry {
        let mut e = Entry::new();
        e.set_index(index);
        e.set_term(term);
        e
    }

    fn size_of<T: protobuf::Message>(m: &T) -> u32 {
        m.compute_size()
    }

    #[test]
    fn test_storage_term() {
        let ents = vec![
            new_entry(3, 3),
            new_entry(4, 4),
            new_entry(5, 5),
        ];

        let mut tests = vec![
            (2, Err(RaftError::Store(StorageError::Compacted))),
            (3, Ok(3)),
            (4, Ok(4)),
            (5, Ok(5)),
        ];
        for (i, (idx, wterm)) in tests.drain(..).enumerate() {
            let td = TempDir::new("tikv-store-test").unwrap();
            let mgr = new_snap_mgr("");
            let store = new_storage_from_ents(mgr, &td, &ents);
            let t = store.rl().term(idx);
            if wterm != t {
                panic!("#{}: expect res {:?}, got {:?}", i, wterm, t);
            }
        }
    }

    #[test]
    fn test_storage_entries() {
        let ents = vec![new_entry(3, 3), new_entry(4, 4), new_entry(5, 5), new_entry(6, 6)];
        let max_u64 = u64::max_value();
        let mut tests = vec![
            (2, 6, max_u64, Err(RaftError::Store(StorageError::Compacted))),
            (3, 4, max_u64, Err(RaftError::Store(StorageError::Compacted))),
            (4, 5, max_u64, Ok(vec![new_entry(4, 4)])),
            (4, 6, max_u64, Ok(vec![new_entry(4, 4), new_entry(5, 5)])),
            (4, 7, max_u64, Ok(vec![new_entry(4, 4), new_entry(5, 5), new_entry(6, 6)])),
            // even if maxsize is zero, the first entry should be returned
            (4, 7, 0, Ok(vec![new_entry(4, 4)])),
            // limit to 2
            (4, 7, (size_of(&ents[1]) + size_of(&ents[2])) as u64,
             Ok(vec![new_entry(4, 4), new_entry(5, 5)])),
            (4, 7, (size_of(&ents[1]) + size_of(&ents[2]) + size_of(&ents[3]) / 2) as u64,
             Ok(vec![new_entry(4, 4), new_entry(5, 5)])),
            (4, 7, (size_of(&ents[1]) + size_of(&ents[2]) + size_of(&ents[3]) - 1) as u64,
             Ok(vec![new_entry(4, 4), new_entry(5, 5)])),
            // all
            (4, 7, (size_of(&ents[1]) + size_of(&ents[2]) + size_of(&ents[3])) as u64,
             Ok(vec![new_entry(4, 4), new_entry(5, 5), new_entry(6, 6)])),
        ];

        for (i, (lo, hi, maxsize, wentries)) in tests.drain(..).enumerate() {
            let td = TempDir::new("tikv-store-test").unwrap();
            let mgr = new_snap_mgr("");
            let store = new_storage_from_ents(mgr, &td, &ents);
            let e = store.rl().entries(lo, hi, maxsize);
            if e != wentries {
                panic!("#{}: expect entries {:?}, got {:?}", i, wentries, e);
            }
        }
    }

    // last_index and first_index are not mutated by PeerStorage on its own,
    // so we don't test them here.

    #[test]
    fn test_storage_compact() {
        let ents = vec![new_entry(3, 3), new_entry(4, 4), new_entry(5, 5)];
        let mut tests = vec![
            (2, Err(RaftError::Store(StorageError::Compacted))),
            (3, Err(RaftError::Store(StorageError::Compacted))),
            (4, Ok(())),
            (5, Ok(())),
        ];
        for (i, (idx, werr)) in tests.drain(..).enumerate() {
            let td = TempDir::new("tikv-store-test").unwrap();
            let mgr = new_snap_mgr("");
            let store = new_storage_from_ents(mgr, &td, &ents);
            let mut ctx = InvokeContext::new(&store.rl());
            let res = store.rl().compact(&mut ctx, idx);
            // TODO check exact error type after refactoring error.
            if res.is_err() ^ werr.is_err() {
                panic!("#{}: want {:?}, got {:?}", i, werr, res);
            }
            if res.is_ok() {
                store.rl().engine.write(ctx.wb).expect("");
            }
        }
    }

    fn get_snap(s: &RaftStorage, mgr: SnapManager) -> Snapshot {
        let store = s.rl();
        let raw_snap = store.raw_snapshot();
        let applied_id = store.applied_index();
        let term = store.term(applied_id).unwrap();
        let key = SnapKey::new(store.get_region_id(), term, applied_id);
        do_snapshot(mgr, &raw_snap, key).unwrap()
    }

    #[test]
    fn test_storage_create_snapshot() {
        let ents = vec![new_entry(3, 3), new_entry(4, 4), new_entry(5, 5)];
        let mut cs = ConfState::new();
        cs.set_nodes(vec![1, 2, 3]);

        let td = TempDir::new("tikv-store-test").unwrap();
        let snap_dir = TempDir::new("snap_dir").unwrap();
        let mgr = new_snap_mgr(snap_dir.path().to_str().unwrap());
        let s = new_storage_from_ents(mgr, &td, &ents);
        let snap = s.wl().snapshot();
        let unavailable = RaftError::Store(StorageError::SnapshotTemporarilyUnavailable);
        assert_eq!(snap.unwrap_err(), unavailable);
        assert_eq!(s.wl().snap_state, SnapState::Pending);

        s.wl().snap_state = SnapState::Generating;
        assert_eq!(s.wl().snapshot().unwrap_err(), unavailable);
        assert_eq!(s.rl().snap_state, SnapState::Generating);

        let snap_dir = TempDir::new("snap").unwrap();
        let snap = get_snap(&s, new_snap_mgr(snap_dir.path().to_str().unwrap()));
        assert_eq!(snap.get_metadata().get_index(), 5);
        assert_eq!(snap.get_metadata().get_term(), 5);
        assert!(!snap.get_data().is_empty());

        let mut data = RaftSnapshotData::new();
        protobuf::Message::merge_from_bytes(&mut data, snap.get_data()).expect("");
        assert_eq!(data.get_region().get_id(), 1);
        assert_eq!(data.get_region().get_peers().len(), 1);

        s.wl().snap_state = SnapState::Snap(snap.clone());
        assert_eq!(s.wl().snapshot(), Ok(snap));
    }

    #[test]
    fn test_storage_append() {
        let ents = vec![new_entry(3, 3), new_entry(4, 4), new_entry(5, 5)];
        let mut tests = vec![
            (
                vec![new_entry(3, 3), new_entry(4, 4), new_entry(5, 5)],
                vec![new_entry(4, 4), new_entry(5, 5)],
            ),
            (
                vec![new_entry(3, 3), new_entry(4, 6), new_entry(5, 6)],
                vec![new_entry(4, 6), new_entry(5, 6)],
            ),
            (
                vec![new_entry(3, 3), new_entry(4, 4), new_entry(5, 5), new_entry(6, 5)],
                vec![new_entry(4, 4), new_entry(5, 5), new_entry(6, 5)],
            ),
            // truncate incoming entries, truncate the existing entries and append
            (
                vec![new_entry(2, 3), new_entry(3, 3), new_entry(4, 5)],
                vec![new_entry(4, 5)],
            ),
            // truncate the existing entries and append
            (
                vec![new_entry(4, 5)],
                vec![new_entry(4, 5)],
            ),
            // direct append
            (
                vec![new_entry(6, 5)],
                vec![new_entry(4, 4), new_entry(5, 5), new_entry(6, 5)],
            ),
        ];
        for (i, (entries, wentries)) in tests.drain(..).enumerate() {
            let td = TempDir::new("tikv-store-test").unwrap();
            let snap_dir = TempDir::new("snap_dir").unwrap();
            let mgr = new_snap_mgr(snap_dir.path().to_str().unwrap());
            let store = new_storage_from_ents(mgr, &td, &ents);
            let mut ctx = InvokeContext::new(&store.rl());
            store.wl().append(&mut ctx, &entries).expect("");
            store.wl().engine.write(ctx.wb).expect("");
            store.wl().local_state = ctx.local_state;
            let li = store.wl().last_index();
            let actual_entries = store.rl().entries(4, li + 1, u64::max_value()).expect("");
            if actual_entries != wentries {
                panic!("#{}: want {:?}, got {:?}", i, wentries, actual_entries);
            }
        }
    }

    #[test]
    fn test_storage_apply_snapshot() {
        let ents = vec![new_entry(3, 3), new_entry(4, 4), new_entry(5, 5)];
        let mut cs = ConfState::new();
        cs.set_nodes(vec![1, 2, 3]);

        let td1 = TempDir::new("tikv-store-test").unwrap();
        let snap_dir = TempDir::new("snap").unwrap();
        let mgr = new_snap_mgr(snap_dir.path().to_str().unwrap());
        let s1 = new_storage_from_ents(mgr.clone(), &td1, &ents);
        let snap1 = get_snap(&s1, mgr.clone());
        assert_eq!(s1.rl().truncated_index(), 3);
        assert_eq!(s1.rl().truncated_term(), 3);

        let key = SnapKey::from_snap(&snap1).unwrap();
        let source_snap = mgr.rl().get_snap_file(&key, true).unwrap();
        let mut dst_snap = mgr.rl().get_snap_file(&key, false).unwrap();
        let mut f = File::open(source_snap.path()).unwrap();
        dst_snap.encode_u64(0).unwrap();
        io::copy(&mut f, &mut dst_snap).unwrap();
        dst_snap.save().unwrap();

        let td2 = TempDir::new("tikv-store-test").unwrap();
        let s2 = new_storage(mgr.clone(), &td2);
        assert_eq!(s2.rl().first_index(), s2.rl().applied_index() + 1);
        let mut ctx = InvokeContext::new(&s2.rl());
        s2.wl().apply_snapshot(&mut ctx, &snap1).unwrap();
        assert_eq!(ctx.local_state.get_applied_index(), 5);
        assert_eq!(ctx.local_state.get_last_index(), 5);
        assert_eq!(ctx.local_state.get_truncated_state().get_index(), 5);
        assert_eq!(ctx.local_state.get_truncated_state().get_term(), 5);
        assert_eq!(s2.rl().first_index(), s2.rl().applied_index() + 1);
    }
}
