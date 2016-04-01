use std::sync::{self, Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::vec::Vec;
use std::error;

use rocksdb::{DB, WriteBatch, Writable};
use rocksdb::rocksdb::Snapshot as RocksDBSnapshot;
use protobuf::{self, Message};

use kvproto::metapb;
use kvproto::raftpb::{Entry, Snapshot, HardState, ConfState};
use kvproto::raft_serverpb::{RaftSnapshotData, KeyValue, RaftTruncatedState};
use util::HandyRwLock;
use raft::{self, Storage, RaftState, StorageError, Error as RaftError, Ready};
use raftstore::{Result, Error};
use super::keys::{self, enc_start_key, enc_end_key};
use super::engine::{Peekable, Iterable, Mutable};

// When we create a region peer, we should initialize its log term/index > 0,
// so that we can force the follower peer to sync the snapshot first.
pub const RAFT_INIT_LOG_TERM: u64 = 5;
pub const RAFT_INIT_LOG_INDEX: u64 = 5;

pub struct PeerStorage {
    engine: Arc<DB>,

    pub region: metapb::Region,
    pub last_index: u64,
    pub applied_index: u64,
    // Truncated state is used for two cases:
    // 1, a truncated state preceded the first log entry.
    // 2, a dummy entry for the start point of the empty log.
    pub truncated_state: RaftTruncatedState,
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
    pub last_index: u64,
    pub applied_index: u64,
    pub region: metapb::Region,
}

impl PeerStorage {
    pub fn new(engine: Arc<DB>, region: &metapb::Region) -> Result<PeerStorage> {
        let mut store = PeerStorage {
            engine: engine,
            region: region.clone(),
            last_index: 0,
            applied_index: 0,
            truncated_state: RaftTruncatedState::new(),
        };

        store.last_index = try!(store.load_last_index());
        store.applied_index = try!(store.load_applied_index(store.engine.as_ref()));
        store.truncated_state = try!(store.load_truncated_state());

        Ok(store)
    }

    pub fn is_initialized(&self) -> bool {
        !self.region.get_peers().is_empty()
    }

    pub fn initial_state(&mut self) -> raft::Result<RaftState> {
        let initialized = self.is_initialized();
        let res: Option<HardState> = try!(self.engine
                                              .get_msg(&keys::raft_hard_state_key(
                                                self.get_region_id())));

        let (mut hard_state, found) = res.map_or((HardState::new(), false), |e| (e, true));

        if !found {
            if initialized {
                hard_state.set_term(RAFT_INIT_LOG_TERM);
                hard_state.set_commit(RAFT_INIT_LOG_INDEX);
                self.last_index = RAFT_INIT_LOG_INDEX;
            } else {
                // This is a new region created from another node.
                // Initialize to 0 so that we can receive a snapshot.
                self.last_index = 0;
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

    pub fn entries(&self, low: u64, high: u64, max_size: u64) -> raft::Result<Vec<Entry>> {
        if low > high {
            return Err(storage_error(format!("low: {} is greater that high: {}", low, high)));
        } else if low <= self.truncated_state.get_index() {
            return Err(RaftError::Store(StorageError::Compacted));
        } else if high > self.last_index + 1 {
            return Err(storage_error(format!("entries' high {} is out of bound lastindex {}",
                                             high,
                                             self.last_index)));
        }

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
        match self.entries(idx, idx + 1, 0) {
            Err(e@RaftError::Store(StorageError::Compacted)) => {
                // Maybe the dummy entry.
                if self.truncated_state.get_index() == idx {
                    return Ok(self.truncated_state.get_term());
                }
                Err(e)

            }
            Err(e) => Err(e),
            Ok(ents) => {
                if ents.is_empty() {
                    // We can't get empty entries,
                    // maybe we have something wrong in entries function.
                    error!("get empty entries");
                    Err(RaftError::Store(StorageError::Unavailable))
                } else {
                    Ok(ents[0].get_term())
                }
            }
        }
    }

    pub fn first_index(&self) -> u64 {
        self.truncated_state.get_index() + 1
    }

    pub fn last_index(&self) -> u64 {
        self.last_index
    }

    pub fn applied_index(&self) -> u64 {
        self.applied_index
    }

    pub fn get_region(&self) -> &metapb::Region {
        &self.region
    }

    pub fn raw_snapshot(&self) -> RocksDBSnapshot {
        self.engine.snapshot()
    }

    pub fn snapshot(&self) -> raft::Result<Snapshot> {
        debug!("begin to generate a snapshot for region {}",
               self.get_region_id());

        // TODO: time snapshot process

        let snap = self.engine.snapshot();
        let applied_index = try!(self.load_applied_index(&snap));
        let region: metapb::Region = try!(snap.get_msg::<metapb::Region>(
                                            &keys::region_info_key(self.get_region_id())).
        and_then(|res| {
            match res {
                None => Err(box_err!("could not find region info")),
                Some(region) => Ok(region),
            }
        }));

        let term = try!(self.term(applied_index));
        let mut snapshot = Snapshot::new();

        // Set snapshot metadata.
        snapshot.mut_metadata().set_index(applied_index);
        snapshot.mut_metadata().set_term(term);

        let mut conf_state = ConfState::new();
        for p in region.get_peers() {
            conf_state.mut_nodes().push(p.get_id());
        }

        snapshot.mut_metadata().set_conf_state(conf_state);

        // Set snapshot data.
        let mut snap_data = RaftSnapshotData::new();
        snap_data.set_region(region);

        // TODO: maybe we don't need to scan log entries.
        let mut data = vec![];
        try!(self.scan_region(&snap,
                              &mut |key, value| {
                                  let mut kv = KeyValue::new();
                                  kv.set_key(key.to_vec());
                                  kv.set_value(value.to_vec());
                                  data.push(kv);
                                  Ok(true)
                              }));

        snap_data.set_data(protobuf::RepeatedField::from_vec(data));

        let mut v = vec![];
        box_try!(snap_data.write_to_vec(&mut v));
        snapshot.set_data(v);

        debug!("generate snapshot ok for region {}", self.get_region_id());

        Ok(snapshot)
    }

    // Append the given entries to the raft log using previous last index or self.last_index.
    // Return the new last index for later update. After we commit in engine, we can set last_index
    // to the return one.
    pub fn append<T: Mutable>(&self,
                              w: &T,
                              prev_last_index: u64,
                              entries: &[Entry])
                              -> Result<u64> {
        debug!("append {} entries for region {}",
               entries.len(),
               self.get_region_id());
        if entries.len() == 0 {
            return Ok(prev_last_index);
        }

        for entry in entries {
            try!(w.put_msg(&keys::raft_log_key(self.get_region_id(), entry.get_index()),
                           entry));
        }

        let last_index = entries[entries.len() - 1].get_index();

        // Delete any previously appended log entries which never committed.
        for i in (last_index + 1)..(prev_last_index + 1) {
            try!(w.delete(&keys::raft_log_key(self.get_region_id(), i)));
        }

        try!(save_last_index(w, self.get_region_id(), last_index));

        Ok(last_index)
    }

    // Apply the peer with given snapshot.
    pub fn apply_snapshot<T: Mutable>(&self, w: &T, snap: &Snapshot) -> Result<ApplySnapResult> {
        debug!("begin to apply snapshot for region {}",
               self.get_region_id());

        let mut snap_data = RaftSnapshotData::new();
        try!(snap_data.merge_from_bytes(snap.get_data()));

        let region_id = self.get_region_id();

        // Apply snapshot should not overwrite current hard state which
        // records the previous vote.
        // TODO: maybe exclude hard state when do snapshot.
        let hard_state_key = keys::raft_hard_state_key(region_id);
        let hard_state: Option<HardState> = try!(self.engine.get_msg(&hard_state_key));

        let region = snap_data.get_region();
        if region.get_id() != region_id {
            return Err(box_err!("mismatch region id {} != {}", region_id, region.get_id()));
        }

        // Delete everything in the region for this peer.
        try!(self.scan_region(self.engine.as_ref(),
                              &mut |key, _| {
                                  try!(w.delete(key));
                                  Ok(true)
                              }));

        // Write the snapshot into the region.
        for kv in snap_data.get_data() {
            try!(w.put(kv.get_key(), kv.get_value()));
        }

        // Restore the hard state
        match hard_state {
            None => try!(w.delete(&hard_state_key)),
            Some(state) => try!(w.put_msg(&hard_state_key, &state)),
        }

        let last_index = snap.get_metadata().get_index();
        try!(save_last_index(w, region_id, last_index));

        debug!("apply snapshot ok for region {}", self.get_region_id());

        Ok(ApplySnapResult {
            last_index: last_index,
            applied_index: last_index,
            region: region.clone(),
        })
    }

    // Discard all log entries prior to compact_index. We must guarantee
    // that the compact_index is not greater than applied index.
    pub fn compact<T: Mutable>(&self, w: &T, compact_index: u64) -> Result<RaftTruncatedState> {
        debug!("compact log entries to prior to {} for region {}",
               compact_index,
               self.get_region_id());

        if compact_index <= self.truncated_state.get_index() {
            return Err(box_err!("try to truncate compacted entries"));
        } else if compact_index > self.applied_index {
            return Err(box_err!("compact index {} > applied index {}",
                                compact_index,
                                self.applied_index));
        }

        let term = try!(self.term(compact_index - 1));
        let start_key = keys::raft_log_key(self.get_region_id(), 0);
        let end_key = keys::raft_log_key(self.get_region_id(), compact_index);

        try!(self.engine.scan(&start_key,
                              &end_key,
                              &mut |key, _| {
                                  try!(w.delete(key));
                                  Ok(true)
                              }));

        let mut state = RaftTruncatedState::new();
        state.set_index(compact_index - 1);
        state.set_term(term);
        try!(w.put_msg(&keys::raft_truncated_state_key(self.get_region_id()),
                       &state));
        Ok(state)
    }

    // Truncated state contains the meta about log preceded the first current entry.
    pub fn load_truncated_state(&self) -> Result<RaftTruncatedState> {
        let res: Option<RaftTruncatedState> = try!(self.engine.get_msg(
                                         &keys::raft_truncated_state_key(self.get_region_id())));

        if let Some(state) = res {
            return Ok(state);
        }

        let mut state = RaftTruncatedState::new();

        if self.is_initialized() {
            // We created this region, use default.
            state.set_index(RAFT_INIT_LOG_INDEX);
            state.set_term(RAFT_INIT_LOG_TERM);
        } else {
            // This is a new region created from another node.
            // Initialize to 0 so that we can receive a snapshot.
            state.set_index(0);
            state.set_term(0);
        }

        Ok(state)
    }

    pub fn load_last_index(&self) -> Result<u64> {
        let n = try!(self.engine.get_u64(&keys::raft_last_index_key(self.get_region_id())));
        // If log is empty, maybe we starts from scratch or have truncated all logs.
        Ok(n.unwrap_or(self.truncated_state.get_index()))
    }

    pub fn get_engine(&self) -> Arc<DB> {
        self.engine.clone()
    }

    pub fn set_last_index(&mut self, last_index: u64) {
        self.last_index = last_index;
    }

    pub fn set_applied_index(&mut self, applied_index: u64) {
        self.applied_index = applied_index;
    }

    pub fn set_truncated_state(&mut self, state: &RaftTruncatedState) {
        self.truncated_state = state.clone();
    }

    pub fn set_region(&mut self, region: &metapb::Region) {
        self.region = region.clone();
    }

    pub fn load_applied_index<T: Peekable>(&self, db: &T) -> Result<u64> {
        let mut applied_index: u64 = 0;
        if self.is_initialized() {
            applied_index = RAFT_INIT_LOG_INDEX;
        }

        let n = try!(db.get_u64(&keys::raft_applied_index_key(self.get_region_id())));
        Ok(n.unwrap_or(applied_index))
    }

    // For region snapshot, we return three key ranges in database for this region.
    // [region raft start, region raft end) -> saving raft entries, applied index, etc.
    // [region meta start, region meta end) -> saving region meta information except raft.
    // [region data start, region data end) -> saving region data.
    fn region_key_ranges(&self) -> Vec<(Vec<u8>, Vec<u8>)> {
        let (start_key, end_key) = (enc_start_key(self.get_region()),
                                    enc_end_key(self.get_region()));

        let region_id = self.get_region_id();
        vec![(keys::region_raft_prefix(region_id),
              keys::region_raft_prefix(region_id + 1)),
             (keys::region_meta_prefix(region_id),
              keys::region_meta_prefix(region_id + 1)),
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

    pub fn handle_raft_ready(&mut self, ready: &Ready) -> Result<Option<metapb::Region>> {
        let wb = WriteBatch::new();
        let mut last_index = self.last_index();
        let mut apply_snap_res = None;
        let region_id = self.get_region_id();
        if !raft::is_empty_snap(&ready.snapshot) {
            try!(wb.delete(&keys::region_tombstone_key(region_id)));
            apply_snap_res = try!(self.apply_snapshot(&wb, &ready.snapshot).map(|res| {
                last_index = res.last_index;
                Some(res)
            }));
        }
        if !ready.entries.is_empty() {
            last_index = try!(self.append(&wb, last_index, &ready.entries));
        }

        if let Some(ref hs) = ready.hs {
            try!(save_hard_state(&wb, region_id, hs));
        }

        try!(self.engine.write(wb));

        self.set_last_index(last_index);
        // If we apply snapshot ok, we should update some infos like applied index too.
        if let Some(res) = apply_snap_res {
            self.set_applied_index(res.applied_index);
            self.set_region(&res.region);
            return Ok(Some(res.region.clone()));
        }

        Ok(None)
    }
}

pub fn save_hard_state<T: Mutable>(w: &T, region_id: u64, state: &HardState) -> Result<()> {
    w.put_msg(&keys::raft_hard_state_key(region_id), state)
}

pub fn save_truncated_state<T: Mutable>(w: &T,
                                        region_id: u64,
                                        state: &RaftTruncatedState)
                                        -> Result<()> {
    w.put_msg(&keys::raft_truncated_state_key(region_id), state)
}

pub fn save_applied_index<T: Mutable>(w: &T, region_id: u64, applied_index: u64) -> Result<()> {
    w.put_u64(&keys::raft_applied_index_key(region_id), applied_index)
}

pub fn save_last_index<T: Mutable>(w: &T, region_id: u64, last_index: u64) -> Result<()> {
    w.put_u64(&keys::raft_last_index_key(region_id), last_index)
}

pub struct RaftStorage {
    store: RwLock<PeerStorage>,
}

impl RaftStorage {
    pub fn new(store: PeerStorage) -> RaftStorage {
        RaftStorage { store: RwLock::new(store) }
    }

    pub fn rl(&self) -> RwLockReadGuard<PeerStorage> {
        self.store.rl()
    }

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
        self.rl().snapshot()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::sync::*;
    use rocksdb::*;
    use kvproto::raftpb::{Entry, ConfState};
    use kvproto::raft_serverpb::RaftSnapshotData;
    use raft::{StorageError, Error as RaftError};
    use tempdir::*;
    use protobuf;
    use raft::Storage;
    use raftstore::store::bootstrap;

    fn new_storage(path: &TempDir) -> RaftStorage {
        let db = DB::open_default(path.path().to_str().unwrap()).unwrap();
        let db = Arc::new(db);
        bootstrap::bootstrap_store(&db, 1, 1).expect("");
        let region = bootstrap::bootstrap_region(&db, 1, 1, 1).expect("");
        RaftStorage::new(PeerStorage::new(db, &region).unwrap())
    }

    fn new_storage_from_ents(path: &TempDir, ents: &[Entry]) -> RaftStorage {
        let store = new_storage(path);
        let wb = WriteBatch::new();
        let li = store.rl().append(&wb, 0, &ents[1..]).expect("");
        store.rl().engine.write(wb).expect("");
        store.wl().set_last_index(li);
        store.wl().truncated_state.set_index(ents[0].get_index());
        store.wl().truncated_state.set_term(ents[0].get_term());
        store.wl().set_applied_index(ents.last().unwrap().get_index());
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
            let store = new_storage_from_ents(&td, &ents);
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
            let store = new_storage_from_ents(&td, &ents);
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
            let store = new_storage_from_ents(&td, &ents);
            let wb = WriteBatch::new();
            let res = store.rl().compact(&wb, idx);
            // TODO check exact error type after refactoring error.
            if res.is_err() ^ werr.is_err() {
                panic!("#{}: want {:?}, got {:?}", i, werr, res);
            }
            if res.is_ok() {
                store.rl().engine.write(wb).expect("");
            }
        }
    }

    #[test]
    fn test_storage_create_snapshot() {
        let ents = vec![new_entry(3, 3), new_entry(4, 4), new_entry(5, 5)];
        let mut cs = ConfState::new();
        cs.set_nodes(vec![1, 2, 3]);

        let td = TempDir::new("tikv-store-test").unwrap();
        let s = new_storage_from_ents(&td, &ents);
        let snap = s.rl().snapshot().unwrap();
        assert_eq!(snap.get_metadata().get_index(), 5);
        assert_eq!(snap.get_metadata().get_term(), 5);
        assert!(!snap.get_data().is_empty());
        let mut data = RaftSnapshotData::new();
        protobuf::Message::merge_from_bytes(&mut data, snap.get_data()).expect("");
        assert_eq!(data.get_region().get_id(), 1);
        assert_eq!(data.get_region().get_peers().len(), 1);
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
            let store = new_storage_from_ents(&td, &ents);
            let mut li = store.rl().last_index();
            let wb = WriteBatch::new();
            li = store.wl().append(&wb, li, &entries).expect("");
            store.wl().set_last_index(li);
            store.wl().engine.write(wb).expect("");
            let actual_entries = store.rl().entries(4, li + 1, u64::max_value()).expect("");
            if actual_entries != wentries {
                panic!("#{}: want {:?}, got {:?}", i, wentries, actual_entries);
            }
        }
    }
}
