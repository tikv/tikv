use std::sync::{self, Arc, RwLock};
use std::vec::Vec;
use std::error;

use rocksdb::DB;
use protobuf::{self, Message};

use proto::metapb;
use proto::raftpb::{Entry, Snapshot, HardState, ConfState};
use proto::raft_serverpb::{RaftSnapshotData, KeyValue};
use raft::{self, Storage, RaftState, StorageError};
use raftserver::{Result, Error};
use super::keys;
use super::engine;
use super::region_meta::{RegionMeta, RAFT_INIT_LOG_INDEX, RAFT_INIT_LOG_TERM};

pub struct RegionStorage {
    engine: Arc<DB>,

    meta: Arc<RwLock<RegionMeta>>,
}

impl RegionStorage {
    pub fn new(engine: Arc<DB>, meta: Arc<RwLock<RegionMeta>>) -> RegionStorage {
        RegionStorage {
            engine: engine,
            meta: meta,
        }
    }
}

fn storage_error<E>(error: E) -> raft::Error
    where E: Into<Box<error::Error + Send + Sync>>
{
    raft::Error::Store(StorageError::Other(error.into()))
}


impl From<Error> for raft::Error {
    fn from(err: Error) -> raft::Error {
        storage_error(err)
    }
}

impl<T> From<sync::PoisonError<T>> for raft::Error {
    fn from(_: sync::PoisonError<T>) -> raft::Error {
        storage_error("lock failed")
    }
}

impl Storage for RegionStorage {
    fn initial_state(&self) -> raft::Result<RaftState> {
        let mut meta = try!(self.meta.write());
        let initialized = meta.is_initialized();
        let res = try!(engine::get_msg::<HardState>(&self.engine,
                                                    &keys::raft_hard_state_key(meta.region_id)));

        let found = res.is_some();
        let mut hard_state = match res {
            Some(state) => state,
            None => HardState::new(), 
        };

        if !found {
            if initialized {
                hard_state.set_term(RAFT_INIT_LOG_TERM);
                hard_state.set_commit(RAFT_INIT_LOG_INDEX);
                meta.last_index = RAFT_INIT_LOG_INDEX;
            } else {
                meta.last_index = 0;
            }
        } else if initialized && hard_state.get_commit() == 0 {
            hard_state.set_commit(RAFT_INIT_LOG_INDEX);
        }

        let mut conf_state = ConfState::new();
        if found || initialized {
            for p in meta.region.get_peers() {
                conf_state.mut_nodes().push(p.get_peer_id());
            }
        }


        Ok(RaftState {
            hard_state: hard_state,
            conf_state: conf_state,
        })
    }

    fn entries(&self, low: u64, high: u64, max_size: u64) -> raft::Result<Vec<Entry>> {
        if low > high {
            return Err(storage_error(format!("low: {} is greater that high: {}", low, high)));
        }

        let mut ents = vec![];
        let mut total_size: u64 = 0;
        let mut next_index = low;
        let mut exceeded_max_size = false;

        let meta = try!(self.meta.read());
        let start_key = keys::raft_log_key(meta.region_id, low);
        let end_key = keys::raft_log_key(meta.region_id, high);


        try!(engine::scan(&self.engine,
                          &start_key,
                          &end_key,
                          &mut |_, value| -> Result<bool> {
                              let mut entry = Entry::new();
                              try!(entry.merge_from_bytes(value));

                              // May meet gap or has been compacted.
                              if entry.get_index() != next_index {
                                  return Ok(true);
                              }

                              next_index += 1;
                              total_size += entry.compute_size() as u64;
                              ents.push(entry);

                              // We only check if max_size > 0.
                              if max_size > 0 {
                                  exceeded_max_size = total_size > max_size;
                              }

                              Ok(!exceeded_max_size)
                          }));

        // If we get the correct number of entries the total size exceeds max_size, returns.
        if ents.len() == (high - low) as usize || exceeded_max_size {
            return Ok(ents);
        }

        // Can we simplify following check?
        if ents.len() > 0 {
            // Here means we meet some error.
            if ents[0].get_index() > low {
                return Err(raft::Error::Store(StorageError::Compacted));
            }

            if meta.last_index <= next_index {
                return Err(raft::Error::Store(StorageError::Unavailable));
            }

            return Err(storage_error(format!("meet a gap in {} {} at index {}",
                                             low,
                                             high,
                                             next_index)));
        }

        let state = try!(meta.get_truncated_state());
        if state.get_index() >= low {
            // The truncated log index >= low.
            return Err(raft::Error::Store(StorageError::Compacted));
        }

        return Err(raft::Error::Store(StorageError::Unavailable));
    }

    fn term(&self, idx: u64) -> raft::Result<u64> {
        match self.entries(idx, idx + 1, 0) {
            Err(e) => {
                match e {
                    raft::Error::Store(StorageError::Compacted) => {
                        let meta = try!(self.meta.read());
                        let state = try!(meta.get_truncated_state());
                        if state.get_index() == idx {
                            return Ok(state.get_term());
                        }
                        return Err(e);
                    }
                    _ => return Err(e),
                }
            }
            Ok(ents) => {
                if ents.len() == 0 {
                    return Ok(0);
                } else {
                    return Ok(ents[0].get_term());
                }
            }
        }
    }

    fn first_index(&self) -> raft::Result<u64> {
        let meta = try!(self.meta.read());
        let first_index = try!(meta.get_first_index());
        Ok(first_index)
    }

    fn last_index(&self) -> raft::Result<u64> {
        let meta = try!(self.meta.read());
        Ok(meta.last_index)
    }

    fn snapshot(&self) -> raft::Result<Snapshot> {
        let snap = self.engine.snapshot();

        // TODO: get applied_index and region info from snapshot.
        let meta = try!(self.meta.read());
        let applied_index = try!(meta.snap_load_applied_index(&snap));

        let res = try!(engine::snap_get_msg::<metapb::Region>(&snap,
                                            &keys::region_info_key(meta.region.get_start_key())));

        let region = match res {
            None => return Err(storage_error(format!("could not find region info"))),
            Some(region) => region,
        };

        let term = try!(self.term(applied_index));

        let mut snapshot = Snapshot::new();

        // Set snapshot metadata.
        snapshot.mut_metadata().set_index(applied_index);
        snapshot.mut_metadata().set_term(term);

        let mut conf_state = ConfState::new();
        for p in region.get_peers() {
            conf_state.mut_nodes().push(p.get_peer_id());
        }

        snapshot.mut_metadata().set_conf_state(conf_state);

        // Set snapshot data.
        let mut snap_data = RaftSnapshotData::new();
        snap_data.set_region(region);

        let mut data = vec![];
        try!(meta.snap_scan_region(&snap,
                                   &mut |key, value| {
                                       let mut kv = KeyValue::new();
                                       kv.set_key(key.to_vec());
                                       kv.set_value(value.to_vec());
                                       data.push(kv);
                                       Ok(true)
                                   }));

        snap_data.set_data(protobuf::RepeatedField::from_vec(data));

        let mut v = vec![];
        try!(snap_data.write_to_vec(&mut v));

        snapshot.set_data(v);

        Ok(snapshot)
    }
}
