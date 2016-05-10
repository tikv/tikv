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

// Copyright 2015 CoreOS, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.


#![allow(dead_code)]
#![allow(deprecated)]
use kvproto::raftpb::{HardState, ConfState, Entry, Snapshot};
use raft::errors::{Result, Error, StorageError};
use std::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use util::{self, HandyRwLock};

#[derive(Debug, Clone)]
pub struct RaftState {
    pub hard_state: HardState,
    pub conf_state: ConfState,
}

pub trait Storage {
    fn initial_state(&self) -> Result<RaftState>;
    fn entries(&self, low: u64, high: u64, max_size: u64) -> Result<Vec<Entry>>;
    fn term(&self, idx: u64) -> Result<u64>;
    fn first_index(&self) -> Result<u64>;
    fn last_index(&self) -> Result<u64>;
    fn snapshot(&self) -> Result<Snapshot>;
}

pub struct MemStorageCore {
    hard_state: HardState,
    snapshot: Snapshot,
    // TODO: maybe vec_deque
    entries: Vec<Entry>,
}

impl Default for MemStorageCore {
    fn default() -> MemStorageCore {
        MemStorageCore {
            // When starting from scratch populate the list with a dummy entry at term zero.
            entries: vec![Entry::new()],
            hard_state: HardState::new(),
            snapshot: Snapshot::new(),
        }
    }
}

impl MemStorageCore {
    pub fn set_hardstate(&mut self, hs: HardState) {
        self.hard_state = hs;
    }

    fn inner_last_index(&self) -> u64 {
        self.entries[0].get_index() + self.entries.len() as u64 - 1
    }

    pub fn apply_snapshot(&mut self, snapshot: Snapshot) -> Result<()> {
        let mut e = Entry::new();
        e.set_term(snapshot.get_metadata().get_term());
        e.set_index(snapshot.get_metadata().get_index());
        self.entries = vec![e];
        self.snapshot = snapshot;
        Ok(())
    }

    pub fn create_snapshot(&mut self,
                           idx: u64,
                           cs: Option<ConfState>,
                           data: Vec<u8>)
                           -> Result<&Snapshot> {
        if idx <= self.snapshot.get_metadata().get_index() {
            return Err(Error::Store(StorageError::SnapshotOutOfDate));
        }

        let offset = self.entries[0].get_index();
        if idx > self.inner_last_index() {
            panic!("snapshot {} is out of bound lastindex({})",
                   idx,
                   self.inner_last_index())
        }
        self.snapshot.mut_metadata().set_index(idx);
        self.snapshot.mut_metadata().set_term(self.entries[(idx - offset) as usize].get_term());
        if let Some(cs) = cs {
            self.snapshot.mut_metadata().set_conf_state(cs)
        }
        self.snapshot.set_data(data);
        Ok(&self.snapshot)
    }

    pub fn compact(&mut self, compact_index: u64) -> Result<()> {
        let offset = self.entries[0].get_index();
        if compact_index <= offset {
            return Err(Error::Store(StorageError::Compacted));
        }
        if compact_index > self.inner_last_index() {
            panic!("compact {} is out of bound lastindex({})",
                   compact_index,
                   self.inner_last_index())
        }

        let i = (compact_index - offset) as usize;
        let entries = self.entries.drain(i..).collect();
        self.entries = entries;
        Ok(())
    }

    pub fn append(&mut self, ents: &[Entry]) -> Result<()> {
        if ents.is_empty() {
            return Ok(());
        }
        let first: u64 = self.entries[0].get_index() + 1;
        let last: u64 = ents[0].get_index() + ents.len() as u64 - 1;

        if last < first {
            return Ok(());
        }
        // truncate compacted entries
        let mut te: &[Entry] = ents;
        if first > ents[0].get_index() {
            let start = (first - ents[0].get_index()) as usize;
            te = &ents[start..ents.len()];
        }


        let offset = te[0].get_index() - self.entries[0].get_index();
        if self.entries.len() as u64 > offset {
            let mut new_entries: Vec<Entry> = vec![];
            new_entries.extend_from_slice(&self.entries[..offset as usize]);
            new_entries.extend_from_slice(te);
            self.entries = new_entries;
        } else if self.entries.len() as u64 == offset {
            self.entries.extend_from_slice(te);
        } else {
            panic!("missing log entry [last: {}, append at: {}]",
                   self.inner_last_index(),
                   te[0].get_index())
        }

        Ok(())
    }
}

pub struct MemStorage {
    core: RwLock<MemStorageCore>,
}

/// A thread-safe in-memory storage implementation.
/// Mainly for tests purpose.
impl MemStorage {
    pub fn new() -> MemStorage {
        MemStorage { ..Default::default() }
    }

    pub fn rl(&self) -> RwLockReadGuard<MemStorageCore> {
        self.core.rl()
    }

    pub fn wl(&self) -> RwLockWriteGuard<MemStorageCore> {
        self.core.wl()
    }
}

impl Default for MemStorage {
    fn default() -> MemStorage {
        MemStorage { core: RwLock::new(MemStorageCore::default()) }
    }
}

impl Storage for MemStorage {
    fn initial_state(&self) -> Result<RaftState> {
        let core = self.rl();
        Ok(RaftState {
            hard_state: core.hard_state.clone(),
            conf_state: core.snapshot.get_metadata().get_conf_state().clone(),
        })
    }

    fn entries(&self, low: u64, high: u64, max_size: u64) -> Result<Vec<Entry>> {
        let core = self.rl();
        let offset = core.entries[0].get_index();
        if low <= offset {
            return Err(Error::Store(StorageError::Compacted));
        }

        if high > core.inner_last_index() + 1 {
            panic!("index out of bound")
        }
        // only contains dummy entries.
        if core.entries.len() == 1 {
            return Err(Error::Store(StorageError::Unavailable));
        }

        let lo = (low - offset) as usize;
        let hi = (high - offset) as usize;
        let ents: &[Entry] = &core.entries[lo..hi];
        Ok(util::limit_size(ents, max_size))
    }

    fn term(&self, idx: u64) -> Result<u64> {
        let core = self.rl();
        let offset = core.entries[0].get_index();
        if idx < offset {
            return Err(Error::Store(StorageError::Compacted));
        }
        Ok(core.entries[(idx - offset) as usize].get_term())
    }

    fn first_index(&self) -> Result<u64> {
        let core = self.rl();
        Ok(core.entries[0].get_index() + 1)
    }

    fn last_index(&self) -> Result<u64> {
        let core = self.rl();
        Ok(core.inner_last_index())
    }

    fn snapshot(&self) -> Result<Snapshot> {
        let core = self.rl();
        Ok(core.snapshot.clone())
    }
}
