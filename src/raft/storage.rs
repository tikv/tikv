
#![allow(dead_code)]
#![allow(deprecated)]
use protobuf;
use proto::raftpb::{HardState, ConfState, Entry, Snapshot};
use raft::errors::{Result, Error, StorageError};

#[derive(Debug, Clone)]
pub struct RaftState {
    hard_state: HardState,
    conf_state: ConfState,
}

pub trait Storage {
    fn initial_state(&self) -> Result<RaftState>;
    fn entries(&self, low: u64, high: u64, max_size: u64) -> Result<&[Entry]>;
    fn term(&self, idx: u64) -> Result<u64>;
    fn first_index(&self) -> Result<u64>;
    fn last_index(&self) -> Result<u64>;
    fn snapshot(&self) -> Result<&Snapshot>;
}

pub struct MemStorage {
    hard_state: HardState,
    snapshot: Snapshot,
    // TODO: maybe vec_deque
    entries: Vec<Entry>,
}

impl MemStorage {
    pub fn new() -> MemStorage {
        MemStorage {
            // When starting from scratch populate the list with a dummy entry at term zero.
            entries: vec![Entry::new()],
            hard_state: HardState::new(),
            snapshot: Snapshot::new(),
        }
    }

    pub fn set_hardstate(&mut self, hs: HardState) {
        self.hard_state = hs;
    }

    fn inner_last_index(&self) -> u64 {
        return self.entries[0].get_index() + self.entries.len() as u64 - 1;
    }

    fn limit_size(entries: &[Entry], max: u64) -> Result<&[Entry]> {
        if entries.len() == 0 {
            return Ok(entries);
        }

        let mut size = protobuf::Message::compute_size(&entries[0]) as u64;
        let mut limit = 1u64;
        while limit < entries.len() as u64 {
            size += protobuf::Message::compute_size(&entries[limit as usize]) as u64;
            if size > max {
                break;
            }
            limit += 1;
        }
        let ents = &entries[..limit as usize];
        Ok(ents)
    }


    pub fn apply_snapshot(&mut self, snapshot: Snapshot) -> Result<()> {
        let mut e = Entry::new();
        e.set_term(snapshot.get_metadata().get_term());
        e.set_index(snapshot.get_metadata().get_index());
        self.entries = vec![e];
        self.snapshot = snapshot;
        Ok(())
    }

    fn create_snapshot(&mut self,
                       idx: u64,
                       cs: Option<ConfState>,
                       data: Vec<u8>)
                       -> Result<&Snapshot> {
        if idx <= self.snapshot.get_metadata().get_index() {
            return Err(Error::Store(StorageError::SnapshotOutOfDate));
        }

        let offset = self.entries[0].get_index();
        if idx > self.last_index().unwrap() {
            panic!("snapshot {} is out of bound lastindex({})",
                   idx,
                   self.last_index().unwrap())
        }
        self.snapshot.mut_metadata().set_index(idx);
        self.snapshot.mut_metadata().set_term(self.entries[(idx - offset) as usize].get_term());
        match cs {
            Some(cs) => self.snapshot.mut_metadata().set_conf_state(cs),
            None => {}
        }
        self.snapshot.set_data(data);
        Ok(&self.snapshot)
    }

    pub fn compact(&mut self, compact_index: u64) -> Result<()> {
        let offset = self.entries[0].get_index();
        if compact_index <= offset {
            return Err(Error::Store(StorageError::Compacted));
        }
        if compact_index > self.last_index().unwrap() {
            panic!("compact {} is out of bound lastindex({})",
                   compact_index,
                   self.last_index().unwrap())
        }

        let i = (compact_index - offset) as usize;
        let entries = self.entries.drain(i..).collect();
        self.entries = entries;
        Ok(())
    }

    pub fn append(&mut self, ents: &[Entry]) -> Result<()> {
        if ents.len() == 0 {
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
                   self.last_index().unwrap(),
                   te[0].get_index())
        }

        Ok(())
    }
}


impl Storage for MemStorage {
    fn initial_state(&self) -> Result<RaftState> {
        Ok(RaftState {
            hard_state: self.hard_state.clone(),
            conf_state: self.snapshot.get_metadata().get_conf_state().clone(),
        })
    }

    fn entries(&self, low: u64, high: u64, max_size: u64) -> Result<&[Entry]> {
        let offset = self.entries[0].get_index();
        if low <= offset {
            return Err(Error::Store(StorageError::Compacted));
        }

        if high > self.inner_last_index() + 1 {
            panic!("index out of bound")
        }
        // only contains dummy entries.
        if self.entries.len() == 1 {
            return Err(Error::Store(StorageError::Unavailable));
        }

        let lo = (low - offset) as usize;
        let hi = (high - offset) as usize;
        let ents: &[Entry] = &self.entries[lo..hi];
        return MemStorage::limit_size(ents, max_size);
    }

    fn term(&self, idx: u64) -> Result<u64> {
        let offset = self.entries[0].get_index();
        if idx < offset {
            return Err(Error::Store(StorageError::Compacted));
        }
        Ok(self.entries[(idx - offset) as usize].get_term())
    }

    fn first_index(&self) -> Result<u64> {
        Ok(self.entries[0].get_index() + 1)
    }

    fn last_index(&self) -> Result<u64> {
        Ok(self.inner_last_index())
    }

    fn snapshot(&self) -> Result<&Snapshot> {
        Ok(&self.snapshot)
    }
}

/// TODO: make MemStorage become actually sync.
/// Currently MemStore is only used for single-thread testing.
unsafe impl Sync for MemStorage {}
