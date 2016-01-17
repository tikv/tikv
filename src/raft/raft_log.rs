
#![allow(dead_code)]
use raft::storage::Storage;
use raft::log_unstable::Unstable;
use proto::raftpb::{Entry, Snapshot};
use raft::errors::{Result, Error, StorageError};
use std::{cmp, u64};
use protobuf;

const NO_LIMIT: u64 = u64::MAX;

/// Raft log implementation
pub struct RaftLog<T>
    where T: Storage
{
    // storage contains all stable entries since the last snapshot.
    store: T,

    // unstable contains all unstable entries and snapshot.
    // they will be saved into storage.
    unstable: Unstable,

    // committed is the highest log position that is known to be in
    // stable storage on a quorum of nodes.
    committed: u64,

    // applied is the highest log position that the application has
    // been instructed to apply to its state machine.
    // Invariant: applied <= committed
    applied: u64,
}

impl<T> ToString for RaftLog<T> where T: Storage
{
    fn to_string(&self) -> String {
        format!("committed={}, applied={}, unstable.offset={}, unstable.entries.len()={}",
                self.committed,
                self.applied,
                self.unstable.offset,
                self.unstable.entries.len())
    }
}


fn limit_size(entries: &[Entry], max_size: u64) -> &[Entry] {
    if entries.len() == 0 {
        return entries;
    }

    let mut size = protobuf::Message::compute_size(&entries[0]) as u64;
    let mut limit = 1usize;
    while limit < entries.len() {
        size += protobuf::Message::compute_size(&entries[limit]) as u64;
        if size > max_size {
            break;
        }
        limit += 1;
    }

    &entries[..limit]
}


impl<T> RaftLog<T> where T: Storage
{
    pub fn new(storage: T) -> RaftLog<T> {
        let first_index = storage.first_index().unwrap_or_else(|e| panic!(e));
        let last_index = storage.last_index().unwrap_or_else(|e| panic!(e));

        // Initialize committed and applied pointers to the time of the last compaction.
        RaftLog {
            store: storage,
            committed: first_index - 1,
            applied: first_index - 1,
            unstable: Unstable::new(last_index + 1),
        }
    }

    pub fn last_term(&self) -> u64 {
        self.term(self.last_index())
            .map_err(|e| panic!("unexpected error when getting the last term({})", e))
            .unwrap()
    }

    pub fn term(&self, idx: u64) -> Result<u64> {
        // the valid term range is [index of dummy entry, last index]
        let dummy_idx = self.first_index() - 1;
        if idx < dummy_idx || idx > self.last_index() {
            return Ok(0u64);
        }

        self.unstable.maybe_term(idx).map(|term| return Ok::<u64, Error>(term));
        self.store.term(idx).map_err(|e| {
            if e == Error::Store(StorageError::Compacted) {
                return e;
            }
            panic!(e)
        })
    }

    pub fn first_index(&self) -> u64 {
        match self.unstable.maybe_first_index() {
            Some(idx) => idx,
            None => self.store.first_index().map_err(|e| panic!(e)).unwrap(),
        }
    }

    pub fn last_index(&self) -> u64 {
        match self.unstable.maybe_last_index() {
            Some(idx) => idx,
            None => self.store.last_index().map_err(|e| panic!(e)).unwrap(),
        }
    }


    // find_conflict finds the index of the conflict.
    // It returns the first pair of conflicting entries between the existing
    // entries and the given entries, if there are any.
    // If there is no conflicting entries, and the existing entries contains
    // all the given entries, zero will be returned.
    // If there is no conflicting entries, but the given entries contains new
    // entries, the index of the first new entry will be returned.
    // An entry is considered to be conflicting if it has the same index but
    // a different term.
    // The first entry MUST have an index equal to the argument 'from'.
    // The index of the given entries MUST be continuously increasing.
    pub fn find_conflict(&self, ents: &[Entry]) -> u64 {
        for e in ents {
            if !self.match_term(e.get_Index(), e.get_Term()) {
                if e.get_Index() <= self.last_index() {
                    info!("found conflict at index {}, [existing term:{}, conflicting term:{}]",
                          e.get_Index(),
                          self.zero_term_on_err_compacted(self.term(e.get_Index())),
                          e.get_Term());
                }
                return e.get_Index();
            }
        }
        0
    }

    pub fn zero_term_on_err_compacted(&self, res: Result<u64>) -> u64 {
        match res {
            Ok(term) => term,
            Err(e) => {
                if e == Error::Store(StorageError::Compacted) {
                    return 0;
                }
                panic!(e)
            }
        }
    }

    pub fn match_term(&self, idx: u64, term: u64) -> bool {
        match self.term(idx) {
            Ok(t) => t == term,
            Err(_) => false,
        }
    }

    // maybe_append returns None if the entries cannot be appended. Otherwise,
    // it returns (last index of new entries, true).
    pub fn maybe_append(&mut self,
                        idx: u64,
                        term: u64,
                        committed: u64,
                        ents: &[Entry])
                        -> Option<u64> {
        let last_new_index = idx + ents.len() as u64;
        if self.match_term(idx, term) {
            let commit_idx = self.find_conflict(ents);
            if commit_idx == 0 {
            } else if commit_idx < self.committed {
                panic!("entry {} conflict with committed entry {}",
                       commit_idx,
                       self.committed)
            } else {
                let offset = idx + 1;
                self.append(&ents[(commit_idx - offset) as usize..]);
            }
            self.commit_to(cmp::min(committed, last_new_index));
            return Some(last_new_index);
        }
        None
    }

    fn commit_to(&mut self, to_commit: u64) {
        // never decrease commit
        if self.committed < to_commit {
            if self.last_index() < to_commit {
                panic!("to_commit {} is out of range [last_index {}]",
                       to_commit,
                       self.last_index())
            }
            self.committed = to_commit;
        }
    }

    fn applied_to(&mut self, idx: u64) {
        if idx == 0 {
            return;
        }
        if self.committed < idx || idx < self.applied {
            panic!("applied({}) is out of range [prev_applied({}), commited({})",
                   idx,
                   self.applied,
                   self.committed)
        }
        self.applied = idx;
    }

    fn stable_to(&mut self, idx: u64, term: u64) {
        self.unstable.stable_to(idx, term)
    }

    fn stable_snap_to(&mut self, idx: u64) {
        self.unstable.stable_snap_to(idx)
    }


    fn append(&mut self, ents: &[Entry]) -> u64 {
        if ents.len() == 0 {
            return self.last_index();
        }

        let after = ents[0].get_Index() - 1;
        if after < self.committed {
            panic!("after {} is out of range [committed {}]",
                   after,
                   self.committed)
        }
        self.unstable.truncate_and_append(ents);
        self.last_index()
    }

    fn unstable_entries(&self) -> Option<&[Entry]> {
        if self.unstable.entries.len() == 0 {
            return None;
        }
        Some(&self.unstable.entries)
    }

    fn entries(&mut self, idx: u64, max_size: u64) -> Result<Vec<Entry>> {
        let last = self.last_index();
        if idx > last {
            return Ok(Vec::<Entry>::new());
        }
        return self.slice(idx, last + 1, max_size);
    }

    fn all_entries(&mut self) -> Vec<Entry> {
        let first_index = self.first_index();
        let ents = self.entries(first_index, NO_LIMIT);
        match ents {
            Err(e) => {
                // try again if there was a racing compaction
                if e == Error::Store(StorageError::Compacted) {
                    return self.all_entries();
                }
                panic!(e)
            }
            Ok(ents) => {
                return ents;
            }
        }
    }

    fn is_upto_date(&self, last_index: u64, term: u64) -> bool {
        term > self.last_term() || (term == self.last_term() && last_index >= self.last_index())
    }


    // next_entries returns all the available entries for execution.
    // If applied is smaller than the index of snapshot, it returns all committed
    // entries after the index of snapshot.
    fn next_entries(&mut self) -> Option<Vec<Entry>> {
        let offset = cmp::max(self.applied + 1, self.first_index());
        let committed = self.committed;
        if committed + 1 > offset {
            match self.slice(offset, committed + 1, NO_LIMIT) {
                Ok(vec) => return Some(vec),
                Err(e) => panic!("{}", e),
            }
        }
        None
    }

    fn has_next_entries(&self) -> bool {
        let offset = cmp::max(self.applied + 1, self.first_index());
        return self.committed + 1 > offset;
    }

    fn snapshot(&self) -> Snapshot {
        if self.unstable.snapshot.is_none() {
            return self.store.snapshot().unwrap().clone();
        }
        self.unstable.get_snapshot()
    }

    fn must_check_outofbounds(&self, low: u64, high: u64) -> Option<Error> {
        if low > high {
            panic!("invalid slice {} > {}", low, high)
        }
        let first_index = self.first_index();
        if low < first_index {
            return Some(Error::Store(StorageError::Compacted));
        }

        let length = self.last_index() - first_index + 1;
        if low < first_index || high > first_index + length {
            panic!("slice[{},{}] out of bound[{},{}]",
                   low,
                   high,
                   first_index,
                   self.last_index())
        }
        None
    }

    fn slice(&mut self, low: u64, high: u64, max_size: u64) -> Result<Vec<Entry>> {
        let err = self.must_check_outofbounds(low, high);
        if err.is_some() {
            return Err(err.unwrap());
        }


        let mut ents: Vec<Entry> = vec![];
        if low == high {
            return Ok(ents);
        }

        if low < self.unstable.offset {
            let stored_entries = self.store
                                     .entries(low, cmp::min(high, self.unstable.offset), max_size);
            if stored_entries.is_err() {
                let e = stored_entries.unwrap_err();
                match e {
                    Error::Store(StorageError::Compacted) => return Err(e),
                    Error::Store(StorageError::Unavailable) => {
                        panic!("entries[{}:{}] is unavailable from storage",
                               low,
                               cmp::min(high, self.unstable.offset))
                    }
                    _ => panic!(e),
                }
            }
            let se = stored_entries.unwrap();
            if (se.len() as u64) < cmp::min(high, self.unstable.offset) - low {
                return Ok(ents);
            }
            ents = se.to_vec();
        }

        if high > self.unstable.offset {
            let offset = self.unstable.offset;
            let unstable = self.unstable.slice(cmp::max(low, offset), high);
            if ents.len() > 0 {
                ents.extend_from_slice(&unstable);
            } else {
                ents = unstable;
            }
        }

        let mut v = Vec::<Entry>::new();
        v.extend_from_slice(limit_size(&ents, max_size));
        Ok(v)
    }
}
