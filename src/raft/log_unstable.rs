use protobuf;
use raft::raftpb::{HardState, ConfState, Entry, Snapshot};
use raft::errors::{Result, Error, StorageError};
use std::collections::VecDeque;

// unstable.entris[i] has raft log position i+unstable.offset.
// Note that unstable.offset may be less than the highest log
// position in storage; this means that the next write to storage
// might need to truncate the log before persisting unstable.entries.
pub struct unstable {
    // the incoming unstable snapshot, if any.
    snapshot: Option<Box<Snapshot>>,
    // all entries that have not yet been written to storage.
    entries: VecDeque<Entry>,
    offset: u64,
}


impl unstable {
    // maybe_first_index returns the last index if it has at least one
    // unstable entry or snapshot.
    fn maybe_first_index(&self) -> Option<u64> {
        self.snapshot
            .as_ref()
            .map_or(None, |snap| Some(snap.get_metadata().get_index() + 1))
    }

    // maybe_last_index returns the last index if it has at least one
    // unstable entry or snapshot.
    fn maybe_last_index(&self) -> Option<u64> {
        match self.entries.len() {
            0 => {
                self.snapshot
                    .as_ref()
                    .map_or(None, |snap| Some(snap.get_metadata().get_index()))
            }
            len => Some(self.offset + len as u64 - 1),
        }
    }

    // maybe_term returns the term of the entry at index idx, if there
    // is any.
    fn maybe_term(&self, idx: u64) -> Option<u64> {
        if idx < self.offset {
            if self.snapshot.is_none() {
                return None;
            }

            let meta = self.snapshot.as_ref().unwrap().get_metadata().clone();
            if idx == meta.get_index() {
                return Some(meta.get_term());
            }
            return None;
        }
        match self.maybe_last_index() {
            None => None,
            Some(last) => {
                if idx > last {
                    return None;
                }
                Some(self.entries[(idx - self.offset) as usize].get_Term())
            }
        }
    }

    fn stable_to(&mut self, idx: u64, term: u64) {
        let t = self.maybe_term(idx);
        if t.is_none() {
            return;
        }

        if t.unwrap() == term && idx >= self.offset {
            let start = idx + 1 - self.offset;
            for _ in 0..start {
                self.entries.pop_front();
            }
            self.offset = idx + 1;
        }
    }

    fn stableSnapTo(&mut self, idx: u64) {
        if self.snapshot.is_none() {
            return;
        }
        if idx == self.snapshot.as_ref().unwrap().get_metadata().get_index() {
            self.snapshot = None;
        }
    }
}
