#![allow(dead_code)]
use raft::raftpb::{Entry, Snapshot};
use std::collections::VecDeque;

// unstable.entris[i] has raft log position i+unstable.offset.
// Note that unstable.offset may be less than the highest log
// position in storage; this means that the next write to storage
// might need to truncate the log before persisting unstable.entries.
pub struct Unstable {
    // the incoming unstable snapshot, if any.
    snapshot: Option<Box<Snapshot>>,
    // all entries that have not yet been written to storage.
    entries: VecDeque<Entry>,
    offset: u64,
}


impl Unstable {
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

    fn stable_snap_to(&mut self, idx: u64) {
        if self.snapshot.is_none() {
            return;
        }
        if idx == self.snapshot.as_ref().unwrap().get_metadata().get_index() {
            self.snapshot = None;
        }
    }

    fn restore(&mut self, snap: Snapshot) {
        self.offset = snap.get_metadata().get_index() + 1;
        self.entries.clear();
        self.snapshot = Some(Box::new(snap));
    }

    fn truncate_and_append(&mut self, ents: &[Entry]) {
        let after = ents[0].get_Index() - 1;
        if after == self.offset + self.entries.len() as u64 - 1 {
            for e in ents {
                self.entries.push_back(e.clone());
            }
        } else if after < self.offset {
            // The log is being truncated to before our current offset
            // portion, so set the offset and replace the entries
            self.offset = after + 1;
            self.entries.clear();

            for e in ents {
                self.entries.push_back(e.clone());
            }
        } else {
            // truncate to after and copy to self.entries
            // then append
            let off = self.offset.clone();
            let cut_ents = self.cut_slice(off, after + 1);
            self.entries.clear();
            for e in cut_ents {
                self.entries.push_back(e);
            }

            for e in ents {
                self.entries.push_back(e.clone());
            }
        }
    }

    fn cut_slice(&mut self, lo: u64, hi: u64) -> Vec<Entry> {
        self.must_check_outofbounds(lo, hi);
        let l = lo as usize;
        let h = lo as usize;
        let off = self.offset as usize;
        return self.entries.drain(l - off..h - off).map(|e| e).collect();
    }

    fn must_check_outofbounds(&self, lo: u64, hi: u64) {
        if lo > hi {
            panic!("invalid unstable.slice {} > {}", lo, hi)
        }
        let upper = self.offset + self.entries.len() as u64;
        if lo < self.offset || hi > upper {
            panic!("unstable.slice[{}, {}] out of bound[{}, {}]",
                   lo,
                   hi,
                   self.offset,
                   upper)
        }
    }
}


mod test {
    use raft::raftpb::{Entry, Snapshot};
    use raft::log_unstable::Unstable;
    pub struct Obj {
        entry: Entry,
        offset: u64,
        snap: Option<Snapshot>,

        wok: bool,
        windex: u64,
    }


    fn new_entry(index: u64, term: u64) -> Entry {
        let mut e = Entry::new();
        e.set_Term(term);
        e.set_Index(index);
        e
    }

    fn new_obj(entry: Entry, offset: u64, snap: Option<Snapshot>, wok: bool, windex: u64) -> Obj {
        Obj {
            entry: entry,
            offset: offset,
            snap: snap,
            wok: wok,
            windex: windex,
        }
    }

    fn new_snapshot(index: u64, term: u64) -> Snapshot {
        use raft::raftpb::{SnapshotMetadata, Snapshot};
        let mut snap = Snapshot::new();
        let mut meta = SnapshotMetadata::new();
        meta.set_index(index);
        meta.set_term(term);
        snap.set_metadata(meta);
        snap
    }


    #[test]
    #[cfg(test)]
    fn test_maybe_first_index() {
        let tests: Vec<Obj> = vec![  
            // no snapshot
            new_obj(new_entry(5,1), 5, None, false, 0),
            new_obj( Entry::new(), 0, None, false, 0,),
            // has snapshot
            new_obj( new_entry(5, 1), 5, Some(new_snapshot(4,1)), true, 5,),
            new_obj( Entry::new(), 5, Some(new_snapshot(4,1)), true, 5,), 
        ];

        for tt in tests {
            let u = Unstable {
                entries: vec![tt.entry].into_iter().collect(),
                offset: tt.offset,
                snapshot: tt.snap.map_or(None, |snap| Some(Box::new(snap))),
            };
            let index = u.maybe_first_index();
            match index {
                None => assert!(!tt.wok),
                Some(index) => assert_eq!(index, tt.windex),
            }
        }
    }
}
