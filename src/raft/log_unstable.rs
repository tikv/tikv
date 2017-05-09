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

// Copyright 2015 The etcd Authors
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


use kvproto::eraftpb::{Entry, Snapshot};

// unstable.entris[i] has raft log position i+unstable.offset.
// Note that unstable.offset may be less than the highest log
// position in storage; this means that the next write to storage
// might need to truncate the log before persisting unstable.entries.
#[derive(Debug, PartialEq, Default)]
pub struct Unstable {
    // the incoming unstable snapshot, if any.
    pub snapshot: Option<Snapshot>,
    // all entries that have not yet been written to storage.
    pub entries: Vec<Entry>,
    pub offset: u64,

    pub tag: String,
}


impl Unstable {
    pub fn new(offset: u64, tag: String) -> Unstable {
        Unstable {
            offset: offset,
            snapshot: None,
            entries: vec![],
            tag: tag,
        }
    }
    // maybe_first_index returns the index of the first possible entry in entries
    // if it has a snapshot.
    pub fn maybe_first_index(&self) -> Option<u64> {
        self.snapshot
            .as_ref()
            .map(|snap| snap.get_metadata().get_index() + 1)
    }

    // maybe_last_index returns the last index if it has at least one
    // unstable entry or snapshot.
    pub fn maybe_last_index(&self) -> Option<u64> {
        match self.entries.len() {
            0 => {
                self.snapshot
                    .as_ref()
                    .map(|snap| snap.get_metadata().get_index())
            }
            len => Some(self.offset + len as u64 - 1),
        }
    }

    // maybe_term returns the term of the entry at index idx, if there
    // is any.
    pub fn maybe_term(&self, idx: u64) -> Option<u64> {
        if idx < self.offset {
            if self.snapshot.is_none() {
                return None;
            }

            let meta = self.snapshot.as_ref().unwrap().get_metadata();
            if idx == meta.get_index() {
                return Some(meta.get_term());
            }
            return None;
        }
        self.maybe_last_index().and_then(|last| {
            if idx > last {
                return None;
            }
            Some(self.entries[(idx - self.offset) as usize].get_term())
        })
    }

    pub fn stable_to(&mut self, idx: u64, term: u64) {
        let t = self.maybe_term(idx);
        if t.is_none() {
            return;
        }

        if t.unwrap() == term && idx >= self.offset {
            let start = idx + 1 - self.offset;
            self.entries.drain(..start as usize);
            self.offset = idx + 1;
        }
    }

    pub fn stable_snap_to(&mut self, idx: u64) {
        if self.snapshot.is_none() {
            return;
        }
        if idx == self.snapshot.as_ref().unwrap().get_metadata().get_index() {
            self.snapshot = None;
        }
    }

    pub fn restore(&mut self, snap: Snapshot) {
        self.entries.clear();
        self.offset = snap.get_metadata().get_index() + 1;
        self.snapshot = Some(snap);
    }

    // append entries to unstable, truncate local block first if overlapped.
    pub fn truncate_and_append(&mut self, ents: &[Entry]) {
        let after = ents[0].get_index();
        if after == self.offset + self.entries.len() as u64 {
            // after is the next index in the self.entries, append directly
            self.entries.extend_from_slice(ents);
        } else if after <= self.offset {
            // The log is being truncated to before our current offset
            // portion, so set the offset and replace the entries
            self.offset = after;
            self.entries.clear();
            self.entries.extend_from_slice(ents);
        } else {
            // truncate to after and copy to self.entries then append
            let off = self.offset;
            self.must_check_outofbounds(off, after);
            self.entries.truncate((after - off) as usize);
            self.entries.extend_from_slice(ents);
        }
    }

    pub fn slice(&self, lo: u64, hi: u64) -> &[Entry] {
        self.must_check_outofbounds(lo, hi);
        let l = lo as usize;
        let h = hi as usize;
        let off = self.offset as usize;
        &self.entries[l - off..h - off]
    }

    pub fn must_check_outofbounds(&self, lo: u64, hi: u64) {
        if lo > hi {
            panic!("{} invalid unstable.slice {} > {}", self.tag, lo, hi)
        }
        let upper = self.offset + self.entries.len() as u64;
        if lo < self.offset || hi > upper {
            panic!("{} unstable.slice[{}, {}] out of bound[{}, {}]",
                   self.tag,
                   lo,
                   hi,
                   self.offset,
                   upper)
        }
    }
}


#[cfg(test)]
mod test {
    use kvproto::eraftpb::{Entry, Snapshot};
    use raft::log_unstable::Unstable;

    fn new_entry(index: u64, term: u64) -> Entry {
        let mut e = Entry::new();
        e.set_term(term);
        e.set_index(index);
        e
    }

    fn new_snapshot(index: u64, term: u64) -> Snapshot {
        use kvproto::eraftpb::{SnapshotMetadata, Snapshot};
        let mut snap = Snapshot::new();
        let mut meta = SnapshotMetadata::new();
        meta.set_index(index);
        meta.set_term(term);
        snap.set_metadata(meta);
        snap
    }

    #[test]
    fn test_maybe_first_index() {
        // entry, offset, snap, wok, windex,
        let tests = vec![// no snapshot
                         (Some(new_entry(5, 1)), 5, None, false, 0),
                         (None, 0, None, false, 0),
                         // has snapshot
                         (Some(new_entry(5, 1)), 5, Some(new_snapshot(4, 1)), true, 5),
                         (None, 5, Some(new_snapshot(4, 1)), true, 5)];

        for (entries, offset, snapshot, wok, windex) in tests {
            let u = Unstable {
                entries: entries.map_or(vec![], |entry| vec![entry]),
                offset: offset,
                snapshot: snapshot,
                ..Default::default()
            };
            let index = u.maybe_first_index();
            match index {
                None => assert!(!wok),
                Some(index) => assert_eq!(index, windex),
            }
        }
    }

    #[test]
    fn test_maybe_last_index() {
        // entry, offset, snap, wok, windex,
        let tests = vec![(Some(new_entry(5, 1)), 5, None, true, 5),
                         (Some(new_entry(5, 1)), 5, Some(new_snapshot(4, 1)), true, 5),
                         // last in snapshot
                         (None, 5, Some(new_snapshot(4, 1)), true, 4),
                         // empty unstable
                         (None, 0, None, false, 0)];

        for (entries, offset, snapshot, wok, windex) in tests {
            let u = Unstable {
                entries: entries.map_or(vec![], |entry| vec![entry]),
                offset: offset,
                snapshot: snapshot,
                ..Default::default()
            };
            let index = u.maybe_last_index();
            match index {
                None => assert!(!wok),
                Some(index) => assert_eq!(index, windex),
            }
        }
    }

    #[test]
    fn test_maybe_term() {
        // entry, offset, snap, index, wok, wterm
        let tests = vec![// term from entries
                         (Some(new_entry(5, 1)), 5, None, 5, true, 1),
                         (Some(new_entry(5, 1)), 5, None, 6, false, 0),
                         (Some(new_entry(5, 1)), 5, None, 4, false, 0),
                         (Some(new_entry(5, 1)), 5, Some(new_snapshot(4, 1)), 5, true, 1),
                         (Some(new_entry(5, 1)), 5, Some(new_snapshot(4, 1)), 6, false, 0),
                         // term from snapshot
                         (Some(new_entry(5, 1)), 5, Some(new_snapshot(4, 1)), 4, true, 1),
                         (Some(new_entry(5, 1)), 5, Some(new_snapshot(4, 1)), 3, false, 0),
                         (None, 5, Some(new_snapshot(4, 1)), 5, false, 0),
                         (None, 5, Some(new_snapshot(4, 1)), 4, true, 1),
                         (None, 0, None, 5, false, 0)];

        for (entries, offset, snapshot, index, wok, wterm) in tests {
            let u = Unstable {
                entries: entries.map_or(vec![], |entry| vec![entry]),
                offset: offset,
                snapshot: snapshot,
                ..Default::default()
            };
            let term = u.maybe_term(index);
            match term {
                None => assert!(!wok),
                Some(term) => assert_eq!(term, wterm),
            }
        }
    }


    #[test]
    fn test_restore() {
        let mut u = Unstable {
            entries: vec![new_entry(5, 1)],
            offset: 5,
            snapshot: Some(new_snapshot(4, 1)),
            ..Default::default()
        };

        let s = new_snapshot(6, 2);
        u.restore(s.clone());

        assert_eq!(u.offset, s.get_metadata().get_index() + 1);
        assert!(u.entries.is_empty());
        assert_eq!(u.snapshot.unwrap(), s);
    }

    #[test]
    fn test_stable_to() {
        // entries, offset, snap, index, term, woffset, wlen
        let tests =
            vec![(vec![], 0, None, 5, 1, 0, 0),
                 // stable to the first entry
                 (vec![new_entry(5, 1)], 5, None, 5, 1, 6, 0),
                 (vec![new_entry(5, 1), new_entry(6, 0)], 5, None, 5, 1, 6, 1),
                 // stable to the first entry and term mismatch
                 (vec![new_entry(6, 2)], 5, None, 6, 1, 5, 1),
                 // stable to old entry
                 (vec![new_entry(5, 1)], 5, None, 4, 1, 5, 1),
                 (vec![new_entry(5, 1)], 5, None, 4, 2, 5, 1),
                 // with snapshot
                 // stable to the first entry
                 (vec![new_entry(5, 1)], 5, Some(new_snapshot(4, 1)), 5, 1, 6, 0),
                 // stable to the first entry
                 (vec![new_entry(5, 1), new_entry(6, 1)], 5, Some(new_snapshot(4, 1)), 5, 1, 6, 1),
                 // stable to the first entry and term mismatch
                 (vec![new_entry(6, 2)], 5, Some(new_snapshot(5, 1)), 6, 1, 5, 1),
                 // stable to snapshot
                 (vec![new_entry(5, 1)], 5, Some(new_snapshot(4, 1)), 4, 1, 5, 1),
                 // stable to old entry
                 (vec![new_entry(5, 2)], 5, Some(new_snapshot(4, 2)), 4, 1, 5, 1)];

        for (entries, offset, snapshot, index, term, woffset, wlen) in tests {
            let mut u = Unstable {
                entries: entries,
                offset: offset,
                snapshot: snapshot,
                ..Default::default()
            };
            u.stable_to(index, term);
            assert_eq!(u.offset, woffset);
            assert_eq!(u.entries.len(), wlen);
        }
    }

    #[test]
    fn test_truncate_and_append() {
        // entries, offset, snap, to_append, woffset, wentries
        let tests =
            vec![// replace to the end
                 (vec![new_entry(5, 1)],
                  5,
                  None,
                  vec![new_entry(6, 1), new_entry(7, 1)],
                  5,
                  vec![new_entry(5, 1), new_entry(6, 1), new_entry(7, 1)]),

                 // replace to unstable entries
                 (vec![new_entry(5, 1)],
                  5,
                  None,
                  vec![new_entry(5, 2), new_entry(6, 2)],
                  5,
                  vec![new_entry(5, 2), new_entry(6, 2)]),

                 (vec![new_entry(5, 1)],
                  5,
                  None,
                  vec![new_entry(4, 2), new_entry(5, 2), new_entry(6, 2)],
                  4,
                  vec![new_entry(4, 2), new_entry(5, 2), new_entry(6, 2)]),

                 // truncate existing entries and append
                 (vec![new_entry(5, 1), new_entry(6, 1), new_entry(7, 1)],
                  5,
                  None,
                  vec![new_entry(6, 2)],
                  5,
                  vec![new_entry(5, 1), new_entry(6, 2)]),

                 (vec![new_entry(5, 1), new_entry(6, 1), new_entry(7, 1)],
                  5,
                  None,
                  vec![new_entry(7, 2), new_entry(8, 2)],
                  5,
                  vec![new_entry(5, 1), new_entry(6, 1), new_entry(7, 2), new_entry(8, 2)])];


        for (entries, offset, snapshot, to_append, woffset, wentries) in tests {
            let mut u = Unstable {
                entries: entries,
                offset: offset,
                snapshot: snapshot,
                ..Default::default()
            };
            u.truncate_and_append(&to_append);
            assert_eq!(u.offset, woffset);
            assert_eq!(u.entries, wentries);
        }
    }
}
