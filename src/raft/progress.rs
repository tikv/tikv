#![allow(dead_code)]

use std::cmp;

#[derive(Debug, PartialEq)]
enum ProgressState {
    Probe,
    Replicate,
    Snapshot,
}

struct Progress {
    matched: u64,
    next_idx: u64,
    // When in ProgressStateProbe, leader sends at most one replication message
    // per heartbeat interval. It also probes actual progress of the follower.
    //
    // When in ProgressStateReplicate, leader optimistically increases next
    // to the latest entry sent after sending replication message. This is
    // an optimized state for fast replicating log entries to the follower.
    //
    // When in ProgressStateSnapshot, leader should have sent out snapshot
    // before and stops sending any replication message.
    state: ProgressState,
    // Paused is used in ProgressStateProbe.
    // When Paused is true, raft should pause sending replication message to this peer.
    paused: bool,
    // pending_snapshot is used in ProgressStateSnapshot.
    // If there is a pending snapshot, the pendingSnapshot will be set to the
    // index of the snapshot. If pendingSnapshot is set, the replication process of
    // this Progress will be paused. raft will not resend snapshot until the pending one
    // is reported to be failed.
    pending_snapshot: u64,

    // recent_active is true if the progress is recently active. Receiving any messages
    // from the corresponding follower indicates the progress is active.
    // RecentActive can be reset to false after an election timeout.
    recent_active: bool,

    // Inflights is a sliding window for the inflight messages.
    // When inflights is full, no more message should be sent.
    // When a leader sends out a message, the index of the last
    // entry should be added to inflights. The index MUST be added
    // into inflights in order.
    // When a leader receives a reply, the previous inflights should
    // be freed by calling inflights.freeTo.
    ins: Inflights,
}


impl Progress {
    pub fn reset_state(&mut self, state: ProgressState) {
        self.paused = false;
        self.recent_active = false;
        self.pending_snapshot = 0;
        self.state = state;
        self.ins.reset();
    }

    pub fn become_probe(&mut self) {
        // If the original state is ProgressStateSnapshot, progress knows that
        // the pending snapshot has been sent to this peer successfully, then
        // probes from pendingSnapshot + 1.
        if self.state == ProgressState::Snapshot {
            let pending_snapshot = self.pending_snapshot;
            self.reset_state(ProgressState::Probe);
            self.next_idx = cmp::max(self.matched + 1, pending_snapshot + 1);
        } else {
            self.reset_state(ProgressState::Probe);
            self.next_idx = self.matched + 1;
        }
    }

    pub fn become_replicate(&mut self) {
        self.reset_state(ProgressState::Replicate);
        self.next_idx = self.matched + 1;
    }

    pub fn become_snapshot(&mut self, snapshot_idx: u64) {
        self.reset_state(ProgressState::Snapshot);
        self.pending_snapshot = snapshot_idx;
    }

    pub fn snapshot_failure(&self) -> bool {
        self.pending_snapshot == 0
    }

    // maybe_snapshot_abort unsets pendingSnapshot if Match is equal or higher than
    // the pendingSnapshot
    pub fn maybe_snapshot_abort(&self) -> bool {
        self.state == ProgressState::Snapshot && self.matched >= self.pending_snapshot
    }

    // maybe_update returns false if the given n index comes from an outdated message.
    // Otherwise it updates the progress and returns true.
    pub fn maybe_update(&mut self, n: u64) -> bool {
        let mut updated = false;
        if self.matched < n {
            self.matched = n;
            updated = true;
            self.resume()
        }

        if self.next_idx < n + 1 {
            self.next_idx = n + 1
        }

        updated
    }

    pub fn optimistic_update(&mut self, n: u64) {
        self.next_idx = n + 1;
    }

    // maybe_decr_to returns false if the given to index comes from an out of order message.
    // Otherwise it decreases the progress next index to min(rejected, last) and returns true.
    pub fn maybe_decr_to(&mut self, rejected: u64, last: u64) -> bool {
        if self.state == ProgressState::Replicate {
            // the rejection must be stale if the progress has matched and "rejected"
            // is smaller than "match".
            if rejected <= self.matched {
                return false;
            }
            self.next_idx = self.matched + 1;
            return true;
        }

        // the rejection must be stale if "rejected" does not match next - 1
        if self.next_idx - 1 != rejected {
            return false;
        }

        self.next_idx = cmp::min(rejected, last + 1);
        if self.next_idx < 1 {
            self.next_idx = 1;
        }
        self.resume();
        true
    }

    pub fn is_paused(&self) -> bool {
        match self.state {
            ProgressState::Probe => self.paused,
            ProgressState::Replicate => self.ins.full(),
            ProgressState::Snapshot => true,
        }
    }

    pub fn resume(&mut self) {
        self.paused = false;
    }

    pub fn pause(&mut self) {
        self.paused = true;
    }
}


#[derive(Debug, Default)]
struct Inflights {
    // the starting index in the buffer
    start: usize,
    // number of inflights in the buffer
    count: usize,

    // ring buffer
    buffer: Vec<u64>,
}

impl Inflights {
    pub fn new(cap: usize) -> Inflights {
        Inflights { buffer: Vec::with_capacity(cap), ..Default::default() }
    }

    // full returns true if the inflights is full.
    pub fn full(&self) -> bool {
        self.count == self.cap()
    }

    pub fn cap(&self) -> usize {
        self.buffer.capacity()
    }

    // add adds an inflight into inflights
    pub fn add(&mut self, inflight: u64) {
        if self.full() {
            panic!("cannot add into a full inflights")
        }

        let mut next = self.start + self.count;
        if next >= self.cap() {
            next -= self.cap();
        }
        self.buffer[next] = inflight;
        self.count += 1;
    }

    // free_to frees the inflights smaller or equal to the given `to` flight.
    pub fn free_to(&mut self, to: u64) {
        if self.count == 0 || to < self.buffer[self.start] {
            // out of the left side of the window
            return;
        }

        let mut i = 0usize;
        let mut idx = self.start;
        while i < self.count {
            if to < self.buffer[idx] {
                // found the first large inflight
                break;
            }


            // increase index and maybe rotate
            idx += 1;
            if idx >= self.cap() {
                idx -= self.cap();
            }

            i += 1;
        }

        // free i inflights and set new start index
        self.count -= i;
        self.start = idx;
    }

    pub fn free_first_one(&mut self) {
        let start = self.start;
        self.free_to(start);
    }


    // resets frees all inflights.
    pub fn reset(&mut self) {
        self.count = 0;
        self.start = 0;
    }
}
