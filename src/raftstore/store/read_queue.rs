// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::collections::VecDeque;
use std::{cmp, u64, usize};

use crate::raftstore::store::fsm::apply;
use crate::raftstore::store::metrics::*;
use crate::raftstore::store::{Callback, Config};

use kvproto::raft_cmdpb::RaftCmdRequest;
use tikv_util::collections::HashMap;
use tikv_util::time::{duration_to_sec, monotonic_raw_now};
use tikv_util::MustConsumeVec;
use time::Timespec;
use uuid::Uuid;

const READ_QUEUE_SHRINK_SIZE: usize = 64;

pub struct ReadIndexRequest {
    pub id: Uuid,
    pub cmds: MustConsumeVec<(RaftCmdRequest, Callback)>,
    pub renew_lease_time: Timespec,
    pub read_index: Option<u64>,
}

impl ReadIndexRequest {
    // Transmutes `self.id` to a 8 bytes slice, so that we can use the payload to do read index.
    pub fn binary_id(&self) -> &[u8] {
        self.id.as_bytes()
    }

    pub fn push_command(&mut self, req: RaftCmdRequest, cb: Callback) {
        RAFT_READ_INDEX_PENDING_COUNT.inc();
        self.cmds.push((req, cb));
    }

    pub fn with_command(
        id: Uuid,
        req: RaftCmdRequest,
        cb: Callback,
        renew_lease_time: Timespec,
    ) -> Self {
        RAFT_READ_INDEX_PENDING_COUNT.inc();
        let mut cmds = MustConsumeVec::with_capacity("callback of index read", 1);
        cmds.push((req, cb));
        ReadIndexRequest {
            id,
            cmds,
            renew_lease_time,
            read_index: None,
        }
    }
}

impl Drop for ReadIndexRequest {
    fn drop(&mut self) {
        let dur = (monotonic_raw_now() - self.renew_lease_time)
            .to_std()
            .unwrap();
        RAFT_READ_INDEX_PENDING_DURATION.observe(duration_to_sec(dur));
    }
}

#[derive(Default)]
pub struct ReadIndexQueue {
    reads: VecDeque<ReadIndexRequest>,
    ready_cnt: usize,
    // How many requests are handled.
    handled_cnt: usize,
    // map[uuid] -> offset in `reads`.
    contexts: HashMap<Uuid, usize>,

    retry_countdown: usize,
}

impl ReadIndexQueue {
    /// Check it's necessary to retry pending read requests or not.
    /// Return true if all such conditions are satisfied:
    /// 1. more than an election timeout elapsed from the last request push;
    /// 2. more than an election timeout elapsed from the last retry;
    /// 3. there are still unresolved requests in the queue.
    pub fn check_needs_retry(&mut self, cfg: &Config) -> bool {
        if self.reads.len() == self.ready_cnt {
            return false;
        }

        if self.retry_countdown == usize::MAX {
            self.retry_countdown = cfg.raft_election_timeout_ticks - 1;
            return false;
        }

        if self.retry_countdown > 0 {
            self.retry_countdown -= 1;
            return false;
        }

        self.retry_countdown = cfg.raft_election_timeout_ticks;
        true
    }

    pub fn has_unresolved(&self) -> bool {
        self.ready_cnt != self.reads.len()
    }

    /// Clear all commands in the queue. if `notify_removed` contains an `region_id`,
    /// notify the request's callback that the region is removed.
    pub fn clear_all(&mut self, notify_removed: Option<u64>) {
        let mut removed = 0;
        for mut read in self.reads.drain(..) {
            removed += read.cmds.len();
            if let Some(region_id) = notify_removed {
                for (_, cb) in read.cmds.drain(..) {
                    apply::notify_req_region_removed(region_id, cb);
                }
            } else {
                read.cmds.clear();
            }
        }
        RAFT_READ_INDEX_PENDING_COUNT.sub(removed as i64);
        self.contexts.clear();
        self.ready_cnt = 0;
        self.handled_cnt = 0;
    }

    pub fn clear_uncommitted_on_role_change(&mut self, term: u64) {
        let mut removed = 0;
        for mut read in self.reads.drain(self.ready_cnt..) {
            removed += read.cmds.len();
            for (_, cb) in read.cmds.drain(..) {
                apply::notify_stale_req(term, cb);
            }
        }
        RAFT_READ_INDEX_PENDING_COUNT.sub(removed as i64);
        // For a follower changes to leader, and then changes to followr again.
        self.contexts.clear();
    }

    pub fn push_back(&mut self, read: ReadIndexRequest, is_leader: bool) {
        if !is_leader {
            let offset = self.handled_cnt + self.reads.len();
            self.contexts.insert(read.id, offset);
        }
        self.reads.push_back(read);
        self.retry_countdown = usize::MAX;
    }

    pub fn back_mut(&mut self) -> Option<&mut ReadIndexRequest> {
        self.reads.back_mut()
    }

    pub fn advance_leader_reads<T>(&mut self, states: T) -> Option<Timespec>
    where
        T: IntoIterator<Item = (Uuid, u64)>,
    {
        let mut ts = None;
        for (uuid, index) in states {
            assert_eq!(uuid, self.reads[self.ready_cnt].id);
            self.reads[self.ready_cnt].read_index = Some(index);
            ts = Some(self.reads[self.ready_cnt].renew_lease_time);
            self.ready_cnt += 1;
        }
        ts
    }

    /// A fast path for `advance_leader_reads` then `pop_front`.
    pub fn advance_leader_read_and_pop(&mut self, uuid: Uuid, index: u64) -> ReadIndexRequest {
        let mut read = self.reads.pop_front().unwrap();
        self.handled_cnt += 1;
        assert_eq!(uuid, read.id);
        read.read_index = Some(index);
        read
    }

    /// update the read index of the requests that before the specified id.
    pub fn advance_replica_reads<T>(&mut self, states: T)
    where
        T: IntoIterator<Item = (Uuid, u64)>,
    {
        let (mut min_changed_offset, mut max_changed_offset) = (usize::MAX, 0);
        for (uuid, index) in states {
            if let Some(offset) = self.contexts.remove(&uuid) {
                let offset = offset.checked_sub(self.handled_cnt).unwrap();
                if let Some(occur_index) = self.reads[offset].read_index {
                    if occur_index < index {
                        continue;
                    }
                }
                self.reads[offset].read_index = Some(index);
                min_changed_offset = cmp::min(min_changed_offset, offset);
                max_changed_offset = cmp::max(max_changed_offset, offset);
                continue;
            }
            debug!(
                "cannot find corresponding read from pending reads";
                "uuid" => ?uuid, "read-index" => index,
            );
        }

        if min_changed_offset != usize::MAX {
            self.ready_cnt = cmp::max(self.ready_cnt, max_changed_offset + 1);
        }
        if max_changed_offset > 0 {
            self.fold(min_changed_offset, max_changed_offset);
        }
    }

    pub fn fold(&mut self, min_changed_offset: usize, max_changed_offset: usize) {
        let mut r_idx = self.reads[max_changed_offset].read_index.unwrap();
        let mut check_offset = max_changed_offset - 1;
        loop {
            let l_idx = self.reads[check_offset].read_index.unwrap_or(u64::MAX);
            if l_idx > r_idx {
                self.reads[check_offset].read_index = Some(r_idx);
            } else if check_offset < min_changed_offset {
                break;
            } else {
                r_idx = l_idx;
            }
            if check_offset == 0 {
                break;
            }
            check_offset -= 1;
        }
    }

    pub fn gc(&mut self) {
        if self.reads.capacity() > READ_QUEUE_SHRINK_SIZE
            && self.reads.len() < READ_QUEUE_SHRINK_SIZE
        {
            self.reads.shrink_to_fit();
            self.contexts.shrink_to_fit();
        }
    }

    pub fn pop_front(&mut self) -> Option<ReadIndexRequest> {
        if self.ready_cnt == 0 {
            return None;
        }
        self.ready_cnt -= 1;
        self.handled_cnt += 1;
        self.reads.pop_front()
    }

    /// Raft could have not been ready to handle the poped task. So put it back into the queue.
    pub fn push_front(&mut self, read: ReadIndexRequest) {
        debug_assert!(read.read_index.is_some());
        self.reads.push_front(read);
        self.ready_cnt += 1;
        self.handled_cnt -= 1;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_read_queue_fold() {
        let mut queue = ReadIndexQueue::default();
        queue.handled_cnt = 125;
        for _ in 0..100 {
            let id = Uuid::new_v4();
            queue.reads.push_back(ReadIndexRequest::with_command(
                id,
                RaftCmdRequest::default(),
                Callback::None,
                Timespec::new(0, 0),
            ));

            let offset = queue.handled_cnt + queue.reads.len() - 1;
            queue.contexts.insert(id, offset);
        }

        queue.advance_replica_reads(Vec::<(Uuid, u64)>::default());
        assert_eq!(queue.ready_cnt, 0);

        queue.advance_replica_reads(vec![(queue.reads[0].id, 100)]);
        assert_eq!(queue.ready_cnt, 1);

        queue.advance_replica_reads(vec![(queue.reads[1].id, 100)]);
        assert_eq!(queue.ready_cnt, 2);

        queue.advance_replica_reads(vec![
            (queue.reads[80].id, 80),
            (queue.reads[84].id, 100),
            (queue.reads[82].id, 70),
            (queue.reads[78].id, 120),
            (queue.reads[77].id, 40),
        ]);
        assert_eq!(queue.ready_cnt, 85);

        queue.advance_replica_reads(vec![
            (queue.reads[20].id, 80),
            (queue.reads[24].id, 100),
            (queue.reads[22].id, 70),
            (queue.reads[18].id, 120),
            (queue.reads[17].id, 40),
        ]);
        assert_eq!(queue.ready_cnt, 85);

        for i in 0..78 {
            assert_eq!(queue.reads[i].read_index.unwrap(), 40, "#{} failed", i);
        }
        for i in 78..83 {
            assert_eq!(queue.reads[i].read_index.unwrap(), 70, "#{} failed", i);
        }
        for i in 84..85 {
            assert_eq!(queue.reads[i].read_index.unwrap(), 100, "#{} failed", i);
        }

        queue.clear_all(None);
    }

    #[test]
    fn test_role_change() {
        let mut queue = ReadIndexQueue::default();
        queue.handled_cnt = 100;

        // Push a pending comand when the peer is follower.
        let id = Uuid::new_v4();
        let req = ReadIndexRequest::with_command(
            id,
            RaftCmdRequest::default(),
            Callback::None,
            Timespec::new(0, 0),
        );
        queue.push_back(req, false);

        // After the peer becomes leader, `advance` could be called before
        // `clear_uncommitted_on_role_change`.
        let mut read = queue.advance_leader_read_and_pop(id, 10);
        read.cmds.clear();

        queue.clear_uncommitted_on_role_change(10);

        let req = ReadIndexRequest::with_command(
            Uuid::new_v4(),
            RaftCmdRequest::default(),
            Callback::None,
            Timespec::new(0, 0),
        );
        queue.push_back(req, true);
        let last_id = queue.reads.back().map(|t| t.id).unwrap();
        queue.advance_leader_reads(vec![(last_id, 10)]);
        assert_eq!(queue.ready_cnt, 1);
        while let Some(mut read) = queue.pop_front() {
            read.cmds.clear();
        }

        // Shouldn't panic when call `advance_replica_reads` with `id` again.
        queue.advance_replica_reads(vec![(id, 10)]);
    }
}
