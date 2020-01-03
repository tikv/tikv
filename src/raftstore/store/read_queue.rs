// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::collections::VecDeque;
use std::{cmp, usize};

use crate::raftstore::store::fsm::apply;
use crate::raftstore::store::metrics::*;
use crate::raftstore::store::Callback;

use engine_rocks::RocksEngine;
use kvproto::raft_cmdpb::RaftCmdRequest;
use tikv_util::collections::HashMap;
use tikv_util::time::{duration_to_sec, monotonic_raw_now};
use tikv_util::MustConsumeVec;
use time::Timespec;
use uuid::Uuid;

const READ_QUEUE_SHRINK_SIZE: usize = 64;

pub(super) struct ReadIndexRequest {
    pub id: Uuid,
    pub cmds: MustConsumeVec<(RaftCmdRequest, Callback<RocksEngine>)>,
    pub renew_lease_time: Timespec,
    pub read_index: Option<u64>,
}

impl ReadIndexRequest {
    // Transmutes `self.id` to a 8 bytes slice, so that we can use the payload to do read index.
    pub(super) fn binary_id(&self) -> &[u8] {
        self.id.as_bytes()
    }

    pub(super) fn push_command(&mut self, req: RaftCmdRequest, cb: Callback<RocksEngine>) {
        RAFT_READ_INDEX_PENDING_COUNT.inc();
        self.cmds.push((req, cb));
    }

    pub(super) fn with_command(
        id: Uuid,
        req: RaftCmdRequest,
        cb: Callback<RocksEngine>,
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
pub(super) struct ReadIndexQueue {
    reads: VecDeque<ReadIndexRequest>,
    ready_cnt: usize,
    // How many requests are handled.
    handled_cnt: usize,
    // map[uuid] -> offset in `reads`.
    contexts: HashMap<Uuid, usize>,
}

impl ReadIndexQueue {
    pub(super) fn on_stop(&mut self) {
        for mut read in self.reads.drain(..) {
            RAFT_READ_INDEX_PENDING_COUNT.sub(read.cmds.len() as i64);
            read.cmds.clear();
        }
        self.contexts.clear();
        self.ready_cnt = 0;
        self.handled_cnt = 0;
    }

    pub(super) fn notify_all_removed(&mut self, region_id: u64) {
        for mut read in self.reads.drain(..) {
            RAFT_READ_INDEX_PENDING_COUNT.sub(read.cmds.len() as i64);
            for (_, cb) in read.cmds.drain(..) {
                apply::notify_req_region_removed(region_id, cb);
            }
        }
        self.contexts.clear();
        self.ready_cnt = 0;
        self.handled_cnt = 0;
    }

    pub(super) fn clear_uncommitted(&mut self, term: u64) {
        for mut read in self.reads.drain(self.ready_cnt..) {
            self.contexts.remove(&read.id);
            RAFT_READ_INDEX_PENDING_COUNT.sub(read.cmds.len() as i64);
            for (_, cb) in read.cmds.drain(..) {
                apply::notify_stale_req(term, cb);
            }
        }
    }

    pub(super) fn push_back(&mut self, read: ReadIndexRequest, is_leader: bool) {
        if is_leader {
            let offset = self.handled_cnt + self.reads.len();
            self.contexts.insert(read.id, offset);
        }
        self.reads.push_back(read);
    }

    pub(super) fn back_mut(&mut self) -> Option<&mut ReadIndexRequest> {
        self.reads.back_mut()
    }

    pub(super) fn advance_leader_reads<T: IntoIterator<Item = (Uuid, u64)>>(
        &mut self,
        states: T,
    ) -> Option<Timespec> {
        let mut ts = None;
        for (uuid, index) in states {
            assert_eq!(uuid, self.reads[self.ready_cnt].id);
            self.reads[self.ready_cnt].read_index = Some(index);
            ts = Some(self.reads[self.ready_cnt].renew_lease_time);
            self.ready_cnt += 1;
        }
        ts
    }

    /// update the read index of the requests that before the specified id.
    pub(super) fn advance_replica_reads<T: IntoIterator<Item = (Uuid, u64)>>(&mut self, states: T) {
        let (mut min_changed_offset, mut max_changed_offset) = (usize::MAX, 0);
        for (uuid, index) in states {
            if let Some(offset) = self.contexts.remove(&uuid) {
                let offset = offset.checked_sub(self.handled_cnt).unwrap();
                self.ready_cnt = cmp::max(self.ready_cnt, offset + 1);

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

        if self.ready_cnt > 1 && max_changed_offset > 0 {
            self.fold(min_changed_offset, max_changed_offset);
        }
    }

    pub(super) fn fold(&mut self, mut min_changed_offset: usize, max_changed_offset: usize) {
        let mut r_offset = max_changed_offset;
        while r_offset >= min_changed_offset && r_offset > 0 {
            let r_idx = self.reads[r_offset].read_index.unwrap();
            let l_offset = r_offset - 1;
            if self.reads[l_offset]
                .read_index
                .map_or(true, |l_idx| l_idx > r_idx)
            {
                self.reads[l_offset].read_index = Some(r_idx);
                // Update `min_changed_offset`, it's required.
                min_changed_offset = cmp::min(min_changed_offset, l_offset);
            }
            r_offset -= 1;
        }
    }

    pub(super) fn gc(&mut self) {
        if self.reads.capacity() > READ_QUEUE_SHRINK_SIZE
            && self.reads.len() < READ_QUEUE_SHRINK_SIZE
        {
            self.reads.shrink_to_fit();
            self.contexts.shrink_to_fit();
        }
    }

    pub(super) fn pop_front(&mut self) -> Option<ReadIndexRequest> {
        if self.ready_cnt == 0 {
            return None;
        }
        self.ready_cnt -= 1;
        self.handled_cnt += 1;
        self.reads.pop_front()
    }

    /// Raft could have not been ready to handle the poped task. So put it back into the queue.
    pub(super) fn push_front(&mut self, read: ReadIndexRequest) {
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

        std::mem::forget(queue);
    }
}
