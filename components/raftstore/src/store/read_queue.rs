// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

// #[PerformanceCriticalPath]
use std::{cmp, collections::VecDeque, mem, u64, usize};

use collections::HashMap;
use engine_traits::Snapshot;
use kvproto::{
    kvrpcpb::LockInfo,
    raft_cmdpb::{self, RaftCmdRequest},
};
use protobuf::Message;
use tikv_util::{
    box_err,
    codec::number::{NumberEncoder, MAX_VAR_U64_LEN},
    debug, error,
    memory::HeapSize,
    time::{duration_to_sec, monotonic_raw_now},
    MustConsumeVec,
};
use time::Timespec;
use uuid::Uuid;

use crate::{
    store::{fsm::apply, metrics::*, Callback, Config},
    Result,
};

const READ_QUEUE_SHRINK_SIZE: usize = 64;

pub struct ReadIndexRequest<S>
where
    S: Snapshot,
{
    pub id: Uuid,
    cmds: MustConsumeVec<(RaftCmdRequest, Callback<S>, Option<u64>)>,
    pub propose_time: Timespec,
    pub read_index: Option<u64>,
    pub addition_request: Option<Box<raft_cmdpb::ReadIndexRequest>>,
    pub locked: Option<Box<LockInfo>>,
    // `true` means it's in `ReadIndexQueue::reads`.
    in_contexts: bool,

    cmds_heap_size: usize,
}

impl<S> ReadIndexRequest<S>
where
    S: Snapshot,
{
    const CMD_SIZE: usize = mem::size_of::<(RaftCmdRequest, Callback<S>, Option<u64>)>();

    pub fn push_command(&mut self, req: RaftCmdRequest, cb: Callback<S>, read_index: u64) {
        RAFT_READ_INDEX_PENDING_COUNT.inc();
        self.cmds_heap_size += req.heap_size();
        self.cmds.push((req, cb, Some(read_index)));
    }

    pub fn with_command(
        id: Uuid,
        req: RaftCmdRequest,
        cb: Callback<S>,
        propose_time: Timespec,
    ) -> Self {
        RAFT_READ_INDEX_PENDING_COUNT.inc();

        // Ignore heap allocations for `Callback`.
        let cmds_heap_size = req.heap_size();

        let mut cmds = MustConsumeVec::with_capacity("callback of index read", 1);
        cmds.push((req, cb, None));
        ReadIndexRequest {
            id,
            cmds,
            propose_time,
            read_index: None,
            addition_request: None,
            locked: None,
            in_contexts: false,
            cmds_heap_size,
        }
    }

    pub fn cmds(&self) -> &[(RaftCmdRequest, Callback<S>, Option<u64>)] {
        &*self.cmds
    }

    pub fn take_cmds(&mut self) -> MustConsumeVec<(RaftCmdRequest, Callback<S>, Option<u64>)> {
        self.cmds_heap_size = 0;
        self.cmds.take()
    }
}

impl<S> Drop for ReadIndexRequest<S>
where
    S: Snapshot,
{
    fn drop(&mut self) {
        let dur = (monotonic_raw_now() - self.propose_time).to_std().unwrap();
        RAFT_READ_INDEX_PENDING_DURATION.observe(duration_to_sec(dur));
    }
}

pub struct ReadIndexQueue<S>
where
    S: Snapshot,
{
    reads: VecDeque<ReadIndexRequest<S>>,
    ready_cnt: usize,
    // How many requests are handled.
    handled_cnt: usize,
    // map[uuid] -> offset in `reads`.
    contexts: HashMap<Uuid, usize>,

    retry_countdown: usize,
}

impl<S> Default for ReadIndexQueue<S>
where
    S: Snapshot,
{
    fn default() -> ReadIndexQueue<S> {
        ReadIndexQueue {
            reads: VecDeque::new(),
            ready_cnt: 0,
            handled_cnt: 0,
            contexts: HashMap::default(),
            retry_countdown: 0,
        }
    }
}

impl<S> ReadIndexQueue<S>
where
    S: Snapshot,
{
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
                for (_, cb, _) in read.cmds.drain(..) {
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
            for (_, cb, _) in read.cmds.drain(..) {
                apply::notify_stale_req(term, cb);
            }
        }
        RAFT_READ_INDEX_PENDING_COUNT.sub(removed as i64);
        // For a follower changes to leader, and then changes to followr again.
        self.contexts.clear();
    }

    pub fn push_back(&mut self, mut read: ReadIndexRequest<S>, is_leader: bool) {
        if !is_leader {
            read.in_contexts = true;
            let offset = self.handled_cnt + self.reads.len();
            self.contexts.insert(read.id, offset);
        }
        self.reads.push_back(read);
        self.retry_countdown = usize::MAX;
    }

    pub fn back_mut(&mut self) -> Option<&mut ReadIndexRequest<S>> {
        self.reads.back_mut()
    }

    pub fn back(&self) -> Option<&ReadIndexRequest<S>> {
        self.reads.back()
    }

    pub fn last_ready(&self) -> Option<&ReadIndexRequest<S>> {
        if self.ready_cnt > 0 {
            return Some(&self.reads[self.ready_cnt - 1]);
        }
        None
    }

    pub fn advance_leader_reads<T>(&mut self, tag: &str, states: T)
    where
        T: IntoIterator<Item = (Uuid, Option<LockInfo>, u64)>,
    {
        let mut states_iter = states.into_iter();
        while let Some((uuid, info, index)) = states_iter.next() {
            let invalid_id = match self.reads.get_mut(self.ready_cnt) {
                Some(r) if r.id == uuid => {
                    r.read_index = Some(index);
                    self.ready_cnt += 1;
                    continue;
                }
                Some(r) => Some((r.id, r.propose_time)),
                None => None,
            };

            error!("{} unexpected uuid detected", tag; "current_id" => ?invalid_id);
            let mut expect_id_track = vec![];
            for i in (0..self.ready_cnt).rev().take(10).rev() {
                expect_id_track.push((i, self.reads.get(i).map(|r| (r.id, r.propose_time))));
            }
            for i in (self.ready_cnt..self.reads.len()).take(10) {
                expect_id_track.push((i, self.reads.get(i).map(|r| (r.id, r.propose_time))));
            }
            let mut actual_id_track = vec![(uuid, info.is_some(), index)];
            for (id, info, index) in states_iter.take(20) {
                actual_id_track.push((id, info.is_some(), index));
            }
            error!("context around"; "expect_id_track" => ?expect_id_track, "actual_id_track" => ?actual_id_track);
            panic!(
                "{} unexpected uuid detected {} != {:?} at {}",
                tag, uuid, invalid_id, self.ready_cnt
            );
        }
    }

    /// update the read index of the requests that before the specified id.
    pub fn advance_replica_reads<T>(&mut self, states: T)
    where
        T: IntoIterator<Item = (Uuid, Option<LockInfo>, u64)>,
    {
        let (mut min_changed_offset, mut max_changed_offset) = (usize::MAX, 0);
        for (uuid, locked, index) in states {
            if let Some(raw_offset) = self.contexts.remove(&uuid) {
                let offset = match raw_offset.checked_sub(self.handled_cnt) {
                    Some(offset) => offset,
                    None => panic!(
                        "advance_replica_reads uuid: {}, offset: {}, handled: {}",
                        uuid, raw_offset, self.handled_cnt
                    ),
                };
                assert_eq!(
                    self.reads[offset].id, uuid,
                    "ReadIndexQueue::reads[{}].uuid: {}, but want: {}",
                    raw_offset, self.reads[offset].id, uuid
                );
                self.reads[offset].in_contexts = false;
                // clear addition_request to indicate lock checking has finished
                self.reads[offset].addition_request = None;
                self.reads[offset].locked = locked.map(Box::new);
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

    fn fold(&mut self, min_changed_offset: usize, max_changed_offset: usize) {
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

    pub fn pop_front(&mut self) -> Option<ReadIndexRequest<S>> {
        if self.ready_cnt == 0 {
            return None;
        }
        self.ready_cnt -= 1;
        self.handled_cnt += 1;
        let mut res = self
            .reads
            .pop_front()
            .expect("read_queue is empty but ready_cnt > 0");
        if res.in_contexts {
            res.in_contexts = false;
            self.contexts.remove(&res.id);
        }
        Some(res)
    }

    /// Raft could have not been ready to handle the poped task. So put it back into the queue.
    pub fn push_front(&mut self, read: ReadIndexRequest<S>) {
        debug_assert!(read.read_index.is_some());
        self.reads.push_front(read);
        self.ready_cnt += 1;
        self.handled_cnt -= 1;
    }
}

const UUID_LEN: usize = 16;
const REQUEST_FLAG: u8 = b'r';
const LOCKED_FLAG: u8 = b'l';

#[derive(Debug, Clone, PartialEq)]
pub struct ReadIndexContext {
    pub id: Uuid,
    pub request: Option<raft_cmdpb::ReadIndexRequest>,
    pub locked: Option<LockInfo>,
}

impl ReadIndexContext {
    pub fn parse(bytes: &[u8]) -> Result<ReadIndexContext> {
        use tikv_util::codec::number::*;

        if bytes.len() < UUID_LEN {
            return Err(box_err!(
                "read index context must contain a {} byte long UUID",
                UUID_LEN
            ));
        }
        let mut res = ReadIndexContext {
            id: Uuid::from_slice(&bytes[..UUID_LEN]).unwrap(),
            request: None,
            locked: None,
        };
        let mut bytes = &bytes[UUID_LEN..];
        while !bytes.is_empty() {
            match read_u8(&mut bytes).unwrap() {
                REQUEST_FLAG => {
                    let len = decode_var_u64(&mut bytes)? as usize;
                    let mut request = raft_cmdpb::ReadIndexRequest::default();
                    request.merge_from_bytes(&bytes[..len])?;
                    bytes = &bytes[len..];
                    res.request = Some(request);
                }
                LOCKED_FLAG => {
                    let len = decode_var_u64(&mut bytes)? as usize;
                    let mut locked = LockInfo::default();
                    locked.merge_from_bytes(&bytes[..len])?;
                    bytes = &bytes[len..];
                    res.locked = Some(locked);
                }
                // just break for forward compatibility
                _ => break,
            }
        }
        Ok(res)
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        Self::fields_to_bytes(self.id, self.request.as_ref(), self.locked.as_ref())
    }

    pub fn fields_to_bytes(
        id: Uuid,
        request: Option<&raft_cmdpb::ReadIndexRequest>,
        locked: Option<&LockInfo>,
    ) -> Vec<u8> {
        let request_size = request.map(Message::compute_size);
        let locked_size = locked.map(Message::compute_size);
        let field_size = |s: Option<u32>| s.map(|s| 1 + MAX_VAR_U64_LEN + s as usize).unwrap_or(0);
        let cap = UUID_LEN + field_size(request_size) + field_size(locked_size);
        let mut b = Vec::with_capacity(cap);
        b.extend_from_slice(id.as_bytes());
        if let Some(request) = request {
            b.push(REQUEST_FLAG);
            b.encode_var_u64(request_size.unwrap() as u64).unwrap();
            request.write_to_vec(&mut b).unwrap();
        }
        if let Some(locked) = locked {
            b.push(LOCKED_FLAG);
            b.encode_var_u64(locked_size.unwrap() as u64).unwrap();
            locked.write_to_vec(&mut b).unwrap();
        }
        b
    }
}

mod memtrace {
    use tikv_util::memory::HeapSize;

    use super::*;

    impl<S> HeapSize for ReadIndexRequest<S>
    where
        S: Snapshot,
    {
        fn heap_size(&self) -> usize {
            let mut size = self.cmds_heap_size + Self::CMD_SIZE * self.cmds.capacity();
            if let Some(ref add) = self.addition_request {
                size += add.heap_size();
            }
            size
        }
    }

    impl<S> HeapSize for ReadIndexQueue<S>
    where
        S: Snapshot,
    {
        #[inline]
        fn heap_size(&self) -> usize {
            let mut size = self.reads.capacity() * mem::size_of::<ReadIndexRequest<S>>()
                // For one Uuid and one usize.
                + 24 * self.contexts.len();
            for read in &self.reads {
                size += read.heap_size();
            }
            size
        }
    }
}

#[cfg(test)]
mod read_index_ctx_tests {
    use super::*;

    // Test backward compatibility
    #[test]
    fn test_single_uuid() {
        let id = Uuid::new_v4();
        // We should be able to parse old version context (a single UUID).
        let ctx = ReadIndexContext::parse(id.as_bytes()).unwrap();
        assert_eq!(
            ctx,
            ReadIndexContext {
                id,
                request: None,
                locked: None,
            }
        );

        // Old version TiKV should be able to parse context without lock checking fields.
        let bytes = ctx.to_bytes();
        assert_eq!(bytes, id.as_bytes());
    }

    #[test]
    fn test_serde_request() {
        let id = Uuid::new_v4();
        let mut request = raft_cmdpb::ReadIndexRequest::default();
        request.set_start_ts(10);
        let mut locked = LockInfo::default();
        locked.set_lock_version(5);
        let ctx = ReadIndexContext {
            id,
            request: Some(request),
            locked: Some(locked),
        };
        let bytes = ctx.to_bytes();
        let parsed_ctx = ReadIndexContext::parse(&bytes).unwrap();
        assert_eq!(ctx, parsed_ctx);
    }
}

#[cfg(test)]
mod tests {
    use engine_test::kv::KvTestSnapshot;

    use super::*;

    #[test]
    fn test_read_queue_fold() {
        let mut queue = ReadIndexQueue::<KvTestSnapshot> {
            handled_cnt: 125,
            ..Default::default()
        };
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

        queue.advance_replica_reads(Vec::new());
        assert_eq!(queue.ready_cnt, 0);

        queue.advance_replica_reads(vec![(queue.reads[0].id, None, 100)]);
        assert_eq!(queue.ready_cnt, 1);

        queue.advance_replica_reads(vec![(queue.reads[1].id, None, 100)]);
        assert_eq!(queue.ready_cnt, 2);

        queue.advance_replica_reads(vec![
            (queue.reads[80].id, None, 80),
            (queue.reads[84].id, None, 100),
            (queue.reads[82].id, None, 70),
            (queue.reads[78].id, None, 120),
            (queue.reads[77].id, None, 40),
        ]);
        assert_eq!(queue.ready_cnt, 85);

        queue.advance_replica_reads(vec![
            (queue.reads[20].id, None, 80),
            (queue.reads[24].id, None, 100),
            (queue.reads[22].id, None, 70),
            (queue.reads[18].id, None, 120),
            (queue.reads[17].id, None, 40),
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
    fn test_become_leader_then_become_follower() {
        let mut queue = ReadIndexQueue::<KvTestSnapshot> {
            handled_cnt: 100,
            ..Default::default()
        };

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
        queue.advance_leader_reads("", vec![(id, None, 10)]);
        while let Some(mut read) = queue.pop_front() {
            read.cmds.clear();
        }

        queue.clear_uncommitted_on_role_change(10);

        let req = ReadIndexRequest::with_command(
            Uuid::new_v4(),
            RaftCmdRequest::default(),
            Callback::None,
            Timespec::new(0, 0),
        );
        queue.push_back(req, true);
        let last_id = queue.reads.back().map(|t| t.id).unwrap();
        queue.advance_leader_reads("", vec![(last_id, None, 10)]);
        assert_eq!(queue.ready_cnt, 1);
        while let Some(mut read) = queue.pop_front() {
            read.cmds.clear();
        }

        // Shouldn't panic when call `advance_replica_reads` with `id` again.
        queue.advance_replica_reads(vec![(id, None, 10)]);
    }

    #[test]
    fn test_retake_leadership() {
        let mut queue = ReadIndexQueue::<KvTestSnapshot> {
            handled_cnt: 100,
            ..Default::default()
        };

        // Push a pending read comand when the peer is leader.
        let id = Uuid::new_v4();
        let req = ReadIndexRequest::with_command(
            id,
            RaftCmdRequest::default(),
            Callback::None,
            Timespec::new(0, 0),
        );
        queue.push_back(req, true);

        // Advance on leader, but the peer is not ready to handle it (e.g. it's in merging).
        queue.advance_leader_reads("", vec![(id, None, 10)]);

        // The leader steps down to follower, clear uncommitted reads.
        queue.clear_uncommitted_on_role_change(10);

        // The peer takes leadership and handles one more read.
        queue.clear_uncommitted_on_role_change(10);
        let id_1 = Uuid::new_v4();
        let req = ReadIndexRequest::with_command(
            id_1,
            RaftCmdRequest::default(),
            Callback::None,
            Timespec::new(0, 0),
        );
        queue.push_back(req, true);

        // Advance on leader again, shouldn't panic.
        queue.advance_leader_reads("", vec![(id_1, None, 10)]);
        while let Some(mut read) = queue.pop_front() {
            read.cmds.clear();
        }
    }

    #[test]
    fn test_advance_replica_reads_out_of_order() {
        let mut queue = ReadIndexQueue::<KvTestSnapshot> {
            handled_cnt: 100,
            ..Default::default()
        };

        let ids: [Uuid; 2] = [Uuid::new_v4(), Uuid::new_v4()];
        for id in &ids {
            // Push a pending read comand when the peer is follower.
            let req = ReadIndexRequest::with_command(
                *id,
                RaftCmdRequest::default(),
                Callback::None,
                Timespec::new(0, 0),
            );
            queue.push_back(req, false);
        }

        queue.advance_replica_reads(vec![(ids[1], None, 100)]);
        assert_eq!(queue.ready_cnt, 2);
        while let Some(mut read) = queue.pop_front() {
            read.cmds.clear();
        }

        queue.advance_replica_reads(vec![(ids[0], None, 100)]);
    }
}
