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

use std::collections::BTreeMap;
use std::fmt::{self, Display, Formatter};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::SyncSender;
use std::sync::Arc;
use std::time::{Duration, Instant};

use kvproto::raft_serverpb::{PeerState, RaftApplyState, RegionLocalState};
use raft::eraftpb::Snapshot as RaftSnapshot;
use rocksdb::{Writable, WriteBatch, DB};

use raftstore::store::engine::{Mutable, Snapshot};
use raftstore::store::peer_storage::{
    JOB_STATUS_CANCELLED, JOB_STATUS_CANCELLING, JOB_STATUS_FAILED, JOB_STATUS_FINISHED,
    JOB_STATUS_PENDING, JOB_STATUS_RUNNING,
};
use raftstore::store::snap::{Error, Result};
use raftstore::store::{
    self, check_abort, keys, ApplyOptions, Peekable, SnapEntry, SnapKey, SnapManager,
};
use storage::CF_RAFT;
use util::threadpool::{DefaultContext, ThreadPool, ThreadPoolBuilder};
use util::time;
use util::timer::Timer;
use util::worker::{Runnable, RunnableWithTimer};
use util::{escape, rocksdb};

use super::super::util;
use super::metrics::*;

use std::collections::Bound::{Excluded, Included, Unbounded};

const GENERATE_POOL_SIZE: usize = 2;

// used to periodically check whether we should delete a stale peer's range in region runner
pub const STALE_PEER_CHECK_INTERVAL: u64 = 10_000; // milliseconds

/// region related task.
#[derive(Debug)]
pub enum Task {
    Gen {
        region_id: u64,
        notifier: SyncSender<RaftSnapshot>,
    },
    Apply {
        region_id: u64,
        status: Arc<AtomicUsize>,
    },
    /// Destroy data between [start_key, end_key).
    ///
    /// The deletion may and may not succeed.
    Destroy {
        region_id: u64,
        start_key: Vec<u8>,
        end_key: Vec<u8>,
    },
}

impl Task {
    pub fn destroy(region_id: u64, start_key: Vec<u8>, end_key: Vec<u8>) -> Task {
        Task::Destroy {
            region_id,
            start_key,
            end_key,
        }
    }
}

impl Display for Task {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match *self {
            Task::Gen { region_id, .. } => write!(f, "Snap gen for {}", region_id),
            Task::Apply { region_id, .. } => write!(f, "Snap apply for {}", region_id),
            Task::Destroy {
                region_id,
                ref start_key,
                ref end_key,
            } => write!(
                f,
                "Destroy {} [{}, {})",
                region_id,
                escape(start_key),
                escape(end_key)
            ),
        }
    }
}

#[derive(Clone)]
struct StalePeerInfo {
    // the start_key is stored as a key in PendingDeleteRanges
    // below are stored as a value in PendingDeleteRanges
    pub region_id: u64,
    pub end_key: Vec<u8>,
    pub timeout: time::Instant,
}

#[derive(Clone, Default)]
struct PendingDeleteRanges {
    ranges: BTreeMap<Vec<u8>, StalePeerInfo>, // start_key -> StalePeerInfo
}

impl PendingDeleteRanges {
    // find ranges that overlap with [start_key, end_key)
    fn find_overlap_ranges(
        &self,
        start_key: &[u8],
        end_key: &[u8],
    ) -> Vec<(u64, Vec<u8>, Vec<u8>)> {
        let mut ranges = Vec::new();
        // find the first range that may overlap with [start_key, end_key)
        let sub_range = self.ranges.range((Unbounded, Excluded(start_key.to_vec())));
        if let Some((s_key, peer_info)) = sub_range.last() {
            if peer_info.end_key > start_key.to_vec() {
                ranges.push((
                    peer_info.region_id,
                    s_key.clone(),
                    peer_info.end_key.clone(),
                ));
            }
        }

        // find the rest ranges that overlap with [start_key, end_key)
        for (s_key, peer_info) in self
            .ranges
            .range((Included(start_key.to_vec()), Excluded(end_key.to_vec())))
        {
            ranges.push((
                peer_info.region_id,
                s_key.clone(),
                peer_info.end_key.clone(),
            ));
        }
        ranges
    }

    // get ranges that overlap with [start_key, end_key)
    pub fn drain_overlap_ranges(
        &mut self,
        start_key: &[u8],
        end_key: &[u8],
    ) -> Vec<(u64, Vec<u8>, Vec<u8>)> {
        let ranges = self.find_overlap_ranges(start_key, end_key);

        for &(_, ref s_key, _) in &ranges {
            self.ranges.remove(s_key).unwrap();
        }
        ranges
    }

    // before an insert is called, must call drain_overlap_ranges to clean the overlap range
    pub fn insert(
        &mut self,
        region_id: u64,
        start_key: Vec<u8>,
        end_key: Vec<u8>,
        timeout: time::Instant,
    ) {
        if !self.find_overlap_ranges(&start_key, &end_key).is_empty() {
            panic!(
                "[region {}] register deleting data in [{}, {}) failed due to overlap",
                region_id,
                escape(&start_key),
                escape(&end_key),
            );
        }
        let info = StalePeerInfo {
            region_id,
            end_key,
            timeout,
        };
        self.ranges.insert(start_key, info);
    }

    pub fn drain_timeout_ranges(&mut self, now: time::Instant) -> Vec<(u64, Vec<u8>, Vec<u8>)> {
        let ranges = self
            .ranges
            .iter()
            .filter(|&(_, info)| info.timeout <= now)
            .map(|(start_key, info)| (info.region_id, start_key.clone(), info.end_key.clone()))
            .collect();
        for &(_, ref start_key, _) in &ranges {
            self.ranges.remove(start_key).unwrap();
        }
        ranges
    }

    pub fn len(&self) -> usize {
        self.ranges.len()
    }
}

#[derive(Clone)]
struct SnapContext {
    kv_db: Arc<DB>,
    raft_db: Arc<DB>,
    batch_size: usize,
    mgr: SnapManager,
    use_delete_range: bool,
    clean_stale_peer_delay: Duration,
    pending_delete_ranges: PendingDeleteRanges,
}

impl SnapContext {
    fn generate_snap(&self, region_id: u64, notifier: SyncSender<RaftSnapshot>) -> Result<()> {
        // do we need to check leader here?
        let raft_db = Arc::clone(&self.raft_db);
        let raw_snap = Snapshot::new(Arc::clone(&self.kv_db));

        let snap = box_try!(store::do_snapshot(
            self.mgr.clone(),
            &raft_db,
            &raw_snap,
            region_id
        ));
        // Only enable the fail point when the region id is equal to 1, which is
        // the id of bootstrapped region in tests.
        fail_point!("region_gen_snap", region_id == 1, |_| Ok(()));
        if let Err(e) = notifier.try_send(snap) {
            info!(
                "[region {}] failed to notify snap result, maybe leadership has changed, \
                 ignore: {:?}",
                region_id, e
            );
        }
        Ok(())
    }

    fn handle_gen(&self, region_id: u64, notifier: SyncSender<RaftSnapshot>) {
        SNAP_COUNTER_VEC
            .with_label_values(&["generate", "all"])
            .inc();
        let gen_histogram = SNAP_HISTOGRAM.with_label_values(&["generate"]);
        let timer = gen_histogram.start_coarse_timer();

        if let Err(e) = self.generate_snap(region_id, notifier) {
            error!("[region {}] failed to generate snap: {:?}!!!", region_id, e);
            return;
        }

        SNAP_COUNTER_VEC
            .with_label_values(&["generate", "success"])
            .inc();
        timer.observe_duration();
    }

    fn apply_snap(&mut self, region_id: u64, abort: Arc<AtomicUsize>) -> Result<()> {
        info!("[region {}] begin apply snap data", region_id);
        fail_point!("region_apply_snap");
        check_abort(&abort)?;
        let region_key = keys::region_state_key(region_id);
        let mut region_state: RegionLocalState =
            match box_try!(self.kv_db.get_msg_cf(CF_RAFT, &region_key)) {
                Some(state) => state,
                None => {
                    return Err(box_err!(
                        "failed to get region_state from {}",
                        escape(&region_key)
                    ))
                }
            };

        // clear up origin data.
        let region = region_state.get_region().clone();
        let start_key = keys::enc_start_key(&region);
        let end_key = keys::enc_end_key(&region);
        check_abort(&abort)?;
        self.cleanup_overlap_ranges(&start_key, &end_key);
        box_try!(util::delete_all_in_range(
            &self.kv_db,
            &start_key,
            &end_key,
            self.use_delete_range
        ));
        check_abort(&abort)?;

        let state_key = keys::apply_state_key(region_id);
        let apply_state: RaftApplyState = match box_try!(self.kv_db.get_msg_cf(CF_RAFT, &state_key))
        {
            Some(state) => state,
            None => {
                return Err(box_err!(
                    "failed to get raftstate from {}",
                    escape(&state_key)
                ))
            }
        };
        let term = apply_state.get_truncated_state().get_term();
        let idx = apply_state.get_truncated_state().get_index();
        let snap_key = SnapKey::new(region_id, term, idx);
        self.mgr.register(snap_key.clone(), SnapEntry::Applying);
        defer!({
            self.mgr.deregister(&snap_key, &SnapEntry::Applying);
        });
        let mut s = box_try!(self.mgr.get_snapshot_for_applying(&snap_key));
        if !s.exists() {
            return Err(box_err!("missing snapshot file {}", s.path()));
        }
        check_abort(&abort)?;
        let timer = Instant::now();
        let options = ApplyOptions {
            db: Arc::clone(&self.kv_db),
            region: region.clone(),
            abort: Arc::clone(&abort),
            write_batch_size: self.batch_size,
        };
        s.apply(options)?;

        let wb = WriteBatch::new();
        region_state.set_state(PeerState::Normal);
        let handle = box_try!(rocksdb::get_cf_handle(&self.kv_db, CF_RAFT));
        box_try!(wb.put_msg_cf(handle, &region_key, &region_state));
        box_try!(wb.delete_cf(handle, &keys::snapshot_raft_state_key(region_id)));
        self.kv_db.write(wb).unwrap_or_else(|e| {
            panic!("{} failed to save apply_snap result: {:?}", region_id, e);
        });
        info!(
            "[region {}] apply new data takes {:?}",
            region_id,
            timer.elapsed()
        );
        Ok(())
    }

    fn handle_apply(&mut self, region_id: u64, status: Arc<AtomicUsize>) {
        status.compare_and_swap(JOB_STATUS_PENDING, JOB_STATUS_RUNNING, Ordering::SeqCst);
        SNAP_COUNTER_VEC.with_label_values(&["apply", "all"]).inc();
        let apply_histogram = SNAP_HISTOGRAM.with_label_values(&["apply"]);
        let timer = apply_histogram.start_coarse_timer();

        match self.apply_snap(region_id, Arc::clone(&status)) {
            Ok(()) => {
                status.swap(JOB_STATUS_FINISHED, Ordering::SeqCst);
                SNAP_COUNTER_VEC
                    .with_label_values(&["apply", "success"])
                    .inc();
            }
            Err(Error::Abort) => {
                warn!("applying snapshot for region {} is aborted.", region_id);
                assert_eq!(
                    status.swap(JOB_STATUS_CANCELLED, Ordering::SeqCst),
                    JOB_STATUS_CANCELLING
                );
                SNAP_COUNTER_VEC
                    .with_label_values(&["apply", "abort"])
                    .inc();
            }
            Err(e) => {
                error!("failed to apply snap: {:?}!!!", e);
                status.swap(JOB_STATUS_FAILED, Ordering::SeqCst);
                SNAP_COUNTER_VEC.with_label_values(&["apply", "fail"]).inc();
            }
        }

        timer.observe_duration();
    }

    fn cleanup_range(
        &mut self,
        region_id: u64,
        start_key: Vec<u8>,
        end_key: Vec<u8>,
        use_delete_files: bool,
    ) {
        if use_delete_files {
            if let Err(e) = util::delete_all_files_in_range(&self.kv_db, &start_key, &end_key) {
                error!(
                    "[region {}] failed to delete files in [{}, {}): {:?}",
                    region_id,
                    escape(&start_key),
                    escape(&end_key),
                    e
                );
                return;
            }
        }
        if let Err(e) =
            util::delete_all_in_range(&self.kv_db, &start_key, &end_key, self.use_delete_range)
        {
            error!(
                "[region {}] failed to delete data in [{}, {}): {:?}",
                region_id,
                escape(&start_key),
                escape(&end_key),
                e
            );
        } else {
            info!(
                "[region {}] succeed in deleting data in [{}, {})",
                region_id,
                escape(&start_key),
                escape(&end_key),
            );
        }
    }

    fn cleanup_overlap_ranges(&mut self, start_key: &[u8], end_key: &[u8]) {
        let overlap_ranges = self
            .pending_delete_ranges
            .drain_overlap_ranges(start_key, end_key);
        for (region_id, s_key, e_key) in overlap_ranges {
            self.cleanup_range(region_id, s_key, e_key, false /* use_delete_files */);
        }
    }

    fn insert_pending_delete_range(
        &mut self,
        region_id: u64,
        start_key: Vec<u8>,
        end_key: Vec<u8>,
    ) -> bool {
        if self.clean_stale_peer_delay.as_secs() == 0 {
            return false;
        }

        self.cleanup_overlap_ranges(&start_key, &end_key);

        info!(
            "[region {}] register deleting data in [{}, {})",
            region_id,
            escape(&start_key),
            escape(&end_key),
        );
        let timeout = time::Instant::now() + self.clean_stale_peer_delay;
        self.pending_delete_ranges
            .insert(region_id, start_key, end_key, timeout);
        true
    }

    fn clean_timeout_ranges(&mut self) {
        STALE_PEER_PENDING_DELETE_RANGE_GAUGE.set(self.pending_delete_ranges.len() as f64);

        let now = time::Instant::now();
        let mut timeout_ranges = self.pending_delete_ranges.drain_timeout_ranges(now);
        for (region_id, start_key, end_key) in timeout_ranges.drain(..) {
            self.cleanup_range(
                region_id, start_key, end_key, true, /* use_delete_files */
            );
        }
    }
}

pub struct Runner {
    pool: ThreadPool<DefaultContext>,
    ctx: SnapContext,
}

impl Runner {
    pub fn new(
        kv_db: Arc<DB>,
        raft_db: Arc<DB>,
        mgr: SnapManager,
        batch_size: usize,
        use_delete_range: bool,
        clean_stale_peer_delay: Duration,
    ) -> Runner {
        Runner {
            pool: ThreadPoolBuilder::with_default_factory(thd_name!("snap generator"))
                .thread_count(GENERATE_POOL_SIZE)
                .build(),
            ctx: SnapContext {
                kv_db,
                raft_db,
                mgr,
                batch_size,
                use_delete_range,
                clean_stale_peer_delay,
                pending_delete_ranges: PendingDeleteRanges::default(),
            },
        }
    }
}

impl Runnable<Task> for Runner {
    fn run(&mut self, task: Task) {
        match task {
            Task::Gen {
                region_id,
                notifier,
            } => {
                // It is safe for now to handle generating and applying snapshot concurrently,
                // but it may not when merge is implemented.
                let ctx = self.ctx.clone();
                self.pool
                    .execute(move |_| ctx.handle_gen(region_id, notifier))
            }
            Task::Apply { region_id, status } => self.ctx.handle_apply(region_id, status),
            Task::Destroy {
                region_id,
                start_key,
                end_key,
            } => {
                // try to delay the range deletion because
                // there might be a coprocessor request related to this range
                if !self.ctx.insert_pending_delete_range(
                    region_id,
                    start_key.clone(),
                    end_key.clone(),
                ) {
                    self.ctx.cleanup_range(
                        region_id, start_key, end_key, false, /* use_delete_files */
                    );
                }
            }
        }
    }

    fn shutdown(&mut self) {
        if let Err(e) = self.pool.stop() {
            warn!("Stop threadpool failed with {:?}", e);
        }
    }
}

impl RunnableWithTimer<Task, ()> for Runner {
    fn on_timeout(&mut self, timer: &mut Timer<()>, _: ()) {
        self.ctx.clean_timeout_ranges();
        timer.add_task(Duration::from_millis(STALE_PEER_CHECK_INTERVAL), ());
    }
}

#[cfg(test)]
mod test {
    use std::thread;
    use std::time::Duration;

    use util::time;

    use super::PendingDeleteRanges;

    fn insert_range(
        pending_delete_ranges: &mut PendingDeleteRanges,
        id: u64,
        s: &str,
        e: &str,
        timeout: time::Instant,
    ) {
        pending_delete_ranges.insert(id, s.as_bytes().to_vec(), e.as_bytes().to_vec(), timeout);
    }

    #[test]
    fn test_pending_delete_ranges() {
        let mut pending_delete_ranges = PendingDeleteRanges::default();
        let delay = Duration::from_millis(100);
        let id = 0;

        let timeout = time::Instant::now() + delay;
        insert_range(&mut pending_delete_ranges, id, "a", "c", timeout);
        insert_range(&mut pending_delete_ranges, id, "m", "n", timeout);
        insert_range(&mut pending_delete_ranges, id, "x", "z", timeout);
        insert_range(&mut pending_delete_ranges, id + 1, "f", "i", timeout);
        insert_range(&mut pending_delete_ranges, id + 1, "p", "t", timeout);
        assert_eq!(pending_delete_ranges.len(), 5);

        thread::sleep(delay / 2);

        //  a____c    f____i    m____n    p____t    x____z
        //              g___________________q
        // when we want to insert [g, q), we first extract overlap ranges,
        // which are [f, i), [m, n), [p, t)
        let timeout = time::Instant::now() + delay;
        let overlap_ranges =
            pending_delete_ranges.drain_overlap_ranges(&b"g".to_vec(), &b"q".to_vec());
        assert_eq!(
            overlap_ranges,
            [
                (id + 1, b"f".to_vec(), b"i".to_vec()),
                (id, b"m".to_vec(), b"n".to_vec()),
                (id + 1, b"p".to_vec(), b"t".to_vec()),
            ]
        );
        assert_eq!(pending_delete_ranges.len(), 2);
        insert_range(&mut pending_delete_ranges, id + 2, "g", "q", timeout);
        assert_eq!(pending_delete_ranges.len(), 3);

        thread::sleep(delay / 2);

        // at t1, [a, c) and [x, z) will timeout
        let now = time::Instant::now();
        let ranges = pending_delete_ranges.drain_timeout_ranges(now);
        assert_eq!(
            ranges,
            [
                (id, b"a".to_vec(), b"c".to_vec()),
                (id, b"x".to_vec(), b"z".to_vec()),
            ]
        );
        assert_eq!(pending_delete_ranges.len(), 1);

        thread::sleep(delay / 2);

        // at t2, [g, q) will timeout
        let now = time::Instant::now();
        let ranges = pending_delete_ranges.drain_timeout_ranges(now);
        assert_eq!(ranges, [(id + 2, b"g".to_vec(), b"q".to_vec())]);
        assert_eq!(pending_delete_ranges.len(), 0);
    }
}
