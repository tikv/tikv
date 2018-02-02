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
use std::sync::Arc;
use std::sync::mpsc::SyncSender;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use rocksdb::{Writable, WriteBatch, DB};
use kvproto::raft_serverpb::{PeerState, RaftApplyState, RegionLocalState};
use kvproto::eraftpb::Snapshot as RaftSnapshot;

use util::threadpool::{DefaultContext, ThreadPool, ThreadPoolBuilder};
use util::time;
use util::worker::Runnable;
use util::{escape, rocksdb};
use raftstore::store::engine::{Mutable, Snapshot};
use raftstore::store::peer_storage::{JOB_STATUS_CANCELLED, JOB_STATUS_CANCELLING,
                                     JOB_STATUS_FAILED, JOB_STATUS_FINISHED, JOB_STATUS_PENDING,
                                     JOB_STATUS_RUNNING};
use raftstore::store::{self, check_abort, keys, ApplyOptions, Peekable, SnapEntry, SnapKey,
                       SnapManager};
use raftstore::store::snap::{Error, Result};
use storage::CF_RAFT;

use super::metrics::*;
use super::super::util;

const GENERATE_POOL_SIZE: usize = 2;

/// region related task.
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
            region_id: region_id,
            start_key: start_key,
            end_key: end_key,
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
struct SnapContext {
    kv_db: Arc<DB>,
    raft_db: Arc<DB>,
    batch_size: usize,
    mgr: SnapManager,
    use_delete_range: bool,
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

    fn apply_snap(
        &self,
        pending_delete_ranges: &mut PendingDeleteRanges,
        region_id: u64,
        abort: Arc<AtomicUsize>,
    ) -> Result<()> {
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
        pending_delete_ranges.remove(start_key.clone(), end_key.clone());
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

    fn handle_apply(
        &self,
        pending_delete_ranges: &mut PendingDeleteRanges,
        region_id: u64,
        status: Arc<AtomicUsize>,
    ) {
        status.compare_and_swap(JOB_STATUS_PENDING, JOB_STATUS_RUNNING, Ordering::SeqCst);
        SNAP_COUNTER_VEC.with_label_values(&["apply", "all"]).inc();
        let apply_histogram = SNAP_HISTOGRAM.with_label_values(&["apply"]);
        let timer = apply_histogram.start_coarse_timer();

        match self.apply_snap(pending_delete_ranges, region_id, Arc::clone(&status)) {
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

    fn handle_destroy(&mut self, start_key: Vec<u8>, end_key: Vec<u8>) {
        if let Err(e) = util::delete_all_files_in_range(&self.kv_db, &start_key, &end_key) {
            error!(
                "failed to delete files in [{}, {}): {:?}",
                escape(&start_key),
                escape(&end_key),
                e
            );
            return;
        }
        if let Err(e) =
            util::delete_all_in_range(&self.kv_db, &start_key, &end_key, self.use_delete_range)
        {
            error!(
                "failed to delete data in [{}, {}): {:?}",
                escape(&start_key),
                escape(&end_key),
                e
            );
        } else {
            info!(
                "succeed in deleting data in [{}, {})",
                escape(&start_key),
                escape(&end_key),
            );
        }
    }
}

struct PendingDeleteRanges {
    delay: Duration,
    ranges: BTreeMap<Vec<u8>, (Vec<u8>, time::Instant)>,
}

impl PendingDeleteRanges {
    // find out the ranges that need to be trimed or discarded
    fn find_overlap(
        &self,
        start_key: Vec<u8>,
        end_key: Vec<u8>,
        discard: &mut Vec<Vec<u8>>,
    ) -> (Option<Vec<u8>>, Option<Vec<u8>>) {
        let mut trim_left = None;
        let mut trim_right = None;
        let mut iter = self.ranges.iter();
        // find the first region that may overlap with [start_key, end_key)
        let mut left = iter.find(|ref ptr| (ptr.1).0 > start_key);
        loop {
            if left.is_none() {
                break;
            }
            let cur = left.unwrap();
            if cur.0 >= &end_key {
                // the new interval does not reach the left boundary, so stop looping
                break;
            } else if (cur.1).0 > end_key {
                // left part of the old region will be trimed
                trim_left = Some(cur.0.clone());
            } else if cur.0 >= &start_key {
                // the old region will be discarded
                discard.push(cur.0.clone());
            } else {
                // right part of the old region will be trimed
                trim_right = Some(cur.0.clone());
            }
            left = iter.next();
        }
        (trim_left, trim_right)
    }

    // filter out the overlap in ranges, after this function the ranges will not overlap
    fn update(&mut self, start_key: Vec<u8>, end_key: Vec<u8>, is_destroy: bool) {
        if start_key >= end_key {
            return;
        }
        let mut discard = Vec::new();
        let (trim_left, trim_right) =
            self.find_overlap(start_key.clone(), end_key.clone(), &mut discard);
        // all the elements that being updated or removed should and must exist
        if let Some(key) = trim_left {
            let cur = self.ranges.remove(&key).unwrap();
            self.ranges.insert(end_key.clone(), cur);
        }
        if let Some(key) = trim_right {
            let cur = self.ranges.get_mut(&key).unwrap();
            cur.0 = start_key.clone();
        }
        for key in &discard {
            self.ranges.remove(&*key).unwrap();
        }
        if is_destroy {
            // no overlap any more, can be safely inserted
            let timeout = time::Instant::now() + self.delay;
            self.ranges.insert(start_key, (end_key, timeout));
        } // else let apply_snapshot deal with deletion
    }

    pub fn insert(&mut self, start_key: Vec<u8>, end_key: Vec<u8>) {
        self.update(start_key, end_key, true);
    }

    pub fn remove(&mut self, start_key: Vec<u8>, end_key: Vec<u8>) {
        self.update(start_key, end_key, false);
    }

    pub fn get_timeout_ranges(&mut self, ranges: &mut Vec<(Vec<u8>, Vec<u8>)>) {
        let now = time::Instant::now();
        for (k, v) in &self.ranges {
            if v.1 <= now {
                ranges.push((k.clone(), v.0.clone()));
            }
        }
        for e in ranges.iter() {
            self.ranges.remove(&e.0).unwrap();
        }
    }

    pub fn get_all_ranges(&mut self, ranges: &mut Vec<(Vec<u8>, Vec<u8>)>) {
        for (k, v) in &self.ranges {
            ranges.push((k.clone(), v.0.clone()));
        }
        self.ranges.clear();
    }
}

pub struct Runner {
    pool: ThreadPool<DefaultContext>,
    ctx: SnapContext,
    pending_delete_ranges: PendingDeleteRanges,
}

impl Runner {
    pub fn new(
        kv_db: Arc<DB>,
        raft_db: Arc<DB>,
        mgr: SnapManager,
        batch_size: usize,
        use_delete_range: bool,
        range_deletion_delay: u64,
    ) -> Runner {
        Runner {
            pool: ThreadPoolBuilder::with_default_factory(thd_name!("snap generator"))
                .thread_count(GENERATE_POOL_SIZE)
                .build(),
            ctx: SnapContext {
                kv_db: kv_db,
                raft_db: raft_db,
                mgr: mgr,
                batch_size: batch_size,
                use_delete_range: use_delete_range,
            },
            pending_delete_ranges: PendingDeleteRanges {
                delay: Duration::from_secs(range_deletion_delay),
                ranges: BTreeMap::new(),
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
            Task::Apply { region_id, status } => {
                self.ctx
                    .handle_apply(&mut self.pending_delete_ranges, region_id, status)
            }
            Task::Destroy {
                region_id,
                start_key,
                end_key,
            } => {
                info!(
                    "[region {}] register deleting data in [{}, {})",
                    region_id,
                    escape(&start_key),
                    escape(&end_key)
                );
                // delay the range deletion becase there might be a coprocessor request related to this range
                self.pending_delete_ranges.insert(start_key, end_key);
            }
        }
    }

    fn on_tick(&mut self) {
        let mut ranges = Vec::new();
        self.pending_delete_ranges.get_timeout_ranges(&mut ranges);
        for e in ranges.drain(..) {
            self.ctx.handle_destroy(e.0, e.1);
        }
    }

    fn shutdown(&mut self) {
        if let Err(e) = self.pool.stop() {
            warn!("Stop threadpool failed with {:?}", e);
        }
        // handle rest of the ranges that need to be destroyed
        let mut ranges = Vec::new();
        self.pending_delete_ranges.get_all_ranges(&mut ranges);
        for e in ranges.drain(..) {
            self.ctx.handle_destroy(e.0, e.1);
        }
    }
}

#[cfg(test)]
mod test {
    use std::collections::BTreeMap;
    use std::thread;
    use std::time::Duration;

    use super::PendingDeleteRanges;

    #[test]
    fn test_pending_delete_ranges_case1() {
        let delay = Duration::from_millis(100);
        let mut pending_delete_ranges = PendingDeleteRanges {
            delay: delay,
            ranges: BTreeMap::new(),
        };
        pending_delete_ranges.insert(b"aa".to_vec(), b"bb".to_vec());
        pending_delete_ranges.insert(b"bb".to_vec(), b"cc".to_vec());

        // not reaching the timeout, so no regions
        let mut ranges = Vec::new();
        pending_delete_ranges.get_timeout_ranges(&mut ranges);
        assert!(ranges.is_empty());

        thread::sleep(delay);

        // reaching the timeout
        pending_delete_ranges.get_timeout_ranges(&mut ranges);
        assert_eq!(
            ranges,
            [
                (b"aa".to_vec(), b"bb".to_vec()),
                (b"bb".to_vec(), b"cc".to_vec())
            ]
        );

        // ranges will be deleted from pending_delete_ranges after popped out
        let mut ranges = Vec::new();
        pending_delete_ranges.get_timeout_ranges(&mut ranges);
        assert!(ranges.is_empty());
    }

    #[test]
    fn test_pending_delete_ranges_case2() {
        let delay = Duration::from_millis(100);
        let mut pending_delete_ranges = PendingDeleteRanges {
            delay: delay,
            ranges: BTreeMap::new(),
        };

        pending_delete_ranges.insert(b"aa".to_vec(), b"bb".to_vec());

        thread::sleep(delay / 2);

        pending_delete_ranges.insert(b"aa".to_vec(), b"bb".to_vec());

        thread::sleep(delay / 2);

        // range has been updated, so will not timeout
        let mut ranges = Vec::new();
        pending_delete_ranges.get_timeout_ranges(&mut ranges);
        assert!(ranges.is_empty());

        thread::sleep(delay / 2);

        // now will timeout
        pending_delete_ranges.get_timeout_ranges(&mut ranges);
        assert_eq!(ranges, [(b"aa".to_vec(), b"bb".to_vec())]);
    }

    #[test]
    fn test_pending_delete_ranges_case3() {
        let delay = Duration::from_millis(100);
        let mut pending_delete_ranges = PendingDeleteRanges {
            delay: delay,
            ranges: BTreeMap::new(),
        };

        pending_delete_ranges.insert(b"aa".to_vec(), b"bb".to_vec());

        thread::sleep(delay / 2);

        pending_delete_ranges.remove(b"aa".to_vec(), b"bb".to_vec());

        thread::sleep(delay / 2);

        // range has been removed
        let mut ranges = Vec::new();
        pending_delete_ranges.get_timeout_ranges(&mut ranges);
        assert!(ranges.is_empty());
    }

    #[test]
    fn test_pending_delete_ranges_case4() {
        let delay = Duration::from_millis(100);
        let mut pending_delete_ranges = PendingDeleteRanges {
            delay: delay,
            ranges: BTreeMap::new(),
        };

        pending_delete_ranges.insert(b"a".to_vec(), b"c".to_vec());
        pending_delete_ranges.insert(b"f".to_vec(), b"i".to_vec());
        pending_delete_ranges.insert(b"m".to_vec(), b"n".to_vec());
        pending_delete_ranges.insert(b"p".to_vec(), b"t".to_vec());
        pending_delete_ranges.insert(b"x".to_vec(), b"z".to_vec());

        thread::sleep(delay / 2);

        pending_delete_ranges.insert(b"g".to_vec(), b"q".to_vec());

        thread::sleep(delay / 2);
        let mut ranges = Vec::new();
        pending_delete_ranges.get_timeout_ranges(&mut ranges);
        assert_eq!(
            ranges,
            [
                (b"a".to_vec(), b"c".to_vec()),
                (b"f".to_vec(), b"g".to_vec()),
                (b"q".to_vec(), b"t".to_vec()),
                (b"x".to_vec(), b"z".to_vec()),
            ]
        );

        thread::sleep(delay / 2);

        let mut ranges = Vec::new();
        pending_delete_ranges.get_timeout_ranges(&mut ranges);
        assert_eq!(ranges, [(b"g".to_vec(), b"q".to_vec())]);
    }
}
