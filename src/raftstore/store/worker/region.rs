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
use kvproto::metapb::Region;

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

use std::collections::Bound::{Excluded, Included, Unbounded};

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
    /// periodically clean stale peer
    CleanStalePeer {},
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
            Task::CleanStalePeer {} => write!(f, "Clean stale peer"),
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

    fn apply_snap(&self, region_id: u64, abort: Arc<AtomicUsize>) -> Result<(Region)> {
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
        Ok((region))
    }

    fn handle_apply(&self, region_id: u64, status: Arc<AtomicUsize>) -> Option<Region> {
        status.compare_and_swap(JOB_STATUS_PENDING, JOB_STATUS_RUNNING, Ordering::SeqCst);
        SNAP_COUNTER_VEC.with_label_values(&["apply", "all"]).inc();
        let apply_histogram = SNAP_HISTOGRAM.with_label_values(&["apply"]);
        let timer = apply_histogram.start_coarse_timer();

        let mut region = None;
        match self.apply_snap(region_id, Arc::clone(&status)) {
            Ok(r) => {
                region = Some(r);
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
        region
    }

    fn handle_destroy(&mut self, region_id: u64, start_key: Vec<u8>, end_key: Vec<u8>) {
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
}

#[derive(Default)]
struct PendingDeleteRanges {
    ranges: BTreeMap<Vec<u8>, (Vec<u8>, u64, time::Instant)>, // start_key -> (end_key, region_id, ttl)
}

impl PendingDeleteRanges {
    // find out the ranges that need to be trimed or discarded
    fn find_overlapped_ranges(&self, start_key: &[u8], end_key: &[u8]) -> Vec<(Vec<u8>, Vec<u8>)> {
        let mut ranges = Vec::new();
        // find the first range that may overlap with [start_key, end_key)
        let sub_range = self.ranges.range((Unbounded, Excluded(start_key.to_vec())));
        if let Some((s_key, &(ref e_key, _, _))) = sub_range.last() {
            if e_key > &start_key.to_vec() {
                ranges.push((s_key.clone(), e_key.clone()));
            }
        }

        // find the rest ranges that overlap with [start_key, end_key)
        for (s_key, &(ref e_key, _, _)) in self.ranges
            .range((Included(start_key.to_vec()), Excluded(end_key.to_vec())))
        {
            ranges.push((s_key.clone(), e_key.clone()));
        }
        ranges
    }

    // filter out the overlap in ranges, after this function the ranges will not overlap
    pub fn remove(&mut self, start_key: &[u8], end_key: &[u8]) {
        if start_key >= end_key {
            return;
        }
        let overlapped_ranges = self.find_overlapped_ranges(start_key, end_key);
        for &(ref s_key, ref e_key) in &overlapped_ranges {
            if s_key < &start_key.to_vec() {
                if e_key <= &end_key.to_vec() {
                    // the right part of the range is trimed
                    // s_key              e_key
                    //   [_ _ _ _ _ _ _ _ _)                 before
                    //            [_ _ _ _ _ _ _ _ _ _)
                    //       start_key            end_key
                    //   [_ _ _ _ )                          after
                    let val = self.ranges.get_mut(s_key).unwrap();
                    val.0 = start_key.to_vec();
                } else {
                    // the middle part of the range is trimed
                    // s_key                   e_key
                    //   [_ _ _ _ _ _ _ _ _ _ _ _)        before
                    //           [_ _ _ _)
                    //      start_key   end_key
                    //   [_ _ _ _)       [_ _ _ _)        after
                    let mut val = self.ranges.remove(s_key).unwrap();
                    self.ranges.insert(end_key.to_vec(), val.clone());
                    val.0 = start_key.to_vec();
                    assert!(self.ranges.insert(s_key.clone(), val).is_none());
                }
            } else if e_key <= &end_key.to_vec() {
                // the whole range is discarded
                //     s_key    e_key
                //       [_ _ _ _)             before
                // start_key     end_key
                //    [_ _ _ _ _ _ _)
                //                             after
                self.ranges.remove(s_key).unwrap();
            } else {
                // the left part of the range is trimed
                //         s_key         e_key
                //           [_ _ _ _ _ _ _)      before
                // start_key      end_key
                //    [_ _ _ _ _ _ _)
                //                  [_ _ _ )      after
                let val = self.ranges.remove(s_key).unwrap();
                assert!(self.ranges.insert(end_key.to_vec(), val).is_none());
            }
        }
    }

    pub fn insert(
        &mut self,
        region_id: u64,
        start_key: Vec<u8>,
        end_key: Vec<u8>,
        timeout: time::Instant,
    ) {
        self.remove(&start_key, &end_key);
        self.ranges.insert(start_key, (end_key, region_id, timeout));
    }

    pub fn drain_timeout_ranges(&mut self, now: time::Instant) -> Vec<(u64, Vec<u8>, Vec<u8>)> {
        let mut ranges = Vec::new();
        for (s_key, &(ref e_key, region_id, ref timeout)) in &self.ranges {
            if timeout <= &now {
                ranges.push((region_id, s_key.clone(), e_key.clone()));
            }
        }
        for &(_, ref s_key, _) in &ranges {
            self.ranges.remove(s_key).unwrap();
        }
        ranges
    }

    pub fn drain_all_ranges(&mut self) -> Vec<(u64, Vec<u8>, Vec<u8>)> {
        let ranges = self.ranges
            .iter()
            .map(|(s_key, &(ref e_key, region_id, _))| (region_id, s_key.clone(), e_key.clone()))
            .collect();
        self.ranges.clear();
        ranges
    }

    pub fn len(&self) -> usize {
        self.ranges.len()
    }
}

pub struct Runner {
    pool: ThreadPool<DefaultContext>,
    ctx: SnapContext,
    clean_stale_peer_delay: Duration,
    pending_delete_ranges: PendingDeleteRanges,
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
                kv_db: kv_db,
                raft_db: raft_db,
                mgr: mgr,
                batch_size: batch_size,
                use_delete_range: use_delete_range,
            },
            clean_stale_peer_delay: clean_stale_peer_delay,
            pending_delete_ranges: PendingDeleteRanges::default(),
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
                if let Some(region) = self.ctx.handle_apply(region_id, status) {
                    let start_key = keys::enc_start_key(&region);
                    let end_key = keys::enc_end_key(&region);
                    self.pending_delete_ranges.remove(&start_key, &end_key);
                }
            }
            Task::Destroy {
                region_id,
                start_key,
                end_key,
            } => {
                if self.clean_stale_peer_delay.as_secs() > 0 {
                    info!(
                        "[region {}] register deleting data in [{}, {})",
                        region_id,
                        escape(&start_key),
                        escape(&end_key)
                    );
                    // delay the range deletion becase there might be a coprocessor request related to this range
                    let timeout = time::Instant::now() + self.clean_stale_peer_delay;
                    self.pending_delete_ranges
                        .insert(region_id, start_key, end_key, timeout);
                } else {
                    self.ctx.handle_destroy(region_id, start_key, end_key);
                }
            }
            Task::CleanStalePeer {} => {
                STALE_PEER_PENDING_DELETE_RANGE_GAUGE_VEC
                    .with_label_values(&["total"])
                    .set(self.pending_delete_ranges.len() as f64);

                let now = time::Instant::now();
                let mut timeout_ranges = self.pending_delete_ranges.drain_timeout_ranges(now);
                for (region_id, start_key, end_key) in timeout_ranges.drain(..) {
                    self.ctx.handle_destroy(region_id, start_key, end_key);
                }
            }
        }
    }

    fn shutdown(&mut self) {
        if let Err(e) = self.pool.stop() {
            warn!("Stop threadpool failed with {:?}", e);
        }
        // handle rest of the ranges that need to be destroyed
        let mut ranges = self.pending_delete_ranges.drain_all_ranges();
        for (region_id, start_key, end_key) in ranges.drain(..) {
            self.ctx.handle_destroy(region_id, start_key, end_key);
        }
    }
}

#[cfg(test)]
mod test {
    use std::collections::BTreeMap;
    use std::thread;
    use std::time::Duration;

    use util::time;

    use super::PendingDeleteRanges;

    #[test]
    fn test_pending_delete_ranges_case1() {
        let mut pending_delete_ranges = PendingDeleteRanges {
            ranges: BTreeMap::new(),
        };
        let delay = Duration::from_millis(100);
        let id = 0;

        let timeout = time::Instant::now() + delay;
        pending_delete_ranges.insert(id, b"aa".to_vec(), b"bb".to_vec(), timeout);
        pending_delete_ranges.insert(id + 1, b"bb".to_vec(), b"cc".to_vec(), timeout);

        // not reaching the timeout, so no regions
        let now = time::Instant::now();
        let ranges = pending_delete_ranges.drain_timeout_ranges(now);
        assert!(ranges.is_empty());

        thread::sleep(delay);

        // reaching the timeout
        let now = time::Instant::now();
        let ranges = pending_delete_ranges.drain_timeout_ranges(now);
        assert_eq!(
            ranges,
            [
                (id, b"aa".to_vec(), b"bb".to_vec()),
                (id + 1, b"bb".to_vec(), b"cc".to_vec())
            ]
        );

        // ranges will be deleted from pending_delete_ranges after popped out
        assert_eq!(pending_delete_ranges.len(), 0);
    }

    #[test]
    fn test_pending_delete_ranges_case2() {
        let mut pending_delete_ranges = PendingDeleteRanges {
            ranges: BTreeMap::new(),
        };
        let delay = Duration::from_millis(100);
        let id = 0;

        let timeout = time::Instant::now() + delay;
        pending_delete_ranges.insert(id, b"aa".to_vec(), b"bb".to_vec(), timeout);

        thread::sleep(delay / 2);

        let timeout = time::Instant::now() + delay;
        pending_delete_ranges.insert(id + 1, b"aa".to_vec(), b"bb".to_vec(), timeout);

        thread::sleep(delay / 2);

        // range has been updated, so will not timeout
        let now = time::Instant::now();
        let ranges = pending_delete_ranges.drain_timeout_ranges(now);
        assert!(ranges.is_empty());

        thread::sleep(delay / 2);

        // now will timeout
        let now = time::Instant::now();
        let ranges = pending_delete_ranges.drain_timeout_ranges(now);
        assert_eq!(ranges, [(id + 1, b"aa".to_vec(), b"bb".to_vec())]);
    }

    #[test]
    fn test_pending_delete_ranges_case3() {
        let mut pending_delete_ranges = PendingDeleteRanges {
            ranges: BTreeMap::new(),
        };
        let delay = Duration::from_millis(100);
        let id = 0;

        let timeout = time::Instant::now() + delay;
        pending_delete_ranges.insert(id, b"aa".to_vec(), b"bb".to_vec(), timeout);

        thread::sleep(delay / 2);

        pending_delete_ranges.remove(&b"aa".to_vec(), &b"bb".to_vec());

        thread::sleep(delay / 2);

        // range has been removed
        let now = time::Instant::now();
        let ranges = pending_delete_ranges.drain_timeout_ranges(now);
        assert!(ranges.is_empty());
    }

    #[test]
    fn test_pending_delete_ranges_case4() {
        let mut pending_delete_ranges = PendingDeleteRanges {
            ranges: BTreeMap::new(),
        };
        let delay = Duration::from_millis(100);
        let id = 0;

        let timeout = time::Instant::now() + delay;
        pending_delete_ranges.insert(id, b"a".to_vec(), b"c".to_vec(), timeout);
        pending_delete_ranges.insert(id + 1, b"f".to_vec(), b"i".to_vec(), timeout);
        pending_delete_ranges.insert(id, b"m".to_vec(), b"n".to_vec(), timeout);
        pending_delete_ranges.insert(id + 1, b"p".to_vec(), b"t".to_vec(), timeout);
        pending_delete_ranges.insert(id, b"x".to_vec(), b"z".to_vec(), timeout);

        thread::sleep(delay / 2);

        let timeout = time::Instant::now() + delay;
        pending_delete_ranges.insert(id + 2, b"g".to_vec(), b"q".to_vec(), timeout);

        thread::sleep(delay / 2);

        let now = time::Instant::now();
        let ranges = pending_delete_ranges.drain_timeout_ranges(now);
        assert_eq!(
            ranges,
            [
                (id, b"a".to_vec(), b"c".to_vec()),
                (id + 1, b"f".to_vec(), b"g".to_vec()),
                (id + 1, b"q".to_vec(), b"t".to_vec()),
                (id, b"x".to_vec(), b"z".to_vec()),
            ]
        );

        thread::sleep(delay / 2);

        let now = time::Instant::now();
        let ranges = pending_delete_ranges.drain_timeout_ranges(now);
        assert_eq!(ranges, [(id + 2, b"g".to_vec(), b"q".to_vec())]);
    }

    #[test]
    fn test_pending_delete_ranges_case5() {
        let mut pending_delete_ranges = PendingDeleteRanges {
            ranges: BTreeMap::new(),
        };
        let delay = Duration::from_millis(100);
        let id = 0;

        let timeout = time::Instant::now() + delay;
        pending_delete_ranges.insert(id, b"a".to_vec(), b"c".to_vec(), timeout);
        pending_delete_ranges.insert(id + 1, b"f".to_vec(), b"t".to_vec(), timeout);
        pending_delete_ranges.insert(id, b"x".to_vec(), b"z".to_vec(), timeout);

        thread::sleep(delay / 2);

        let timeout = time::Instant::now() + delay;
        pending_delete_ranges.insert(id + 2, b"g".to_vec(), b"q".to_vec(), timeout);

        thread::sleep(delay / 2);

        let now = time::Instant::now();
        let ranges = pending_delete_ranges.drain_timeout_ranges(now);
        assert_eq!(
            ranges,
            [
                (id, b"a".to_vec(), b"c".to_vec()),
                (id + 1, b"f".to_vec(), b"g".to_vec()),
                (id + 1, b"q".to_vec(), b"t".to_vec()),
                (id, b"x".to_vec(), b"z".to_vec()),
            ]
        );

        thread::sleep(delay / 2);

        let now = time::Instant::now();
        let ranges = pending_delete_ranges.drain_timeout_ranges(now);
        assert_eq!(ranges, [(id + 2, b"g".to_vec(), b"q".to_vec())]);
    }
}
