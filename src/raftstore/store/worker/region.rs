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


use std::fmt::{self, Display, Formatter};
use std::sync::Arc;
use std::sync::mpsc::SyncSender;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;
use std::cmp::Reverse;

use rocksdb::{Writable, WriteBatch, DB};
use kvproto::raft_serverpb::{PeerState, RaftApplyState, RegionLocalState};
use kvproto::eraftpb::Snapshot as RaftSnapshot;

use util::threadpool::{DefaultContext, ThreadPool, ThreadPoolBuilder};
use util::worker::Runnable;
use util::{escape, rocksdb};
use raftstore::store::engine::{Mutable, Snapshot};
use raftstore::store::peer_storage::{JOB_STATUS_CANCELLED, JOB_STATUS_CANCELLING,
                                     JOB_STATUS_FAILED, JOB_STATUS_FINISHED, JOB_STATUS_PENDING,
                                     JOB_STATUS_RUNNING};
use raftstore::store::{self, check_abort, keys, ApplyOptions, Peekable, SnapEntry, SnapKey,
                       SnapManager};
use raftstore::store::snap::{Error, Result, SnapshotDeleter};
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
        let raft_db = self.raft_db.clone();
        let raw_snap = Snapshot::new(self.kv_db.clone());

        let mut old_snaps = None;
        while self.mgr.get_total_snap_size() > self.mgr.max_total_snap_size() {
            if old_snaps.is_none() {
                let snaps = match self.mgr.list_idle_snap() {
                    Ok(snaps) => snaps,
                    Err(_) => break,
                };
                let mut key_and_snaps = Vec::with_capacity(snaps.len());
                for (key, is_sending) in snaps {
                    if !is_sending {
                        continue;
                    }
                    let snap = match self.mgr.get_snapshot_for_sending(&key) {
                        Ok(snap) => snap,
                        Err(_) => continue,
                    };
                    if !snap.meta().is_ok() {
                        continue;
                    }
                    key_and_snaps.push((key, snap));
                }
                key_and_snaps
                    .sort_by_key(|item| Reverse(item.1.meta().unwrap().modified().unwrap()));
                old_snaps = Some(key_and_snaps);
            }
            match old_snaps.as_mut().unwrap().pop() {
                Some((key, snap)) => self.mgr.delete_snapshot(&key, snap.as_ref(), false),
                None => return Err(Error::TooManySnapshots),
            };
        }

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
                region_id,
                e
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

    fn apply_snap(&self, region_id: u64, abort: Arc<AtomicUsize>) -> Result<()> {
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
        let apply_state: RaftApplyState =
            match box_try!(self.kv_db.get_msg_cf(CF_RAFT, &state_key)) {
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
            db: self.kv_db.clone(),
            region: region.clone(),
            abort: abort.clone(),
            write_batch_size: self.batch_size,
        };
        s.apply(options)?;

        let wb = WriteBatch::new();
        region_state.set_state(PeerState::Normal);
        let handle = box_try!(rocksdb::get_cf_handle(&self.kv_db, CF_RAFT));
        box_try!(wb.put_msg_cf(handle, &region_key, &region_state));
        box_try!(wb.delete_cf(
            handle,
            &keys::snapshot_raft_state_key(region_id)
        ));
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

    fn handle_apply(&self, region_id: u64, status: Arc<AtomicUsize>) {
        status.compare_and_swap(JOB_STATUS_PENDING, JOB_STATUS_RUNNING, Ordering::SeqCst);
        SNAP_COUNTER_VEC.with_label_values(&["apply", "all"]).inc();
        let apply_histogram = SNAP_HISTOGRAM.with_label_values(&["apply"]);
        let timer = apply_histogram.start_coarse_timer();

        match self.apply_snap(region_id, status.clone()) {
            Ok(()) => {
                status.swap(JOB_STATUS_FINISHED, Ordering::SeqCst);
                SNAP_COUNTER_VEC.with_label_values(&["apply", "success"]).inc();
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

    fn handle_destroy(&mut self, region_id: u64, start_key: Vec<u8>, end_key: Vec<u8>) {
        info!(
            "[region {}] deleting data in [{}, {})",
            region_id,
            escape(&start_key),
            escape(&end_key)
        );
        if let Err(e) =
            util::delete_all_in_range(&self.kv_db, &start_key, &end_key, self.use_delete_range)
        {
            error!(
                "failed to delete data in [{}, {}): {:?}",
                escape(&start_key),
                escape(&end_key),
                e
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
                // It safe for now to handle generating and applying snapshot concurrently,
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
            } => self.ctx.handle_destroy(region_id, start_key, end_key),
        }
    }

    fn shutdown(&mut self) {
        if let Err(e) = self.pool.stop() {
            warn!("Stop threadpool failed with {:?}", e);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{mpsc, Arc};
    use tempdir::TempDir;
    use kvproto::metapb::Peer;
    use kvproto::eraftpb::Entry;
    use kvproto::raft_serverpb::{RaftApplyState, RegionLocalState};
    use rocksdb::DB;

    use util::rocksdb;
    use storage::{ALL_CFS, CF_DEFAULT, CF_LOCK, CF_RAFT, CF_WRITE};
    use raftstore::store::keys;
    use raftstore::store::engine::Mutable;

    use super::SnapContext;
    use raftstore::store::snap::*;
    use raftstore::store::snap::tests::*;

    fn get_test_snap_context(kv_db: Arc<DB>, raft_db: Arc<DB>, mgr: SnapManager) -> SnapContext {
        SnapContext {
            kv_db: kv_db,
            raft_db: raft_db,
            mgr: mgr,
            batch_size: 1,
            use_delete_range: false,
        }
    }

    fn get_test_kv_and_raft_db(
        kv_path: &TempDir,
        raft_path: &TempDir,
        regions: &[u64],
    ) -> (Arc<DB>, Arc<DB>) {
        let kv_path_str = kv_path.path().to_str().unwrap();
        let raft_path_str = raft_path.path().to_str().unwrap();
        let kv = rocksdb::new_engine(kv_path_str, ALL_CFS, None).unwrap();
        let raft = rocksdb::new_engine(raft_path_str, &[CF_DEFAULT], None).unwrap();

        for &region_id in regions {
            // Put a raft log into raft engine.
            let mut raft_log: Entry = Entry::new();
            raft_log.set_term(5);
            raft_log.set_index(10);
            let handle = rocksdb::get_cf_handle(&raft, CF_DEFAULT).unwrap();
            raft.put_msg_cf(handle, &keys::raft_log_key(region_id, 10), &raft_log)
                .unwrap();

            // Put apply state into kv engine.
            let mut apply_state = RaftApplyState::new();
            apply_state.set_applied_index(10);
            apply_state.mut_truncated_state().set_index(9);
            let handle = rocksdb::get_cf_handle(&kv, CF_RAFT).unwrap();
            kv.put_msg_cf(handle, &keys::apply_state_key(region_id), &apply_state)
                .unwrap();

            // Put region info into kv engine.
            let region = get_test_region(region_id, 1, 1);
            let mut region_state = RegionLocalState::new();
            region_state.set_region(region);
            let handle = rocksdb::get_cf_handle(&kv, CF_RAFT).unwrap();
            kv.put_msg_cf(handle, &keys::region_state_key(region_id), &region_state)
                .unwrap();

            // Put some data into kv engine.
            for (i, cf) in [CF_LOCK, CF_DEFAULT, CF_WRITE].iter().enumerate() {
                let handle = rocksdb::get_cf_handle(&kv, cf).unwrap();
                let mut p = Peer::new();
                p.set_store_id(1);
                p.set_id((i + 1) as u64);
                let key = keys::data_key(b"akey");
                kv.put_msg_cf(handle, &key[..], &p).unwrap();
            }
        }
        (Arc::new(kv), Arc::new(raft))
    }

    #[test]
    fn test_snapshot_max_total_size() {
        let regions: Vec<u64> = (0..20).collect();
        let kv_path = TempDir::new("test-snapshot-max-total-size-db").unwrap();
        let raft_path = TempDir::new("test-snapshot-max-total-size-raft").unwrap();
        let (kv, raft) = get_test_kv_and_raft_db(&kv_path, &raft_path, &regions);

        let snapfiles_path = TempDir::new("test-snapshot-max-total-size-snapshots").unwrap();
        let snap_mgr = SnapManagerBuilder::default()
            .max_total_size(10240)
            .build(snapfiles_path.path().to_str().unwrap(), None);

        let snap_ctx = get_test_snap_context(kv.clone(), raft.clone(), snap_mgr);
        let (tx, _) = mpsc::sync_channel(20);
        for (i, region_id) in regions.into_iter().enumerate() {
            snap_ctx.generate_snap(region_id, tx.clone()).unwrap();
            if i < 6 {
                // sizeof(snapshot) == 1918 bytes.
                assert_eq!(snap_ctx.mgr.get_total_snap_size() as usize, 1918 * (1 + i));
            } else {
                assert_eq!(snap_ctx.mgr.get_total_snap_size() as usize, 1918 * 6);
            }
        }
    }
}
