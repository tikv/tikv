// Copyright 2018 PingCAP, Inc.
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

use super::engine::{Engine, Error as EngineError, ScanMode, StatisticsSummary};
use super::metrics::*;
use super::mvcc::{MvccReader, MvccTxn};
use super::{Callback, Error, Key, Result};
use kvproto::kvrpcpb::Context;
use std::fmt::{self, Display, Formatter};
use std::mem;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use util::time::{duration_to_sec, SlowTimer};
use util::worker::{self, Builder, Runnable, ScheduleError, Worker};

// TODO: make it configurable.
pub const GC_BATCH_SIZE: usize = 512;

/// After the GC scan of a key, output a message to the log if there are at least this many
/// versions of the key.
const GC_LOG_FOUND_VERSION_THRESHOLD: usize = 30;

/// After the GC delete versions of a key, output a message to the log if at least this many
/// versions are deleted.
const GC_LOG_DELETED_VERSION_THRESHOLD: usize = 30;

pub const GC_MAX_PENDING_TASKS: usize = 2;
const GC_SNAPSHOT_TIMEOUT_SECS: u64 = 10;
const GC_TASK_SLOW_SECONDS: u64 = 30;

struct GCTask {
    pub ctx: Context,
    pub safe_point: u64,
    pub callback: Callback<()>,
}

impl Display for GCTask {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        let epoch = format!("{:?}", self.ctx.region_epoch.as_ref());
        f.debug_struct("GCTask")
            .field("region", &self.ctx.get_region_id())
            .field("epoch", &epoch)
            .field("safe_point", &self.safe_point)
            .finish()
    }
}

/// `GCRunner` is used to perform GC on the engine
struct GCRunner<E: Engine> {
    engine: E,
    ratio_threshold: f64,

    stats: StatisticsSummary,
}

impl<E: Engine> GCRunner<E> {
    pub fn new(engine: E, ratio_threshold: f64) -> GCRunner<E> {
        GCRunner {
            engine,
            ratio_threshold,

            stats: StatisticsSummary::default(),
        }
    }

    fn get_snapshot(&self, ctx: &mut Context) -> Result<E::Snap> {
        let timeout = Duration::from_secs(GC_SNAPSHOT_TIMEOUT_SECS);
        match wait_op!(|cb| self.engine.async_snapshot(ctx, cb), timeout) {
            Some((cb_ctx, Ok(snapshot))) => {
                if let Some(term) = cb_ctx.term {
                    ctx.set_term(term);
                }
                Ok(snapshot)
            }
            Some((_, Err(e))) => Err(e),
            None => Err(EngineError::Timeout(timeout)),
        }.map_err(Error::from)
    }

    /// Scan keys in the region. Returns scanned keys if any, and a key indicating scan progress
    fn scan_keys(
        &mut self,
        ctx: &mut Context,
        safe_point: u64,
        from: Option<Key>,
        limit: usize,
    ) -> Result<(Vec<Key>, Option<Key>)> {
        let snapshot = self.get_snapshot(ctx)?;
        let mut reader = MvccReader::new(
            snapshot,
            Some(ScanMode::Forward),
            !ctx.get_not_fill_cache(),
            None,
            None,
            ctx.get_isolation_level(),
        );

        let is_range_start = from.is_none();

        // range start gc with from == None, and this is an optimization to
        // skip gc before scanning all data.
        let skip_gc = is_range_start && !reader.need_gc(safe_point, self.ratio_threshold);
        let res = if skip_gc {
            KV_GC_SKIPPED_COUNTER.inc();
            Ok((vec![], None))
        } else {
            reader
                .scan_keys(from, limit)
                .map_err(Error::from)
                .and_then(|(keys, next)| {
                    if keys.is_empty() {
                        assert!(next.is_none());
                        if is_range_start {
                            KV_GC_EMPTY_RANGE_COUNTER.inc();
                        }
                    }
                    Ok((keys, next))
                })
        };
        self.stats.add_statistics(reader.get_statistics());
        res
    }

    /// Clean up outdated data.
    fn gc_keys(
        &mut self,
        ctx: &mut Context,
        safe_point: u64,
        keys: Vec<Key>,
        mut next_scan_key: Option<Key>,
    ) -> Result<Option<Key>> {
        let snapshot = self.get_snapshot(ctx)?;
        let mut txn = MvccTxn::new(snapshot, 0, !ctx.get_not_fill_cache()).unwrap();
        for k in keys {
            let gc_info = txn.gc(k.clone(), safe_point)?;

            if gc_info.found_versions >= GC_LOG_FOUND_VERSION_THRESHOLD {
                info!(
                    "[region {}] GC found at least {} versions for key {}",
                    ctx.get_region_id(),
                    gc_info.found_versions,
                    k
                );
            }
            // TODO: we may delete only part of the versions in a batch, which may not beyond
            // the logging threshold `GC_LOG_DELETED_VERSION_THRESHOLD`.
            if gc_info.deleted_versions as usize >= GC_LOG_DELETED_VERSION_THRESHOLD {
                info!(
                    "[region {}] GC deleted {} versions for key {}",
                    ctx.get_region_id(),
                    gc_info.deleted_versions,
                    k
                );
            }

            if !gc_info.is_completed {
                next_scan_key = Some(k);
                break;
            }
        }
        self.stats.add_statistics(&txn.take_statistics());

        let modifies = txn.into_modifies();
        if !modifies.is_empty() {
            self.engine.write(ctx, modifies)?;
        }
        Ok(next_scan_key)
    }

    pub fn gc(&mut self, ctx: &mut Context, safe_point: u64) -> Result<()> {
        debug!(
            "doing gc on region {}, safe_point {}",
            ctx.get_region_id(),
            safe_point
        );

        let mut next_key = None;
        loop {
            let (keys, next) = self
                .scan_keys(ctx, safe_point, next_key, GC_BATCH_SIZE)
                .map_err(|e| {
                    warn!("gc scan_keys failed on region {}: {:?}", safe_point, &e);
                    e
                })?;
            if keys.is_empty() {
                break;
            }

            next_key = self.gc_keys(ctx, safe_point, keys, next).map_err(|e| {
                warn!("gc_keys failed on region {}: {:?}", safe_point, &e);
                e
            })?;
            if next_key.is_none() {
                break;
            }
        }

        debug!(
            "gc on region {}, safe_point {} has finished",
            ctx.get_region_id(),
            safe_point
        );
        Ok(())
    }
}

impl<E: Engine> Runnable<GCTask> for GCRunner<E> {
    fn run(&mut self, mut task: GCTask) {
        GC_GCTASK_COUNTER.inc();

        let timer = SlowTimer::from_secs(GC_TASK_SLOW_SECONDS);
        let result = self.gc(&mut task.ctx, task.safe_point);
        GC_DURATION_HISTOGRAM.observe(duration_to_sec(timer.elapsed()));
        slow_log!(timer, "{}", task);

        if result.is_err() {
            GC_GCTASK_FAIL_COUNTER.inc();
        }
        (task.callback)(result);
    }

    // The default implementation of `run_batch` prints a warning to log when it takes over 1 second
    // to handle a task. It's not proper here, so override it to remove the log.
    fn run_batch(&mut self, tasks: &mut Vec<GCTask>) {
        for task in tasks.drain(..) {
            self.run(task);
        }
    }

    fn on_tick(&mut self) {
        let stats = mem::replace(&mut self.stats, StatisticsSummary::default());
        for (cf, details) in stats.stat.details() {
            for (tag, count) in details {
                GC_KEYS_COUNTER_VEC
                    .with_label_values(&[cf, tag])
                    .inc_by(count as i64);
            }
        }
    }
}

/// `GCWorker` is used to schedule GC operations
#[derive(Clone)]
pub struct GCWorker<E: Engine> {
    engine: E,
    ratio_threshold: f64,
    worker: Arc<Mutex<Worker<GCTask>>>,
    worker_scheduler: worker::Scheduler<GCTask>,
}

impl<E: Engine> GCWorker<E> {
    pub fn new(engine: E, ratio_threshold: f64) -> GCWorker<E> {
        let worker = Arc::new(Mutex::new(
            Builder::new("gc-worker")
                .pending_capacity(GC_MAX_PENDING_TASKS)
                .create(),
        ));
        let worker_scheduler = worker.lock().unwrap().scheduler();
        GCWorker {
            engine,
            ratio_threshold,
            worker,
            worker_scheduler,
        }
    }

    pub fn start(&self) -> Result<()> {
        let runner = GCRunner::new(self.engine.clone(), self.ratio_threshold);
        self.worker
            .lock()
            .unwrap()
            .start(runner)
            .map_err(|e| box_err!("failed to start gc_worker, err: {:?}", e))
    }

    pub fn stop(&self) -> Result<()> {
        let h = self.worker.lock().unwrap().stop().unwrap();
        if let Err(e) = h.join() {
            Err(box_err!("failed to join gc_worker handle, err: {:?}", e))
        } else {
            Ok(())
        }
    }

    pub fn async_gc(&self, ctx: Context, safe_point: u64, callback: Callback<()>) -> Result<()> {
        self.worker_scheduler
            .schedule(GCTask {
                ctx,
                safe_point,
                callback,
            })
            .or_else(|e| match e {
                ScheduleError::Full(task) => {
                    GC_TOO_BUSY_COUNTER.inc();
                    (task.callback)(Err(Error::GCWorkerTooBusy));
                    Ok(())
                }
                _ => Err(box_err!("failed to schedule gc task: {:?}", e)),
            })
    }
}
