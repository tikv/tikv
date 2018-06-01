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

use super::engine::{Engine, ScanMode, StatisticsSummary};
use super::metrics::*;
use super::mvcc::{MvccReader, MvccTxn, MAX_TXN_WRITE_SIZE};
use super::txn::GC_BATCH_SIZE;
use super::{Callback, Error, Key, Result};
use kvproto::kvrpcpb::Context;
use prometheus::local::{LocalHistogram, LocalIntCounter, LocalIntCounterVec};
use std::fmt::{self, Display, Formatter};
use std::sync::{Arc, Mutex};
use util::worker::{self, Builder, Runnable, ScheduleError, Worker};

pub const GC_MAX_PENDING: usize = 10;

struct GCTask {
    pub ctx: Context,
    pub safe_point: u64,
    pub callback: Callback<()>,
}

impl Display for GCTask {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        let epoch = match self.ctx.region_epoch.as_ref() {
            None => "None".to_string(),
            Some(e) => format!("{{{}, {}}}", e.conf_ver, e.version),
        };
        f.debug_struct("GCTask")
            .field("region", &self.ctx.region_id)
            .field("epoch", &epoch)
            .field("safe_point", &self.safe_point)
            .finish()
    }
}

// `GCRunner` is used to perform GC on the engine
struct GCRunner {
    engine: Box<Engine>,
    ratio_threshold: f64,

    stats: StatisticsSummary,
    empty_range_counter: LocalIntCounter,
    skipped_counter: LocalIntCounter,
    duration_histogram: LocalHistogram,
    gctask_counter: LocalIntCounter,
    gctask_fail_counter: LocalIntCounter,
    gc_keys_counter: LocalIntCounterVec,
}

impl GCRunner {
    pub fn new(engine: Box<Engine>, ratio_threshold: f64) -> GCRunner {
        GCRunner {
            engine,
            ratio_threshold,

            stats: StatisticsSummary::default(),
            empty_range_counter: KV_GC_EMPTY_RANGE_COUNTER.local(),
            skipped_counter: KV_GC_SKIPPED_COUNTER.local(),
            duration_histogram: GC_DURATION_HISTOGRAM.local(),
            gctask_counter: GC_GCTASK_COUNTER.local(),
            gctask_fail_counter: GC_GCTASK_FAIL_COUNTER.local(),
            gc_keys_counter: GC_KEYS_COUNTER_VEC.local(),
        }
    }

    // Scan keys in the region. Returns scanned keys if any, and a key indicating scan progress
    fn gc_scan_keys(
        &mut self,
        ctx: &Context,
        safe_point: u64,
        from: Option<Key>,
    ) -> Result<(Option<Vec<Key>>, Option<Key>)> {
        let is_range_start_gc = from.is_none();

        let snapshot = self.engine.snapshot(ctx)?;
        let mut reader = MvccReader::new(
            snapshot,
            Some(ScanMode::Forward),
            !ctx.get_not_fill_cache(),
            None,
            None,
            ctx.get_isolation_level(),
        );
        let res = if !reader.need_gc(safe_point, self.ratio_threshold) {
            self.skipped_counter.inc();
            Ok((None, None))
        } else {
            reader
                .scan_keys(from, GC_BATCH_SIZE)
                .map_err(Error::from)
                .and_then(|(keys, next)| {
                    if keys.is_empty() {
                        if is_range_start_gc {
                            self.empty_range_counter.inc();
                        }
                        Ok((None, None))
                    } else {
                        Ok((Some(keys), next))
                    }
                })
        };
        self.stats.add_statistics(reader.get_statistics());
        res
    }

    // Clean up outdated data.
    fn gc_keys(
        &mut self,
        ctx: &Context,
        safe_point: u64,
        keys: Vec<Key>,
        mut next_scan_key: Option<Key>,
    ) -> Result<Option<Key>> {
        let snapshot = self.engine.snapshot(ctx)?;
        let mut txn = MvccTxn::new(
            snapshot,
            0,
            Some(ScanMode::Forward),
            ctx.get_isolation_level(),
            !ctx.get_not_fill_cache(),
        );
        for k in keys {
            txn.gc(&k, safe_point)?;
            if txn.write_size() >= MAX_TXN_WRITE_SIZE {
                next_scan_key = Some(k);
                break;
            }
        }
        self.stats.add_statistics(txn.get_statistics());

        let modifies = txn.into_modifies();
        if !modifies.is_empty() {
            self.engine.write(ctx, modifies)?;
        }
        Ok(next_scan_key)
    }

    pub fn gc(&mut self, ctx: Context, safe_point: u64) -> Result<()> {
        let _gc_timer = self.duration_histogram.start_coarse_timer();

        debug!(
            "doing gc on region {}, safe_point {}",
            ctx.region_id, safe_point
        );

        let mut next_key = None;
        loop {
            let (keys, next) = self.gc_scan_keys(&ctx, safe_point, next_key).map_err(|e| {
                warn!("gc_scan_keys failed on region {}: {:?}", safe_point, &e);
                e
            })?;
            if keys.is_none() {
                break;
            }

            next_key = self.gc_keys(&ctx, safe_point, keys.unwrap(), next)
                .map_err(|e| {
                    warn!("gc_keys failed on region {}: {:?}", safe_point, &e);
                    e
                })?;
            if next_key.is_none() {
                break;
            }

            debug!(
                "doing gc on region {}, safe_point {}, scan_from {:?}",
                ctx.region_id, safe_point, next_key
            );
        }

        debug!(
            "gc on region {}, safe_point {} has finished",
            ctx.region_id, safe_point
        );
        Ok(())
    }
}

impl Runnable<GCTask> for GCRunner {
    fn run(&mut self, task: GCTask) {
        self.gctask_counter.inc();
        let result = self.gc(task.ctx, task.safe_point);
        if result.is_err() {
            self.gctask_fail_counter.inc();
        }
        (task.callback)(result);
    }

    fn on_tick(&mut self) {
        for (cf, details) in self.stats.stat.details() {
            for (tag, count) in details {
                self.gc_keys_counter
                    .with_label_values(&[cf, tag])
                    .inc_by(count as i64);
            }
        }
        self.empty_range_counter.flush();
        self.skipped_counter.flush();
        self.duration_histogram.flush();
        self.gctask_counter.flush();
        self.gctask_fail_counter.flush();
        self.gc_keys_counter.flush();
    }
}

// `GCWorker` is used to schedule GC operations
#[derive(Clone)]
pub struct GCWorker {
    engine: Box<Engine>,
    ratio_threshold: f64,
    worker: Arc<Mutex<Worker<GCTask>>>,
    worker_scheduler: worker::Scheduler<GCTask>,
}

impl GCWorker {
    pub fn new(engine: Box<Engine>, ratio_threshold: f64) -> GCWorker {
        let worker = Arc::new(Mutex::new(
            Builder::new("gc-scheduler")
                .pending_capacity(GC_MAX_PENDING)
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
                    (task.callback)(Err(Error::GCWorkerTooBusy));
                    Ok(())
                }
                _ => Err(box_err!("failed to schedule gc task: {:?}", e)),
            })
    }
}
