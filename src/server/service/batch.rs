// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::server::load_statistics::ThreadLoad;
use crate::server::metrics::*;
use crate::server::service::kv::{
    batch_commands_request, batch_commands_response, future_batch_get_command,
    future_raw_batch_get_command, poll_future_notify,
};
use crate::storage::{kv::Engine, lock_manager::LockManager, txn::PointGetCommand, Storage};
use kvproto::kvrpcpb::*;
use tikv_util::collections::HashMap;
use tikv_util::metrics::HistogramReader;
use tikv_util::mpsc::batch::Sender;

const REQUEST_BATCH_LIMITER_SAMPLE_WINDOW: usize = 30;
const REQUEST_BATCH_LIMITER_LOW_LOAD_RATIO: f32 = 0.3;

#[derive(Hash, PartialEq, Eq, Debug)]
struct RegionVerId {
    region_id: u64,
    conf_ver: u64,
    version: u64,
    term: u64,
}

impl RegionVerId {
    #[inline]
    fn from_context(ctx: &Context) -> Self {
        RegionVerId {
            region_id: ctx.get_region_id(),
            conf_ver: ctx.get_region_epoch().get_conf_ver(),
            version: ctx.get_region_epoch().get_version(),
            term: ctx.get_term(),
        }
    }
}

#[derive(Hash, PartialEq, Eq, Debug)]
struct ReadId {
    region: RegionVerId,
    // None in this field stands for transactional read.
    cf: Option<String>,
}

impl ReadId {
    #[inline]
    fn from_context_cf(ctx: &Context, cf: Option<String>) -> Self {
        ReadId {
            region: RegionVerId::from_context(ctx),
            cf,
        }
    }
}

/// Batcher buffers specific requests in one stream of `batch_commands` in a batch for bulk submit.
trait Batcher<E: Engine, L: LockManager> {
    /// Try to batch single batch_command request, returns whether the request is stashed.
    /// One batcher must only process requests from one unique command stream.
    fn filter(
        &mut self,
        request_id: u64,
        request: &mut batch_commands_request::request::Cmd,
    ) -> bool;

    /// Submit all batched requests to store. `is_empty` always returns true after this operation.
    /// Returns number of fused commands been submitted.
    fn submit(
        &mut self,
        tx: &Sender<(u64, batch_commands_response::Response)>,
        storage: &Storage<E, L>,
    ) -> usize;

    /// Whether this batcher is empty of buffered requests.
    fn is_empty(&self) -> bool;
}

#[derive(Hash, PartialEq, Eq, PartialOrd, Ord)]
enum BatchableRequestKind {
    PointGet,
    Prewrite,
    Commit,
}

impl BatchableRequestKind {
    fn as_str(&self) -> &str {
        match self {
            BatchableRequestKind::PointGet => &"point_get",
            BatchableRequestKind::Prewrite => &"prewrite",
            BatchableRequestKind::Commit => &"commit",
        }
    }
}

/// BatchLimiter controls submit timing of request batch.
struct BatchLimiter {
    cmd: BatchableRequestKind,
    timeout: Option<Duration>,
    last_submit_time: Instant,
    latency_reader: HistogramReader,
    latency_estimation: f64,
    thread_load_reader: Arc<ThreadLoad>,
    thread_load_estimation: usize,
    sample_size: usize,
    enable_batch: bool,
    batch_input: usize,
}

impl BatchLimiter {
    /// Construct a new `BatchLimiter` with provided timeout-duration,
    /// and reader on latency and thread-load.
    fn new(
        cmd: BatchableRequestKind,
        timeout: Option<Duration>,
        latency_reader: HistogramReader,
        thread_load_reader: Arc<ThreadLoad>,
    ) -> Self {
        BatchLimiter {
            cmd,
            timeout,
            last_submit_time: Instant::now(),
            latency_reader,
            latency_estimation: 0.0,
            thread_load_reader,
            thread_load_estimation: 100,
            sample_size: 0,
            enable_batch: false,
            batch_input: 0,
        }
    }

    /// Whether this batch limiter is disabled.
    #[inline]
    fn disabled(&self) -> bool {
        self.timeout.is_none()
    }

    /// Whether the batch is timely due to be submitted.
    #[inline]
    fn is_due(&self, now: Instant) -> bool {
        if let Some(timeout) = self.timeout {
            now - self.last_submit_time >= timeout
        } else {
            true
        }
    }

    /// Whether current batch needs more requests.
    #[inline]
    fn needs_more(&self) -> bool {
        self.enable_batch
    }

    /// Observe a tick from timer guard. Limiter will update statistics at this point.
    #[inline]
    fn observe_tick(&mut self) {
        if self.disabled() {
            return;
        }
        self.sample_size += 1;
        if self.enable_batch {
            // check if thread load is too low, which means busy hour has passed.
            if self.thread_load_reader.load()
                < (self.thread_load_estimation as f32 * REQUEST_BATCH_LIMITER_LOW_LOAD_RATIO)
                    as usize
            {
                self.enable_batch = false;
            }
        } else if self.sample_size > REQUEST_BATCH_LIMITER_SAMPLE_WINDOW {
            self.sample_size = 0;
            let latency = self.latency_reader.read_latest_avg() * 1000.0;
            let load = self.thread_load_reader.load();
            self.latency_estimation = (self.latency_estimation + latency) / 2.0;
            if load > 70 {
                // thread load is less sensitive to workload,
                // a small barrier here to make sure we have good samples of thread load.
                let timeout = self.timeout.unwrap();
                if latency > timeout.as_millis() as f64 * 2.0 {
                    self.thread_load_estimation = (self.thread_load_estimation + load) / 2;
                }
                if self.latency_estimation > timeout.as_millis() as f64 * 2.0 {
                    self.enable_batch = true;
                    self.latency_estimation = 0.0;
                }
            }
        }
    }

    /// Observe the size of commands been examined by batcher.
    /// Command may not be batched but must have the valid type for this batch.
    #[inline]
    fn observe_input(&mut self, size: usize) {
        self.batch_input += size;
    }

    /// Observe the time and output size of one batch submit.
    #[inline]
    fn observe_submit(&mut self, now: Instant, size: usize) {
        self.last_submit_time = now;
        if self.enable_batch {
            REQUEST_BATCH_SIZE_HISTOGRAM_VEC
                .with_label_values(&[self.cmd.as_str()])
                .observe(self.batch_input as f64);
            if size > 0 {
                REQUEST_BATCH_RATIO_HISTOGRAM_VEC
                    .with_label_values(&[self.cmd.as_str()])
                    .observe(self.batch_input as f64 / size as f64);
            }
        }
        self.batch_input = 0;
    }
}

/// ReadBatcher batches normal-priority `raw_get` and `get` requests to the same region.
struct ReadBatcher {
    router: HashMap<ReadId, (Vec<u64>, Vec<PointGetCommand>)>,
}

impl ReadBatcher {
    fn new() -> Self {
        ReadBatcher {
            router: HashMap::default(),
        }
    }

    fn is_batchable_context(ctx: &Context) -> bool {
        ctx.get_priority() == CommandPri::Normal && !ctx.get_replica_read()
    }

    fn add_get(&mut self, request_id: u64, request: &mut GetRequest) {
        let id = ReadId::from_context_cf(request.get_context(), None);
        let command = PointGetCommand::from_get(request);
        match self.router.get_mut(&id) {
            Some((reqs, commands)) => {
                reqs.push(request_id);
                commands.push(command);
            }
            None => {
                self.router.insert(id, (vec![request_id], vec![command]));
            }
        }
    }

    fn add_raw_get(&mut self, request_id: u64, request: &mut RawGetRequest) {
        let cf = Some(request.take_cf());
        let id = ReadId::from_context_cf(request.get_context(), cf);
        let command = PointGetCommand::from_raw_get(request);
        match self.router.get_mut(&id) {
            Some((reqs, commands)) => {
                reqs.push(request_id);
                commands.push(command);
            }
            None => {
                self.router.insert(id, (vec![request_id], vec![command]));
            }
        }
    }
}

impl<E: Engine, L: LockManager> Batcher<E, L> for ReadBatcher {
    fn filter(
        &mut self,
        request_id: u64,
        request: &mut batch_commands_request::request::Cmd,
    ) -> bool {
        match request {
            batch_commands_request::request::Cmd::Get(req)
                if Self::is_batchable_context(req.get_context()) =>
            {
                self.add_get(request_id, req);
                true
            }
            batch_commands_request::request::Cmd::RawGet(req)
                if Self::is_batchable_context(req.get_context()) =>
            {
                self.add_raw_get(request_id, req);
                true
            }
            _ => false,
        }
    }

    fn submit(
        &mut self,
        tx: &Sender<(u64, batch_commands_response::Response)>,
        storage: &Storage<E, L>,
    ) -> usize {
        let mut output = 0;
        for (id, (reqs, commands)) in self.router.drain() {
            let tx = tx.clone();
            output += 1;
            match id.cf {
                Some(cf) => {
                    let f = future_raw_batch_get_command(storage, tx, reqs, cf, commands);
                    poll_future_notify(f);
                }
                None => {
                    let f = future_batch_get_command(storage, tx, reqs, commands);
                    poll_future_notify(f);
                }
            }
        }
        output
    }

    fn is_empty(&self) -> bool {
        self.router.is_empty()
    }
}

type ReqBatcherInner<E, L> = (BatchLimiter, Box<dyn Batcher<E, L> + Send>);

/// ReqBatcher manages multiple `Batcher`s which batch requests from one unique stream of `batch_commands`
// and controls the submit timing of those batchers based on respective `BatchLimiter`.
pub struct ReqBatcher<E: Engine, L: LockManager> {
    inners: BTreeMap<BatchableRequestKind, ReqBatcherInner<E, L>>,
    tx: Sender<(u64, batch_commands_response::Response)>,
}

impl<E: Engine, L: LockManager> ReqBatcher<E, L> {
    /// Constructs a new `ReqBatcher` which provides batching of one request stream with specific response channel.
    pub fn new(
        tx: Sender<(u64, batch_commands_response::Response)>,
        timeout: Option<Duration>,
        readpool_thread_load: Arc<ThreadLoad>,
    ) -> Self {
        let mut inners = BTreeMap::<BatchableRequestKind, ReqBatcherInner<E, L>>::default();
        inners.insert(
            BatchableRequestKind::PointGet,
            (
                BatchLimiter::new(
                    BatchableRequestKind::PointGet,
                    timeout,
                    HistogramReader::new(GRPC_MSG_HISTOGRAM_VEC.kv_get.clone()),
                    readpool_thread_load,
                ),
                Box::new(ReadBatcher::new()),
            ),
        );
        ReqBatcher { inners, tx }
    }

    /// Try to batch single batch_command request, returns whether the request is stashed.
    /// One batcher can only accept requests from one unique command stream.
    pub fn filter(
        &mut self,
        request_id: u64,
        request: &mut batch_commands_request::Request,
    ) -> bool {
        if let Some(ref mut cmd) = request.cmd {
            if let Some((limiter, batcher)) = match cmd {
                batch_commands_request::request::Cmd::Prewrite(_) => {
                    self.inners.get_mut(&BatchableRequestKind::Prewrite)
                }
                batch_commands_request::request::Cmd::Commit(_) => {
                    self.inners.get_mut(&BatchableRequestKind::Commit)
                }
                batch_commands_request::request::Cmd::Get(_)
                | batch_commands_request::request::Cmd::RawGet(_) => {
                    self.inners.get_mut(&BatchableRequestKind::PointGet)
                }
                _ => None,
            } {
                // in normal mode, batch requests inside one `batch_commands`.
                // in cross-command mode, only batch request when limiter permits.
                if limiter.disabled() || limiter.needs_more() {
                    limiter.observe_input(1);
                    return batcher.filter(request_id, cmd);
                }
            }
        }
        false
    }

    /// Check all batchers and submit if their limiters see fit.
    /// Called by anyone with a suitable timeslice for executing commands.
    #[inline]
    pub fn maybe_submit(&mut self, storage: &Storage<E, L>) {
        let mut now = None;
        for (limiter, batcher) in self.inners.values_mut() {
            if limiter.disabled() || !limiter.needs_more() {
                if now.is_none() {
                    now = Some(Instant::now());
                }
                limiter.observe_submit(now.unwrap(), batcher.submit(&self.tx, storage));
            }
        }
    }

    /// Check all batchers and submit if their wait duration has exceeded the max limit.
    /// Called repeatedly every `request-batch-wait-duration` interval after the batcher starts working.
    #[inline]
    pub fn should_submit(&mut self, storage: &Storage<E, L>) {
        let now = Instant::now();
        for (limiter, batcher) in self.inners.values_mut() {
            limiter.observe_tick();
            if limiter.is_due(now) {
                limiter.observe_submit(now, batcher.submit(&self.tx, storage));
            }
        }
    }

    /// Whether or not every batcher is empty of buffered requests.
    #[inline]
    pub fn is_empty(&self) -> bool {
        for (_, batcher) in self.inners.values() {
            if !batcher.is_empty() {
                return false;
            }
        }
        true
    }
}
