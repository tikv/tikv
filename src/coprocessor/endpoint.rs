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

use std::{mem, thread, usize};
use std::time::Duration;
use std::sync::{Arc, Mutex};
use std::cell::RefCell;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::fmt::{self, Debug, Display, Formatter};

use protobuf::{CodedInputStream, Message as PbMsg};
use grpc::Error as GrpcError;
use futures::{future, stream};
use futures_cpupool::{Builder as CpuPoolBuilder, CpuPool};

use tipb::select::{self, DAGRequest, SelectRequest};
use tipb::analyze::{AnalyzeReq, AnalyzeType};
use tipb::executor::ExecType;
use tipb::schema::ColumnInfo;
use kvproto::coprocessor::{KeyRange, Request, Response};
use kvproto::errorpb::{self, ServerIsBusy};
use kvproto::kvrpcpb::{CommandPri, IsolationLevel};

use util::time::{duration_to_sec, Instant};
use util::worker::{BatchRunnable, FutureScheduler, Scheduler};
use util::collections::HashMap;
use server::{Config, OnResponse, ResponseStream};
use storage::{self, engine, Engine, FlowStatistics, Snapshot, Statistics, StatisticsSummary};
use storage::engine::Error as EngineError;
use pd::PdTask;

use super::codec::mysql;
use super::codec::datum::Datum;
use super::select::select::SelectContext;
use super::dag::DAGContext;
use super::statistics::analyze::AnalyzeContext;
use super::metrics::*;
use super::{Error, Result};

pub const REQ_TYPE_SELECT: i64 = 101;
pub const REQ_TYPE_INDEX: i64 = 102;
pub const REQ_TYPE_DAG: i64 = 103;
pub const REQ_TYPE_ANALYZE: i64 = 104;

// If a request has been handled for more than 60 seconds, the client should
// be timeout already, so it can be safely aborted.
const REQUEST_MAX_HANDLE_SECS: u64 = 60;
// If handle time is larger than the lower bound, the query is considered as slow query.
const SLOW_QUERY_LOWER_BOUND: f64 = 1.0; // 1 second.

const DEFAULT_ERROR_CODE: i32 = 1;

pub const SINGLE_GROUP: &'static [u8] = b"SingleGroup";

const OUTDATED_ERROR_MSG: &'static str = "request outdated.";

const ENDPOINT_IS_BUSY: &'static str = "endpoint is busy";

struct CopContextInner {
    select_stats: StatisticsSummary,
    index_stats: StatisticsSummary,
    flow_stats: HashMap<u64, FlowStatistics>,
    timer: Instant,
    pd_task_sender: FutureScheduler<PdTask>,
}

impl CopContextInner {
    fn new(pd_task_sender: FutureScheduler<PdTask>) -> Self {
        CopContextInner {
            select_stats: StatisticsSummary::default(),
            index_stats: StatisticsSummary::default(),
            flow_stats: map![],
            timer: Instant::now_coarse(),
            pd_task_sender: pd_task_sender,
        }
    }

    fn get_statistics(&mut self, type_str: &str) -> &mut StatisticsSummary {
        match type_str {
            STR_REQ_TYPE_SELECT => &mut self.select_stats,
            STR_REQ_TYPE_INDEX => &mut self.index_stats,
            _ => {
                warn!("unknown STR_REQ_TYPE: {}", type_str);
                &mut self.select_stats
            }
        }
    }

    fn add_statistics(&mut self, type_str: &str, stats: &Statistics) {
        self.get_statistics(type_str).add_statistics(stats);
    }

    fn add_flow_stats_by_region(&mut self, region_id: u64, stats: &Statistics) {
        let flow_stats = self.flow_stats
            .entry(region_id)
            .or_insert_with(FlowStatistics::default);
        flow_stats.add(&stats.write.flow_stats);
        flow_stats.add(&stats.data.flow_stats);
    }

    fn collect(&mut self, region_id: u64, scan_tag: &str, stats: &Statistics) {
        self.add_statistics(scan_tag, stats);
        self.add_flow_stats_by_region(region_id, stats);

        let new_timer = Instant::now_coarse();
        if new_timer.duration_since(self.timer) > Duration::from_secs(1) {
            self.timer = new_timer;
            for type_str in &[STR_REQ_TYPE_SELECT, STR_REQ_TYPE_INDEX] {
                let this_statistics = self.get_statistics(type_str);
                if this_statistics.count == 0 {
                    continue;
                }
                for (cf, details) in this_statistics.stat.details() {
                    for (tag, count) in details {
                        COPR_SCAN_DETAILS
                            .with_label_values(&[type_str, cf, tag])
                            .inc_by(count as f64)
                            .unwrap();
                    }
                }
                mem::replace(this_statistics, StatisticsSummary::default());
            }
            let pd_task = PdTask::ReadStats {
                read_stats: mem::replace(&mut self.flow_stats, map![]),
            };
            if let Err(e) = self.pd_task_sender.schedule(pd_task) {
                error!("send coprocessor statistics: {:?}", e);
            }
        }
    }
}

struct CopContext(RefCell<CopContextInner>);

unsafe impl Sync for CopContext {}

#[derive(Clone)]
struct CopContextPool {
    cop_ctxs: Arc<HashMap<thread::ThreadId, CopContext>>,
}

impl CopContextPool {
    fn new(ctxs: Arc<HashMap<thread::ThreadId, CopContext>>) -> Self {
        CopContextPool { cop_ctxs: ctxs }
    }

    // Must run in CpuPool.
    fn collect(&self, region_id: u64, scan_tag: &str, stats: &Statistics) {
        let thread_id = thread::current().id();
        let cop_ctx = self.cop_ctxs.get(&thread_id).unwrap();
        cop_ctx.0.borrow_mut().collect(region_id, scan_tag, stats);
    }
}

struct ExecutorPool {
    pool: CpuPool,
    contexts: CopContextPool,
}

pub struct Host {
    engine: Box<Engine>,
    sched: Scheduler<Task>,
    reqs: HashMap<u64, Vec<RequestTask>>,
    last_req_id: u64,
    pool: ExecutorPool,
    low_priority_pool: ExecutorPool,
    high_priority_pool: ExecutorPool,
    max_running_task_count: usize,
    running_task_count: Arc<AtomicUsize>,
    batch_row_limit: usize,
    stream_batch_row_limit: usize,
}

impl Host {
    pub fn new(
        engine: Box<Engine>,
        scheduler: Scheduler<Task>,
        cfg: &Config,
        pd_task_sender: FutureScheduler<PdTask>,
    ) -> Host {
        let create_pool = |name_prefix: &str, size: usize| {
            let cop_ctxs = Arc::new(Mutex::new((map![], 0)));
            let cpu_pool = {
                let cop_ctxs = cop_ctxs.clone();
                let sender = pd_task_sender.clone();
                CpuPoolBuilder::new()
                    .name_prefix(name_prefix)
                    .pool_size(size)
                    .after_start(move || {
                        let thread_id = thread::current().id();
                        let cop_ctx =
                            CopContext(RefCell::new(CopContextInner::new(sender.clone())));
                        let mut map_counter = cop_ctxs.lock().unwrap();
                        map_counter.0.insert(thread_id, cop_ctx);
                        map_counter.1 += 1;
                    })
                    .create()
            };
            loop {
                // For now we can only use a Mutex to collect thread_ids.
                // With next release of futures-cpupool, we can use a channel.
                thread::sleep(Duration::from_millis(10));
                let mut map_counter = cop_ctxs.lock().unwrap();
                if map_counter.1 >= size {
                    let cop_ctxs = mem::replace(&mut map_counter.0, map![]);
                    return ExecutorPool {
                        pool: cpu_pool,
                        contexts: CopContextPool::new(Arc::new(cop_ctxs)),
                    };
                }
            }
        };

        Host {
            engine: engine,
            sched: scheduler,
            reqs: HashMap::default(),
            last_req_id: 0,
            pool: create_pool("endpoint-normal-pool", cfg.end_point_concurrency),
            low_priority_pool: create_pool("endpoint-low-pool", cfg.end_point_concurrency),
            high_priority_pool: create_pool("endpoint-high-pool", cfg.end_point_concurrency),
            max_running_task_count: cfg.end_point_max_tasks,
            batch_row_limit: cfg.end_point_batch_row_limit,
            stream_batch_row_limit: cfg.end_point_stream_batch_row_limit,
            running_task_count: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn running_task_count(&self) -> usize {
        self.running_task_count.load(Ordering::Acquire)
    }

    fn notify_batch_failed<E: Into<Error> + Debug>(&mut self, e: E, batch_id: u64) {
        debug!("failed to handle batch request: {:?}", e);
        let resp = err_resp(e.into());
        for t in self.reqs.remove(&batch_id).unwrap() {
            t.on_resp.respond(resp.clone());
        }
    }

    fn handle_request(&mut self, snap: Box<Snapshot>, mut t: RequestTask) {
        t.metrics.stop_record_waiting();
        if let Err(e) = t.check_outdated() {
            return t.on_resp.respond(err_resp(e));
        }

        let pool_and_ctx_pool = match t.req.get_context().get_priority() {
            CommandPri::Low => &mut self.low_priority_pool,
            CommandPri::High => &mut self.high_priority_pool,
            CommandPri::Normal => &mut self.pool,
        };
        let pool = &mut pool_and_ctx_pool.pool;
        let mut ctx_pool = pool_and_ctx_pool.contexts.clone();
        let task_count = self.running_task_count.clone();

        let (mut req, cop_req, req_ctx, on_resp) = (t.req, t.cop_req, t.ctx, t.on_resp);
        let mut metrics = t.metrics;
        let ranges = req.take_ranges().into_vec();
        let batch_row_limit = self.batch_row_limit;
        let stream_batch_row_limit = self.stream_batch_row_limit;
        let mut statistics = Statistics::default();

        fn on_finish(
            running_task_count: &AtomicUsize,
            stats: &Statistics,
            metrics: &mut CopMetrics,
            ctxs: &mut CopContextPool,
        ) {
            metrics.stop_record_handling(stats);
            ctxs.collect(metrics.region_id, metrics.scan_tag, stats);
            running_task_count.fetch_sub(1, Ordering::Release);
        }

        match cop_req {
            CopRequest::Select(sel) => {
                let mut ctx = match SelectContext::new(sel, snap, req_ctx, batch_row_limit) {
                    Ok(ctx) => ctx,
                    Err(e) => return on_resp.respond(err_resp(e)),
                };
                let do_request = move || {
                    let resp = ctx.handle_request(ranges).unwrap_or_else(err_resp);
                    ctx.collect_statistics_into(&mut statistics);
                    on_finish(&task_count, &statistics, &mut metrics, &mut ctx_pool);
                    future::ok::<_, ()>(on_resp.respond(resp))
                };
                pool.spawn_fn(do_request).forget();
            }
            CopRequest::DAG(dag) => {
                let mut ctx = match DAGContext::new(
                    dag,
                    ranges,
                    snap,
                    req_ctx,
                    batch_row_limit,
                    stream_batch_row_limit,
                ) {
                    Ok(ctx) => ctx,
                    Err(e) => return on_resp.respond(err_resp(e)),
                };
                if !on_resp.is_streaming() {
                    let do_request = move || {
                        let res = ctx.handle_request();
                        let resp = res.unwrap_or_else(err_resp);
                        ctx.collect_statistics_into(&mut statistics);
                        on_finish(&task_count, &statistics, &mut metrics, &mut ctx_pool);
                        future::ok::<_, ()>(on_resp.respond(resp))
                    };
                    return pool.spawn_fn(do_request).forget();
                }
                // For streaming.
                let pool_1 = pool.clone();
                let f = move || {
                    let s = stream::unfold(Some(ctx), move |ctx_opt| {
                        ctx_opt.and_then(|mut ctx| {
                            let (resp, remain) = ctx.handle_streaming_request()
                                .unwrap_or_else(|e| (err_resp(e), false));
                            if remain {
                                return Some(future::ok::<_, GrpcError>((resp, Some(ctx))));
                            }
                            let mut stats = Statistics::default();
                            ctx.collect_statistics_into(&mut stats);
                            on_finish(&task_count, &stats, &mut metrics, &mut ctx_pool);
                            Some(future::ok::<_, GrpcError>((resp, None)))
                        })
                    });
                    let pre_resolve_size = 2;
                    let mut resp_stream = ResponseStream::new(pre_resolve_size);
                    resp_stream.pre_resolve_and_spawn(s, pool_1);
                    future::ok::<_, ()>(on_resp.respond_stream(resp_stream))
                };
                pool.spawn_fn(f).forget()
            }
            CopRequest::Analyze(analyze) => {
                let ctx = AnalyzeContext::new(analyze, ranges, snap, &req_ctx);
                let do_request = move || {
                    let resp = ctx.handle_request(&mut statistics).unwrap_or_else(err_resp);
                    on_finish(&task_count, &statistics, &mut metrics, &mut ctx_pool);
                    future::ok::<_, ()>(on_resp.respond(resp))
                };
                pool.spawn_fn(do_request).forget();
            }
        }
    }

    fn handle_snapshot_result(&mut self, id: u64, snapshot: engine::Result<Box<Snapshot>>) {
        let snap = match snapshot {
            Ok(s) => s,
            Err(e) => return self.notify_batch_failed(e, id),
        };
        for req in self.reqs.remove(&id).unwrap() {
            self.handle_request(snap.clone(), req);
        }
    }
}

pub enum Task {
    Request(RequestTask),
    SnapRes(u64, engine::Result<Box<Snapshot>>),
    BatchSnapRes(Vec<(u64, engine::Result<Box<Snapshot>>)>),
    RetryRequests(Vec<u64>),
}

impl Display for Task {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match *self {
            Task::Request(ref req) => write!(f, "{}", req),
            Task::SnapRes(req_id, _) => write!(f, "snapres [{}]", req_id),
            Task::BatchSnapRes(_) => write!(f, "batch snapres"),
            Task::RetryRequests(ref retry) => write!(f, "retry on task ids: {:?}", retry),
        }
    }
}

#[derive(Debug)]
enum CopRequest {
    Select(SelectRequest),
    DAG(DAGRequest),
    Analyze(AnalyzeReq),
}

#[derive(Debug)]
pub struct ReqContext {
    // The deadline before which the task should be responded.
    pub deadline: Instant,
    pub isolation_level: IsolationLevel,
    pub fill_cache: bool,
    // whether is a table scan request.
    pub table_scan: bool,
}

impl ReqContext {
    #[inline]
    fn get_scan_tag(&self) -> &'static str {
        if self.table_scan {
            STR_REQ_TYPE_SELECT
        } else {
            STR_REQ_TYPE_INDEX
        }
    }

    pub fn check_if_outdated(&self) -> Result<()> {
        let now = Instant::now_coarse();
        if self.deadline <= now {
            return Err(Error::Outdated(self.deadline, now, self.get_scan_tag()));
        }
        Ok(())
    }
}

#[derive(Debug)]
struct CopMetrics {
    region_id: u64,
    ranges_len: usize,
    first_range: Option<KeyRange>,
    scan_tag: &'static str,
    pri_str: &'static str,
    timer: Instant,
    wait_time: f64,
    start_ts: u64,
}

impl CopMetrics {
    fn stop_record_waiting(&mut self) {
        if self.wait_time > 0f64 {
            return;
        }
        self.wait_time = duration_to_sec(self.timer.elapsed());
        COPR_REQ_WAIT_TIME
            .with_label_values(&[self.scan_tag])
            .observe(self.wait_time);

        COPR_PENDING_REQS
            .with_label_values(&[self.scan_tag, self.pri_str])
            .add(1.0);
    }

    fn stop_record_handling(&mut self, stats: &Statistics) {
        self.stop_record_waiting();
        let query_time = duration_to_sec(self.timer.elapsed());
        COPR_REQ_HISTOGRAM_VEC
            .with_label_values(&[self.scan_tag])
            .observe(query_time);

        let handle_time = query_time - self.wait_time;
        COPR_REQ_HANDLE_TIME
            .with_label_values(&[self.scan_tag])
            .observe(handle_time);

        COPR_SCAN_KEYS
            .with_label_values(&[self.scan_tag])
            .observe(stats.total_op_count() as f64);

        COPR_PENDING_REQS
            .with_label_values(&[self.scan_tag, self.pri_str])
            .dec();

        if handle_time > SLOW_QUERY_LOWER_BOUND {
            info!(
                "[region {}] handle {:?} [{}] takes {:?} [keys: {}, hit: {}, \
                 ranges: {} ({:?})]",
                self.region_id,
                self.start_ts,
                self.scan_tag,
                handle_time,
                stats.total_op_count(),
                stats.total_processed(),
                self.ranges_len,
                self.first_range,
            );
        }
    }
}

#[derive(Debug)]
pub struct RequestTask {
    req: Request,
    cop_req: CopRequest,
    ctx: ReqContext,
    on_resp: OnResponse,
    metrics: CopMetrics,
}

impl RequestTask {
    pub fn new(req: Request, on_resp: OnResponse, recursion_limit: u32) -> Result<RequestTask> {
        let table_scan;
        let (start_ts, cop_req) = match req.get_tp() {
            tp @ REQ_TYPE_SELECT | tp @ REQ_TYPE_INDEX => {
                let mut is = CodedInputStream::from_bytes(req.get_data());
                is.set_recursion_limit(recursion_limit);
                let mut sel = SelectRequest::new();
                box_try!(sel.merge_from(&mut is));
                table_scan = tp == REQ_TYPE_SELECT;
                (sel.get_start_ts(), CopRequest::Select(sel))
            }
            REQ_TYPE_DAG => {
                let mut is = CodedInputStream::from_bytes(req.get_data());
                is.set_recursion_limit(recursion_limit);
                let mut dag = DAGRequest::new();
                box_try!(dag.merge_from(&mut is));
                table_scan = dag.get_executors()
                    .iter()
                    .next()
                    .map_or(false, |scan| scan.get_tp() == ExecType::TypeTableScan);
                (dag.get_start_ts(), CopRequest::DAG(dag))
            }
            REQ_TYPE_ANALYZE => {
                let mut is = CodedInputStream::from_bytes(req.get_data());
                is.set_recursion_limit(recursion_limit);
                let mut analyze = AnalyzeReq::new();
                box_try!(analyze.merge_from(&mut is));
                table_scan = analyze.get_tp() == AnalyzeType::TypeColumn;
                (analyze.get_start_ts(), CopRequest::Analyze(analyze))
            }
            tp => return Err(box_err!("unsupported tp {}", tp)),
        };

        let timer = Instant::now_coarse();
        let deadline = timer + Duration::from_secs(REQUEST_MAX_HANDLE_SECS);
        let req_ctx = ReqContext {
            deadline: deadline,
            isolation_level: req.get_context().get_isolation_level(),
            fill_cache: !req.get_context().get_not_fill_cache(),
            table_scan: table_scan,
        };
        let metrics = CopMetrics {
            region_id: req.get_context().get_region_id(),
            ranges_len: req.get_ranges().len(),
            first_range: req.get_ranges().get(0).cloned(),
            scan_tag: req_ctx.get_scan_tag(),
            pri_str: get_req_pri_str(req.get_context().get_priority()),
            timer: timer,
            wait_time: 0f64,
            start_ts: start_ts,
        };

        Ok(RequestTask {
            req: req,
            cop_req: cop_req,
            ctx: req_ctx,
            on_resp: on_resp,
            metrics: metrics,
        })
    }

    #[inline]
    fn check_outdated(&self) -> Result<()> {
        self.ctx.check_if_outdated()
    }

    pub fn priority(&self) -> CommandPri {
        self.req.get_context().get_priority()
    }

    fn get_request_key(&self) -> (u64, u64, u64) {
        let ctx = self.req.get_context();
        let region_id = ctx.get_region_id();
        let version = ctx.get_region_epoch().get_version();
        let peer_id = ctx.get_peer().get_id();
        (region_id, version, peer_id)
    }
}

impl Display for RequestTask {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(
            f,
            "request [context {:?}, tp: {}, ranges: {} ({:?})]",
            self.req.get_context(),
            self.req.get_tp(),
            self.req.get_ranges().len(),
            self.req.get_ranges().get(0)
        )
    }
}

impl BatchRunnable<Task> for Host {
    fn run_batch(&mut self, tasks: &mut Vec<Task>) {
        let mut grouped_reqs = map![];
        for task in tasks.drain(..) {
            match task {
                Task::Request(req) => {
                    if let Err(e) = req.check_outdated() {
                        req.on_resp.respond(err_resp(e));
                        continue;
                    }
                    let key = req.get_request_key();
                    grouped_reqs.entry(key).or_insert_with(Vec::new).push(req);
                }
                Task::SnapRes(q_id, snap_res) => {
                    self.handle_snapshot_result(q_id, snap_res);
                }
                Task::BatchSnapRes(batch) => for (q_id, snap_res) in batch {
                    self.handle_snapshot_result(q_id, snap_res);
                },
                Task::RetryRequests(retry) => for id in retry {
                    if let Err(e) = {
                        let ctx = self.reqs[&id][0].req.get_context();
                        let sched = self.sched.clone();
                        self.engine.async_snapshot(ctx, box move |(_, res)| {
                            sched.schedule(Task::SnapRes(id, res)).unwrap()
                        })
                    } {
                        self.notify_batch_failed(e, id);
                    }
                },
            }
        }

        if grouped_reqs.is_empty() {
            return;
        }

        let mut batch = Vec::with_capacity(grouped_reqs.len());
        let start_id = self.last_req_id + 1;
        for (_, reqs) in grouped_reqs {
            let max_running_task_count = self.max_running_task_count;
            if self.running_task_count() >= max_running_task_count {
                for req in reqs {
                    let resp = err_resp(Error::Full(max_running_task_count));
                    req.on_resp.respond(resp);
                }
                continue;
            }

            for _ in &reqs {
                self.running_task_count.fetch_add(1, Ordering::Release);
            }
            self.last_req_id += 1;
            batch.push(reqs[0].req.get_context().clone());
            self.reqs.insert(self.last_req_id, reqs);
        }
        let end_id = self.last_req_id;

        let sched = self.sched.clone();
        let on_finished: engine::BatchCallback<Box<Snapshot>> = box move |results: Vec<_>| {
            let mut ready = Vec::with_capacity(results.len());
            let mut retry = Vec::new();
            for (id, res) in (start_id..end_id + 1).zip(results) {
                match res {
                    Some((_, res)) => ready.push((id, res)),
                    None => retry.push(id),
                }
            }

            if !ready.is_empty() {
                sched.schedule(Task::BatchSnapRes(ready)).unwrap();
            }
            if !retry.is_empty() {
                BATCH_REQUEST_TASKS
                    .with_label_values(&["retry"])
                    .observe(retry.len() as f64);
                sched.schedule(Task::RetryRequests(retry)).unwrap();
            }
        };

        BATCH_REQUEST_TASKS
            .with_label_values(&["all"])
            .observe(batch.len() as f64);

        if let Err(e) = self.engine.async_batch_snapshot(batch, on_finished) {
            for id in start_id..end_id + 1 {
                let err = e.maybe_clone().unwrap_or_else(|| {
                    error!("async snapshot batch failed error {:?}", e);
                    EngineError::Other(box_err!("{:?}", e))
                });
                self.notify_batch_failed(err, id);
            }
        }
    }
}

fn err_resp(e: Error) -> Response {
    let mut resp = Response::new();
    match e {
        Error::Region(e) => {
            let tag = storage::get_tag_from_header(&e);
            COPR_REQ_ERROR.with_label_values(&[tag]).inc();
            resp.set_region_error(e);
        }
        Error::Locked(info) => {
            resp.set_locked(info);
            COPR_REQ_ERROR.with_label_values(&["lock"]).inc();
        }
        Error::Outdated(deadline, now, scan_tag) => {
            let elapsed =
                now.duration_since(deadline) + Duration::from_secs(REQUEST_MAX_HANDLE_SECS);
            COPR_REQ_ERROR.with_label_values(&["outdated"]).inc();
            OUTDATED_REQ_WAIT_TIME
                .with_label_values(&[scan_tag])
                .observe(elapsed.as_secs() as f64);

            resp.set_other_error(OUTDATED_ERROR_MSG.to_owned());
        }
        Error::Full(allow) => {
            COPR_REQ_ERROR.with_label_values(&["full"]).inc();
            let mut errorpb = errorpb::Error::new();
            errorpb.set_message(format!("running batches reach limit {}", allow));
            let mut server_is_busy_err = ServerIsBusy::new();
            server_is_busy_err.set_reason(ENDPOINT_IS_BUSY.to_owned());
            errorpb.set_server_is_busy(server_is_busy_err);
            resp.set_region_error(errorpb);
        }
        Error::Other(_) => {
            resp.set_other_error(format!("{}", e));
            COPR_REQ_ERROR.with_label_values(&["other"]).inc();
        }
    }
    resp
}

pub fn to_pb_error(err: &Error) -> select::Error {
    let mut e = select::Error::new();
    e.set_code(DEFAULT_ERROR_CODE);
    e.set_msg(format!("{}", err));
    e
}

pub fn prefix_next(key: &[u8]) -> Vec<u8> {
    let mut nk = key.to_vec();
    if nk.is_empty() {
        nk.push(0);
        return nk;
    }
    let mut i = nk.len() - 1;
    loop {
        if nk[i] == 255 {
            nk[i] = 0;
        } else {
            nk[i] += 1;
            return nk;
        }
        if i == 0 {
            nk = key.to_vec();
            nk.push(0);
            return nk;
        }
        i -= 1;
    }
}

/// `is_point` checks if the key range represents a point.
pub fn is_point(range: &KeyRange) -> bool {
    range.get_end() == &*prefix_next(range.get_start())
}

#[inline]
pub fn get_pk(col: &ColumnInfo, h: i64) -> Datum {
    if mysql::has_unsigned_flag(col.get_flag() as u64) {
        // PK column is unsigned
        Datum::U64(h as u64)
    } else {
        Datum::I64(h)
    }
}

pub const STR_REQ_TYPE_SELECT: &'static str = "select";
pub const STR_REQ_TYPE_INDEX: &'static str = "index";
pub const STR_REQ_PRI_LOW: &'static str = "low";
pub const STR_REQ_PRI_NORMAL: &'static str = "normal";
pub const STR_REQ_PRI_HIGH: &'static str = "high";

#[inline]
pub fn get_req_pri_str(pri: CommandPri) -> &'static str {
    match pri {
        CommandPri::Low => STR_REQ_PRI_LOW,
        CommandPri::Normal => STR_REQ_PRI_NORMAL,
        CommandPri::High => STR_REQ_PRI_HIGH,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use storage::engine::{self, TEMP_DIR};
    use std::time::Duration;
    use std::sync::mpsc;

    use kvproto::coprocessor::Request;
    use tipb::select::DAGRequest;
    use tipb::expression::Expr;
    use tipb::executor::Executor;

    use util::worker::{Builder as WorkerBuilder, FutureWorker};
    use util::time::Instant;

    #[test]
    fn test_get_reg_scan_tag() {
        let mut ctx = ReqContext {
            deadline: Instant::now_coarse(),
            isolation_level: IsolationLevel::RC,
            fill_cache: true,
            table_scan: true,
        };
        assert_eq!(ctx.get_scan_tag(), STR_REQ_TYPE_SELECT);
        ctx.table_scan = false;
        assert_eq!(ctx.get_scan_tag(), STR_REQ_TYPE_INDEX);
    }

    #[test]
    fn test_req_outdated() {
        let mut worker = WorkerBuilder::new("test-endpoint").batch_size(30).create();
        let engine = engine::new_local_engine(TEMP_DIR, &[]).unwrap();
        let mut cfg = Config::default();
        cfg.end_point_concurrency = 1;
        let pd_worker = FutureWorker::new("test-pd-worker");
        let end_point = Host::new(engine, worker.scheduler(), &cfg, pd_worker.scheduler());
        worker.start(end_point).unwrap();

        let mut req = Request::new();
        req.set_tp(REQ_TYPE_DAG);
        let (tx, rx) = mpsc::channel();
        let on_resp = OnResponse::Unary(box move |msg| tx.send(msg).unwrap());
        let mut task = RequestTask::new(req, on_resp, 1000).unwrap();
        task.ctx.deadline -= Duration::from_secs(super::REQUEST_MAX_HANDLE_SECS);

        worker.schedule(Task::Request(task)).unwrap();
        let resp = rx.recv_timeout(Duration::from_secs(3)).unwrap();
        assert!(!resp.get_other_error().is_empty());
        assert_eq!(resp.get_other_error(), super::OUTDATED_ERROR_MSG);
    }

    #[test]
    fn test_too_many_reqs() {
        let mut worker = WorkerBuilder::new("test-endpoint").batch_size(30).create();
        let engine = engine::new_local_engine(TEMP_DIR, &[]).unwrap();
        let mut cfg = Config::default();
        cfg.end_point_concurrency = 1;
        let pd_worker = FutureWorker::new("test-pd-worker");
        let mut end_point = Host::new(engine, worker.scheduler(), &cfg, pd_worker.scheduler());
        end_point.max_running_task_count = 3;
        worker.start(end_point).unwrap();
        let (tx, rx) = mpsc::channel();
        for pos in 0..30 * 4 {
            let tx = tx.clone();
            let mut req = Request::new();
            req.set_tp(REQ_TYPE_DAG);
            if pos % 3 == 0 {
                req.mut_context().set_priority(CommandPri::Low);
            } else if pos % 3 == 1 {
                req.mut_context().set_priority(CommandPri::Normal);
            } else {
                req.mut_context().set_priority(CommandPri::High);
            }
            let on_resp = OnResponse::Unary(box move |msg| {
                thread::sleep(Duration::from_millis(100));
                let _ = tx.send(msg);
            });

            let task = RequestTask::new(req, on_resp, 1000).unwrap();
            worker.schedule(Task::Request(task)).unwrap();
        }
        for _ in 0..120 {
            let resp = rx.recv_timeout(Duration::from_secs(3)).unwrap();
            if !resp.has_region_error() {
                continue;
            }
            assert!(resp.get_region_error().has_server_is_busy());
            return;
        }
        panic!("suppose to get ServerIsBusy error.");
    }

    #[test]
    fn test_stack_guard() {
        let mut expr = Expr::new();
        for _ in 0..10 {
            let mut e = Expr::new();
            e.mut_children().push(expr);
            expr = e;
        }
        let mut e = Executor::new();
        e.mut_selection().mut_conditions().push(expr);
        let mut dag = DAGRequest::new();
        dag.mut_executors().push(e);
        let mut req = Request::new();
        req.set_tp(REQ_TYPE_DAG);
        req.set_data(dag.write_to_bytes().unwrap());

        let err = RequestTask::new(req, OnResponse::Unary(box |_| ()), 5).unwrap_err();
        let s = format!("{:?}", err);
        assert!(
            s.contains("Recursion"),
            "parse should fail due to recursion limit {}",
            s
        );
    }
}
