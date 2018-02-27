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

use std::usize;
use std::time::Duration;
use std::sync::Arc;
use std::fmt::{self, Debug, Display, Formatter};

use tipb::select::{self, DAGRequest};
use tipb::analyze::{AnalyzeReq, AnalyzeType};
use tipb::executor::ExecType;
use tipb::schema::ColumnInfo;
use protobuf::{CodedInputStream, Message as PbMsg};
use kvproto::coprocessor::{KeyRange, Request, Response};
use kvproto::errorpb::{self, ServerIsBusy};
use kvproto::kvrpcpb::{CommandPri, ExecDetails, HandleTime, IsolationLevel};

use util::time::{duration_to_sec, Instant};
use util::worker::{FutureScheduler, Runnable, Scheduler};
use util::collections::HashMap;
use util::threadpool::{Context, ContextFactory, ThreadPool, ThreadPoolBuilder};
use server::{Config, OnResponse};
use storage::{self, engine, Engine, Snapshot};
use storage::engine::Error as EngineError;
use pd::PdTask;

use super::codec::mysql;
use super::codec::datum::Datum;
use super::dag::DAGContext;
use super::statistics::analyze::AnalyzeContext;
use super::metrics::*;
use super::local_metrics::{BasicLocalMetrics, ExecLocalMetrics};
use super::dag::executor::ExecutorMetrics;
use super::{Error, Result};

pub const REQ_TYPE_DAG: i64 = 103;
pub const REQ_TYPE_ANALYZE: i64 = 104;

// If a request has been handled for more than 60 seconds, the client should
// be timeout already, so it can be safely aborted.
pub const DEFAULT_REQUEST_MAX_HANDLE_SECS: u64 = 60;
// If handle time is larger than the lower bound, the query is considered as slow query.
const SLOW_QUERY_LOWER_BOUND: f64 = 1.0; // 1 second.

const DEFAULT_ERROR_CODE: i32 = 1;

pub const SINGLE_GROUP: &[u8] = b"SingleGroup";

const OUTDATED_ERROR_MSG: &str = "request outdated.";

const ENDPOINT_IS_BUSY: &str = "endpoint is busy";

pub struct Host {
    engine: Box<Engine>,
    sched: Scheduler<Task>,
    reqs: HashMap<u64, Vec<RequestTask>>,
    last_req_id: u64,
    pool: ThreadPool<CopContext>,
    low_priority_pool: ThreadPool<CopContext>,
    high_priority_pool: ThreadPool<CopContext>,
    max_running_task_count: usize,
    batch_row_limit: usize,
    request_max_handle_duration: Duration,
}

struct CopContextFactory {
    sender: FutureScheduler<PdTask>,
}

impl ContextFactory<CopContext> for CopContextFactory {
    fn create(&self) -> CopContext {
        CopContext::new(self.sender.clone())
    }
}

struct CopContext {
    exec_local_metrics: ExecLocalMetrics,
    basic_local_metrics: BasicLocalMetrics,
}

impl CopContext {
    fn new(sender: FutureScheduler<PdTask>) -> CopContext {
        CopContext {
            exec_local_metrics: ExecLocalMetrics::new(sender),
            basic_local_metrics: Default::default(),
        }
    }
}

impl Context for CopContext {
    fn on_tick(&mut self) {
        self.exec_local_metrics.flush();
        self.basic_local_metrics.flush();
    }
}

impl Host {
    pub fn new(
        engine: Box<Engine>,
        scheduler: Scheduler<Task>,
        cfg: &Config,
        r: FutureScheduler<PdTask>,
    ) -> Host {
        Host {
            engine: engine,
            sched: scheduler,
            reqs: HashMap::default(),
            last_req_id: 0,
            max_running_task_count: cfg.end_point_max_tasks,
            batch_row_limit: cfg.end_point_batch_row_limit,
            pool: ThreadPoolBuilder::new(
                thd_name!("endpoint-normal-pool"),
                CopContextFactory { sender: r.clone() },
            ).thread_count(cfg.end_point_concurrency)
                .stack_size(cfg.end_point_stack_size.0 as usize)
                .build(),
            low_priority_pool: ThreadPoolBuilder::new(
                thd_name!("endpoint-low-pool"),
                CopContextFactory { sender: r.clone() },
            ).thread_count(cfg.end_point_concurrency)
                .stack_size(cfg.end_point_stack_size.0 as usize)
                .build(),
            high_priority_pool: ThreadPoolBuilder::new(
                thd_name!("endpoint-high-pool"),
                CopContextFactory { sender: r.clone() },
            ).thread_count(cfg.end_point_concurrency)
                .stack_size(cfg.end_point_stack_size.0 as usize)
                .build(),
            request_max_handle_duration: Duration::from_secs(
                cfg.end_point_request_max_handle_duration.as_secs(),
            ),
        }
    }

    fn running_task_count(&self) -> usize {
        self.pool.get_task_count() + self.low_priority_pool.get_task_count()
            + self.high_priority_pool.get_task_count()
    }

    fn handle_snapshot_result(&mut self, id: u64, snapshot: engine::Result<Box<Snapshot>>) {
        let reqs = self.reqs.remove(&id).unwrap();
        let mut local_metrics = BasicLocalMetrics::default();
        let request_max_handle_duration = self.request_max_handle_duration;
        let snap = match snapshot {
            Ok(s) => s,
            Err(e) => {
                notify_batch_failed(e, reqs, &mut local_metrics, request_max_handle_duration);
                return;
            }
        };

        if self.running_task_count() >= self.max_running_task_count {
            notify_batch_failed(
                Error::Full(self.max_running_task_count),
                reqs,
                &mut local_metrics,
                request_max_handle_duration,
            );
            return;
        }

        let batch_row_limit = self.batch_row_limit;
        for req in reqs {
            let pri = req.priority();
            let pri_str = get_req_pri_str(pri);
            let type_str = req.ctx.get_scan_tag();
            let end_point = TiDbEndPoint::new(snap.clone());

            let pool = match pri {
                CommandPri::Low => &mut self.low_priority_pool,
                CommandPri::High => &mut self.high_priority_pool,
                CommandPri::Normal => &mut self.pool,
            };
            COPR_PENDING_REQS
                .with_label_values(&[type_str, pri_str])
                .inc();
            pool.execute(move |ctx: &mut CopContext| {
                // decrease pending task
                COPR_PENDING_REQS
                    .with_label_values(&[type_str, pri_str])
                    .dec();
                let region_id = req.req.get_context().get_region_id();
                let stats = end_point.handle_request(
                    req,
                    batch_row_limit,
                    &mut ctx.basic_local_metrics,
                    request_max_handle_duration,
                );
                ctx.exec_local_metrics.collect(type_str, region_id, stats);
            });
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

enum CopRequest {
    DAG(DAGRequest),
    Analyze(AnalyzeReq),
}

pub struct ReqContext {
    pub isolation_level: IsolationLevel,
    pub fill_cache: bool,
    // whether is a table scan request.
    pub table_scan: bool,
}

impl ReqContext {
    #[inline]
    pub fn get_scan_tag(&self) -> &'static str {
        if self.table_scan {
            STR_REQ_TYPE_SELECT
        } else {
            STR_REQ_TYPE_INDEX
        }
    }
}

pub struct RequestTask {
    req: Request,
    start_ts: Option<u64>,
    wait_time: Option<f64>,
    timer: Instant,
    metrics: ExecutorMetrics,
    on_resp: OnResponse,
    cop_req: Option<Result<CopRequest>>,
    ctx: Arc<ReqContext>,
}

impl RequestTask {
    pub fn new(
        req: Request,
        on_resp: OnResponse,
        recursion_limit: u32,
    ) -> RequestTask {
        let timer = Instant::now_coarse();
        let mut start_ts = None;
        let tp = req.get_tp();
        let mut table_scan = false;
        let cop_req = match tp {
            REQ_TYPE_DAG => {
                let mut is = CodedInputStream::from_bytes(req.get_data());
                is.set_recursion_limit(recursion_limit);
                let mut dag = DAGRequest::new();
                if let Err(e) = dag.merge_from(&mut is) {
                    Err(box_err!(e))
                } else {
                    start_ts = Some(dag.get_start_ts());
                    if let Some(scan) = dag.get_executors().iter().next() {
                        if scan.get_tp() == ExecType::TypeTableScan {
                            table_scan = true;
                        }
                    }
                    Ok(CopRequest::DAG(dag))
                }
            }
            REQ_TYPE_ANALYZE => {
                let mut is = CodedInputStream::from_bytes(req.get_data());
                is.set_recursion_limit(recursion_limit);
                let mut analyze = AnalyzeReq::new();
                if let Err(e) = analyze.merge_from(&mut is) {
                    Err(box_err!(e))
                } else {
                    start_ts = Some(analyze.get_start_ts());
                    if analyze.get_tp() == AnalyzeType::TypeColumn {
                        table_scan = true;
                    }
                    Ok(CopRequest::Analyze(analyze))
                }
            }

            _ => Err(box_err!("unsupported tp {}", tp)),
        };
        let req_ctx = ReqContext {
            isolation_level: req.get_context().get_isolation_level(),
            fill_cache: !req.get_context().get_not_fill_cache(),
            table_scan: table_scan,
        };
        RequestTask {
            req: req,
            start_ts: start_ts,
            wait_time: None,
            timer: timer,
            metrics: Default::default(),
            on_resp: on_resp,
            cop_req: Some(cop_req),
            ctx: Arc::new(req_ctx),
        }
    }

    #[inline]
    fn check_outdated(&self, request_max_handle_duration: Duration) -> Result<()> {
        let now = Instant::now_coarse();
        let deadline = self.timer + request_max_handle_duration;
        if deadline <= now {
            return Err(Error::Outdated(deadline, now, self.ctx.get_scan_tag()));
        }
        Ok(())
    }

    fn stop_record_waiting(&mut self, metrics: &mut BasicLocalMetrics) {
        if self.wait_time.is_some() {
            return;
        }
        let wait_time = duration_to_sec(self.timer.elapsed());
        metrics
            .wait_time
            .with_label_values(&[self.ctx.get_scan_tag()])
            .observe(wait_time);
        self.wait_time = Some(wait_time);
    }

    fn stop_record_handling(&mut self, metrics: &mut BasicLocalMetrics) -> Option<ExecDetails> {
        self.stop_record_waiting(metrics);
        let query_time = duration_to_sec(self.timer.elapsed());
        let type_str = self.ctx.get_scan_tag();
        metrics
            .req_time
            .with_label_values(&[type_str])
            .observe(query_time);
        let wait_time = self.wait_time.unwrap();
        let handle_time = query_time - wait_time;
        metrics
            .handle_time
            .with_label_values(&[type_str])
            .observe(handle_time);

        metrics
            .scan_keys
            .with_label_values(&[type_str])
            .observe(self.metrics.cf_stats.total_op_count() as f64);

        let mut handle = HandleTime::new();
        handle.set_process_ms((handle_time * 1000.0) as i64);
        handle.set_wait_ms((wait_time * 1000.0) as i64);

        let mut exec_details = ExecDetails::new();
        if handle_time > SLOW_QUERY_LOWER_BOUND {
            info!(
                "[region {}] handle {:?} [{}] takes {:?} [keys: {}, hit: {}, \
                 ranges: {} ({:?})]",
                self.req.get_context().get_region_id(),
                self.start_ts,
                type_str,
                handle_time,
                self.metrics.cf_stats.total_op_count(),
                self.metrics.cf_stats.total_processed(),
                self.req.get_ranges().len(),
                self.req.get_ranges().get(0)
            );
            exec_details.set_scan_detail(self.metrics.cf_stats.scan_detail());
            exec_details.set_handle_time(handle);
            return Some(exec_details);
        }

        let ctx = self.req.get_context();
        if !ctx.get_handle_time() && !ctx.get_scan_detail() {
            return None;
        }

        if ctx.get_handle_time() {
            exec_details.set_handle_time(handle);
        }

        if ctx.get_scan_detail() {
            exec_details.set_scan_detail(self.metrics.cf_stats.scan_detail());
        }
        Some(exec_details)
    }

    pub fn priority(&self) -> CommandPri {
        self.req.get_context().get_priority()
    }

    pub fn start_time(&self) -> Instant {
        self.timer
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

impl Runnable<Task> for Host {
    // TODO: limit pending reqs
    fn run(&mut self, _: Task) {
        panic!("Shouldn't call Host::run directly");
    }

    #[allow(for_kv_map)]
    fn run_batch(&mut self, tasks: &mut Vec<Task>) {
        let mut grouped_reqs = map![];
        let mut local_metrics = BasicLocalMetrics::default();
        let request_max_handle_duration = self.request_max_handle_duration;
        for task in tasks.drain(..) {
            match task {
                Task::Request(req) => {
                    if let Err(e) = req.check_outdated(request_max_handle_duration) {
                        on_error(e, req, &mut local_metrics, request_max_handle_duration);
                        continue;
                    }
                    let key = {
                        let ctx = req.req.get_context();
                        (
                            ctx.get_region_id(),
                            ctx.get_region_epoch().get_version(),
                            ctx.get_peer().get_id(),
                        )
                    };
                    let group = grouped_reqs.entry(key).or_insert_with(Vec::new);
                    group.push(req);
                }
                Task::SnapRes(q_id, snap_res) => {
                    self.handle_snapshot_result(q_id, snap_res);
                }
                Task::BatchSnapRes(batch) => for (q_id, snap_res) in batch {
                    self.handle_snapshot_result(q_id, snap_res);
                },
                Task::RetryRequests(retry) => for id in retry {
                    let reqs = self.reqs.remove(&id).unwrap();
                    let sched = self.sched.clone();
                    if let Err(e) = self.engine
                        .async_snapshot(reqs[0].req.get_context(), box move |(_, res)| {
                            sched.schedule(Task::SnapRes(id, res)).unwrap()
                        }) {
                        notify_batch_failed(
                            e,
                            reqs,
                            &mut local_metrics,
                            request_max_handle_duration,
                        );
                    } else {
                        self.reqs.insert(id, reqs);
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
            self.last_req_id += 1;
            let id = self.last_req_id;
            let ctx = reqs[0].req.get_context().clone();
            batch.push(ctx);
            self.reqs.insert(id, reqs);
        }
        let end_id = self.last_req_id;

        let sched = self.sched.clone();
        let on_finished: engine::BatchCallback<Box<Snapshot>> = box move |results: Vec<_>| {
            let mut ready = Vec::with_capacity(results.len());
            let mut retry = Vec::new();
            for (id, res) in (start_id..end_id + 1).zip(results) {
                match res {
                    Some((_, res)) => {
                        ready.push((id, res));
                    }
                    None => {
                        retry.push(id);
                    }
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
                let reqs = self.reqs.remove(&id).unwrap();
                let err = e.maybe_clone().unwrap_or_else(|| {
                    error!("async snapshot batch failed error {:?}", e);
                    EngineError::Other(box_err!("{:?}", e))
                });
                notify_batch_failed(
                    err,
                    reqs,
                    &mut local_metrics,
                    self.request_max_handle_duration,
                );
            }
        }
    }

    fn shutdown(&mut self) {
        if let Err(e) = self.pool.stop() {
            warn!("Stop threadpool failed with {:?}", e);
        }
        if let Err(e) = self.low_priority_pool.stop() {
            warn!("Stop threadpool failed with {:?}", e);
        }
        if let Err(e) = self.high_priority_pool.stop() {
            warn!("Stop threadpool failed with {:?}", e);
        }
    }
}

fn err_resp(
    e: Error,
    metrics: &mut BasicLocalMetrics,
    request_max_handle_duration: Duration,
) -> Response {
    let mut resp = Response::new();
    let tag = match e {
        Error::Region(e) => {
            let tag = storage::get_tag_from_header(&e);
            resp.set_region_error(e);
            tag
        }
        Error::Locked(info) => {
            resp.set_locked(info);
            "lock"
        }
        Error::Outdated(deadline, now, scan_tag) => {
            let elapsed = now.duration_since(deadline) + request_max_handle_duration;
            metrics
                .outdate_time
                .with_label_values(&[scan_tag])
                .observe(elapsed.as_secs() as f64);
            resp.set_other_error(OUTDATED_ERROR_MSG.to_owned());
            "outdated"
        }
        Error::Full(allow) => {
            let mut errorpb = errorpb::Error::new();
            errorpb.set_message(format!("running batches reach limit {}", allow));
            let mut server_is_busy_err = ServerIsBusy::new();
            server_is_busy_err.set_reason(ENDPOINT_IS_BUSY.to_owned());
            errorpb.set_server_is_busy(server_is_busy_err);
            resp.set_region_error(errorpb);
            "full"
        }
        Error::Other(_) => {
            resp.set_other_error(format!("{}", e));
            "other"
        }
    };
    metrics
        .error_cnt
        .with_label_values(&[tag])
        .inc_by(1.0)
        .unwrap();
    resp
}

fn on_error(
    e: Error,
    req: RequestTask,
    metrics: &mut BasicLocalMetrics,
    request_max_handle_duration: Duration,
) -> ExecutorMetrics {
    let resp = err_resp(e, metrics, request_max_handle_duration);
    respond(resp, req, metrics)
}

fn notify_batch_failed<E: Into<Error> + Debug>(
    e: E,
    reqs: Vec<RequestTask>,
    metrics: &mut BasicLocalMetrics,
    request_max_handle_duration: Duration,
) {
    debug!("failed to handle batch request: {:?}", e);
    let resp = err_resp(e.into(), metrics, request_max_handle_duration);
    for t in reqs {
        respond(resp.clone(), t, metrics);
    }
}

fn respond(
    mut resp: Response,
    mut t: RequestTask,
    metrics: &mut BasicLocalMetrics,
) -> ExecutorMetrics {
    if let Some(exec_details) = t.stop_record_handling(metrics) {
        resp.set_exec_details(exec_details);
    }
    (t.on_resp)(resp);
    t.metrics
}

pub struct TiDbEndPoint {
    snap: Box<Snapshot>,
}

impl TiDbEndPoint {
    pub fn new(snap: Box<Snapshot>) -> TiDbEndPoint {
        TiDbEndPoint { snap: snap }
    }
}

impl TiDbEndPoint {
    fn handle_request(
        self,
        mut t: RequestTask,
        batch_row_limit: usize,
        metrics: &mut BasicLocalMetrics,
        request_max_handle_duration: Duration,
    ) -> ExecutorMetrics {
        t.stop_record_waiting(metrics);

        if let Err(e) = t.check_outdated(request_max_handle_duration) {
            return on_error(e, t, metrics, request_max_handle_duration);
        }

        let resp = match t.cop_req.take().unwrap() {
            Ok(CopRequest::DAG(dag)) => self.handle_dag(dag, &mut t, batch_row_limit, request_max_handle_duration),
            Ok(CopRequest::Analyze(analyze)) => self.handle_analyze(analyze, &mut t),
            Err(err) => Err(err),
        };
        match resp {
            Ok(r) => respond(r, t, metrics),
            Err(e) => on_error(e, t, metrics, request_max_handle_duration),
        }
    }

    pub fn handle_dag(
        self,
        dag: DAGRequest,
        t: &mut RequestTask,
        batch_row_limit: usize,
        request_max_handle_duration: Duration,
    ) -> Result<Response> {
        let ranges = t.req.take_ranges().into_vec();
        let deadline = t.start_time() + request_max_handle_duration;
        let mut ctx = DAGContext::new(dag, ranges, self.snap, Arc::clone(&t.ctx), batch_row_limit, deadline)?;
        let res = ctx.handle_request();
        ctx.collect_metrics_into(&mut t.metrics);
        res
    }

    pub fn handle_analyze(self, analyze: AnalyzeReq, t: &mut RequestTask) -> Result<Response> {
        let ranges = t.req.take_ranges().into_vec();
        let ctx = AnalyzeContext::new(analyze, ranges, self.snap, t.ctx.as_ref());
        ctx.handle_request(&mut t.metrics)
    }
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

pub const STR_REQ_TYPE_SELECT: &str = "select";
pub const STR_REQ_TYPE_INDEX: &str = "index";
pub const STR_REQ_PRI_LOW: &str = "low";
pub const STR_REQ_PRI_NORMAL: &str = "normal";
pub const STR_REQ_PRI_HIGH: &str = "high";

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
    use std::sync::*;
    use std::thread;
    use std::time::Duration;
    use std::ops::Sub;

    use kvproto::coprocessor::Request;
    use tipb::select::DAGRequest;
    use tipb::expression::Expr;
    use tipb::executor::Executor;

    use util::config::ReadableDuration;
    use util::worker::{Builder as WorkerBuilder, FutureWorker};

    #[test]
    fn test_get_reg_scan_tag() {
        let mut ctx = ReqContext {
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
        cfg.end_point_request_max_handle_duration = ReadableDuration::secs(0);
        cfg.end_point_concurrency = 1;
        let pd_worker = FutureWorker::new("test-pd-worker");
        let end_point = Host::new(engine, worker.scheduler(), &cfg, pd_worker.scheduler());
        worker.start(end_point).unwrap();
        let (tx, rx) = mpsc::channel();
        let mut task = RequestTask::new(
            Request::new(),
            box move |msg| {
                tx.send(msg).unwrap();
            },
            1000,
        );
        let ctx = ReqContext {
            isolation_level: task.ctx.isolation_level,
            fill_cache: task.ctx.fill_cache,
            table_scan: task.ctx.table_scan,
        };
        task.ctx = Arc::new(ctx);
        worker.schedule(Task::Request(task)).unwrap();
        let resp = rx.recv_timeout(Duration::from_secs(3)).unwrap();
        assert!(!resp.get_other_error().is_empty());
        assert_eq!(resp.get_other_error(), super::OUTDATED_ERROR_MSG);
        worker.stop();
    }

    #[test]
    fn test_exec_details_with_long_query() {
        let mut worker = WorkerBuilder::new("test-endpoint").batch_size(30).create();
        let engine = engine::new_local_engine(TEMP_DIR, &[]).unwrap();
        let mut cfg = Config::default();
        cfg.end_point_concurrency = 1;
        let pd_worker = FutureWorker::new("test-pd-worker");
        let end_point = Host::new(engine, worker.scheduler(), &cfg, pd_worker.scheduler());
        worker.start(end_point).unwrap();
        let (tx, rx) = mpsc::channel();
        let mut task = RequestTask::new(
            Request::new(),
            box move |msg| {
                tx.send(msg).unwrap();
            },
            1000,
        );
        let ctx = ReqContext {
            isolation_level: task.ctx.isolation_level,
            fill_cache: task.ctx.fill_cache,
            table_scan: task.ctx.table_scan,
        };
        task.ctx = Arc::new(ctx);
        let mut metrics = BasicLocalMetrics::default();
        task.stop_record_waiting(&mut metrics);
        task.timer = task.timer.sub(Duration::from_secs(
            (super::SLOW_QUERY_LOWER_BOUND * 2.0) as u64,
        ));
        worker.schedule(Task::Request(task)).unwrap();
        let resp = rx.recv_timeout(Duration::from_secs(3)).unwrap();

        // check exec details
        let exec_details = resp.get_exec_details();
        assert!(exec_details.has_handle_time());
        assert!(exec_details.has_scan_detail());
        worker.stop();
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
            if pos % 3 == 0 {
                req.mut_context().set_priority(CommandPri::Low);
            } else if pos % 3 == 1 {
                req.mut_context().set_priority(CommandPri::Normal);
            } else {
                req.mut_context().set_priority(CommandPri::High);
            }
            let task = RequestTask::new(
                req,
                box move |msg| {
                    thread::sleep(Duration::from_millis(100));
                    let _ = tx.send(msg);
                },
                1000,
            );
            worker.schedule(Task::Request(task)).unwrap();
        }
        for _ in 0..120 {
            let resp = rx.recv_timeout(Duration::from_secs(3)).unwrap();
            if !resp.has_region_error() {
                continue;
            }
            assert!(resp.get_region_error().has_server_is_busy());
            worker.stop();
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
        RequestTask::new(req.clone(), box move |_| unreachable!(), 100);
        RequestTask::new(
            req,
            box move |res| {
                let s = format!("{:?}", res);
                assert!(
                    s.contains("Recursion"),
                    "parse should fail due to recursion limit {}",
                    s
                );
            },
            5,
        );
    }
}
