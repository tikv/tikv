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
use storage::{self, engine, Engine, FlowStatistics, Snapshot, Statistics, StatisticsSummary};
use storage::engine::Error as EngineError;
use pd::PdTask;

use super::codec::mysql;
use super::codec::datum::Datum;
use super::dag::DAGContext;
use super::statistics::analyze::AnalyzeContext;
use super::metrics::*;
use super::local_metrics::*;
use super::{Error, Result};

pub const REQ_TYPE_DAG: i64 = 103;
pub const REQ_TYPE_ANALYZE: i64 = 104;

// If a request has been handled for more than 60 seconds, the client should
// be timeout already, so it can be safely aborted.
const REQUEST_MAX_HANDLE_SECS: u64 = 60;
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
}

pub type CopRequestStatistics = HashMap<u64, FlowStatistics>;

pub trait CopSender: Send + Clone {
    fn send(&self, CopRequestStatistics) -> Result<()>;
}

struct CopContextFactory {
    sender: FutureScheduler<PdTask>,
}

impl ContextFactory<CopContext> for CopContextFactory {
    fn create(&self) -> CopContext {
        CopContext {
            sender: self.sender.clone(),
            select_stats: Default::default(),
            index_stats: Default::default(),
            request_stats: HashMap::default(),
            scan_counter: ScanCounter::default(),
        }
    }
}

struct CopContext {
    select_stats: StatisticsSummary,
    index_stats: StatisticsSummary,
    request_stats: CopRequestStatistics,
    sender: FutureScheduler<PdTask>,
    scan_counter: ScanCounter,
}

impl CopContext {
    fn add_statistics(&mut self, type_str: &str, stats: &Statistics) {
        self.get_statistics(type_str).add_statistics(stats);
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

    fn add_statistics_by_region(&mut self, region_id: u64, stats: &Statistics) {
        let flow_stats = self.request_stats
            .entry(region_id)
            .or_insert_with(FlowStatistics::default);
        flow_stats.add(&stats.write.flow_stats);
        flow_stats.add(&stats.data.flow_stats);
    }

    fn add_scan_count(&mut self, scan_counter: &mut ScanCounter) {
        self.scan_counter.merge(scan_counter);
    }

    fn flush_scan_count(&mut self) {
        self.scan_counter.flush();
    }
}

impl Context for CopContext {
    fn on_tick(&mut self) {
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
            *this_statistics = Default::default();
        }
        if !self.request_stats.is_empty() {
            if let Err(e) = self.sender.schedule(PdTask::ReadStats {
                read_stats: self.request_stats.clone(),
            }) {
                error!("send coprocessor statistics: {:?}", e);
            };
            self.request_stats.clear();
        }
        self.flush_scan_count();
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
        }
    }

    fn running_task_count(&self) -> usize {
        self.pool.get_task_count() + self.low_priority_pool.get_task_count()
            + self.high_priority_pool.get_task_count()
    }

    fn handle_snapshot_result(&mut self, id: u64, snapshot: engine::Result<Box<Snapshot>>) {
        let reqs = self.reqs.remove(&id).unwrap();
        let snap = match snapshot {
            Ok(s) => s,
            Err(e) => {
                notify_batch_failed(e, reqs);
                return;
            }
        };

        if self.running_task_count() >= self.max_running_task_count {
            notify_batch_failed(Error::Full(self.max_running_task_count), reqs);
            return;
        }

        let batch_row_limit = self.batch_row_limit;
        for req in reqs {
            let pri = req.priority();
            let pri_str = get_req_pri_str(pri);
            let type_str = req.ctx.get_scan_tag();
            COPR_PENDING_REQS
                .with_label_values(&[type_str, pri_str])
                .add(1.0);
            let end_point = TiDbEndPoint::new(snap.clone());

            let pool = match pri {
                CommandPri::Low => &mut self.low_priority_pool,
                CommandPri::High => &mut self.high_priority_pool,
                CommandPri::Normal => &mut self.pool,
            };
            pool.execute(move |ctx: &mut CopContext| {
                let region_id = req.req.get_context().get_region_id();
                let CopStats {
                    stats,
                    mut scan_counter,
                } = end_point.handle_request(req, batch_row_limit);
                ctx.add_statistics(type_str, &stats);
                ctx.add_statistics_by_region(region_id, &stats);
                ctx.add_scan_count(&mut scan_counter);
                COPR_PENDING_REQS
                    .with_label_values(&[type_str, pri_str])
                    .dec();
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

pub struct RequestTask {
    req: Request,
    start_ts: Option<u64>,
    wait_time: Option<f64>,
    timer: Instant,
    statistics: Statistics,
    scan_counter: ScanCounter,
    on_resp: OnResponse,
    cop_req: Option<Result<CopRequest>>,
    ctx: Arc<ReqContext>,
}

impl RequestTask {
    pub fn new(req: Request, on_resp: OnResponse, recursion_limit: u32) -> RequestTask {
        let timer = Instant::now_coarse();
        let deadline = timer + Duration::from_secs(REQUEST_MAX_HANDLE_SECS);
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
            deadline: deadline,
            isolation_level: req.get_context().get_isolation_level(),
            fill_cache: !req.get_context().get_not_fill_cache(),
            table_scan: table_scan,
        };
        RequestTask {
            req: req,
            start_ts: start_ts,
            wait_time: None,
            timer: timer,
            statistics: Default::default(),
            scan_counter: ScanCounter::default(),
            on_resp: on_resp,
            cop_req: Some(cop_req),
            ctx: Arc::new(req_ctx),
        }
    }

    #[inline]
    fn check_outdated(&self) -> Result<()> {
        self.ctx.check_if_outdated()
    }

    fn stop_record_waiting(&mut self) {
        if self.wait_time.is_some() {
            return;
        }
        let wait_time = duration_to_sec(self.timer.elapsed());
        COPR_REQ_WAIT_TIME
            .with_label_values(&[self.ctx.get_scan_tag()])
            .observe(wait_time);
        self.wait_time = Some(wait_time);
    }

    fn stop_record_handling(&mut self) -> Option<ExecDetails> {
        self.stop_record_waiting();

        let query_time = duration_to_sec(self.timer.elapsed());
        let type_str = self.ctx.get_scan_tag();
        COPR_REQ_HISTOGRAM_VEC
            .with_label_values(&[type_str])
            .observe(query_time);
        let wait_time = self.wait_time.unwrap();
        let handle_time = query_time - wait_time;
        COPR_REQ_HANDLE_TIME
            .with_label_values(&[type_str])
            .observe(handle_time);
        COPR_SCAN_KEYS
            .with_label_values(&[type_str])
            .observe(self.statistics.total_op_count() as f64);

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
                self.statistics.total_op_count(),
                self.statistics.total_processed(),
                self.req.get_ranges().len(),
                self.req.get_ranges().get(0)
            );
            exec_details.set_scan_detail(self.statistics.scan_detail());
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
            exec_details.set_scan_detail(self.statistics.scan_detail());
        }
        Some(exec_details)
    }

    pub fn priority(&self) -> CommandPri {
        self.req.get_context().get_priority()
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
        for task in tasks.drain(..) {
            match task {
                Task::Request(req) => {
                    if let Err(e) = req.check_outdated() {
                        on_error(e, req);
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
                        notify_batch_failed(e, reqs);
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
                notify_batch_failed(err, reqs);
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

struct CopStats {
    stats: Statistics,
    scan_counter: ScanCounter,
}

fn on_error(e: Error, req: RequestTask) -> CopStats {
    let resp = err_resp(e);
    respond(resp, req)
}

fn notify_batch_failed<E: Into<Error> + Debug>(e: E, reqs: Vec<RequestTask>) {
    debug!("failed to handle batch request: {:?}", e);
    let resp = err_resp(e.into());
    for t in reqs {
        respond(resp.clone(), t);
    }
}

fn respond(mut resp: Response, mut t: RequestTask) -> CopStats {
    if let Some(exec_details) = t.stop_record_handling() {
        resp.set_exec_details(exec_details);
    }

    (t.on_resp)(resp);
    CopStats {
        stats: t.statistics,
        scan_counter: t.scan_counter,
    }
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
    fn handle_request(self, mut t: RequestTask, batch_row_limit: usize) -> CopStats {
        t.stop_record_waiting();

        if let Err(e) = t.check_outdated() {
            return on_error(e, t);
        }

        let resp = match t.cop_req.take().unwrap() {
            Ok(CopRequest::DAG(dag)) => self.handle_dag(dag, &mut t, batch_row_limit),
            Ok(CopRequest::Analyze(analyze)) => self.handle_analyze(analyze, &mut t),
            Err(err) => Err(err),
        };
        match resp {
            Ok(r) => respond(r, t),
            Err(e) => on_error(e, t),
        }
    }

    pub fn handle_dag(
        self,
        dag: DAGRequest,
        t: &mut RequestTask,
        batch_row_limit: usize,
    ) -> Result<Response> {
        let ranges = t.req.take_ranges().into_vec();
        let mut ctx = DAGContext::new(dag, ranges, self.snap, Arc::clone(&t.ctx), batch_row_limit)?;
        let res = ctx.handle_request();
        ctx.collect_statistics_into(&mut t.statistics);
        ctx.collect_metrics_into(&mut t.scan_counter);
        res
    }

    pub fn handle_analyze(self, analyze: AnalyzeReq, t: &mut RequestTask) -> Result<Response> {
        let ranges = t.req.take_ranges().into_vec();
        let ctx = AnalyzeContext::new(analyze, ranges, self.snap, t.ctx.as_ref());
        ctx.handle_request(&mut t.statistics)
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
        let (tx, rx) = mpsc::channel();
        let mut task = RequestTask::new(
            Request::new(),
            box move |msg| {
                tx.send(msg).unwrap();
            },
            1000,
        );
        let ctx = ReqContext {
            deadline: task.ctx.deadline - Duration::from_secs(super::REQUEST_MAX_HANDLE_SECS),
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
            deadline: task.ctx.deadline - Duration::from_secs(super::REQUEST_MAX_HANDLE_SECS),
            isolation_level: task.ctx.isolation_level,
            fill_cache: task.ctx.fill_cache,
            table_scan: task.ctx.table_scan,
        };
        task.ctx = Arc::new(ctx);
        task.stop_record_waiting();
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
