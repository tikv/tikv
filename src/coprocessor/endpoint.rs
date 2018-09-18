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

use std::fmt::{self, Debug, Display, Formatter};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use std::usize;

use futures::sync::mpsc as futures_mpsc;
use futures::{future, stream};
use protobuf::{CodedInputStream, Message as PbMsg};

use kvproto::coprocessor::{KeyRange, Request, Response};
use kvproto::errorpb::{self, ServerIsBusy};
use kvproto::kvrpcpb::CommandPri;
use tipb::analyze::{AnalyzeReq, AnalyzeType};
use tipb::checksum::{ChecksumRequest, ChecksumScanOn};
use tipb::executor::ExecType;
use tipb::select::DAGRequest;

use server::readpool::{self, ReadPool};
use server::{Config, OnResponse};
use storage::{self, engine, Engine};
use util::collections::HashMap;
use util::worker::{Runnable, Scheduler};

use super::checksum::ChecksumContext;
use super::dag::executor::ExecutorMetrics;
use super::dag::DAGContext;
use super::local_metrics::BasicLocalMetrics;
use super::metrics::*;
use super::statistics::analyze::AnalyzeContext;
use super::tracker::Tracker;
use super::*;

// If a request has been handled for more than 60 seconds, the client should
// be timeout already, so it can be safely aborted.
pub const DEFAULT_REQUEST_MAX_HANDLE_SECS: u64 = 60;

const OUTDATED_ERROR_MSG: &str = "request outdated.";

const ENDPOINT_IS_BUSY: &str = "endpoint is busy";

pub struct Host<E: Engine> {
    engine: E,
    sched: Scheduler<Task<E>>,
    reqs: HashMap<u64, Vec<RequestTask>>,
    last_req_id: u64,
    pool: ReadPool<ReadPoolContext>,
    basic_local_metrics: BasicLocalMetrics,
    // TODO: Deprecate after totally switching to read pool
    max_running_task_count: usize,
    // TODO: Deprecate after totally switching to read pool
    running_task_count: Arc<AtomicUsize>,
    batch_row_limit: usize,
    stream_batch_row_limit: usize,
    request_max_handle_duration: Duration,
}

impl<E: Engine> Host<E> {
    pub fn new(
        engine: E,
        sched: Scheduler<Task<E>>,
        cfg: &Config,
        pool: ReadPool<ReadPoolContext>,
    ) -> Self {
        // Use read pool's max task config
        let max_running_task_count = pool.get_max_tasks();
        Host {
            engine,
            sched,
            reqs: HashMap::default(),
            last_req_id: 0,
            pool,
            basic_local_metrics: BasicLocalMetrics::default(),
            // TODO: Deprecate after totally switching to read pool
            max_running_task_count,
            batch_row_limit: cfg.end_point_batch_row_limit,
            stream_batch_row_limit: cfg.end_point_stream_batch_row_limit,
            request_max_handle_duration: cfg.end_point_request_max_handle_duration.0,
            // TODO: Deprecate after totally switching to read pool
            running_task_count: Arc::new(AtomicUsize::new(0)),
        }
    }

    #[inline]
    // TODO: Deprecate after totally switching to read pool
    fn running_task_count(&self) -> usize {
        self.running_task_count.load(Ordering::Acquire)
    }

    fn notify_failed<Err: Into<Error> + Debug>(&mut self, e: Err, reqs: Vec<RequestTask>) {
        debug!("failed to handle batch request: {:?}", e);
        let resp = err_multi_resp(e.into(), reqs.len(), &mut self.basic_local_metrics);
        for t in reqs {
            t.on_resp.respond(resp.clone());
        }
    }

    #[inline]
    fn notify_batch_failed<Err: Into<Error> + Debug>(&mut self, e: Err, batch_id: u64) {
        let reqs = self.reqs.remove(&batch_id).unwrap();
        self.notify_failed(e, reqs);
    }

    fn handle_request(&mut self, snap: E::Snap, t: RequestTask) {
        // Collect metrics into it before requests enter into execute pool.
        // Otherwize collect into thread local contexts.
        let metrics = &mut self.basic_local_metrics;

        if let Err(e) = t.check_outdated() {
            let resp = err_resp(e, metrics);
            t.on_resp.respond(resp);
            return;
        }

        let (ranges, cop_req, on_resp) = (t.ranges, t.cop_req, t.on_resp);
        let mut tracker = t.tracker;

        let priority = readpool::Priority::from(tracker.req_ctx.context.get_priority());
        let pool = self.pool.get_pool_by_priority(priority);
        let ctxd = pool.get_context_delegators();
        tracker.ctx_pool(ctxd);

        let batch_row_limit = {
            if !on_resp.is_streaming() {
                self.batch_row_limit
            } else {
                self.stream_batch_row_limit
            }
        };

        match cop_req {
            CopRequest::DAG(dag) => {
                let mut ctx =
                    match DAGContext::new(dag, ranges, snap, &tracker.req_ctx, batch_row_limit) {
                        Ok(ctx) => ctx,
                        Err(e) => {
                            on_resp.respond(err_resp(e, metrics));
                            return;
                        }
                    };
                if !on_resp.is_streaming() {
                    let do_request = move |_| {
                        tracker.on_handle_start();
                        let mut resp = ctx.handle_request().unwrap_or_else(|e| {
                            let mut metrics = tracker.get_basic_metrics();
                            err_resp(e, &mut metrics)
                        });
                        let mut exec_metrics = ExecutorMetrics::default();
                        ctx.collect_metrics_into(&mut exec_metrics);
                        tracker.on_handle_finish(Some(&mut resp), exec_metrics);
                        on_resp.respond(resp);
                        future::ok::<_, ()>(())
                    };
                    pool.spawn(do_request).forget();
                    return;
                }
                // For streaming.
                let s = stream::unfold((ctx, false), move |(mut ctx, finished)| {
                    if finished {
                        return None;
                    }
                    tracker.on_handle_start();
                    let (mut item, finished) = ctx.handle_streaming_request().unwrap_or_else(|e| {
                        let mut metrics = tracker.get_basic_metrics();
                        (Some(err_resp(e, &mut metrics)), true)
                    });
                    let mut exec_metrics = ExecutorMetrics::default();
                    ctx.collect_metrics_into(&mut exec_metrics);
                    tracker.on_handle_finish(item.as_mut(), exec_metrics);
                    item.map(|resp| {
                        future::ok::<_, futures_mpsc::SendError<_>>((resp, (ctx, finished)))
                    })
                });
                pool.spawn(move |_| on_resp.respond_stream(s)).forget();
            }
            CopRequest::Analyze(analyze) => {
                let mut ctx = AnalyzeContext::new(analyze, ranges, snap, &tracker.req_ctx).unwrap();
                let do_request = move |_| {
                    tracker.on_handle_start();
                    let mut resp = ctx.handle_request().unwrap_or_else(|e| {
                        let mut metrics = tracker.get_basic_metrics();
                        err_resp(e, &mut metrics)
                    });
                    let mut exec_metrics = ExecutorMetrics::default();
                    ctx.collect_metrics_into(&mut exec_metrics);
                    tracker.on_handle_finish(Some(&mut resp), exec_metrics);
                    on_resp.respond(resp);
                    future::ok::<_, ()>(())
                };
                pool.spawn(do_request).forget();
            }
            CopRequest::Checksum(checksum) => {
                let mut ctx =
                    ChecksumContext::new(checksum, ranges, snap, &tracker.req_ctx).unwrap();
                let do_request = move |_| {
                    tracker.on_handle_start();
                    let mut resp = ctx.handle_request().unwrap_or_else(|e| {
                        let mut metrics = tracker.get_basic_metrics();
                        err_resp(e, &mut metrics)
                    });
                    let mut exec_metrics = ExecutorMetrics::default();
                    ctx.collect_metrics_into(&mut exec_metrics);
                    tracker.on_handle_finish(Some(&mut resp), exec_metrics);
                    on_resp.respond(resp);
                    future::ok::<_, ()>(())
                };
                pool.spawn(do_request).forget();
            }
        }
    }

    fn handle_snapshot_result(&mut self, id: u64, snapshot: engine::Result<E::Snap>) {
        let snap = match snapshot {
            Ok(s) => s,
            Err(e) => return self.notify_batch_failed(e, id),
        };
        for req in self.reqs.remove(&id).unwrap() {
            self.handle_request(snap.clone(), req);
        }
    }
}

pub enum Task<E: Engine> {
    Request(RequestTask),
    SnapRes(u64, engine::Result<E::Snap>),
}

impl<E: Engine> Display for Task<E> {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match *self {
            Task::Request(ref req) => write!(f, "{}", req),
            Task::SnapRes(req_id, _) => write!(f, "snapres [{}]", req_id),
        }
    }
}

#[derive(Debug)]
enum CopRequest {
    DAG(DAGRequest),
    Analyze(AnalyzeReq),
    Checksum(ChecksumRequest),
}

#[derive(Debug)]
pub struct RequestTask {
    cop_req: CopRequest,
    ranges: Vec<KeyRange>,
    on_resp: OnResponse<Response>,
    tracker: Tracker,
}

impl RequestTask {
    pub fn new(
        peer: String,
        mut req: Request,
        on_resp: OnResponse<Response>,
        recursion_limit: u32,
    ) -> Result<RequestTask> {
        let mut table_scan = false;
        let mut is_desc_scan: Option<bool> = None; // only used in slow query logs
        let (start_ts, cop_req) = match req.get_tp() {
            REQ_TYPE_DAG => {
                let mut is = CodedInputStream::from_bytes(req.get_data());
                is.set_recursion_limit(recursion_limit);
                let mut dag = DAGRequest::new();
                box_try!(dag.merge_from(&mut is));
                if let Some(scan) = dag.get_executors().iter().next() {
                    // the first executor must be table scan or index scan.
                    table_scan = scan.get_tp() == ExecType::TypeTableScan;
                    if table_scan {
                        is_desc_scan = Some(scan.get_tbl_scan().get_desc());
                    } else {
                        is_desc_scan = Some(scan.get_idx_scan().get_desc());
                    }
                }
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
            REQ_TYPE_CHECKSUM => {
                let mut is = CodedInputStream::from_bytes(req.get_data());
                is.set_recursion_limit(recursion_limit);
                let mut checksum = ChecksumRequest::new();
                box_try!(checksum.merge_from(&mut is));
                table_scan = checksum.get_scan_on() == ChecksumScanOn::Table;
                (checksum.get_start_ts(), CopRequest::Checksum(checksum))
            }
            tp => return Err(box_err!("unsupported tp {}", tp)),
        };

        let req_ctx = ReqContext::new(
            make_tag(table_scan),
            req.take_context(),
            req.get_ranges(),
            Some(peer),
            is_desc_scan,
            Some(start_ts),
        );

        let request_tracker = Tracker::new(req_ctx);

        COPR_PENDING_REQS
            .with_label_values(&[request_tracker.req_ctx.tag, request_tracker.pri_str])
            .inc();

        Ok(RequestTask {
            cop_req,
            ranges: req.take_ranges().into_vec(),
            on_resp,
            tracker: request_tracker,
        })
    }

    #[inline]
    fn check_outdated(&self) -> Result<()> {
        self.tracker.req_ctx.deadline.check_if_exceeded()
    }

    pub fn priority(&self) -> CommandPri {
        self.tracker.req_ctx.context.get_priority()
    }

    pub fn set_max_handle_duration(&mut self, request_max_handle_duration: Duration) {
        self.tracker
            .req_ctx
            .set_max_handle_duration(request_max_handle_duration);
    }

    fn get_request_key(&self) -> (u64, u64, u64) {
        let ctx = &self.tracker.req_ctx.context;
        let region_id = ctx.get_region_id();
        let version = ctx.get_region_epoch().get_version();
        let peer_id = ctx.get_peer().get_id();
        (region_id, version, peer_id)
    }
}

impl Display for RequestTask {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        f.debug_struct("RequestTask")
            .field("req_ctx", &self.tracker.req_ctx)
            .finish()
    }
}

impl<E: Engine> Runnable<Task<E>> for Host<E> {
    // TODO: limit pending reqs
    fn run(&mut self, _: Task<E>) {
        panic!("Shouldn't call Host::run directly");
    }

    #[cfg_attr(feature = "cargo-clippy", allow(for_kv_map))]
    fn run_batch(&mut self, tasks: &mut Vec<Task<E>>) {
        let mut grouped_reqs = map![];
        for task in tasks.drain(..) {
            match task {
                Task::Request(mut req) => {
                    req.set_max_handle_duration(self.request_max_handle_duration);
                    if let Err(e) = req.check_outdated() {
                        let resp = err_resp(e, &mut self.basic_local_metrics);
                        req.on_resp.respond(resp);
                        continue;
                    }
                    let key = req.get_request_key();
                    grouped_reqs.entry(key).or_insert_with(Vec::new).push(req);
                }
                Task::SnapRes(q_id, snap_res) => {
                    self.handle_snapshot_result(q_id, snap_res);
                }
            }
        }

        for (_, mut reqs) in grouped_reqs {
            let max_running_task_count = self.max_running_task_count;
            if self.running_task_count() >= max_running_task_count {
                self.notify_failed(Error::Full(max_running_task_count), reqs);
                continue;
            }
            for req in &mut reqs {
                let task_count = Arc::clone(&self.running_task_count);
                req.tracker.task_count(task_count);
            }

            self.last_req_id += 1;
            let id = self.last_req_id;
            let sched = self.sched.clone();
            if let Err(e) = self
                .engine
                .async_snapshot(&reqs[0].tracker.req_ctx.context, box move |(_, res)| {
                    sched.schedule(Task::SnapRes(id, res)).unwrap()
                }) {
                self.notify_failed(e, reqs);
                continue;
            }
            self.reqs.insert(id, reqs);
        }

        self.basic_local_metrics.flush();
    }
}

fn make_tag(is_table_scan: bool) -> &'static str {
    if is_table_scan {
        "select"
    } else {
        "index"
    }
}

fn err_multi_resp(e: Error, count: usize, metrics: &mut BasicLocalMetrics) -> Response {
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
        Error::Outdated(elapsed, scan_tag) => {
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
        Error::Other(_) | Error::Eval(_) => {
            resp.set_other_error(format!("{}", e));
            "other"
        }
    };
    metrics
        .error_cnt
        .with_label_values(&[tag])
        .inc_by(count as i64);
    resp
}

pub fn err_resp(e: Error, metrics: &mut BasicLocalMetrics) -> Response {
    err_multi_resp(e, 1, metrics)
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::sync::oneshot;
    use futures::Future;
    use storage::engine::{self, TEMP_DIR};

    use kvproto::coprocessor::Request;
    use tipb::executor::Executor;
    use tipb::expression::Expr;
    use tipb::select::DAGRequest;

    use util::config::ReadableDuration;
    use util::worker::{Builder as WorkerBuilder, FutureWorker};

    #[test]
    fn test_req_outdated() {
        let mut worker = WorkerBuilder::new("test-endpoint").batch_size(30).create();
        let engine = engine::new_local_engine(TEMP_DIR, &[]).unwrap();
        let mut cfg = Config::default();
        cfg.end_point_request_max_handle_duration = ReadableDuration::secs(0);
        let pd_worker = FutureWorker::new("test-pd-worker");
        let read_pool = ReadPool::new(
            "readpool",
            &readpool::Config::default_with_concurrency(1),
            || || ReadPoolContext::new(pd_worker.scheduler()),
        );
        let end_point = Host::new(engine, worker.scheduler(), &cfg, read_pool);
        worker.start(end_point).unwrap();

        let peer = String::from("127.0.0.1");

        let mut req = Request::new();
        req.set_tp(REQ_TYPE_DAG);
        let (tx, rx) = oneshot::channel();
        let on_resp = OnResponse::Unary(tx);
        let task = RequestTask::new(peer, req, on_resp, 1000).unwrap();

        worker.schedule(Task::Request(task)).unwrap();
        let resp = rx.wait().unwrap();
        assert!(!resp.get_other_error().is_empty());
        assert_eq!(resp.get_other_error(), super::OUTDATED_ERROR_MSG);
        worker.stop();
    }

    #[test]
    fn test_too_many_reqs() {
        let mut worker = WorkerBuilder::new("test-endpoint").batch_size(5).create();
        let engine = engine::new_local_engine(TEMP_DIR, &[]).unwrap();
        let cfg = Config::default();
        let pd_worker = FutureWorker::new("test-pd-worker");
        let read_pool = ReadPool::new(
            "readpool",
            &readpool::Config::default_with_concurrency(1),
            || || ReadPoolContext::new(pd_worker.scheduler()),
        );
        let mut end_point = Host::new(engine, worker.scheduler(), &cfg, read_pool);
        end_point.max_running_task_count = 1;
        worker.start(end_point).unwrap();
        let result_futures: Vec<_> = (0..30 * 4)
            .map(|pos| {
                let (tx, rx) = oneshot::channel();
                let mut req = Request::new();
                req.set_tp(REQ_TYPE_DAG);
                if pos % 3 == 0 {
                    req.mut_context().set_priority(CommandPri::Low);
                } else if pos % 3 == 1 {
                    req.mut_context().set_priority(CommandPri::Normal);
                } else {
                    req.mut_context().set_priority(CommandPri::High);
                }
                let on_resp = OnResponse::Unary(tx);
                let task = RequestTask::new(String::from("127.0.0.1"), req, on_resp, 1000).unwrap();
                worker.schedule(Task::Request(task)).unwrap();
                rx
            })
            .collect();
        let results = future::join_all(result_futures).wait().unwrap();
        assert_eq!(results.len(), 30 * 4);
        for resp in results {
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

        let (tx, _rx) = oneshot::channel();
        let err =
            RequestTask::new(String::from("127.0.0.1"), req, OnResponse::Unary(tx), 5).unwrap_err();
        let s = format!("{:?}", err);
        assert!(
            s.contains("Recursion"),
            "parse should fail due to recursion limit {}",
            s
        );
    }

    #[test]
    fn test_deconstruct_request_tracker() {
        let mut worker = WorkerBuilder::new("test-endpoint").batch_size(1).create();
        let engine = engine::new_local_engine(TEMP_DIR, &[]).unwrap();
        let cfg = Config::default();
        let pd_worker = FutureWorker::new("test-pd-worker");
        let read_pool = ReadPool::new(
            "readpool",
            &readpool::Config::default_with_concurrency(1),
            || || ReadPoolContext::new(pd_worker.scheduler()),
        );
        let mut end_point = Host::new(engine, worker.scheduler(), &cfg, read_pool);
        end_point.max_running_task_count = 1;
        worker.start(end_point).unwrap();

        let mut req = Request::new();
        req.set_tp(REQ_TYPE_DAG);

        let (tx, rx) = oneshot::channel();
        let task =
            RequestTask::new(String::from("127.0.0.1"), req, OnResponse::Unary(tx), 1000).unwrap();
        worker.schedule(Task::Request(task)).unwrap();

        let resp = rx.wait().unwrap();
        assert!(format!("{:?}", resp.get_other_error()).contains("has no executor"));
        worker.stop();
    }
}
