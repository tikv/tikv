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
use futures::Future;

use tipb::select::{self, DAGRequest};
use tipb::analyze::{AnalyzeReq, AnalyzeType};
use tipb::executor::ExecType;
use tipb::schema::ColumnInfo;
use protobuf::{CodedInputStream, Message as PbMsg};
use kvproto::coprocessor::{self as copproto, KeyRange, Request, Response};
use kvproto::errorpb::{self, ServerIsBusy};
use kvproto::kvrpcpb::{Context, ExecDetails, HandleTime, IsolationLevel};

use util::time::Instant;
use util::readpool::{self, ReadPool};
use server::Config;
use storage::{self, Engine, Snapshot, Statistics};
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
    read_pool: ReadPool,
    batch_row_limit: usize,
    recursion_limit: u32,
}

impl Clone for Host {
    fn clone(&self) -> Self {
        Host {
            engine: self.engine.clone(),
            read_pool: self.read_pool.clone(),
            ..*self
        }
    }
}

impl Host {
    pub fn new(engine: Box<Engine>, cfg: &Config, read_pool: readpool::ReadPool) -> Host {
        Host {
            engine,
            read_pool,
            batch_row_limit: cfg.end_point_batch_row_limit,
            recursion_limit: cfg.end_point_recursion_limit,
        }
    }

    /// Handle a grpc request according to `Request`.
    pub fn handle_request(
        &self,
        request: copproto::Request,
    ) -> impl Future<Item = copproto::Response, Error = ()> {
        let request_task = RequestTask::from_request(request, self.recursion_limit);
        self.handle_request_task(request_task)
    }

    /// Handle a grpc request according to `RequestTask`. Used in tests.
    fn handle_request_task(
        &self,
        task: RequestTask,
    ) -> impl Future<Item = copproto::Response, Error = ()> {
        static CMD: &'static str = "coprocessor";
        static CMD_TYPE: &'static str = "cop";

        let batch_row_limit = self.batch_row_limit;

        let engine = self.engine.clone();
        let priority = readpool::Priority::from(task.req.get_context().get_priority());

        let begin_time = Instant::now_coarse();

        self.read_pool
            .future_execute(priority, move |ctxd| {
                engine.future_snapshot(task.req.get_context())
                .then(move |r| {
                    let elapsed = begin_time.elapsed_secs();
                    r.map(|r| (r, elapsed))
                })
                // map engine::Error -> coprocessor::Error
                .map_err(Error::from)
                .and_then(move |(snapshot, wait_elapsed): (Box<Snapshot>, f64)| {
                    task.ctx.check_if_outdated()?;

                    let should_respond_handle_time = task.req.get_context().get_handle_time();
                    let should_respond_scan_detail = task.req.get_context().get_scan_detail();

                    // Process request.
                    let begin_time = Instant::now_coarse();
                    let mut cop_stats = CopStats::default();

                    if task.cop_req.is_err() {
                        match task.cop_req {
                            Err(e) => return Err(e),
                            _ => unreachable!(),
                        }
                    }

                    let result = task.cop_req.unwrap().handle(
                        snapshot,
                        task.req,
                        task.ctx,
                        batch_row_limit,
                        &mut cop_stats
                    );

                    // Attach execution details if requested.
                    let mut exec_details = ExecDetails::new();
                    let process_elapsed = begin_time.elapsed_secs();
                    if process_elapsed > SLOW_QUERY_LOWER_BOUND || should_respond_handle_time {
                        let mut handle_time = HandleTime::new();
                        handle_time.set_process_ms((process_elapsed * 1000.0) as i64);
                        handle_time.set_wait_ms((wait_elapsed * 1000.0) as i64);
                        exec_details.set_handle_time(handle_time);
                    }
                    if process_elapsed > SLOW_QUERY_LOWER_BOUND || should_respond_scan_detail {
                        exec_details.set_scan_detail(cop_stats.stats.scan_detail());
                    }

                    // TODO: report statistics
                    result.map(move |mut resp| {
                        resp.set_exec_details(exec_details);
                        resp
                    })
                })
            })
            // map readpool::Full -> coprocessor::Error
            .map_err(|_| Error::Full)
            .flatten()
            .or_else(move |err| {
                // let ctx = ctxd.clone();
                let mut resp = Response::new();
                match err {
                    Error::Region(e) => {
                        let tag = storage::get_tag_from_header(&e);
                        COPR_REQ_ERROR.with_label_values(&[tag]).inc();
                        resp.set_region_error(e);
                    }
                    Error::Locked(info) => {
                        resp.set_locked(info);
                        COPR_REQ_ERROR.with_label_values(&["lock"]).inc();
                    }
                    Error::Outdated(deadline, now) => {
                        COPR_REQ_ERROR.with_label_values(&["outdated"]).inc();
                        resp.set_other_error(OUTDATED_ERROR_MSG.to_owned());
                    }
                    Error::Full => {
                        COPR_REQ_ERROR.with_label_values(&["full"]).inc();
                        let mut errorpb = errorpb::Error::new();
                        errorpb.set_message("running batches reach limit".to_string());
                        let mut server_is_busy_err = ServerIsBusy::new();
                        server_is_busy_err.set_reason(ENDPOINT_IS_BUSY.to_owned());
                        errorpb.set_server_is_busy(server_is_busy_err);
                        resp.set_region_error(errorpb);
                    }
                    Error::Other(_) => {
                        resp.set_other_error(format!("{}", err));
                        COPR_REQ_ERROR.with_label_values(&["other"]).inc();
                    }
                }
                Ok::<Response, ()>(resp)
            })
    }
}

trait CopRequest: Send {
    fn handle(
        &mut self,
        snap: Box<Snapshot>,
        req: Request,
        ctx: Arc<ReqContext>,
        batch_row_limit: usize,
        stats: &mut CopStats,
    ) -> Result<Response>;
}

struct DAGCopRequest {
    req: Option<DAGRequest>,
}

impl DAGCopRequest {
    fn new(req: DAGRequest) -> DAGCopRequest {
        DAGCopRequest { req: Some(req) }
    }
}

impl CopRequest for DAGCopRequest {
    fn handle(
        &mut self,
        snap: Box<Snapshot>,
        mut req: Request,
        ctx: Arc<ReqContext>,
        batch_row_limit: usize,
        stats: &mut CopStats,
    ) -> Result<Response> {
        let ranges = req.take_ranges().into_vec();
        let mut dag_ctx = DAGContext::new(
            self.req.take().unwrap(),
            ranges,
            snap,
            Arc::clone(&ctx),
            batch_row_limit,
        )?;
        let res = dag_ctx.handle_request();
        dag_ctx.collect_statistics_into(&mut stats.stats);
        dag_ctx.collect_metrics_into(&mut stats.scan_counter);
        res
    }
}

struct AnalyzeCopRequest {
    req: Option<AnalyzeReq>,
}

impl AnalyzeCopRequest {
    fn new(req: AnalyzeReq) -> AnalyzeCopRequest {
        AnalyzeCopRequest { req: Some(req) }
    }
}

impl CopRequest for AnalyzeCopRequest {
    fn handle(
        &mut self,
        snap: Box<Snapshot>,
        mut req: Request,
        ctx: Arc<ReqContext>,
        _batch_row_limit: usize,
        stats: &mut CopStats,
    ) -> Result<Response> {
        let ranges = req.take_ranges().into_vec();
        let analyze_ctx = AnalyzeContext::new(self.req.take().unwrap(), ranges, snap, ctx.as_ref());
        analyze_ctx.handle_request(&mut stats.stats)
    }
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
    fn new(ctx: &Context, table_scan: bool) -> ReqContext {
        let deadline = Instant::now_coarse() + Duration::from_secs(REQUEST_MAX_HANDLE_SECS);
        ReqContext {
            deadline,
            isolation_level: ctx.get_isolation_level(),
            fill_cache: !ctx.get_not_fill_cache(),
            table_scan,
        }
    }

    pub fn check_if_outdated(&self) -> Result<()> {
        let now = Instant::now_coarse();
        if self.deadline <= now {
            return Err(Error::Outdated(self.deadline, now));
        }
        Ok(())
    }
}

struct RequestTask {
    req: Request,
    cop_req: Result<Box<CopRequest>>,
    ctx: Arc<ReqContext>,
}

impl RequestTask {
    fn from_request(req: Request, recursion_limit: u32) -> RequestTask {
        let tp = req.get_tp();
        let mut table_scan = false;
        let cop_req: Result<Box<CopRequest>> = match tp {
            REQ_TYPE_DAG => {
                let mut is = CodedInputStream::from_bytes(req.get_data());
                is.set_recursion_limit(recursion_limit);
                let mut dag = DAGRequest::new();
                if let Err(e) = dag.merge_from(&mut is) {
                    Err(box_err!(e))
                } else {
                    if let Some(scan) = dag.get_executors().iter().next() {
                        if scan.get_tp() == ExecType::TypeTableScan {
                            table_scan = true;
                        }
                    }
                    Ok(box DAGCopRequest::new(dag))
                }
            }
            REQ_TYPE_ANALYZE => {
                let mut is = CodedInputStream::from_bytes(req.get_data());
                is.set_recursion_limit(recursion_limit);
                let mut analyze = AnalyzeReq::new();
                if let Err(e) = analyze.merge_from(&mut is) {
                    Err(box_err!(e))
                } else {
                    if analyze.get_tp() == AnalyzeType::TypeColumn {
                        table_scan = true;
                    }
                    Ok(box AnalyzeCopRequest::new(analyze))
                }
            }
            _ => Err(box_err!("unsupported tp {}", tp)),
        };
        let req_ctx = ReqContext::new(req.get_context(), table_scan);
        RequestTask {
            req,
            cop_req,
            ctx: Arc::new(req_ctx),
        }
    }
}

#[derive(Default)]
struct CopStats {
    stats: Statistics,
    scan_counter: ScanCounter,
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

#[cfg(test)]
mod tests {
    use super::*;
    use storage::engine::{self, TEMP_DIR};
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;
    use std::result;
    use std::sync::mpsc::{channel, Sender};

    use kvproto::coprocessor::Request;
    use kvproto::kvrpcpb::CommandPri;

    use util::time::Instant;

    /// Tests whether an outdated process results in outdated error.
    #[test]
    fn test_req_outdated() {
        struct MyCopRequest {}
        impl CopRequest for MyCopRequest {
            fn handle(
                &mut self,
                _snap: Box<Snapshot>,
                _req: Request,
                _ctx: Arc<ReqContext>,
                _batch_row_limit: usize,
                _stats: &mut CopStats,
            ) -> Result<Response> {
                let t = Instant::now_coarse();
                Err(Error::Outdated(t, t))
            }
        }

        let engine = engine::new_local_engine(TEMP_DIR, &[]).unwrap();
        let read_pool = readpool::ReadPool::new(&readpool::Config::default_for_test(), None);
        let end_point = Host::new(engine, &Config::default(), read_pool);

        let req = Request::new();
        let ctx = req.get_context().clone();

        let task = RequestTask {
            req,
            cop_req: Ok(box MyCopRequest {}),
            ctx: Arc::new(ReqContext::new(&ctx, false)),
        };

        let resp: Response = end_point.handle_request_task(task).wait().unwrap();
        assert_eq!(resp.get_other_error(), super::OUTDATED_ERROR_MSG);
    }

    #[test]
    fn test_exec_details_with_long_query() {
        struct MyCopRequest {}
        impl CopRequest for MyCopRequest {
            fn handle(
                &mut self,
                _snap: Box<Snapshot>,
                _req: Request,
                _ctx: Arc<ReqContext>,
                _batch_row_limit: usize,
                _stats: &mut CopStats,
            ) -> Result<Response> {
                thread::sleep(Duration::from_secs((SLOW_QUERY_LOWER_BOUND * 2.0) as u64));
                Ok(Response::new())
            }
        }

        let engine = engine::new_local_engine(TEMP_DIR, &[]).unwrap();
        let read_pool = readpool::ReadPool::new(&readpool::Config::default_for_test(), None);
        let end_point = Host::new(engine, &Config::default(), read_pool);

        let req = Request::new();
        let ctx = req.get_context().clone();

        let task = RequestTask {
            req,
            cop_req: Ok(box MyCopRequest {}),
            ctx: Arc::new(ReqContext::new(&ctx, false)),
        };

        let resp: Response = end_point.handle_request_task(task).wait().unwrap();
        let exec_details = resp.get_exec_details();
        assert!(exec_details.has_handle_time());
        assert!(exec_details.has_scan_detail());

        let expect_process_ms = (SLOW_QUERY_LOWER_BOUND * 2.0) as i64 * 1000;
        let time_delta: i64 = exec_details.get_handle_time().get_process_ms() - expect_process_ms;
        assert!(time_delta.abs() < 100);
    }

    fn wait_on_new_thread<F>(sender: Sender<result::Result<F::Item, F::Error>>, future: F)
    where
        F: Future + Send + 'static,
        F::Item: Send + 'static,
        F::Error: Send + 'static,
    {
        thread::spawn(move || {
            let r = future.wait();
            sender.send(r).unwrap();
        });
    }

    #[test]
    fn test_too_many_reqs() {
        struct MyCopRequest {}
        impl CopRequest for MyCopRequest {
            fn handle(
                &mut self,
                _snap: Box<Snapshot>,
                _req: Request,
                _ctx: Arc<ReqContext>,
                _batch_row_limit: usize,
                _stats: &mut CopStats,
            ) -> Result<Response> {
                thread::sleep(Duration::from_millis(100));
                Ok(Response::new())
            }
        }

        let engine = engine::new_local_engine(TEMP_DIR, &[]).unwrap();
        let read_pool = readpool::ReadPool::new(
            &readpool::Config {
                normal_concurrency: 2,
                max_tasks_normal: 4,
                ..readpool::Config::default_for_test()
            },
            None,
        );
        let end_point = Host::new(engine, &Config::default(), read_pool);

        let make_task = || {
            let mut req = Request::new();
            req.mut_context().set_priority(CommandPri::Normal);

            let ctx = req.get_context().clone();

            RequestTask {
                req,
                cop_req: Ok(box MyCopRequest {}),
                ctx: Arc::new(ReqContext::new(&ctx, false)),
            }
        };

        let (tx, rx) = channel();

        for _ in 0..10 {
            let task = make_task();
            let resp_future = end_point.handle_request_task(task);
            wait_on_new_thread(tx.clone(), resp_future);
        }

        // Since each valid request needs 100ms to process, while server-busy requests are
        // returned immediately, we got server-busy requests first.
        for _ in 0..6 {
            let resp: Response = rx.recv().unwrap().unwrap();
            assert!(resp.has_region_error());
            assert!(resp.get_region_error().has_server_is_busy());
        }
        for _ in 0..4 {
            let resp: Response = rx.recv().unwrap().unwrap();
            assert!(!resp.has_region_error());
        }
        // Wait some time to check that there is no more response remaining in the queue
        assert!(rx.recv_timeout(Duration::from_secs(1)).is_err());
    }
}
