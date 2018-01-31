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
use kvproto::kvrpcpb::{ExecDetails, HandleTime, IsolationLevel};

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

    pub fn handle_request(
        &self,
        request: copproto::Request,
    ) -> impl Future<Item = copproto::Response, Error = ()> {
        static CMD: &'static str = "coprocessor";
        static CMD_TYPE: &'static str = "cop";

        let batch_row_limit = self.batch_row_limit;
        let recursion_limit = self.recursion_limit;

        let engine = self.engine.clone();
        let priority = readpool::Priority::from(request.get_context().get_priority());

        let begin_time = Instant::now_coarse();

        self.read_pool.future_execute(priority, move |ctxd| {
            engine.future_snapshot(request.get_context())
                .then(move |r| {
                    let elapsed = begin_time.elapsed_secs();
                    r.map(|r| (r, elapsed))
                })
                // engine::Error -> coprocessor::Error
                .map_err(Error::from)
                .and_then(move |(snapshot, wait_elapsed): (Box<Snapshot>, f64)| {
                    let should_respond_handle_time = request.get_context().get_handle_time();
                    let should_respond_scan_detail = request.get_context().get_scan_detail();

                    // Process request.
                    let begin_time = Instant::now_coarse();
                    let mut cop_stats = CopStats::default();
                    let result = RequestTask::new(request, recursion_limit)
                        .process(snapshot, batch_row_limit, &mut cop_stats);

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
                .or_else(move |err| {
                    // let ctx = ctxd.clone();
                    let mut resp = Response::new();
                    match err {
                        Error::Region(e) => {
                            let tag = storage::get_tag_from_header(&e);
                            // COPR_REQ_ERROR.with_label_values(&[tag]).inc();
                            resp.set_region_error(e);
                        }
                        Error::Locked(info) => {
                            resp.set_locked(info);
                            // COPR_REQ_ERROR.with_label_values(&["lock"]).inc();
                        }
                        Error::Outdated(deadline, now) => {
                            //let elapsed =
                            //    now.duration_since(deadline) +
                            // Duration::from_secs(REQUEST_MAX_HANDLE_SECS);
                            // COPR_REQ_ERROR.with_label_values(&["outdated"]).inc();
                            // OUTDATED_REQ_WAIT_TIME
                            //    .with_label_values(&[scan_tag])
                            //    .observe(elapsed.as_secs() as f64);

                            resp.set_other_error(OUTDATED_ERROR_MSG.to_owned());
                        }
                        Error::Full(allow) => {
                            // COPR_REQ_ERROR.with_label_values(&["full"]).inc();
                            let mut errorpb = errorpb::Error::new();
                            errorpb.set_message(format!("running batches reach limit {}", allow));
                            // let mut server_is_busy_err = ServerIsBusy::new();
                            // server_is_busy_err.set_reason(ENDPOINT_IS_BUSY.to_owned());
                            // errorpb.set_server_is_busy(server_is_busy_err);
                            resp.set_region_error(errorpb);
                        }
                        Error::Other(_) => {
                            resp.set_other_error(format!("{}", err));
                            // COPR_REQ_ERROR.with_label_values(&["other"]).inc();
                        }
                    }
                    Ok::<Response, ()>(resp)
                })
        })
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
    cop_req: Option<Result<CopRequest>>,
    ctx: Arc<ReqContext>,
}

impl RequestTask {
    fn new(req: Request, recursion_limit: u32) -> RequestTask {
        let deadline = Instant::now_coarse() + Duration::from_secs(REQUEST_MAX_HANDLE_SECS);
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
            cop_req: Some(cop_req),
            ctx: Arc::new(req_ctx),
        }
    }

    #[inline]
    fn check_outdated(&self) -> Result<()> {
        self.ctx.check_if_outdated()
    }

    fn process(
        &mut self,
        snap: Box<Snapshot>,
        batch_row_limit: usize,
        cop_stats: &mut CopStats,
    ) -> Result<Response> {
        self.check_outdated()?;
        match self.cop_req.take().unwrap() {
            Ok(CopRequest::DAG(dag)) => handle_dag(snap, dag, self, batch_row_limit, cop_stats),
            Ok(CopRequest::Analyze(analyze)) => handle_analyze(snap, analyze, self, cop_stats),
            Err(err) => Err(err),
        }
    }
}

#[derive(Default)]
struct CopStats {
    stats: Statistics,
    scan_counter: ScanCounter,
}

fn handle_dag(
    snap: Box<Snapshot>,
    dag: DAGRequest,
    t: &mut RequestTask,
    batch_row_limit: usize,
    stats: &mut CopStats,
) -> Result<Response> {
    let ranges = t.req.take_ranges().into_vec();
    let mut ctx = DAGContext::new(dag, ranges, snap, Arc::clone(&t.ctx), batch_row_limit)?;
    let res = ctx.handle_request();
    ctx.collect_statistics_into(&mut stats.stats);
    ctx.collect_metrics_into(&mut stats.scan_counter);
    res
}

fn handle_analyze(
    snap: Box<Snapshot>,
    analyze: AnalyzeReq,
    t: &mut RequestTask,
    stats: &mut CopStats,
) -> Result<Response> {
    let ranges = t.req.take_ranges().into_vec();
    let ctx = AnalyzeContext::new(analyze, ranges, snap, t.ctx.as_ref());
    ctx.handle_request(&mut stats.stats)
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
