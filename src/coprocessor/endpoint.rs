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
use std::time::{Duration, Instant};
use std::rc::Rc;
use std::fmt::{self, Debug, Display, Formatter};
use tipb::select::{self, Chunk, DAGRequest, SelectRequest};
use tipb::schema::ColumnInfo;
use protobuf::Message as PbMsg;
use kvproto::coprocessor::{KeyRange, Request, Response};
use kvproto::errorpb::{self, ServerIsBusy};
use kvproto::kvrpcpb::CommandPri;

use util::time::duration_to_sec;
use util::worker::{BatchRunnable, Scheduler};
use util::collections::HashMap;
use util::threadpool::{FifoQueue, ThreadPool};
use server::OnResponse;
use storage::{self, engine, Engine, Snapshot, SnapshotStore, Statistics};

use super::codec::mysql;
use super::codec::datum::Datum;
use super::select::select::SelectContext;
use super::select::xeval::EvalContext;
use super::dag::DAGContext;
use super::metrics::*;
use super::{Error, Result};

pub const REQ_TYPE_SELECT: i64 = 101;
pub const REQ_TYPE_INDEX: i64 = 102;
pub const REQ_TYPE_DAG: i64 = 103;
pub const BATCH_ROW_COUNT: usize = 64;

// If a request has been handled for more than 60 seconds, the client should
// be timeout already, so it can be safely aborted.
const REQUEST_MAX_HANDLE_SECS: u64 = 60;
// Assume a request can be finished in 0.1ms, a request at position x will wait about
// 0.0001 * x secs to be actual started. Hence the queue should have at most
// REQUEST_MAX_HANDLE_SECS / 0.0001 request.
const DEFAULT_MAX_RUNNING_TASK_COUNT: usize = REQUEST_MAX_HANDLE_SECS as usize * 10_000;
// If handle time is larger than the lower bound, the query is considered as slow query.
const SLOW_QUERY_LOWER_BOUND: f64 = 1.0; // 1 second.

const DEFAULT_ERROR_CODE: i32 = 1;

pub const SINGLE_GROUP: &'static [u8] = b"SingleGroup";

const OUTDATED_ERROR_MSG: &'static str = "request outdated.";

const ENDPOINT_IS_BUSY: &'static str = "endpoint is busy";

pub struct Host {
    engine: Box<Engine>,
    sched: Scheduler<Task>,
    reqs: HashMap<u64, Vec<RequestTask>>,
    last_req_id: u64,
    pool: ThreadPool<FifoQueue<u64>, u64>,
    low_priority_pool: ThreadPool<FifoQueue<u64>, u64>,
    high_priority_pool: ThreadPool<FifoQueue<u64>, u64>,
    max_running_task_count: usize,
}

impl Host {
    pub fn new(engine: Box<Engine>, scheduler: Scheduler<Task>, concurrency: usize) -> Host {
        Host {
            engine: engine,
            sched: scheduler,
            reqs: HashMap::default(),
            last_req_id: 0,
            max_running_task_count: DEFAULT_MAX_RUNNING_TASK_COUNT,
            pool: ThreadPool::new(
                thd_name!("endpoint-normal-pool"),
                concurrency,
                FifoQueue::new(),
            ),
            low_priority_pool: ThreadPool::new(
                thd_name!("endpoint-low-pool"),
                concurrency,
                FifoQueue::new(),
            ),
            high_priority_pool: ThreadPool::new(
                thd_name!("endpoint-high-pool"),
                concurrency,
                FifoQueue::new(),
            ),
        }
    }

    fn running_task_count(&self) -> usize {
        self.pool.get_task_count() + self.low_priority_pool.get_task_count() +
            self.high_priority_pool.get_task_count()
    }
}

pub enum Task {
    Request(RequestTask),
    SnapRes(u64, engine::Result<Box<Snapshot>>),
}

impl Display for Task {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match *self {
            Task::Request(ref req) => write!(f, "{}", req),
            Task::SnapRes(req_id, _) => write!(f, "snapres [{}]", req_id),
        }
    }
}

enum CopRequest {
    Select(SelectRequest),
    DAG(DAGRequest),
}

pub struct RequestTask {
    req: Request,
    start_ts: Option<u64>,
    wait_time: Option<f64>,
    timer: Instant,
    // The deadline before which the task should be responded.
    deadline: Instant,
    statistics: Statistics,
    on_resp: OnResponse,
    cop_req: Option<Result<CopRequest>>,
}

impl RequestTask {
    pub fn new(req: Request, on_resp: OnResponse) -> RequestTask {
        let timer = Instant::now();
        let deadline = timer + Duration::from_secs(REQUEST_MAX_HANDLE_SECS);
        let mut start_ts = None;
        let tp = req.get_tp();
        let cop_req = match tp {
            REQ_TYPE_SELECT | REQ_TYPE_INDEX => {
                let mut sel = SelectRequest::new();
                if let Err(e) = sel.merge_from_bytes(req.get_data()) {
                    Err(box_err!(e))
                } else {
                    start_ts = Some(sel.get_start_ts());
                    Ok(CopRequest::Select(sel))
                }
            }
            REQ_TYPE_DAG => {
                let mut dag = DAGRequest::new();
                if let Err(e) = dag.merge_from_bytes(req.get_data()) {
                    Err(box_err!(e))
                } else {
                    start_ts = Some(dag.get_start_ts());
                    Ok(CopRequest::DAG(dag))
                }
            }
            _ => Err(box_err!("unsupported tp {}", tp)),
        };
        RequestTask {
            req: req,
            start_ts: start_ts,
            wait_time: None,
            timer: timer,
            deadline: deadline,
            statistics: Default::default(),
            on_resp: on_resp,
            cop_req: Some(cop_req),
        }
    }

    #[inline]
    fn check_outdated(&self) -> Result<()> {
        check_if_outdated(self.deadline, self.req.get_tp())
    }

    fn stop_record_waiting(&mut self) {
        if self.wait_time.is_some() {
            return;
        }
        let wait_time = duration_to_sec(self.timer.elapsed());
        COPR_REQ_WAIT_TIME
            .with_label_values(&[get_req_type_str(self.req.get_tp())])
            .observe(wait_time);
        self.wait_time = Some(wait_time);
    }

    fn stop_record_handling(&mut self) {
        self.stop_record_waiting();

        let handle_time = duration_to_sec(self.timer.elapsed());
        let type_str = get_req_type_str(self.req.get_tp());
        COPR_REQ_HISTOGRAM_VEC
            .with_label_values(&[type_str])
            .observe(handle_time);
        let wait_time = self.wait_time.unwrap();
        COPR_REQ_HANDLE_TIME
            .with_label_values(&[type_str])
            .observe(handle_time - wait_time);


        COPR_SCAN_KEYS
            .with_label_values(&[type_str])
            .observe(self.statistics.total_op_count() as f64);

        // for (cf, details) in self.statistics.details() {
        //     for (tag, count) in details {
        //         COPR_SCAN_DETAILS.with_label_values(&[type_str, cf, tag])
        //             .observe(count as f64);
        //     }
        // }

        if handle_time > SLOW_QUERY_LOWER_BOUND {
            info!(
                "[region {}] handle {:?} [{}] takes {:?} [waiting: {:?}, keys: {}, hit: {}, \
                 ranges: {} ({:?})]",
                self.req.get_context().get_region_id(),
                self.start_ts,
                type_str,
                handle_time,
                wait_time,
                self.statistics.total_op_count(),
                self.statistics.total_processed(),
                self.req.get_ranges().len(),
                self.req.get_ranges().get(0)
            );
        }
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

impl BatchRunnable<Task> for Host {
    // TODO: limit pending reqs
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
                    let mut group = grouped_reqs.entry(key).or_insert_with(Vec::new);
                    group.push(req);
                }
                Task::SnapRes(q_id, snap_res) => {
                    let reqs = self.reqs.remove(&q_id).unwrap();
                    let snap = match snap_res {
                        Ok(s) => s,
                        Err(e) => {
                            notify_batch_failed(e, reqs);
                            continue;
                        }
                    };

                    if self.running_task_count() >= self.max_running_task_count {
                        notify_batch_failed(Error::Full(self.max_running_task_count), reqs);
                        continue;
                    }

                    for req in reqs {
                        let pri = req.priority();
                        let pri_str = get_req_pri_str(pri);
                        let type_str = get_req_type_str(req.req.get_tp());
                        COPR_PENDING_REQS
                            .with_label_values(&[type_str, pri_str])
                            .add(1.0);
                        let end_point = TiDbEndPoint::new(snap.clone());
                        let txn_id = req.start_ts.unwrap_or_default();

                        if pri == CommandPri::Low {
                            self.low_priority_pool.execute(txn_id, move || {
                                end_point.handle_request(req);
                                COPR_PENDING_REQS
                                    .with_label_values(&[type_str, pri_str])
                                    .dec();
                            });
                        } else if pri == CommandPri::High {
                            self.high_priority_pool.execute(txn_id, move || {
                                end_point.handle_request(req);
                                COPR_PENDING_REQS
                                    .with_label_values(&[type_str, pri_str])
                                    .dec();
                            });
                        } else {
                            self.pool.execute(txn_id, move || {
                                end_point.handle_request(req);
                                COPR_PENDING_REQS
                                    .with_label_values(&[type_str, pri_str])
                                    .dec();
                            });
                        }
                    }
                }
            }
        }
        for (_, reqs) in grouped_reqs {
            self.last_req_id += 1;
            let id = self.last_req_id;
            let sched = self.sched.clone();
            if let Err(e) = self.engine.async_snapshot(
                reqs[0].req.get_context(),
                box move |(_, res)| sched.schedule(Task::SnapRes(id, res)).unwrap(),
            ) {
                notify_batch_failed(e, reqs);
                continue;
            }
            self.reqs.insert(id, reqs);
        }
    }

    fn shutdown(&mut self) {
        if let Err(e) = self.pool.stop() {
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
        Error::Outdated(deadline, now, tp) => {
            let t = get_req_type_str(tp);
            let elapsed =
                now.duration_since(deadline) + Duration::from_secs(REQUEST_MAX_HANDLE_SECS);
            COPR_REQ_ERROR.with_label_values(&["outdated"]).inc();
            OUTDATED_REQ_WAIT_TIME
                .with_label_values(&[t])
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

fn on_error(e: Error, req: RequestTask) {
    let resp = err_resp(e);
    respond(resp, req)
}

fn notify_batch_failed<E: Into<Error> + Debug>(e: E, reqs: Vec<RequestTask>) {
    debug!("failed to handle batch request: {:?}", e);
    let resp = err_resp(e.into());
    for t in reqs {
        respond(resp.clone(), t)
    }
}

pub fn check_if_outdated(deadline: Instant, tp: i64) -> Result<()> {
    let now = Instant::now();
    if deadline <= now {
        return Err(Error::Outdated(deadline, now, tp));
    }
    Ok(())
}

fn respond(resp: Response, mut t: RequestTask) {
    t.stop_record_handling();
    (t.on_resp)(resp)
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
    fn handle_request(&self, mut t: RequestTask) {
        t.stop_record_waiting();
        if let Err(e) = t.check_outdated() {
            on_error(e, t);
            return;
        }
        let resp = match t.cop_req.take().unwrap() {
            Ok(CopRequest::Select(sel)) => self.handle_select(sel, &mut t),
            Ok(CopRequest::DAG(dag)) => self.handle_dag(dag, &mut t),
            Err(err) => Err(err),
        };
        match resp {
            Ok(r) => respond(r, t),
            Err(e) => on_error(e, t),
        }
    }

    fn handle_select(&self, sel: SelectRequest, t: &mut RequestTask) -> Result<Response> {
        let snap = SnapshotStore::new(
            self.snap.as_ref(),
            sel.get_start_ts(),
            t.req.get_context().get_isolation_level(),
        );
        let ctx = try!(SelectContext::new(sel, snap, t.deadline, &mut t.statistics));
        let range = t.req.get_ranges().to_vec();
        debug!("scanning range: {:?}", range);
        ctx.handle_request(t.req.get_tp(), range)
    }

    pub fn handle_dag(&self, dag: DAGRequest, t: &mut RequestTask) -> Result<Response> {
        let ranges = t.req.get_ranges().to_vec();
        let eval_ctx = Rc::new(box_try!(EvalContext::new(
            dag.get_time_zone_offset(),
            dag.get_flags()
        )));
        let ctx = DAGContext::new(
            dag,
            t.deadline,
            ranges,
            self.snap.as_ref(),
            eval_ctx.clone(),
            t.req.get_context().get_isolation_level(),
        );
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

#[inline]
pub fn get_chunk(chunks: &mut Vec<Chunk>) -> &mut Chunk {
    if chunks
        .last()
        .map_or(true, |chunk| chunk.get_rows_meta().len() >= BATCH_ROW_COUNT)
    {
        let chunk = Chunk::new();
        chunks.push(chunk);
    }
    chunks.last_mut().unwrap()
}

pub const STR_REQ_TYPE_SELECT: &'static str = "select";
pub const STR_REQ_TYPE_INDEX: &'static str = "index";
pub const STR_REQ_TYPE_DAG: &'static str = "dag";
pub const STR_REQ_TYPE_UNKNOWN: &'static str = "unknown";

#[inline]
pub fn get_req_type_str(tp: i64) -> &'static str {
    match tp {
        REQ_TYPE_SELECT => STR_REQ_TYPE_SELECT,
        REQ_TYPE_INDEX => STR_REQ_TYPE_INDEX,
        REQ_TYPE_DAG => STR_REQ_TYPE_DAG,
        _ => STR_REQ_TYPE_UNKNOWN,
    }
}

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
    use util::worker::Worker;
    use storage::engine::{self, TEMP_DIR};

    use kvproto::coprocessor::Request;

    use std::sync::*;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_get_req_type_str() {
        assert_eq!(get_req_type_str(REQ_TYPE_SELECT), STR_REQ_TYPE_SELECT);
        assert_eq!(get_req_type_str(REQ_TYPE_INDEX), STR_REQ_TYPE_INDEX);
        assert_eq!(get_req_type_str(REQ_TYPE_DAG), STR_REQ_TYPE_DAG);
        assert_eq!(get_req_type_str(0), STR_REQ_TYPE_UNKNOWN);
    }

    #[test]
    fn test_req_outdated() {
        let mut worker = Worker::new("test-endpoint");
        let engine = engine::new_local_engine(TEMP_DIR, &[]).unwrap();
        let end_point = Host::new(engine, worker.scheduler(), 1);
        worker.start_batch(end_point, 30).unwrap();
        let (tx, rx) = mpsc::channel();
        let mut task = RequestTask::new(Request::new(), box move |msg| { tx.send(msg).unwrap(); });
        task.deadline -= Duration::from_secs(super::REQUEST_MAX_HANDLE_SECS);
        worker.schedule(Task::Request(task)).unwrap();
        let resp = rx.recv_timeout(Duration::from_secs(3)).unwrap();
        assert!(!resp.get_other_error().is_empty());
        assert_eq!(resp.get_other_error(), super::OUTDATED_ERROR_MSG);
    }

    #[test]
    fn test_too_many_reqs() {
        let mut worker = Worker::new("test-endpoint");
        let engine = engine::new_local_engine(TEMP_DIR, &[]).unwrap();
        let mut end_point = Host::new(engine, worker.scheduler(), 1);
        end_point.max_running_task_count = 3;
        worker.start_batch(end_point, 30).unwrap();
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
            let task = RequestTask::new(req, box move |msg| {
                thread::sleep(Duration::from_millis(100));
                let _ = tx.send(msg);
            });
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
}
