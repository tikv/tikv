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
use futures::{future, Future};

use kvproto::coprocessor as copproto;
use kvproto::errorpb::{self, ServerIsBusy};
use kvproto::kvrpcpb;

use util::time::Instant;
use util::readpool::{self, ReadPool};
use server::Config;
use storage::{self, Engine, Snapshot};
use pd::PdTask;

use super::dag::CopRequestDAG;
use super::statistics::CopRequestAnalyze;
use super::metrics::*;
use super::local_metrics::*;
use super::*;

const REQ_TYPE_DAG: i64 = 103;
const REQ_TYPE_ANALYZE: i64 = 104;

// If handle time is larger than the lower bound, the query is considered as slow query.
const SLOW_QUERY_LOWER_BOUND: f64 = 1.0; // 1 second.

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

    fn parse_raw_request(&self, request: copproto::Request) -> Result<Box<CopRequest>> {
        let tp = request.get_tp();
        match tp {
            REQ_TYPE_DAG => {
                let r = CopRequestDAG::new(request, self.recursion_limit, self.batch_row_limit);
                match r {
                    Err(e) => Err(e),
                    Ok(cop_req) => Ok(box cop_req),
                }
            }
            REQ_TYPE_ANALYZE => {
                let r = CopRequestAnalyze::new(request, self.recursion_limit);
                match r {
                    Err(e) => Err(e),
                    Ok(cop_req) => Ok(box cop_req),
                }
            }
            _ => Err(box_err!("unsupported tp {}", tp)),
        }
    }

    /// Handle a grpc request by the given `copproto::Request`.
    pub fn handle_request(
        &self,
        request: copproto::Request,
    ) -> Box<Future<Item = copproto::Response, Error = ()> + Send> {
        let req_result = self.parse_raw_request(request);
        match req_result {
            // There might be errors when parsing the request.
            Err(e) => box future::ok::<copproto::Response, ()>(make_error_response(e)),

            // Normally we should fall into this branch.
            Ok(cop_req) => box self.handle_coprocessor_request(cop_req),
        }
    }

    /// Handle a grpc request by the given `CopRequest`.
    fn handle_coprocessor_request(
        &self,
        mut req: Box<CopRequest>,
    ) -> impl Future<Item = copproto::Response, Error = ()> {
        static CMD: &'static str = "coprocessor";
        static CMD_TYPE: &'static str = "cop";

        let engine = self.engine.clone();
        let priority = readpool::Priority::from(req.get_context().get_priority());

        let begin_time = Instant::now_coarse();

        self.read_pool
            .future_execute(priority, move |ctxd| {
                engine.future_snapshot(req.get_context())
                .then(move |r| {
                    let elapsed = begin_time.elapsed_secs();
                    r.map(|r| (r, elapsed))
                })
                // map engine::Error -> coprocessor::Error
                .map_err(Error::from)
                .and_then(move |(snapshot, wait_elapsed): (Box<Snapshot>, f64)| {
                    req.get_cop_context().check_if_outdated()?;

                    let should_respond_handle_time = req.get_context().get_handle_time();
                    let should_respond_scan_detail = req.get_context().get_scan_detail();

                    // Process request.
                    let begin_time = Instant::now_coarse();
                    let mut cop_stats = CopStats::default();

                    let result = req.handle(snapshot, &mut cop_stats);

                    // Attach execution details if requested.
                    let mut exec_details = kvrpcpb::ExecDetails::new();
                    let process_elapsed = begin_time.elapsed_secs();
                    if process_elapsed > SLOW_QUERY_LOWER_BOUND || should_respond_handle_time {
                        let mut handle_time = kvrpcpb::HandleTime::new();
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
            .or_else(|e| Ok(make_error_response(e)))
    }
}

fn make_error_response(err: Error) -> copproto::Response {
    let mut resp = copproto::Response::new();
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
        Error::Outdated(deadline, now, tag) => {
            let elapsed =
                now.duration_since(deadline) + Duration::from_secs(REQUEST_MAX_HANDLE_SECS);
            COPR_REQ_ERROR.with_label_values(&["outdated"]).inc();
            OUTDATED_REQ_WAIT_TIME
                .with_label_values(&[&tag])
                .observe(elapsed.as_secs() as f64);

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
    resp
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;
    use std::result;
    use std::sync::mpsc::{channel, Sender};
    use protobuf::Message;

    use kvproto::coprocessor as copproto;
    use kvproto::kvrpcpb;
    use tipb::expression::Expr;
    use tipb::executor::Executor;
    use tipb::select::DAGRequest;

    use storage::engine::{self, TEMP_DIR};
    use util::time::Instant;

    struct DummyCopRequest {
        ctx: Arc<CopContext>,
        raw_req: copproto::Request,
        on_handle: Box<Fn() -> Result<coppb::Response> + Send>,
    }

    impl CopRequest for DummyCopRequest {
        fn handle(
            &mut self,
            _snapshot: Box<Snapshot>,
            _stats: &mut CopStats,
        ) -> Result<coppb::Response> {
            (self.on_handle)()
        }

        fn get_context(&self) -> &kvrpcpb::Context {
            self.raw_req.get_context()
        }

        fn get_cop_context(&self) -> Arc<CopContext> {
            Arc::clone(&self.ctx)
        }
    }

    impl DummyCopRequest {
        fn new(on_handle: Box<Fn() -> Result<coppb::Response> + Send>) -> DummyCopRequest {
            let mut req = copproto::Request::new();
            req.mut_context().set_priority(kvrpcpb::CommandPri::Normal);
            DummyCopRequest {
                ctx: Arc::new(CopContext::new(req.get_context(), "dummy")),
                raw_req: req,
                on_handle,
            }
        }
    }

    /// Tests whether an outdated process results in outdated error.
    #[test]
    fn test_req_outdated() {
        let engine = engine::new_local_engine(TEMP_DIR, &[]).unwrap();
        let read_pool = readpool::ReadPool::new(&readpool::Config::default_for_test(), None);
        let end_point = Host::new(engine, &Config::default(), read_pool);

        let cop_req = DummyCopRequest::new(box || {
            let t = Instant::now_coarse();
            Err(Error::Outdated(t, t, "dummy".to_string()))
        });
        let resp: copproto::Response = end_point
            .handle_coprocessor_request(box cop_req)
            .wait()
            .unwrap();
        assert_eq!(resp.get_other_error(), super::OUTDATED_ERROR_MSG);
    }

    #[test]
    fn test_exec_details_with_long_query() {
        let engine = engine::new_local_engine(TEMP_DIR, &[]).unwrap();
        let read_pool = readpool::ReadPool::new(&readpool::Config::default_for_test(), None);
        let end_point = Host::new(engine, &Config::default(), read_pool);

        let cop_req = DummyCopRequest::new(box || {
            thread::sleep(Duration::from_secs((SLOW_QUERY_LOWER_BOUND * 2.0) as u64));
            Ok(copproto::Response::new())
        });
        let resp: copproto::Response = end_point
            .handle_coprocessor_request(box cop_req)
            .wait()
            .unwrap();
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

        let (tx, rx) = channel();

        for _ in 0..10 {
            let cop_req = DummyCopRequest::new(box || {
                thread::sleep(Duration::from_millis(100));
                Ok(copproto::Response::new())
            });
            let resp_future = end_point.handle_coprocessor_request(box cop_req);
            wait_on_new_thread(tx.clone(), resp_future);
        }

        // Since each valid request needs 100ms to process, while server-busy requests are
        // returned immediately, we got server-busy requests first.
        for _ in 0..6 {
            let resp: copproto::Response = rx.recv().unwrap().unwrap();
            assert!(resp.has_region_error());
            assert!(resp.get_region_error().has_server_is_busy());
        }
        for _ in 0..4 {
            let resp: copproto::Response = rx.recv().unwrap().unwrap();
            assert!(!resp.has_region_error());
        }
        // Wait some time to check that there is no more response remaining in the queue
        assert!(rx.recv_timeout(Duration::from_secs(1)).is_err());
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
        let mut req = copproto::Request::new();
        req.set_tp(REQ_TYPE_DAG);
        req.set_data(dag.write_to_bytes().unwrap());

        let engine = engine::new_local_engine(TEMP_DIR, &[]).unwrap();
        let read_pool = readpool::ReadPool::new(&readpool::Config::default_for_test(), None);
        let end_point = Host::new(
            engine,
            &Config {
                end_point_recursion_limit: 5,
                ..Config::default()
            },
            read_pool,
        );

        let resp: copproto::Response = end_point.handle_request(req).wait().unwrap();
        let s = format!("{:?}", resp);
        assert!(s.contains("Recursion"));
    }
}
