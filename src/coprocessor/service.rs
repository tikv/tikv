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

use std::mem;
use std::sync::Arc;
use futures::{future, stream, Future, Stream};
use futures::sync::mpsc;
use protobuf::{CodedInputStream, Message};

use kvproto::{coprocessor as coppb, errorpb, kvrpcpb};
use tipb::executor::ExecType;
use tipb::select::DAGRequest;
use tipb::analyze::{AnalyzeReq, AnalyzeType};
use tipb::checksum::{ChecksumRequest, ChecksumScanOn};

use util;
use util::futurepool;
use util::time::{self, Instant};
use storage;
use server::Config;
use server::readpool::{self, ReadPool};

use super::*;
use super::util as cop_util;
use super::dag::DAGContext;
use super::statistics::analyze::AnalyzeContext;
use super::checksum::ChecksumContext;
use super::local_metrics::BasicLocalMetrics;
use super::dag::executor::ExecutorMetrics;

// If handle time is larger than the lower bound, the query is considered as slow query.
const SLOW_QUERY_LOWER_BOUND: f64 = 1.0; // 1 second.

const OUTDATED_ERROR_MSG: &str = "request outdated.";
const COPROCESSOR_BUSY_ERROR_MSG: &str = "coprocessor is busy";

pub struct Service {
    engine: Box<storage::Engine>,
    read_pool: ReadPool<ReadPoolContext>,
    recursion_limit: u32,
    batch_row_limit: usize,
    stream_batch_row_limit: usize,
    stream_channel_size: usize,
    max_handle_duration: Duration,
}

impl Clone for Service {
    fn clone(&self) -> Self {
        Service {
            engine: self.engine.clone(),
            read_pool: self.read_pool.clone(),
            ..*self
        }
    }
}

impl util::AssertSend for Service {}

impl Service {
    pub fn new(
        cfg: &Config,
        engine: Box<storage::Engine>,
        read_pool: ReadPool<ReadPoolContext>,
    ) -> Service {
        Service {
            engine,
            read_pool,
            recursion_limit: cfg.end_point_recursion_limit,
            batch_row_limit: cfg.end_point_batch_row_limit,
            stream_batch_row_limit: cfg.end_point_stream_batch_row_limit,
            stream_channel_size: cfg.end_point_stream_channel_size,
            max_handle_duration: cfg.end_point_request_max_handle_duration.0,
        }
    }

    /// Parse the raw `Request` to create `RequestHandlerBuilder` and `ReqContext`.
    /// Returns `Err` if fails.
    fn try_parse_request(
        &self,
        mut req: coppb::Request,
        is_streaming: bool,
    ) -> Result<(RequestHandlerBuilder, Arc<ReqContext>)> {
        let (context, data, ranges) = (
            req.take_context(),
            req.take_data(),
            req.take_ranges().to_vec(),
        );

        let mut is = CodedInputStream::from_bytes(&data);
        is.set_recursion_limit(self.recursion_limit);

        // `Arc` is required because when scheduling handlers (i.e. outside the handler) we need
        // it to determine whether or not it is outdated, while when running handlers (i.e. inside
        // the handler) we also need it to determine whether or not it is outdated.
        let req_ctx: Arc<ReqContext>;
        let builder: RequestHandlerBuilder;

        match req.get_tp() {
            REQ_TYPE_DAG => {
                let mut dag = DAGRequest::new();
                box_try!(dag.merge_from(&mut is));
                let mut table_scan = false;
                if let Some(scan) = dag.get_executors().iter().next() {
                    table_scan = scan.get_tp() == ExecType::TypeTableScan;
                }
                req_ctx = Arc::new(ReqContext::new(
                    context,
                    dag.get_start_ts(),
                    table_scan,
                    self.max_handle_duration,
                ));
                let req_ctx_cloned = Arc::clone(&req_ctx);
                let batch_row_limit = self.get_batch_row_limit(is_streaming);
                builder = box move |snap| {
                    DAGContext::new(dag, ranges, snap, req_ctx_cloned, batch_row_limit)
                        .map(|ctx| ctx.into_boxed())
                };
            }
            REQ_TYPE_ANALYZE => {
                let mut analyze = AnalyzeReq::new();
                box_try!(analyze.merge_from(&mut is));
                let table_scan = analyze.get_tp() == AnalyzeType::TypeColumn;
                req_ctx = Arc::new(ReqContext::new(
                    context,
                    analyze.get_start_ts(),
                    table_scan,
                    self.max_handle_duration,
                ));
                let req_ctx_cloned = Arc::clone(&req_ctx);
                builder = box move |snap| {
                    AnalyzeContext::new(analyze, ranges, snap, &req_ctx_cloned)
                        .map(|ctx| ctx.into_boxed())
                };
            }
            REQ_TYPE_CHECKSUM => {
                let mut checksum = ChecksumRequest::new();
                box_try!(checksum.merge_from(&mut is));
                let table_scan = checksum.get_scan_on() == ChecksumScanOn::Table;
                req_ctx = Arc::new(ReqContext::new(
                    context,
                    checksum.get_start_ts(),
                    table_scan,
                    self.max_handle_duration,
                ));
                let req_ctx_cloned = Arc::clone(&req_ctx);
                builder = box move |snap| {
                    ChecksumContext::new(checksum, ranges, snap, &req_ctx_cloned)
                        .map(|ctx| ctx.into_boxed())
                };
            }
            tp => return Err(box_err!("unsupported tp {}", tp)),
        };
        Ok((builder, req_ctx))
    }

    /// Parse the raw `Request` to create `RequestHandlerBuilder` and `ReqContext`.
    #[inline]
    fn parse_request(
        &self,
        req: coppb::Request,
        is_streaming: bool,
    ) -> (RequestHandlerBuilder, Arc<ReqContext>) {
        match self.try_parse_request(req, is_streaming) {
            Ok(v) => v,
            Err(e) => {
                let builder = box move |_| Ok(cop_util::ErrorRequestHandler::new(e).into_boxed());
                let req_ctx = Arc::new(ReqContext::default());
                (builder, req_ctx)
            }
        }
    }

    #[inline]
    fn get_batch_row_limit(&self, is_streaming: bool) -> usize {
        if is_streaming {
            self.stream_batch_row_limit
        } else {
            self.batch_row_limit
        }
    }

    #[inline]
    fn async_snapshot(
        engine: Box<storage::Engine>,
        ctx: &kvrpcpb::Context,
    ) -> impl Future<Item = Box<storage::Snapshot + 'static>, Error = Error> {
        let (callback, future) = util::future::paired_future_callback();
        let val = engine.async_snapshot(ctx, callback);
        future::result(val)
            .and_then(|_| future.map_err(|cancel| storage::engine::Error::Other(box_err!(cancel))))
            .and_then(|(_ctx, result)| result)
            // map engine::Error -> coprocessor::Error
            .map_err(Error::from)
    }

    fn handle_unary_request_by_custom_handler(
        &self,
        req_ctx: Arc<ReqContext>,
        handler_builder: RequestHandlerBuilder,
    ) -> impl Future<Item = coppb::Response, Error = ()> {
        let mut tracker = Tracker::new(Arc::clone(&req_ctx));

        let engine = self.engine.clone();
        let priority = readpool::Priority::from(req_ctx.context.get_priority());

        let result = self.read_pool.future_execute(priority, move |ctxd| {
            tracker.set_track_target(ctxd);

            Service::async_snapshot(engine, &req_ctx.context)
                .map_err(|e| (e, None))
                .and_then(move |snapshot| {
                    let mut handler: Box<RequestHandler> = match handler_builder(snapshot) {
                        Ok(handler) => handler,
                        Err(e) => cop_util::ErrorRequestHandler::new(e).into_boxed(),
                    };

                    future::result(req_ctx.check_if_outdated())
                        .map_err(|e| (e, None))
                        .and_then(move |_| {
                            tracker.before_all_items();
                            tracker.begin_item();

                            let result = handler.handle_request();
                            let exec_metrics = {
                                let mut metrics = ExecutorMetrics::default();
                                handler.collect_metrics_into(&mut metrics);
                                metrics
                            };

                            tracker.end_item(exec_metrics);
                            let opt_exec_details = tracker.get_item_exec_details();
                            tracker.after_all_items();
                            tracker.track();

                            // Attach execution details (if any) no matter succeeded or not.
                            match result {
                                Ok(resp) => Ok((resp, opt_exec_details)),
                                Err(e) => Err((e, opt_exec_details)),
                            }
                        })
                })
        });
        future::result(result)
            .map_err(|_| (Error::Full, None))
            .flatten()
            .then(|result| {
                let resp_with_exec_details = match result {
                    Ok((resp, exec_details)) => (resp, exec_details),
                    Err((e, exec_details)) => {
                        // Even error responses need execution details.
                        let mut metrics = BasicLocalMetrics::default();
                        (make_error_response(e, &mut metrics), exec_details)
                    }
                };
                Ok(resp_with_exec_details)
            })
            .map(|(mut resp, opt_exec_details)| {
                // If execution details are available, attach it in the response.
                if let Some(exec_details) = opt_exec_details {
                    resp.set_exec_details(exec_details);
                }
                resp
            })
    }

    #[inline]
    pub fn handle_unary_request(
        &self,
        req: coppb::Request,
    ) -> impl Future<Item = coppb::Response, Error = ()> {
        let (handler_builder, req_ctx) = self.parse_request(req, false);
        self.handle_unary_request_by_custom_handler(req_ctx, handler_builder)
    }

    fn handle_stream_request_by_custom_handler(
        &self,
        req_ctx: Arc<ReqContext>,
        handler_builder: RequestHandlerBuilder,
    ) -> impl Stream<Item = coppb::Response, Error = ()> {
        let mut tracker = Tracker::new(Arc::clone(&req_ctx));
        let (tx, rx) = mpsc::channel::<(Result<coppb::Response>, Option<kvrpcpb::ExecDetails>)>(
            self.stream_channel_size,
        );
        let engine = self.engine.clone();
        let priority = readpool::Priority::from(req_ctx.context.get_priority());

        let (tx1, tx2) = (tx.clone(), tx.clone());
        let result = self.read_pool.future_execute(priority, move |ctxd| {
            tracker.set_track_target(ctxd);

            Service::async_snapshot(engine, &req_ctx.context)
                .and_then(move |snapshot| {
                    let handler = match handler_builder(snapshot) {
                        Ok(handler) => handler,
                        Err(e) => cop_util::ErrorRequestHandler::new(e).into_boxed(),
                    };

                    future::result(req_ctx.check_if_outdated()).and_then(move |_| {
                        tracker.before_all_items();
                        stream::unfold((handler, false), move |(mut handler, finished)| {
                            if finished {
                                return None;
                            }

                            tracker.begin_item();

                            let result = handler.handle_streaming_request();
                            let exec_metrics = {
                                let mut metrics = ExecutorMetrics::default();
                                handler.collect_metrics_into(&mut metrics);
                                metrics
                            };

                            tracker.end_item(exec_metrics);
                            let opt_exec_details = tracker.get_item_exec_details();

                            match result {
                                Ok((None, _)) => {
                                    // if we get `None`, stream is always considered finished.
                                    None
                                }
                                Ok((Some(resp), finished)) => {
                                    let yielded = (resp, opt_exec_details);
                                    let next_state = (handler, finished);
                                    Some(Ok((yielded, next_state)))
                                }
                                Err(e) => Some(Err((e, opt_exec_details))),
                            }
                        }).then(|r| {
                            let r = match r {
                                Ok((r, exec_details)) => (Ok(r), exec_details),
                                Err((e, exec_details)) => (Err(e), exec_details),
                            };
                            Ok::<_, mpsc::SendError<_>>(r)
                        })
                            .forward(tx1)
                            .then(move |_| {
                                // ignore sink send failures
                                Ok(())
                            })
                    })
                })
                .map_err(move |e| {
                    stream::once::<_, mpsc::SendError<_>>(Ok((Err(e), None)))
                        .forward(tx2)
                        .then(|_| {
                            // ignore sink send failures
                            Ok::<_, ()>(())
                        })
                        .wait()
                        .unwrap();
                })
        });

        match result {
            Err(_) => {
                stream::once::<_, mpsc::SendError<_>>(Ok((Err(Error::Full), None)))
                    .forward(tx)
                    .then(|_| {
                        // ignore sink send failures
                        Ok::<_, ()>(())
                    })
                    .wait()
                    .unwrap();
            }
            Ok(cpu_future) => {
                // keep running on the FuturePool
                cpu_future.forget();
            }
        }

        rx.map(|(result, opt_exec_details)| {
            let mut resp = match result {
                Ok(resp) => resp,
                Err(e) => {
                    let mut metrics = BasicLocalMetrics::default();
                    make_error_response(e, &mut metrics)
                }
            };
            // If execution details are available, attach it in the response.
            if let Some(exec_details) = opt_exec_details {
                resp.set_exec_details(exec_details);
            }
            resp
        })
    }

    #[inline]
    pub fn handle_stream_request(
        &self,
        req: coppb::Request,
    ) -> impl Stream<Item = coppb::Response, Error = ()> {
        let (handler_builder, req_ctx) = self.parse_request(req, true);
        self.handle_stream_request_by_custom_handler(req_ctx, handler_builder)
    }
}

#[derive(Debug, Clone)]
struct Tracker {
    request_begin_at: Instant,
    item_begin_at: Instant,

    // temp results
    current_stage: usize,
    wait_time: Duration,
    req_time: Duration,
    item_process_time: Duration,
    total_process_time: Duration,
    total_exec_metrics: ExecutorMetrics,

    // info
    req_ctx: Arc<ReqContext>,

    // target
    ctxd: Option<futurepool::ContextDelegators<ReadPoolContext>>,
}

impl Tracker {
    pub fn new(req_ctx: Arc<ReqContext>) -> Tracker {
        Tracker {
            request_begin_at: Instant::now_coarse(),
            item_begin_at: Instant::now_coarse(),

            current_stage: 0,
            wait_time: Duration::default(),
            req_time: Duration::default(),
            item_process_time: Duration::default(),
            total_process_time: Duration::default(),
            total_exec_metrics: ExecutorMetrics::default(),

            req_ctx,

            ctxd: None,
        }
    }

    pub fn before_all_items(&mut self) {
        assert!(self.current_stage == 0);
        self.wait_time = Instant::now_coarse() - self.request_begin_at;
        self.current_stage = 1;
    }

    pub fn begin_item(&mut self) {
        assert!(self.current_stage == 1 || self.current_stage == 3);
        self.item_begin_at = Instant::now_coarse();
        self.current_stage = 2;
    }

    pub fn end_item(&mut self, mut exec_metrics: ExecutorMetrics) {
        assert!(self.current_stage == 2);
        self.item_process_time = Instant::now_coarse() - self.item_begin_at;
        self.total_process_time += self.item_process_time;
        self.total_exec_metrics.merge(&mut exec_metrics);
        self.current_stage = 3;
    }

    pub fn get_item_exec_details(&self) -> Option<kvrpcpb::ExecDetails> {
        assert!(self.current_stage == 3);
        let is_slow_query = time::duration_to_sec(self.item_process_time) > SLOW_QUERY_LOWER_BOUND;
        let mut exec_details = kvrpcpb::ExecDetails::new();
        if self.req_ctx.context.get_handle_time() || is_slow_query {
            let mut handle = kvrpcpb::HandleTime::new();
            handle.set_process_ms(time::duration_to_sec(self.item_process_time) as i64);
            handle.set_wait_ms(time::duration_to_sec(self.wait_time) as i64);
            exec_details.set_handle_time(handle);
        }
        if self.req_ctx.context.get_scan_detail() || is_slow_query {
            let detail = self.total_exec_metrics.cf_stats.scan_detail();
            exec_details.set_scan_detail(detail);
        }
        if exec_details.has_handle_time() || exec_details.has_scan_detail() {
            Some(exec_details)
        } else {
            None
        }
    }

    pub fn after_all_items(&mut self) {
        assert!(self.current_stage == 3);
        self.req_time = Instant::now_coarse() - self.request_begin_at;
        self.current_stage = 4;
    }

    pub fn set_track_target(&mut self, ctxd: futurepool::ContextDelegators<ReadPoolContext>) {
        self.ctxd = Some(ctxd);
    }

    pub fn track(&mut self) {
        if self.ctxd.is_none() {
            return;
        }
        if self.current_stage != 4 {
            return;
        }
        // TODO: Log Slow Queries
        let total_exec_metrics =
            mem::replace(&mut self.total_exec_metrics, ExecutorMetrics::default());
        let ctxd = self.ctxd.take().unwrap();
        let scan_tag = self.req_ctx.get_scan_tag();
        let mut thread_ctx = ctxd.current_thread_context_mut();
        thread_ctx
            .basic_local_metrics
            .wait_time
            .with_label_values(&[scan_tag])
            .observe(time::duration_to_sec(self.wait_time));
        thread_ctx
            .basic_local_metrics
            .handle_time
            .with_label_values(&[scan_tag])
            .observe(time::duration_to_sec(self.total_process_time));
        thread_ctx
            .basic_local_metrics
            .req_time
            .with_label_values(&[scan_tag])
            .observe(time::duration_to_sec(self.req_time));
        thread_ctx
            .basic_local_metrics
            .scan_keys
            .with_label_values(&[scan_tag])
            .observe(total_exec_metrics.cf_stats.total_op_count() as f64);
        thread_ctx.collect(
            self.req_ctx.context.get_region_id(),
            scan_tag,
            total_exec_metrics,
        );
        self.current_stage = 5;
    }
}

impl Drop for Tracker {
    fn drop(&mut self) {
        // Sometimes it is not easy to get back the tracker after passing it to an item generator
        // so that we utilize the `Drop` trait to make sure that stages after items are called
        // properly.
        if self.current_stage == 3 {
            self.after_all_items();
        }
        self.track();
    }
}

fn make_error_response(e: Error, metrics: &mut BasicLocalMetrics) -> coppb::Response {
    let mut resp = coppb::Response::new();
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
        Error::Full => {
            let mut errorpb = errorpb::Error::new();
            let mut server_is_busy_err = errorpb::ServerIsBusy::new();
            server_is_busy_err.set_reason(COPROCESSOR_BUSY_ERROR_MSG.to_owned());
            errorpb.set_server_is_busy(server_is_busy_err);
            resp.set_region_error(errorpb);
            "full"
        }
        Error::Other(_) => {
            resp.set_other_error(format!("{}", e));
            "other"
        }
    };
    metrics.error_cnt.with_label_values(&[tag]).inc();
    resp
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::vec;
    use std::sync::{atomic, mpsc, Arc};
    use std::thread;

    use storage::engine::{self, TEMP_DIR};

    use tipb::expression::Expr;
    use tipb::executor::Executor;

    /// an unary `RequestHandler` that always produces a fixture.
    struct UnaryFixture {
        handle_duration_millis: u64,
        result: Option<Result<coppb::Response>>,
    }

    impl UnaryFixture {
        pub fn new(result: Result<coppb::Response>) -> UnaryFixture {
            UnaryFixture {
                handle_duration_millis: 0,
                result: Some(result),
            }
        }

        pub fn new_with_duration(
            result: Result<coppb::Response>,
            handle_duration_millis: u64,
        ) -> UnaryFixture {
            UnaryFixture {
                handle_duration_millis,
                result: Some(result),
            }
        }
    }

    impl RequestHandler for UnaryFixture {
        fn handle_request(&mut self) -> Result<coppb::Response> {
            thread::sleep(Duration::from_millis(self.handle_duration_millis));
            self.result.take().unwrap()
        }
    }

    /// a streaming `RequestHandler` that always produces a fixture.
    struct StreamFixture {
        result_len: usize,
        result_iter: vec::IntoIter<Result<coppb::Response>>,
        nth: usize,
    }

    impl StreamFixture {
        pub fn new(result: Vec<Result<coppb::Response>>) -> StreamFixture {
            StreamFixture {
                result_len: result.len(),
                result_iter: result.into_iter(),
                nth: 0,
            }
        }
    }

    impl RequestHandler for StreamFixture {
        fn handle_streaming_request(&mut self) -> Result<(Option<coppb::Response>, bool)> {
            let is_finished = if self.result_len == 0 {
                true
            } else {
                self.nth >= (self.result_len - 1)
            };
            let ret = match self.result_iter.next() {
                None => {
                    assert!(is_finished);
                    Ok((None, is_finished))
                }
                Some(Ok(resp)) => Ok((Some(resp), is_finished)),
                Some(Err(e)) => Err(e),
            };
            self.nth += 1;
            ret
        }
    }

    /// a streaming `RequestHandler` that produces values according a closure.
    struct StreamFromClosure {
        result_generator: Box<Fn(usize) -> HandlerStreamStepResult + Send>,
        nth: usize,
    }

    impl StreamFromClosure {
        pub fn new<F>(result_generator: F) -> StreamFromClosure
        where
            F: Fn(usize) -> HandlerStreamStepResult + Send + 'static,
        {
            StreamFromClosure {
                result_generator: box result_generator,
                nth: 0,
            }
        }
    }

    impl RequestHandler for StreamFromClosure {
        fn handle_streaming_request(&mut self) -> Result<(Option<coppb::Response>, bool)> {
            let result = (self.result_generator)(self.nth);
            self.nth += 1;
            result
        }
    }

    #[test]
    fn test_outdated_request() {
        let engine = engine::new_local_engine(TEMP_DIR, &[]).unwrap();
        let read_pool = ReadPool::new("readpool", &readpool::Config::default_for_test(), || {
            || ReadPoolContext::new(None)
        });
        let service = Service::new(&Config::default(), engine, read_pool);

        // a normal request
        let handler_builder =
            box |_| Ok(UnaryFixture::new(Ok(coppb::Response::new())).into_boxed());
        let resp = service
            .handle_unary_request_by_custom_handler(
                Arc::new(ReqContext::default()),
                handler_builder,
            )
            .wait()
            .unwrap();
        assert!(resp.get_other_error().is_empty());

        // an outdated request
        let handler_builder =
            box |_| Ok(UnaryFixture::new(Ok(coppb::Response::new())).into_boxed());
        let outdated_req_ctx =
            ReqContext::new(kvrpcpb::Context::new(), 0, false, Duration::from_secs(0));
        let resp = service
            .handle_unary_request_by_custom_handler(Arc::new(outdated_req_ctx), handler_builder)
            .wait()
            .unwrap();
        assert_eq!(resp.get_other_error(), OUTDATED_ERROR_MSG);
    }

    #[test]
    fn test_stack_guard() {
        let engine = engine::new_local_engine(TEMP_DIR, &[]).unwrap();
        let read_pool = ReadPool::new("readpool", &readpool::Config::default_for_test(), || {
            || ReadPoolContext::new(None)
        });
        let service = Service::new(
            &Config {
                end_point_recursion_limit: 5,
                ..Config::default()
            },
            engine,
            read_pool,
        );

        let req = {
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
            let mut req = coppb::Request::new();
            req.set_tp(REQ_TYPE_DAG);
            req.set_data(dag.write_to_bytes().unwrap());
            req
        };

        let resp: coppb::Response = service.handle_unary_request(req).wait().unwrap();
        assert!(!resp.get_other_error().is_empty());
    }

    #[test]
    fn test_invalid_req_type() {
        let engine = engine::new_local_engine(TEMP_DIR, &[]).unwrap();
        let read_pool = ReadPool::new("readpool", &readpool::Config::default_for_test(), || {
            || ReadPoolContext::new(None)
        });
        let service = Service::new(&Config::default(), engine, read_pool);

        let mut req = coppb::Request::new();
        req.set_tp(9999);

        let resp: coppb::Response = service.handle_unary_request(req).wait().unwrap();
        assert!(!resp.get_other_error().is_empty());
    }

    #[test]
    fn test_invalid_req_body() {
        let engine = engine::new_local_engine(TEMP_DIR, &[]).unwrap();
        let read_pool = ReadPool::new("readpool", &readpool::Config::default_for_test(), || {
            || ReadPoolContext::new(None)
        });
        let service = Service::new(&Config::default(), engine, read_pool);

        let mut req = coppb::Request::new();
        req.set_tp(REQ_TYPE_DAG);
        req.set_data(vec![1, 2, 3]);

        let resp = service.handle_unary_request(req).wait().unwrap();
        assert!(!resp.get_other_error().is_empty());
    }

    #[test]
    fn test_full() {
        let engine = engine::new_local_engine(TEMP_DIR, &[]).unwrap();
        let read_pool = ReadPool::new(
            "readpool",
            &readpool::Config {
                normal_concurrency: 1,
                max_tasks_normal: 2,
                ..readpool::Config::default_for_test()
            },
            || || ReadPoolContext::new(None),
        );
        let service = Service::new(&Config::default(), engine, read_pool);

        let (tx, rx) = mpsc::channel();

        // first 2 requests are processed as normal and laters are returned as errors
        for i in 0..5 {
            let mut response = coppb::Response::new();
            response.set_data(vec![1, 2, i]);

            let mut context = kvrpcpb::Context::new();
            context.set_priority(kvrpcpb::CommandPri::Normal);

            let handler_builder =
                box |_| Ok(UnaryFixture::new_with_duration(Ok(response), 1000).into_boxed());
            let future = service.handle_unary_request_by_custom_handler(
                Arc::new(ReqContext::default()),
                handler_builder,
            );
            let tx = tx.clone();
            thread::spawn(move || tx.send(future.wait().unwrap()));
            thread::sleep(Duration::from_millis(100));
        }

        // verify
        for _ in 2..5 {
            let resp: coppb::Response = rx.recv().unwrap();
            assert_eq!(resp.get_data().len(), 0);
            assert!(resp.has_region_error());
            assert!(resp.get_region_error().has_server_is_busy());
            assert_eq!(
                resp.get_region_error().get_server_is_busy().get_reason(),
                COPROCESSOR_BUSY_ERROR_MSG
            );
        }
        for i in 0..2 {
            let resp = rx.recv().unwrap();
            assert_eq!(resp.get_data(), [1, 2, i]);
            assert!(!resp.has_region_error());
        }
    }

    #[test]
    fn test_error_unary_response() {
        let engine = engine::new_local_engine(TEMP_DIR, &[]).unwrap();
        let read_pool = ReadPool::new("readpool", &readpool::Config::default_for_test(), || {
            || ReadPoolContext::new(None)
        });
        let service = Service::new(&Config::default(), engine, read_pool);

        let handler_builder =
            box |_| Ok(UnaryFixture::new(Err(Error::Other(box_err!("foo")))).into_boxed());
        let resp = service
            .handle_unary_request_by_custom_handler(
                Arc::new(ReqContext::default()),
                handler_builder,
            )
            .wait()
            .unwrap();
        assert_eq!(resp.get_data().len(), 0);
        assert!(!resp.get_other_error().is_empty());
    }

    #[test]
    #[allow(needless_range_loop)]
    fn test_error_streaming_response() {
        let engine = engine::new_local_engine(TEMP_DIR, &[]).unwrap();
        let read_pool = ReadPool::new("readpool", &readpool::Config::default_for_test(), || {
            || ReadPoolContext::new(None)
        });
        let service = Service::new(&Config::default(), engine, read_pool);

        // Fail immediately
        let handler_builder =
            box |_| Ok(StreamFixture::new(vec![Err(Error::Other(box_err!("foo")))]).into_boxed());
        let resp_vec = service
            .handle_stream_request_by_custom_handler(
                Arc::new(ReqContext::default()),
                handler_builder,
            )
            .collect()
            .wait()
            .unwrap();
        assert_eq!(resp_vec.len(), 1);
        assert_eq!(resp_vec[0].get_data().len(), 0);
        assert!(!resp_vec[0].get_other_error().is_empty());

        // Fail after some success responses
        let mut responses = Vec::new();
        for i in 0..5 {
            let mut resp = coppb::Response::new();
            resp.set_data(vec![1, 2, i]);
            responses.push(Ok(resp));
        }
        responses.push(Err(Error::Other(box_err!("foo"))));

        let handler_builder = box move |_| Ok(StreamFixture::new(responses).into_boxed());
        let resp_vec = service
            .handle_stream_request_by_custom_handler(
                Arc::new(ReqContext::default()),
                handler_builder,
            )
            .collect()
            .wait()
            .unwrap();
        assert_eq!(resp_vec.len(), 6);
        for i in 0..5 {
            assert_eq!(resp_vec[i].get_data(), [1, 2, i as u8]);
        }
        assert_eq!(resp_vec[5].get_data().len(), 0);
        assert!(!resp_vec[5].get_other_error().is_empty());
    }

    #[test]
    fn test_empty_streaming_response() {
        let engine = engine::new_local_engine(TEMP_DIR, &[]).unwrap();
        let read_pool = ReadPool::new("readpool", &readpool::Config::default_for_test(), || {
            || ReadPoolContext::new(None)
        });
        let service = Service::new(&Config::default(), engine, read_pool);

        let handler_builder = box |_| Ok(StreamFixture::new(vec![]).into_boxed());
        let resp_vec = service
            .handle_stream_request_by_custom_handler(
                Arc::new(ReqContext::default()),
                handler_builder,
            )
            .collect()
            .wait()
            .unwrap();
        assert_eq!(resp_vec.len(), 0);
    }

    // TODO: Test panic?

    #[test]
    fn test_special_streaming_handlers() {
        let engine = engine::new_local_engine(TEMP_DIR, &[]).unwrap();
        let read_pool = ReadPool::new("readpool", &readpool::Config::default_for_test(), || {
            || ReadPoolContext::new(None)
        });
        let service = Service::new(&Config::default(), engine, read_pool);

        // handler returns `finished == true` should not be called again.
        let counter = Arc::new(atomic::AtomicIsize::new(0));
        let counter_clone = Arc::clone(&counter);
        let handler = StreamFromClosure::new(move |nth| match nth {
            0 => {
                let mut resp = coppb::Response::new();
                resp.set_data(vec![1, 2, 7]);
                Ok((Some(resp), true))
            }
            _ => {
                // we cannot use `unreachable!()` here because CpuPool catches panic.
                counter_clone.store(1, atomic::Ordering::SeqCst);
                Err(box_err!("unreachable"))
            }
        });
        let handler_builder = box |_| Ok(handler.into_boxed());
        let resp_vec = service
            .handle_stream_request_by_custom_handler(
                Arc::new(ReqContext::default()),
                handler_builder,
            )
            .collect()
            .wait()
            .unwrap();
        assert_eq!(resp_vec.len(), 1);
        assert_eq!(resp_vec[0].get_data(), [1, 2, 7]);
        assert_eq!(counter.load(atomic::Ordering::SeqCst), 0);

        // handler returns `None` but `finished == false` should not be called again.
        let counter = Arc::new(atomic::AtomicIsize::new(0));
        let counter_clone = Arc::clone(&counter);
        let handler = StreamFromClosure::new(move |nth| match nth {
            0 => {
                let mut resp = coppb::Response::new();
                resp.set_data(vec![1, 2, 13]);
                Ok((Some(resp), false))
            }
            1 => Ok((None, false)),
            _ => {
                counter_clone.store(1, atomic::Ordering::SeqCst);
                Err(box_err!("unreachable"))
            }
        });
        let handler_builder = box |_| Ok(handler.into_boxed());
        let resp_vec = service
            .handle_stream_request_by_custom_handler(
                Arc::new(ReqContext::default()),
                handler_builder,
            )
            .collect()
            .wait()
            .unwrap();
        assert_eq!(resp_vec.len(), 1);
        assert_eq!(resp_vec[0].get_data(), [1, 2, 13]);
        assert_eq!(counter.load(atomic::Ordering::SeqCst), 0);

        // handler returns `Err(..)` should not be called again.
        let counter = Arc::new(atomic::AtomicIsize::new(0));
        let counter_clone = Arc::clone(&counter);
        let handler = StreamFromClosure::new(move |nth| match nth {
            0 => {
                let mut resp = coppb::Response::new();
                resp.set_data(vec![1, 2, 23]);
                Ok((Some(resp), false))
            }
            1 => Err(box_err!("foo")),
            _ => {
                counter_clone.store(1, atomic::Ordering::SeqCst);
                Err(box_err!("unreachable"))
            }
        });
        let handler_builder = box |_| Ok(handler.into_boxed());
        let resp_vec = service
            .handle_stream_request_by_custom_handler(
                Arc::new(ReqContext::default()),
                handler_builder,
            )
            .collect()
            .wait()
            .unwrap();
        assert_eq!(resp_vec.len(), 2);
        assert_eq!(resp_vec[0].get_data(), [1, 2, 23]);
        assert!(!resp_vec[1].get_other_error().is_empty());
        assert_eq!(counter.load(atomic::Ordering::SeqCst), 0);
    }

    #[test]
    fn test_channel_size() {
        let engine = engine::new_local_engine(TEMP_DIR, &[]).unwrap();
        let read_pool = ReadPool::new("readpool", &readpool::Config::default_for_test(), || {
            || ReadPoolContext::new(None)
        });
        let service = Service::new(
            &Config {
                end_point_stream_channel_size: 3,
                ..Config::default()
            },
            engine,
            read_pool,
        );

        let counter = Arc::new(atomic::AtomicIsize::new(0));
        let counter_clone = Arc::clone(&counter);
        let handler = StreamFromClosure::new(move |nth| {
            // produce an infinite stream
            let mut resp = coppb::Response::new();
            resp.set_data(vec![1, 2, nth as u8]);
            counter_clone.fetch_add(1, atomic::Ordering::SeqCst);
            Ok((Some(resp), false))
        });
        let handler_builder = box |_| Ok(handler.into_boxed());
        let resp_vec = service
            .handle_stream_request_by_custom_handler(
                Arc::new(ReqContext::default()),
                handler_builder,
            )
            .take(7)
            .collect()
            .wait()
            .unwrap();
        assert_eq!(resp_vec.len(), 7);
        assert!(counter.load(atomic::Ordering::SeqCst) < 14);
    }
}
