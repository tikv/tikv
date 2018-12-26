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

use std::time::Duration;

use futures::sync::mpsc;
use futures::{future, stream, Future, Stream};
use protobuf::{CodedInputStream, Message};

use kvproto::{coprocessor as coppb, errorpb, kvrpcpb};
use tipb::analyze::{AnalyzeReq, AnalyzeType};
use tipb::checksum::{ChecksumRequest, ChecksumScanOn};
use tipb::executor::ExecType;
use tipb::select::DAGRequest;

use server::readpool::{self, ReadPool};
use server::Config;
use storage::{self, Engine};
use util::Either;

use coprocessor::dag::executor::ExecutorMetrics;
use coprocessor::metrics::*;
use coprocessor::tracker::Tracker;
use coprocessor::util as cop_util;
use coprocessor::*;

const OUTDATED_ERROR_MSG: &str = "request outdated.";
const BUSY_ERROR_MSG: &str = "server is busy (coprocessor full).";

pub struct Endpoint<E: Engine> {
    engine: E,
    read_pool: ReadPool<ReadPoolContext>,
    recursion_limit: u32,
    batch_row_limit: usize,
    stream_batch_row_limit: usize,
    stream_channel_size: usize,
    max_handle_duration: Duration,
}

impl<E: Engine> Clone for Endpoint<E> {
    fn clone(&self) -> Self {
        Self {
            engine: self.engine.clone(),
            read_pool: self.read_pool.clone(),
            ..*self
        }
    }
}

impl<E: Engine> ::util::AssertSend for Endpoint<E> {}

impl<E: Engine> Endpoint<E> {
    pub fn new(cfg: &Config, engine: E, read_pool: ReadPool<ReadPoolContext>) -> Self {
        Self {
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
        peer: Option<String>,
        is_streaming: bool,
    ) -> Result<(RequestHandlerBuilder<E::Snap>, ReqContext)> {
        let (context, data, ranges) = (
            req.take_context(),
            req.take_data(),
            req.take_ranges().to_vec(),
        );

        let mut is = CodedInputStream::from_bytes(&data);
        is.set_recursion_limit(self.recursion_limit);

        let req_ctx: ReqContext;
        let builder: RequestHandlerBuilder<E::Snap>;

        match req.get_tp() {
            REQ_TYPE_DAG => {
                let mut dag = DAGRequest::new();
                box_try!(dag.merge_from(&mut is));
                let mut table_scan = false;
                let mut is_desc_scan = false;
                if let Some(scan) = dag.get_executors().iter().next() {
                    table_scan = scan.get_tp() == ExecType::TypeTableScan;
                    if table_scan {
                        is_desc_scan = scan.get_tbl_scan().get_desc();
                    } else {
                        is_desc_scan = scan.get_idx_scan().get_desc();
                    }
                }
                req_ctx = ReqContext::new(
                    make_tag(table_scan),
                    context,
                    ranges.as_slice(),
                    self.max_handle_duration,
                    peer,
                    Some(is_desc_scan),
                    Some(dag.get_start_ts()),
                );
                let batch_row_limit = self.get_batch_row_limit(is_streaming);
                builder = box move |snap, req_ctx: &_| {
                    // See rust-lang#41078 to know why we have `: &_` here.
                    dag::DAGContext::new(dag, ranges, snap, req_ctx, batch_row_limit)
                        .map(|h| h.into_boxed())
                };
            }
            REQ_TYPE_ANALYZE => {
                let mut analyze = AnalyzeReq::new();
                box_try!(analyze.merge_from(&mut is));
                let table_scan = analyze.get_tp() == AnalyzeType::TypeColumn;
                req_ctx = ReqContext::new(
                    make_tag(table_scan),
                    context,
                    ranges.as_slice(),
                    self.max_handle_duration,
                    peer,
                    None,
                    Some(analyze.get_start_ts()),
                );
                builder = box move |snap, req_ctx: &_| {
                    statistics::analyze::AnalyzeContext::new(analyze, ranges, snap, req_ctx)
                        .map(|h| h.into_boxed())
                };
            }
            REQ_TYPE_CHECKSUM => {
                let mut checksum = ChecksumRequest::new();
                box_try!(checksum.merge_from(&mut is));
                let table_scan = checksum.get_scan_on() == ChecksumScanOn::Table;
                req_ctx = ReqContext::new(
                    make_tag(table_scan),
                    context,
                    ranges.as_slice(),
                    self.max_handle_duration,
                    peer,
                    None,
                    Some(checksum.get_start_ts()),
                );
                builder = box move |snap, req_ctx: &_| {
                    checksum::ChecksumContext::new(checksum, ranges, snap, req_ctx)
                        .map(|h| h.into_boxed())
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
        peer: Option<String>,
        is_streaming: bool,
    ) -> (RequestHandlerBuilder<E::Snap>, ReqContext) {
        match self.try_parse_request(req, peer, is_streaming) {
            Ok(v) => v,
            Err(err) => {
                // If there are errors when parsing requests, create a dummy request handler.
                let builder =
                    box |_, _: &_| Ok(cop_util::ErrorRequestHandler::new(err).into_boxed());
                let req_ctx = ReqContext::new(
                    "invalid",
                    kvrpcpb::Context::new(),
                    &[],
                    Duration::from_secs(60), // Large enough to avoid becoming outdated error
                    None,
                    None,
                    None,
                );
                (builder, req_ctx)
            }
        }
    }

    /// Get the batch row limit configuration.
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
        engine: E,
        ctx: &kvrpcpb::Context,
    ) -> impl Future<Item = E::Snap, Error = Error> {
        let (callback, future) = ::util::future::paired_future_callback();
        let val = engine.async_snapshot(ctx, callback);
        future::result(val)
            .and_then(|_| future.map_err(|cancel| storage::engine::Error::Other(box_err!(cancel))))
            .and_then(|(_ctx, result)| result)
            // map engine::Error -> coprocessor::Error
            .map_err(Error::from)
    }

    /// The real implementation of handling a unary request.
    ///
    /// It first retrieves a snapshot, then builds the `RequestHandler` over the snapshot and
    /// the given `handler_builder`. Finally, it calls the unary request interface of the
    /// `RequestHandler` to process the request and produce a result.
    // TODO: Convert to use async / await.
    fn handle_unary_request_impl(
        engine: E,
        tracker: Box<Tracker>,
        handler_builder: RequestHandlerBuilder<E::Snap>,
    ) -> impl Future<Item = coppb::Response, Error = Error> {
        // When this function is being executed, it may be queued for a long time, so that
        // deadline may exceed.
        future::result(tracker.req_ctx.deadline.check_if_exceeded())
            .and_then(move |_| {
                Self::async_snapshot(engine, &tracker.req_ctx.context)
                    .map(|snapshot| (tracker, snapshot))
            })
            .and_then(move |(tracker, snapshot)| {
                // When snapshot is retrieved, deadline may exceed.
                future::result(tracker.req_ctx.deadline.check_if_exceeded())
                    .map(|_| (tracker, snapshot))
            })
            .and_then(move |(tracker, snapshot)| {
                future::result(handler_builder.call_box((snapshot, &tracker.req_ctx)))
                    .map(|handler| (tracker, handler))
            })
            .and_then(|(mut tracker, mut handler)| {
                tracker.on_begin_all_items();
                tracker.on_begin_item();

                // There might be errors when handling requests. In this case, we still need its
                // execution metrics.
                let result = handler.handle_request();
                let exec_metrics = {
                    let mut metrics = ExecutorMetrics::default();
                    handler.collect_metrics_into(&mut metrics);
                    metrics
                };

                tracker.on_finish_item(Some(exec_metrics));
                let exec_details = tracker.get_item_exec_details();

                tracker.on_finish_all_items();

                future::result(result)
                    .or_else(|e| Ok::<_, Error>(make_error_response(e)))
                    .map(|mut resp| {
                        resp.set_exec_details(exec_details);
                        resp
                    })
            })
    }

    /// Handle a unary request and run on the read pool. Returns a future producing the
    /// result, which must be a `Response` and will never fail. If there are errors during
    /// handling, they will be embedded in the `Response`.
    fn handle_unary_request(
        &self,
        req_ctx: ReqContext,
        handler_builder: RequestHandlerBuilder<E::Snap>,
    ) -> impl Future<Item = coppb::Response, Error = ()> {
        let engine = self.engine.clone();
        let priority = readpool::Priority::from(req_ctx.context.get_priority());
        let mut tracker = box Tracker::new(req_ctx);

        let result = self.read_pool.future_execute(priority, move |ctxd| {
            tracker.attach_ctxd(ctxd);

            Self::handle_unary_request_impl(engine, tracker, handler_builder)
        });

        future::result(result)
            // If the read pool is full, an error response will be returned directly.
            .map_err(|_| Error::Full)
            .flatten()
            .or_else(|e| Ok(make_error_response(e)))
    }

    #[inline]
    pub fn parse_and_handle_unary_request(
        &self,
        req: coppb::Request,
        peer: Option<String>,
    ) -> impl Future<Item = coppb::Response, Error = ()> {
        let (handler_builder, req_ctx) = self.parse_request(req, peer, false);
        self.handle_unary_request(req_ctx, handler_builder)
    }

    /// The real implementation of handling a stream request.
    ///
    /// It first retrieves a snapshot, then builds the `RequestHandler` over the snapshot and
    /// the given `handler_builder`. Finally, it calls the stream request interface of the
    /// `RequestHandler` multiple times to process the request and produce multiple results.
    // TODO: Convert to use async / await.
    fn handle_stream_request_impl(
        engine: E,
        tracker: Box<Tracker>,
        handler_builder: RequestHandlerBuilder<E::Snap>,
    ) -> impl Stream<Item = coppb::Response, Error = Error> {
        // When this function is being executed, it may be queued for a long time, so that
        // deadline may exceed.

        let tracker_and_handler_future = future::result(
            tracker.req_ctx.deadline.check_if_exceeded(),
        ).and_then(move |_| {
            Self::async_snapshot(engine, &tracker.req_ctx.context)
                .map(|snapshot| (tracker, snapshot))
        })
            .and_then(move |(tracker, snapshot)| {
                // When snapshot is retrieved, deadline may exceed.
                future::result(tracker.req_ctx.deadline.check_if_exceeded())
                    .map(|_| (tracker, snapshot))
            })
            .and_then(move |(tracker, snapshot)| {
                future::result(handler_builder.call_box((snapshot, &tracker.req_ctx)))
                    .map(|handler| (tracker, handler))
            });

        tracker_and_handler_future
            .map(|(mut tracker, handler)| {
                tracker.on_begin_all_items();

                // The state is `Option<(tracker, handler, finished)>`, `None` indicates finished.
                // For every stream item except the last one, the type is `Either::Left(Response)`.
                // For last stream item, the type is `Either::Right(Tracker)` so that we can do
                // more things for tracker later.
                let initial_state = Some((tracker, handler, false));
                stream::unfold(initial_state, |state| {
                    match state {
                        Some((mut tracker, mut handler, finished)) => {
                            if finished {
                                // Emit tracker as the last item.
                                let yielded = Either::Right(tracker);
                                let next_state = None;
                                return Some(Ok((yielded, next_state)));
                            }

                            // There are future items
                            tracker.on_begin_item();

                            let result = handler.handle_streaming_request();
                            let exec_metrics = {
                                let mut metrics = ExecutorMetrics::default();
                                handler.collect_metrics_into(&mut metrics);
                                metrics
                            };

                            tracker.on_finish_item(Some(exec_metrics));
                            let exec_details = tracker.get_item_exec_details();

                            let (mut resp, finished) = match result {
                                Err(e) => (make_error_response(e), true),
                                Ok((None, _)) => {
                                    let yielded = Either::Right(tracker);
                                    let next_state = None;
                                    return Some(Ok((yielded, next_state)));
                                }
                                Ok((Some(resp), finished)) => (resp, finished),
                            };
                            resp.set_exec_details(exec_details);

                            let yielded = Either::Left(resp);
                            let next_state = Some((tracker, handler, finished));
                            Some(Ok((yielded, next_state)))
                        }
                        None => {
                            // Finished
                            None
                        }
                    }
                }).filter_map(|resp_or_tracker| match resp_or_tracker {
                    Either::Left(resp) => Some(resp),
                    Either::Right(mut tracker) => {
                        tracker.on_finish_all_items();
                        None
                    }
                })
            })
            .flatten_stream()
    }

    /// Handle a stream request and run on the read pool. Returns a stream producing each
    /// result, which must be a `Response` and will never fail. If there are errors during
    /// handling, they will be embedded in the `Response`.
    fn handle_stream_request(
        &self,
        req_ctx: ReqContext,
        handler_builder: RequestHandlerBuilder<E::Snap>,
    ) -> impl Stream<Item = coppb::Response, Error = ()> {
        let (tx, rx) = mpsc::channel::<coppb::Response>(self.stream_channel_size);
        let engine = self.engine.clone();
        let priority = readpool::Priority::from(req_ctx.context.get_priority());
        // Must be created befure `future_execute`, otherwise wait time is not tracked.
        let mut tracker = box Tracker::new(req_ctx);

        let tx1 = tx.clone();
        let result = self.read_pool.future_execute(priority, move |ctxd| {
            tracker.attach_ctxd(ctxd);

            Self::handle_stream_request_impl(engine, tracker, handler_builder)
                .or_else(|e| Ok::<_, mpsc::SendError<_>>(make_error_response(e)))
                // Although returning `Ok()` from `or_else` will continue the stream,
                // our stream has already ended when error is returned.
                // Thus the stream will not continue any more even after we converting errors
                // into a response.
                .forward(tx1)
        });

        match result {
            Err(_) => {
                stream::once::<_, mpsc::SendError<_>>(Ok(make_error_response(Error::Full)))
                    .forward(tx)
                    .then(|_| {
                        // ignore sink send failures
                        Ok::<_, ()>(())
                    })
                    // Should not be blocked, since the channel is large enough to hold 1 value.
                    .wait()
                    .unwrap();
            }
            Ok(cpu_future) => {
                // Keep running stream producer
                cpu_future.forget();
            }
        }

        rx
    }

    #[inline]
    pub fn parse_and_handle_stream_request(
        &self,
        req: coppb::Request,
        peer: Option<String>,
    ) -> impl Stream<Item = coppb::Response, Error = ()> {
        let (handler_builder, req_ctx) = self.parse_request(req, peer, true);
        self.handle_stream_request(req_ctx, handler_builder)
    }
}

fn make_tag(is_table_scan: bool) -> &'static str {
    if is_table_scan {
        "select"
    } else {
        "index"
    }
}

fn make_error_response(e: Error) -> coppb::Response {
    error!("{:?}", e);
    let mut resp = coppb::Response::new();
    let tag;
    match e {
        Error::Region(e) => {
            tag = storage::get_tag_from_header(&e);
            resp.set_region_error(e);
        }
        Error::Locked(info) => {
            tag = "lock";
            resp.set_locked(info);
        }
        Error::Outdated(elapsed, scan_tag) => {
            tag = "outdated";
            OUTDATED_REQ_WAIT_TIME
                .with_label_values(&[scan_tag])
                .observe(elapsed.as_secs() as f64);
            resp.set_other_error(OUTDATED_ERROR_MSG.to_owned());
        }
        Error::Full => {
            tag = "full";
            let mut errorpb = errorpb::Error::new();
            errorpb.set_message("Coprocessor end-point is full".to_owned());
            let mut server_is_busy_err = errorpb::ServerIsBusy::new();
            server_is_busy_err.set_reason(BUSY_ERROR_MSG.to_owned());
            errorpb.set_server_is_busy(server_is_busy_err);
            resp.set_region_error(errorpb);
        }
        Error::Other(_) | Error::Eval(_) => {
            tag = "other";
            resp.set_other_error(format!("{}", e));
        }
    };
    COPR_REQ_ERROR.with_label_values(&[tag]).inc();
    resp
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::{atomic, mpsc, Arc};
    use std::thread;
    use std::vec;

    use tipb::executor::Executor;
    use tipb::expression::Expr;

    use storage::TestEngineBuilder;
    use util::worker::FutureWorker;

    /// A unary `RequestHandler` that always produces a fixture.
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

    /// A streaming `RequestHandler` that always produces a fixture.
    struct StreamFixture {
        result_len: usize,
        result_iter: vec::IntoIter<Result<coppb::Response>>,
        handle_durations_millis: vec::IntoIter<u64>,
        nth: usize,
    }

    impl StreamFixture {
        pub fn new(result: Vec<Result<coppb::Response>>) -> StreamFixture {
            let len = result.len();
            StreamFixture {
                result_len: len,
                result_iter: result.into_iter(),
                handle_durations_millis: vec![0; len].into_iter(),
                nth: 0,
            }
        }

        pub fn new_with_duration(
            result: Vec<Result<coppb::Response>>,
            handle_durations_millis: Vec<u64>,
        ) -> StreamFixture {
            assert_eq!(result.len(), handle_durations_millis.len());
            StreamFixture {
                result_len: result.len(),
                result_iter: result.into_iter(),
                handle_durations_millis: handle_durations_millis.into_iter(),
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
                Some(val) => {
                    let handle_duration_ms = self.handle_durations_millis.next().unwrap();
                    thread::sleep(Duration::from_millis(handle_duration_ms));
                    match val {
                        Ok(resp) => Ok((Some(resp), is_finished)),
                        Err(e) => Err(e),
                    }
                }
            };
            self.nth += 1;

            ret
        }
    }

    /// A streaming `RequestHandler` that produces values according a closure.
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
        let pd_worker = FutureWorker::new("test-pd-worker");
        let engine = TestEngineBuilder::new().build().unwrap();
        let read_pool = ReadPool::new("readpool", &readpool::Config::default_for_test(), || {
            || ReadPoolContext::new(pd_worker.scheduler())
        });
        let cop = Endpoint::new(&Config::default(), engine, read_pool);

        // a normal request
        let handler_builder =
            box |_, _: &_| Ok(UnaryFixture::new(Ok(coppb::Response::new())).into_boxed());
        let resp = cop
            .handle_unary_request(ReqContext::default_for_test(), handler_builder)
            .wait()
            .unwrap();
        assert!(resp.get_other_error().is_empty());

        // an outdated request
        let handler_builder =
            box |_, _: &_| Ok(UnaryFixture::new(Ok(coppb::Response::new())).into_boxed());
        let outdated_req_ctx = ReqContext::new(
            "test",
            kvrpcpb::Context::new(),
            &[],
            Duration::from_secs(0),
            None,
            None,
            None,
        );
        let resp = cop
            .handle_unary_request(outdated_req_ctx, handler_builder)
            .wait()
            .unwrap();
        assert_eq!(resp.get_other_error(), OUTDATED_ERROR_MSG);
    }

    #[test]
    fn test_stack_guard() {
        let pd_worker = FutureWorker::new("test-pd-worker");
        let engine = TestEngineBuilder::new().build().unwrap();
        let read_pool = ReadPool::new("readpool", &readpool::Config::default_for_test(), || {
            || ReadPoolContext::new(pd_worker.scheduler())
        });
        let cop = Endpoint::new(
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

        let resp: coppb::Response = cop
            .parse_and_handle_unary_request(req, None)
            .wait()
            .unwrap();
        assert!(!resp.get_other_error().is_empty());
    }

    #[test]
    fn test_invalid_req_type() {
        let pd_worker = FutureWorker::new("test-pd-worker");
        let engine = TestEngineBuilder::new().build().unwrap();
        let read_pool = ReadPool::new("readpool", &readpool::Config::default_for_test(), || {
            || ReadPoolContext::new(pd_worker.scheduler())
        });
        let cop = Endpoint::new(&Config::default(), engine, read_pool);

        let mut req = coppb::Request::new();
        req.set_tp(9999);

        let resp: coppb::Response = cop
            .parse_and_handle_unary_request(req, None)
            .wait()
            .unwrap();
        assert!(!resp.get_other_error().is_empty());
    }

    #[test]
    fn test_invalid_req_body() {
        let pd_worker = FutureWorker::new("test-pd-worker");
        let engine = TestEngineBuilder::new().build().unwrap();
        let read_pool = ReadPool::new("readpool", &readpool::Config::default_for_test(), || {
            || ReadPoolContext::new(pd_worker.scheduler())
        });
        let cop = Endpoint::new(&Config::default(), engine, read_pool);

        let mut req = coppb::Request::new();
        req.set_tp(REQ_TYPE_DAG);
        req.set_data(vec![1, 2, 3]);

        let resp = cop
            .parse_and_handle_unary_request(req, None)
            .wait()
            .unwrap();
        assert!(!resp.get_other_error().is_empty());
    }

    #[test]
    fn test_full() {
        let pd_worker = FutureWorker::new("test-pd-worker");
        let engine = TestEngineBuilder::new().build().unwrap();
        let read_pool = ReadPool::new(
            "readpool",
            &readpool::Config {
                normal_concurrency: 1,
                max_tasks_per_worker_normal: 2,
                ..readpool::Config::default_for_test()
            },
            || || ReadPoolContext::new(pd_worker.scheduler()),
        );
        let cop = Endpoint::new(&Config::default(), engine, read_pool);

        let (tx, rx) = mpsc::channel();

        // first 2 requests are processed as normal and laters are returned as errors
        for i in 0..5 {
            let mut response = coppb::Response::new();
            response.set_data(vec![1, 2, i]);

            let mut context = kvrpcpb::Context::new();
            context.set_priority(kvrpcpb::CommandPri::Normal);

            let handler_builder =
                box |_, _: &_| Ok(UnaryFixture::new_with_duration(Ok(response), 1000).into_boxed());
            let future = cop.handle_unary_request(ReqContext::default_for_test(), handler_builder);
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
                BUSY_ERROR_MSG
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
        let pd_worker = FutureWorker::new("test-pd-worker");
        let engine = TestEngineBuilder::new().build().unwrap();
        let read_pool = ReadPool::new("readpool", &readpool::Config::default_for_test(), || {
            || ReadPoolContext::new(pd_worker.scheduler())
        });
        let cop = Endpoint::new(&Config::default(), engine, read_pool);

        let handler_builder =
            box |_, _: &_| Ok(UnaryFixture::new(Err(Error::Other(box_err!("foo")))).into_boxed());
        let resp = cop
            .handle_unary_request(ReqContext::default_for_test(), handler_builder)
            .wait()
            .unwrap();
        assert_eq!(resp.get_data().len(), 0);
        assert!(!resp.get_other_error().is_empty());
    }

    #[test]
    #[cfg_attr(feature = "cargo-clippy", allow(needless_range_loop))]
    fn test_error_streaming_response() {
        let pd_worker = FutureWorker::new("test-pd-worker");
        let engine = TestEngineBuilder::new().build().unwrap();
        let read_pool = ReadPool::new("readpool", &readpool::Config::default_for_test(), || {
            || ReadPoolContext::new(pd_worker.scheduler())
        });
        let cop = Endpoint::new(&Config::default(), engine, read_pool);

        // Fail immediately
        let handler_builder = box |_, _: &_| {
            Ok(StreamFixture::new(vec![Err(Error::Other(box_err!("foo")))]).into_boxed())
        };
        let resp_vec = cop
            .handle_stream_request(ReqContext::default_for_test(), handler_builder)
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

        let handler_builder = box |_, _: &_| Ok(StreamFixture::new(responses).into_boxed());
        let resp_vec = cop
            .handle_stream_request(ReqContext::default_for_test(), handler_builder)
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
        let pd_worker = FutureWorker::new("test-pd-worker");
        let engine = TestEngineBuilder::new().build().unwrap();
        let read_pool = ReadPool::new("readpool", &readpool::Config::default_for_test(), || {
            || ReadPoolContext::new(pd_worker.scheduler())
        });
        let cop = Endpoint::new(&Config::default(), engine, read_pool);

        let handler_builder = box |_, _: &_| Ok(StreamFixture::new(vec![]).into_boxed());
        let resp_vec = cop
            .handle_stream_request(ReqContext::default_for_test(), handler_builder)
            .collect()
            .wait()
            .unwrap();
        assert_eq!(resp_vec.len(), 0);
    }

    // TODO: Test panic?

    #[test]
    fn test_special_streaming_handlers() {
        let pd_worker = FutureWorker::new("test-pd-worker");
        let engine = TestEngineBuilder::new().build().unwrap();
        let read_pool = ReadPool::new("readpool", &readpool::Config::default_for_test(), || {
            || ReadPoolContext::new(pd_worker.scheduler())
        });
        let cop = Endpoint::new(&Config::default(), engine, read_pool);

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
        let handler_builder = box move |_, _: &_| Ok(handler.into_boxed());
        let resp_vec = cop
            .handle_stream_request(ReqContext::default_for_test(), handler_builder)
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
        let handler_builder = box move |_, _: &_| Ok(handler.into_boxed());
        let resp_vec = cop
            .handle_stream_request(ReqContext::default_for_test(), handler_builder)
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
        let handler_builder = box move |_, _: &_| Ok(handler.into_boxed());
        let resp_vec = cop
            .handle_stream_request(ReqContext::default_for_test(), handler_builder)
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
        let pd_worker = FutureWorker::new("test-pd-worker");
        let engine = TestEngineBuilder::new().build().unwrap();
        let read_pool = ReadPool::new("readpool", &readpool::Config::default_for_test(), || {
            || ReadPoolContext::new(pd_worker.scheduler())
        });
        let cop = Endpoint::new(
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
        let handler_builder = box move |_, _: &_| Ok(handler.into_boxed());
        let resp_vec = cop
            .handle_stream_request(ReqContext::default_for_test(), handler_builder)
            .take(7)
            .collect()
            .wait()
            .unwrap();
        assert_eq!(resp_vec.len(), 7);
        assert!(counter.load(atomic::Ordering::SeqCst) < 14);
    }

    #[test]
    fn test_handle_time() {
        use util::config::ReadableDuration;

        /// Asserted that the snapshot can be retrieved in 500ms.
        const SNAPSHOT_DURATION_MS: i64 = 500;

        /// Asserted that the delay caused by OS scheduling other tasks is smaller than 200ms.
        /// This is mostly for CI.
        const HANDLE_ERROR_MS: i64 = 200;

        /// The acceptable error range for a coarse timer. Note that we use CLOCK_MONOTONIC_COARSE
        /// which can be slewed by time adjustment code (e.g., NTP, PTP).
        const COARSE_ERROR_MS: i64 = 50;

        /// The duration that payload executes.
        const PAYLOAD_SMALL: i64 = 3000;
        const PAYLOAD_LARGE: i64 = 6000;

        let pd_worker = FutureWorker::new("test-pd-worker");
        let engine = TestEngineBuilder::new().build().unwrap();
        let read_pool = ReadPool::new(
            "readpool",
            &readpool::Config::default_with_concurrency(1),
            || || ReadPoolContext::new(pd_worker.scheduler()),
        );
        let mut config = Config::default();
        config.end_point_request_max_handle_duration =
            ReadableDuration::millis((PAYLOAD_SMALL + PAYLOAD_LARGE) as u64 * 2);

        let cop = Endpoint::new(&config, engine, read_pool);

        let (tx, rx) = ::std::sync::mpsc::channel();

        // A request that requests execution details.
        let mut req_with_exec_detail = ReqContext::default_for_test();
        req_with_exec_detail.context.set_handle_time(true);

        {
            let mut wait_time: i64 = 0;

            // Request 1: Unary, success response.
            let handler_builder = box |_, _: &_| {
                Ok(UnaryFixture::new_with_duration(
                    Ok(coppb::Response::new()),
                    PAYLOAD_SMALL as u64,
                ).into_boxed())
            };
            let resp_future_1 =
                cop.handle_unary_request(req_with_exec_detail.clone(), handler_builder);
            let sender = tx.clone();
            thread::spawn(move || sender.send(vec![resp_future_1.wait().unwrap()]).unwrap());
            // Sleep a while to make sure that thread is spawn and snapshot is taken.
            thread::sleep(Duration::from_millis(SNAPSHOT_DURATION_MS as u64));

            // Request 2: Unary, error response.
            let handler_builder = box |_, _: &_| {
                Ok(
                    UnaryFixture::new_with_duration(Err(box_err!("foo")), PAYLOAD_LARGE as u64)
                        .into_boxed(),
                )
            };
            let resp_future_2 =
                cop.handle_unary_request(req_with_exec_detail.clone(), handler_builder);
            let sender = tx.clone();
            thread::spawn(move || sender.send(vec![resp_future_2.wait().unwrap()]).unwrap());
            thread::sleep(Duration::from_millis(SNAPSHOT_DURATION_MS as u64));

            // Response 1
            let resp = &rx.recv().unwrap()[0];
            assert!(resp.get_other_error().is_empty());
            assert_ge!(
                resp.get_exec_details().get_handle_time().get_process_ms(),
                PAYLOAD_SMALL - COARSE_ERROR_MS
            );
            assert_lt!(
                resp.get_exec_details().get_handle_time().get_process_ms(),
                PAYLOAD_SMALL + HANDLE_ERROR_MS + COARSE_ERROR_MS
            );
            assert_ge!(
                resp.get_exec_details().get_handle_time().get_wait_ms(),
                wait_time - HANDLE_ERROR_MS - COARSE_ERROR_MS
            );
            assert_lt!(
                resp.get_exec_details().get_handle_time().get_wait_ms(),
                wait_time + HANDLE_ERROR_MS + COARSE_ERROR_MS
            );
            wait_time += PAYLOAD_SMALL - SNAPSHOT_DURATION_MS;

            // Response 2
            let resp = &rx.recv().unwrap()[0];
            assert!(!resp.get_other_error().is_empty());
            assert_ge!(
                resp.get_exec_details().get_handle_time().get_process_ms(),
                PAYLOAD_LARGE - COARSE_ERROR_MS
            );
            assert_lt!(
                resp.get_exec_details().get_handle_time().get_process_ms(),
                PAYLOAD_LARGE + HANDLE_ERROR_MS + COARSE_ERROR_MS
            );
            assert_ge!(
                resp.get_exec_details().get_handle_time().get_wait_ms(),
                wait_time - HANDLE_ERROR_MS - COARSE_ERROR_MS
            );
            assert_lt!(
                resp.get_exec_details().get_handle_time().get_wait_ms(),
                wait_time + HANDLE_ERROR_MS + COARSE_ERROR_MS
            );
        }

        {
            let mut wait_time: i64 = 0;

            // Request 1: Unary, success response.
            let handler_builder = box |_, _: &_| {
                Ok(UnaryFixture::new_with_duration(
                    Ok(coppb::Response::new()),
                    PAYLOAD_LARGE as u64,
                ).into_boxed())
            };
            let resp_future_1 =
                cop.handle_unary_request(req_with_exec_detail.clone(), handler_builder);
            let sender = tx.clone();
            thread::spawn(move || sender.send(vec![resp_future_1.wait().unwrap()]).unwrap());
            // Sleep a while to make sure that thread is spawn and snapshot is taken.
            thread::sleep(Duration::from_millis(SNAPSHOT_DURATION_MS as u64));

            // Request 2: Stream.
            let handler_builder = box |_, _: &_| {
                Ok(StreamFixture::new_with_duration(
                    vec![
                        Ok(coppb::Response::new()),
                        Err(box_err!("foo")),
                        Ok(coppb::Response::new()),
                    ],
                    vec![
                        PAYLOAD_SMALL as u64,
                        PAYLOAD_LARGE as u64,
                        PAYLOAD_SMALL as u64,
                    ],
                ).into_boxed())
            };
            let resp_future_3 =
                cop.handle_stream_request(req_with_exec_detail.clone(), handler_builder);
            let sender = tx.clone();
            thread::spawn(move || {
                sender
                    .send(resp_future_3.collect().wait().unwrap())
                    .unwrap()
            });

            // Response 1
            let resp = &rx.recv().unwrap()[0];
            assert!(resp.get_other_error().is_empty());
            assert_ge!(
                resp.get_exec_details().get_handle_time().get_process_ms(),
                PAYLOAD_LARGE - COARSE_ERROR_MS
            );
            assert_lt!(
                resp.get_exec_details().get_handle_time().get_process_ms(),
                PAYLOAD_LARGE + HANDLE_ERROR_MS + COARSE_ERROR_MS
            );
            assert_ge!(
                resp.get_exec_details().get_handle_time().get_wait_ms(),
                wait_time - HANDLE_ERROR_MS - COARSE_ERROR_MS
            );
            assert_lt!(
                resp.get_exec_details().get_handle_time().get_wait_ms(),
                wait_time + HANDLE_ERROR_MS + COARSE_ERROR_MS
            );
            wait_time += PAYLOAD_LARGE - SNAPSHOT_DURATION_MS;

            // Response 2
            let resp = &rx.recv().unwrap();
            assert_eq!(resp.len(), 2);
            assert!(resp[0].get_other_error().is_empty());
            assert_ge!(
                resp[0]
                    .get_exec_details()
                    .get_handle_time()
                    .get_process_ms(),
                PAYLOAD_SMALL - COARSE_ERROR_MS
            );
            assert_lt!(
                resp[0]
                    .get_exec_details()
                    .get_handle_time()
                    .get_process_ms(),
                PAYLOAD_SMALL + HANDLE_ERROR_MS + COARSE_ERROR_MS
            );
            assert_ge!(
                resp[0].get_exec_details().get_handle_time().get_wait_ms(),
                wait_time - HANDLE_ERROR_MS - COARSE_ERROR_MS
            );
            assert_lt!(
                resp[0].get_exec_details().get_handle_time().get_wait_ms(),
                wait_time + HANDLE_ERROR_MS + COARSE_ERROR_MS
            );

            assert!(!resp[1].get_other_error().is_empty());
            assert_ge!(
                resp[1]
                    .get_exec_details()
                    .get_handle_time()
                    .get_process_ms(),
                PAYLOAD_LARGE - COARSE_ERROR_MS
            );
            assert_lt!(
                resp[1]
                    .get_exec_details()
                    .get_handle_time()
                    .get_process_ms(),
                PAYLOAD_LARGE + HANDLE_ERROR_MS + COARSE_ERROR_MS
            );
            assert_ge!(
                resp[1].get_exec_details().get_handle_time().get_wait_ms(),
                wait_time - HANDLE_ERROR_MS - COARSE_ERROR_MS
            );
            assert_lt!(
                resp[1].get_exec_details().get_handle_time().get_wait_ms(),
                wait_time + HANDLE_ERROR_MS + COARSE_ERROR_MS
            );
        }
    }
}
