// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use std::marker::PhantomData;
use std::time::Duration;

use async_stream::try_stream;
use futures::sync::mpsc;
use futures::{future, stream, Future, Stream};
use futures03::prelude::*;

use kvproto::{coprocessor as coppb, errorpb, kvrpcpb};
#[cfg(feature = "protobuf-codec")]
use protobuf::CodedInputStream;
use protobuf::Message;
use tipb::{AnalyzeReq, AnalyzeType};
use tipb::{ChecksumRequest, ChecksumScanOn};
use tipb::{DagRequest, ExecType};

use crate::server::Config;
use crate::storage::kv::with_tls_engine;
use crate::storage::kv::{Error as KvError, ErrorInner as KvErrorInner};
use crate::storage::{self, Engine, Snapshot, SnapshotStore};
use tikv_util::future_pool::FuturePool;

use crate::coprocessor::cache::CachedRequestHandler;
use crate::coprocessor::metrics::*;
use crate::coprocessor::tracker::Tracker;
use crate::coprocessor::*;

/// A pool to build and run Coprocessor request handlers.
pub struct Endpoint<E: Engine> {
    /// The thread pool to run Coprocessor requests.
    read_pool_high: FuturePool,
    read_pool_normal: FuturePool,
    read_pool_low: FuturePool,

    /// The recursion limit when parsing Coprocessor Protobuf requests.
    ///
    /// Note that this limit is ignored if we are using Prost.
    recursion_limit: u32,

    batch_row_limit: usize,
    stream_batch_row_limit: usize,
    stream_channel_size: usize,
    enable_batch_if_possible: bool,

    /// The soft time limit of handling Coprocessor requests.
    max_handle_duration: Duration,

    _phantom: PhantomData<E>,
}

impl<E: Engine> Clone for Endpoint<E> {
    fn clone(&self) -> Self {
        Self {
            read_pool_high: self.read_pool_high.clone(),
            read_pool_normal: self.read_pool_normal.clone(),
            read_pool_low: self.read_pool_low.clone(),
            ..*self
        }
    }
}

impl<E: Engine> tikv_util::AssertSend for Endpoint<E> {}

impl<E: Engine> Endpoint<E> {
    pub fn new(cfg: &Config, mut read_pool: Vec<FuturePool>) -> Self {
        let read_pool_high = read_pool.remove(2);
        let read_pool_normal = read_pool.remove(1);
        let read_pool_low = read_pool.remove(0);

        Self {
            read_pool_high,
            read_pool_normal,
            read_pool_low,
            recursion_limit: cfg.end_point_recursion_limit,
            batch_row_limit: cfg.end_point_batch_row_limit,
            enable_batch_if_possible: cfg.end_point_enable_batch_if_possible,
            stream_batch_row_limit: cfg.end_point_stream_batch_row_limit,
            stream_channel_size: cfg.end_point_stream_channel_size,
            max_handle_duration: cfg.end_point_request_max_handle_duration.0,
            _phantom: Default::default(),
        }
    }

    fn get_read_pool(&self, priority: kvrpcpb::CommandPri) -> &FuturePool {
        match priority {
            kvrpcpb::CommandPri::High => &self.read_pool_high,
            kvrpcpb::CommandPri::Normal => &self.read_pool_normal,
            kvrpcpb::CommandPri::Low => &self.read_pool_low,
        }
    }

    /// Parse the raw `Request` to create `RequestHandlerBuilder` and `ReqContext`.
    /// Returns `Err` if fails.
    fn parse_request(
        &self,
        mut req: coppb::Request,
        peer: Option<String>,
        is_streaming: bool,
    ) -> Result<(RequestHandlerBuilder<E::Snap>, ReqContext)> {
        // This `Parser` is here because rust-proto supports customising its
        // recursion limit and Prost does not. Therefore we end up doing things
        // a bit differently for the two codecs.
        #[cfg(feature = "protobuf-codec")]
        struct Parser<'a> {
            input: CodedInputStream<'a>,
        }

        #[cfg(feature = "protobuf-codec")]
        impl<'a> Parser<'a> {
            fn new(data: &'a [u8], recursion_limit: u32) -> Parser<'a> {
                let mut input = CodedInputStream::from_bytes(data);
                input.set_recursion_limit(recursion_limit);
                Parser { input }
            }

            fn merge_to(&mut self, target: &mut impl Message) -> Result<()> {
                box_try!(target.merge_from(&mut self.input));
                Ok(())
            }
        }

        #[cfg(feature = "prost-codec")]
        struct Parser<'a> {
            input: &'a [u8],
        }

        #[cfg(feature = "prost-codec")]
        impl<'a> Parser<'a> {
            fn new(input: &'a [u8], _: u32) -> Parser<'a> {
                Parser { input }
            }

            fn merge_to(&self, target: &mut impl Message) -> Result<()> {
                box_try!(target.merge_from_bytes(&self.input));
                Ok(())
            }
        }

        fail_point!("coprocessor_parse_request", |_| Err(box_err!(
            "unsupported tp (failpoint)"
        )));

        let (context, data, ranges, mut start_ts) = (
            req.take_context(),
            req.take_data(),
            req.take_ranges().to_vec(),
            req.get_start_ts(),
        );
        let cache_match_version = if req.get_is_cache_enabled() {
            Some(req.get_cache_if_match_version())
        } else {
            None
        };

        // Prost and rust-proto require different mutability.
        #[allow(unused_mut)]
        let mut parser = Parser::new(&data, self.recursion_limit);
        let req_ctx: ReqContext;
        let builder: RequestHandlerBuilder<E::Snap>;

        match req.get_tp() {
            REQ_TYPE_DAG => {
                let mut dag = DagRequest::default();
                parser.merge_to(&mut dag)?;
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
                if start_ts == 0 {
                    start_ts = dag.get_start_ts_fallback();
                }

                req_ctx = ReqContext::new(
                    make_tag(table_scan),
                    context,
                    ranges.as_slice(),
                    self.max_handle_duration,
                    peer,
                    Some(is_desc_scan),
                    Some(start_ts),
                    cache_match_version,
                );
                let batch_row_limit = self.get_batch_row_limit(is_streaming);
                let enable_batch_if_possible = self.enable_batch_if_possible;
                builder = Box::new(move |snap, req_ctx: &ReqContext| {
                    // TODO: Remove explicit type once rust-lang#41078 is resolved
                    let data_version = snap.get_data_version();
                    let store = SnapshotStore::new(
                        snap,
                        start_ts.into(),
                        req_ctx.context.get_isolation_level(),
                        !req_ctx.context.get_not_fill_cache(),
                        req_ctx.bypass_locks.clone(),
                    );
                    dag::build_handler(
                        dag,
                        ranges,
                        start_ts,
                        store,
                        data_version,
                        req_ctx.deadline,
                        batch_row_limit,
                        is_streaming,
                        enable_batch_if_possible,
                    )
                });
            }
            REQ_TYPE_ANALYZE => {
                let mut analyze = AnalyzeReq::default();
                parser.merge_to(&mut analyze)?;
                let table_scan = analyze.get_tp() == AnalyzeType::TypeColumn;
                if start_ts == 0 {
                    start_ts = analyze.get_start_ts_fallback();
                }

                req_ctx = ReqContext::new(
                    make_tag(table_scan),
                    context,
                    ranges.as_slice(),
                    self.max_handle_duration,
                    peer,
                    None,
                    Some(start_ts),
                    cache_match_version,
                );
                builder = Box::new(move |snap, req_ctx: &_| {
                    // TODO: Remove explicit type once rust-lang#41078 is resolved
                    statistics::analyze::AnalyzeContext::new(
                        analyze, ranges, start_ts, snap, req_ctx,
                    )
                    .map(|h| h.into_boxed())
                });
            }
            REQ_TYPE_CHECKSUM => {
                let mut checksum = ChecksumRequest::default();
                parser.merge_to(&mut checksum)?;
                let table_scan = checksum.get_scan_on() == ChecksumScanOn::Table;
                if start_ts == 0 {
                    start_ts = checksum.get_start_ts_fallback();
                }

                req_ctx = ReqContext::new(
                    make_tag(table_scan),
                    context,
                    ranges.as_slice(),
                    self.max_handle_duration,
                    peer,
                    None,
                    Some(start_ts),
                    cache_match_version,
                );
                builder = Box::new(move |snap, req_ctx: &_| {
                    // TODO: Remove explicit type once rust-lang#41078 is resolved
                    checksum::ChecksumContext::new(checksum, ranges, start_ts, snap, req_ctx)
                        .map(|h| h.into_boxed())
                });
            }
            tp => return Err(box_err!("unsupported tp {}", tp)),
        };
        Ok((builder, req_ctx))
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
        engine: &E,
        ctx: &kvrpcpb::Context,
    ) -> impl std::future::Future<Output = Result<E::Snap>> {
        let (callback, future) = tikv_util::future::paired_std_future_callback();
        let val = engine.async_snapshot(ctx, callback);
        // make engine not cross yield point
        async move {
            val?; // propagate error
            let (_ctx, result) = future
                .map_err(|cancel| KvError::from(KvErrorInner::Other(box_err!(cancel))))
                .await?;
            // map storage::kv::Error -> storage::txn::Error -> storage::Error
            result.map_err(Error::from)
        }
    }

    /// The real implementation of handling a unary request.
    ///
    /// It first retrieves a snapshot, then builds the `RequestHandler` over the snapshot and
    /// the given `handler_builder`. Finally, it calls the unary request interface of the
    /// `RequestHandler` to process the request and produce a result.
    async fn handle_unary_request_impl(
        mut tracker: Box<Tracker>,
        handler_builder: RequestHandlerBuilder<E::Snap>,
    ) -> Result<coppb::Response> {
        // When this function is being executed, it may be queued for a long time, so that
        // deadline may exceed.
        tracker.req_ctx.deadline.check()?;

        // Safety: spawning this function using a `FuturePool` ensures that a TLS engine
        // exists.
        let snapshot = unsafe {
            with_tls_engine(|engine| Self::async_snapshot(engine, &tracker.req_ctx.context))
        }
        .await?;
        // When snapshot is retrieved, deadline may exceed.
        tracker.req_ctx.deadline.check()?;

        let mut handler = if tracker.req_ctx.cache_match_version.is_some()
            && tracker.req_ctx.cache_match_version == snapshot.get_data_version()
        {
            // Build a cached request handler instead if cache version is matching.
            CachedRequestHandler::builder()(snapshot, &tracker.req_ctx)?
        } else {
            handler_builder(snapshot, &tracker.req_ctx)?
        };

        tracker.on_begin_all_items();
        tracker.on_begin_item();
        // There might be errors when handling requests. In this case, we still need its
        // execution metrics.
        let result = handler.handle_request().await;

        let mut storage_stats = Statistics::default();
        handler.collect_scan_statistics(&mut storage_stats);

        tracker.on_finish_item(Some(storage_stats));
        let exec_details = tracker.get_item_exec_details();

        tracker.on_finish_all_items();
        let mut resp = match result {
            Ok(resp) => {
                COPR_RESP_SIZE.inc_by(resp.data.len() as i64);
                resp
            }
            Err(e) => make_error_response(e),
        };
        resp.set_exec_details(exec_details);
        Ok(resp)
    }

    /// Handle a unary request and run on the read pool.
    ///
    /// Returns `Err(err)` if the read pool is full. Returns `Ok(future)` in other cases.
    /// The future inside may be an error however.
    fn handle_unary_request(
        &self,
        req_ctx: ReqContext,
        handler_builder: RequestHandlerBuilder<E::Snap>,
    ) -> Result<impl Future<Item = coppb::Response, Error = Error>> {
        let read_pool = self.get_read_pool(req_ctx.context.get_priority());
        // box the tracker so that moving it is cheap.
        let tracker = Box::new(Tracker::new(req_ctx));

        read_pool
            .spawn_handle(move || {
                // FIXME: This is an unnecessary box just in order to satisfy the Unpin
                // requirement of compat. Remove it after using a thread pool accepting
                // !Unpin std Futures.
                Box::pin(Self::handle_unary_request_impl(tracker, handler_builder)).compat()
            })
            .map_err(|_| Error::MaxPendingTasksExceeded)
    }

    /// Parses and handles a unary request. Returns a future that will never fail. If there are
    /// errors during parsing or handling, they will be converted into a `Response` as the success
    /// result of the future.
    #[inline]
    pub fn parse_and_handle_unary_request(
        &self,
        req: coppb::Request,
        peer: Option<String>,
    ) -> impl Future<Item = coppb::Response, Error = ()> {
        let result_of_future =
            self.parse_request(req, peer, false)
                .and_then(|(handler_builder, req_ctx)| {
                    self.handle_unary_request(req_ctx, handler_builder)
                });

        future::result(result_of_future)
            .flatten()
            .or_else(|e| Ok(make_error_response(e)))
    }

    /// The real implementation of handling a stream request.
    ///
    /// It first retrieves a snapshot, then builds the `RequestHandler` over the snapshot and
    /// the given `handler_builder`. Finally, it calls the stream request interface of the
    /// `RequestHandler` multiple times to process the request and produce multiple results.
    fn handle_stream_request_impl(
        mut tracker: Box<Tracker>,
        handler_builder: RequestHandlerBuilder<E::Snap>,
    ) -> impl futures03::stream::Stream<Item = Result<coppb::Response>> {
        try_stream! {
            // When this function is being executed, it may be queued for a long time, so that
            // deadline may exceed.
            tracker.req_ctx.deadline.check()?;

            // Safety: spawning this function using a `FuturePool` ensures that a TLS engine
            // exists.
            let snapshot = unsafe {
                with_tls_engine(|engine| Self::async_snapshot(engine, &tracker.req_ctx.context))
            }
            .await?;
            // When snapshot is retrieved, deadline may exceed.
            tracker.req_ctx.deadline.check()?;

            let mut handler = handler_builder(snapshot, &tracker.req_ctx)?;

            tracker.on_begin_all_items();

            loop {
                tracker.on_begin_item();

                let result = handler.handle_streaming_request();
                let mut storage_stats = Statistics::default();
                handler.collect_scan_statistics(&mut storage_stats);

                tracker.on_finish_item(Some(storage_stats));
                let exec_details = tracker.get_item_exec_details();

                match result {
                    Err(e) => {
                        let mut resp = make_error_response(e);
                        resp.set_exec_details(exec_details);
                        yield resp;
                        break;
                    },
                    Ok((None, _)) => break,
                    Ok((Some(mut resp), finished)) => {
                        COPR_RESP_SIZE.inc_by(resp.data.len() as i64);
                        resp.set_exec_details(exec_details);
                        yield resp;
                        if finished {
                            break;
                        }
                    }
                }
            }
            tracker.on_finish_all_items();
        }
    }

    /// Handle a stream request and run on the read pool.
    ///
    /// Returns `Err(err)` if the read pool is full. Returns `Ok(stream)` in other cases.
    /// The stream inside may produce errors however.
    fn handle_stream_request(
        &self,
        req_ctx: ReqContext,
        handler_builder: RequestHandlerBuilder<E::Snap>,
    ) -> Result<impl Stream<Item = coppb::Response, Error = Error>> {
        let (tx, rx) = mpsc::channel::<Result<coppb::Response>>(self.stream_channel_size);
        let read_pool = self.get_read_pool(req_ctx.context.get_priority());
        let tracker = Box::new(Tracker::new(req_ctx));

        read_pool
            .spawn(move || {
                // FIXME: This is an unnecessary box just in order to satisfy the Unpin
                // requirement of compat. Remove it after using a thread pool accepting
                // !Unpin std Futures.
                Box::pin(Self::handle_stream_request_impl(tracker, handler_builder))
                    .compat() // Stream<Resp, Error>
                    .then(Ok::<_, mpsc::SendError<_>>) // Stream<Result<Resp, Error>, MpscError>
                    .forward(tx)
            })
            .map_err(|_| Error::MaxPendingTasksExceeded)?;
        Ok(rx.then(|r| r.unwrap()))
    }

    /// Parses and handles a stream request. Returns a stream that produce each result in a
    /// `Response` and will never fail. If there are errors during parsing or handling, they will
    /// be converted into a `Response` as the only stream item.
    #[inline]
    pub fn parse_and_handle_stream_request(
        &self,
        req: coppb::Request,
        peer: Option<String>,
    ) -> impl Stream<Item = coppb::Response, Error = ()> {
        let result_of_stream =
            self.parse_request(req, peer, true)
                .and_then(|(handler_builder, req_ctx)| {
                    self.handle_stream_request(req_ctx, handler_builder)
                }); // Result<Stream<Resp, Error>, Error>

        stream::once(result_of_stream) // Stream<Stream<Resp, Error>, Error>
            .flatten() // Stream<Resp, Error>
            .or_else(|e| Ok(make_error_response(e))) // Stream<Resp, ()>
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
    warn!(
        "error-response";
        "err" => %e
    );
    let mut resp = coppb::Response::default();
    let tag;
    match e {
        Error::Region(e) => {
            tag = storage::get_tag_from_header(&e);
            resp.set_region_error(e);
        }
        Error::Locked(info) => {
            tag = "meet_lock";
            resp.set_locked(info);
        }
        Error::MaxExecuteTimeExceeded => {
            tag = "max_execute_time_exceeded";
            resp.set_other_error(e.to_string());
        }
        Error::MaxPendingTasksExceeded => {
            tag = "max_pending_tasks_exceeded";
            let mut server_is_busy_err = errorpb::ServerIsBusy::default();
            server_is_busy_err.set_reason(e.to_string());
            let mut errorpb = errorpb::Error::default();
            errorpb.set_message(e.to_string());
            errorpb.set_server_is_busy(server_is_busy_err);
            resp.set_region_error(errorpb);
        }
        Error::Other(_) => {
            tag = "other";
            resp.set_other_error(e.to_string());
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

    use tipb::Executor;
    use tipb::Expr;

    use crate::config::CoprReadPoolConfig;
    use crate::coprocessor::readpool_impl::build_read_pool_for_test;
    use crate::storage::kv::RocksEngine;
    use crate::storage::TestEngineBuilder;
    use protobuf::Message;

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

    #[async_trait]
    impl RequestHandler for UnaryFixture {
        async fn handle_request(&mut self) -> Result<coppb::Response> {
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
        result_generator: Box<dyn Fn(usize) -> HandlerStreamStepResult + Send>,
        nth: usize,
    }

    impl StreamFromClosure {
        pub fn new<F>(result_generator: F) -> StreamFromClosure
        where
            F: Fn(usize) -> HandlerStreamStepResult + Send + 'static,
        {
            StreamFromClosure {
                result_generator: Box::new(result_generator),
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
        let engine = TestEngineBuilder::new().build().unwrap();
        let read_pool = build_read_pool_for_test(&CoprReadPoolConfig::default_for_test(), engine);
        let cop = Endpoint::<RocksEngine>::new(&Config::default(), read_pool);

        // a normal request
        let handler_builder =
            Box::new(|_, _: &_| Ok(UnaryFixture::new(Ok(coppb::Response::default())).into_boxed()));
        let resp = cop
            .handle_unary_request(ReqContext::default_for_test(), handler_builder)
            .unwrap()
            .wait()
            .unwrap();
        assert!(resp.get_other_error().is_empty());

        // an outdated request
        let handler_builder =
            Box::new(|_, _: &_| Ok(UnaryFixture::new(Ok(coppb::Response::default())).into_boxed()));
        let outdated_req_ctx = ReqContext::new(
            "test",
            kvrpcpb::Context::default(),
            &[],
            Duration::from_secs(0),
            None,
            None,
            None,
            None,
        );
        assert!(cop
            .handle_unary_request(outdated_req_ctx, handler_builder)
            .unwrap()
            .wait()
            .is_err());
    }

    #[test]
    fn test_stack_guard() {
        let engine = TestEngineBuilder::new().build().unwrap();
        let read_pool = build_read_pool_for_test(&CoprReadPoolConfig::default_for_test(), engine);
        let mut cop = Endpoint::<RocksEngine>::new(&Config::default(), read_pool);
        cop.recursion_limit = 100;

        let req = {
            let mut expr = Expr::default();
            // The recursion limit in Prost and rust-protobuf (by default) is 100 (for rust-protobuf,
            // that limit is set to 1000 as a configuration default).
            for _ in 0..101 {
                let mut e = Expr::default();
                e.mut_children().push(expr);
                expr = e;
            }
            let mut e = Executor::default();
            e.mut_selection().mut_conditions().push(expr);
            let mut dag = DagRequest::default();
            dag.mut_executors().push(e);
            let mut req = coppb::Request::default();
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
        let engine = TestEngineBuilder::new().build().unwrap();
        let read_pool = build_read_pool_for_test(&CoprReadPoolConfig::default_for_test(), engine);
        let cop = Endpoint::<RocksEngine>::new(&Config::default(), read_pool);

        let mut req = coppb::Request::default();
        req.set_tp(9999);

        let resp: coppb::Response = cop
            .parse_and_handle_unary_request(req, None)
            .wait()
            .unwrap();
        assert!(!resp.get_other_error().is_empty());
    }

    #[test]
    fn test_invalid_req_body() {
        let engine = TestEngineBuilder::new().build().unwrap();
        let read_pool = build_read_pool_for_test(&CoprReadPoolConfig::default_for_test(), engine);
        let cop = Endpoint::<RocksEngine>::new(&Config::default(), read_pool);

        let mut req = coppb::Request::default();
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
        use crate::storage::kv::{destroy_tls_engine, set_tls_engine};
        use std::sync::Mutex;
        use tikv_util::future_pool::Builder;

        let engine = TestEngineBuilder::new().build().unwrap();

        let read_pool = CoprReadPoolConfig {
            normal_concurrency: 1,
            max_tasks_per_worker_normal: 2,
            ..CoprReadPoolConfig::default_for_test()
        }
        .to_future_pool_configs()
        .into_iter()
        .map(|config| {
            let engine = Arc::new(Mutex::new(engine.clone()));
            Builder::from_config(config)
                .name_prefix("coprocessor_endpoint_test_full")
                .after_start(move || set_tls_engine(engine.lock().unwrap().clone()))
                // Safety: we call `set_` and `destroy_` with the same engine type.
                .before_stop(|| unsafe { destroy_tls_engine::<RocksEngine>() })
                .build()
        })
        .collect();

        let cop = Endpoint::<RocksEngine>::new(&Config::default(), read_pool);

        let (tx, rx) = mpsc::channel();

        // first 2 requests are processed as normal and laters are returned as errors
        for i in 0..5 {
            let mut response = coppb::Response::default();
            response.set_data(vec![1, 2, i]);

            let mut context = kvrpcpb::Context::default();
            context.set_priority(kvrpcpb::CommandPri::Normal);

            let handler_builder = Box::new(|_, _: &_| {
                Ok(UnaryFixture::new_with_duration(Ok(response), 1000).into_boxed())
            });
            let result_of_future =
                cop.handle_unary_request(ReqContext::default_for_test(), handler_builder);
            match result_of_future {
                Err(full_error) => {
                    tx.send(Err(full_error)).unwrap();
                }
                Ok(future) => {
                    let tx = tx.clone();
                    thread::spawn(move || {
                        tx.send(future.wait()).unwrap();
                    });
                }
            }
            thread::sleep(Duration::from_millis(100));
        }

        // verify
        for _ in 2..5 {
            assert!(rx.recv().unwrap().is_err());
        }
        for i in 0..2 {
            let resp = rx.recv().unwrap().unwrap();
            assert_eq!(resp.get_data(), [1, 2, i]);
            assert!(!resp.has_region_error());
        }
    }

    #[test]
    fn test_error_unary_response() {
        let engine = TestEngineBuilder::new().build().unwrap();
        let read_pool = build_read_pool_for_test(&CoprReadPoolConfig::default_for_test(), engine);
        let cop = Endpoint::<RocksEngine>::new(&Config::default(), read_pool);

        let handler_builder =
            Box::new(|_, _: &_| Ok(UnaryFixture::new(Err(box_err!("foo"))).into_boxed()));
        let resp = cop
            .handle_unary_request(ReqContext::default_for_test(), handler_builder)
            .unwrap()
            .wait()
            .unwrap();
        assert_eq!(resp.get_data().len(), 0);
        assert!(!resp.get_other_error().is_empty());
    }

    #[test]
    fn test_error_streaming_response() {
        let engine = TestEngineBuilder::new().build().unwrap();
        let read_pool = build_read_pool_for_test(&CoprReadPoolConfig::default_for_test(), engine);
        let cop = Endpoint::<RocksEngine>::new(&Config::default(), read_pool);

        // Fail immediately
        let handler_builder =
            Box::new(|_, _: &_| Ok(StreamFixture::new(vec![Err(box_err!("foo"))]).into_boxed()));
        let resp_vec = cop
            .handle_stream_request(ReqContext::default_for_test(), handler_builder)
            .unwrap()
            .collect()
            .wait()
            .unwrap();
        assert_eq!(resp_vec.len(), 1);
        assert_eq!(resp_vec[0].get_data().len(), 0);
        assert!(!resp_vec[0].get_other_error().is_empty());

        // Fail after some success responses
        let mut responses = Vec::new();
        for i in 0..5 {
            let mut resp = coppb::Response::default();
            resp.set_data(vec![1, 2, i]);
            responses.push(Ok(resp));
        }
        responses.push(Err(box_err!("foo")));

        let handler_builder = Box::new(|_, _: &_| Ok(StreamFixture::new(responses).into_boxed()));
        let resp_vec = cop
            .handle_stream_request(ReqContext::default_for_test(), handler_builder)
            .unwrap()
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
        let engine = TestEngineBuilder::new().build().unwrap();
        let read_pool = build_read_pool_for_test(&CoprReadPoolConfig::default_for_test(), engine);
        let cop = Endpoint::<RocksEngine>::new(&Config::default(), read_pool);

        let handler_builder = Box::new(|_, _: &_| Ok(StreamFixture::new(vec![]).into_boxed()));
        let resp_vec = cop
            .handle_stream_request(ReqContext::default_for_test(), handler_builder)
            .unwrap()
            .collect()
            .wait()
            .unwrap();
        assert_eq!(resp_vec.len(), 0);
    }

    // TODO: Test panic?

    #[test]
    fn test_special_streaming_handlers() {
        let engine = TestEngineBuilder::new().build().unwrap();
        let read_pool = build_read_pool_for_test(&CoprReadPoolConfig::default_for_test(), engine);
        let cop = Endpoint::<RocksEngine>::new(&Config::default(), read_pool);

        // handler returns `finished == true` should not be called again.
        let counter = Arc::new(atomic::AtomicIsize::new(0));
        let counter_clone = Arc::clone(&counter);
        let handler = StreamFromClosure::new(move |nth| match nth {
            0 => {
                let mut resp = coppb::Response::default();
                resp.set_data(vec![1, 2, 7]);
                Ok((Some(resp), true))
            }
            _ => {
                // we cannot use `unreachable!()` here because CpuPool catches panic.
                counter_clone.store(1, atomic::Ordering::SeqCst);
                Err(box_err!("unreachable"))
            }
        });
        let handler_builder = Box::new(move |_, _: &_| Ok(handler.into_boxed()));
        let resp_vec = cop
            .handle_stream_request(ReqContext::default_for_test(), handler_builder)
            .unwrap()
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
                let mut resp = coppb::Response::default();
                resp.set_data(vec![1, 2, 13]);
                Ok((Some(resp), false))
            }
            1 => Ok((None, false)),
            _ => {
                counter_clone.store(1, atomic::Ordering::SeqCst);
                Err(box_err!("unreachable"))
            }
        });
        let handler_builder = Box::new(move |_, _: &_| Ok(handler.into_boxed()));
        let resp_vec = cop
            .handle_stream_request(ReqContext::default_for_test(), handler_builder)
            .unwrap()
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
                let mut resp = coppb::Response::default();
                resp.set_data(vec![1, 2, 23]);
                Ok((Some(resp), false))
            }
            1 => Err(box_err!("foo")),
            _ => {
                counter_clone.store(1, atomic::Ordering::SeqCst);
                Err(box_err!("unreachable"))
            }
        });
        let handler_builder = Box::new(move |_, _: &_| Ok(handler.into_boxed()));
        let resp_vec = cop
            .handle_stream_request(ReqContext::default_for_test(), handler_builder)
            .unwrap()
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
        let engine = TestEngineBuilder::new().build().unwrap();
        let read_pool = build_read_pool_for_test(&CoprReadPoolConfig::default_for_test(), engine);
        let cop = Endpoint::<RocksEngine>::new(
            &Config {
                end_point_stream_channel_size: 3,
                ..Config::default()
            },
            read_pool,
        );

        let counter = Arc::new(atomic::AtomicIsize::new(0));
        let counter_clone = Arc::clone(&counter);
        let handler = StreamFromClosure::new(move |nth| {
            // produce an infinite stream
            let mut resp = coppb::Response::default();
            resp.set_data(vec![1, 2, nth as u8]);
            counter_clone.fetch_add(1, atomic::Ordering::SeqCst);
            Ok((Some(resp), false))
        });
        let handler_builder = Box::new(move |_, _: &_| Ok(handler.into_boxed()));
        let resp_vec = cop
            .handle_stream_request(ReqContext::default_for_test(), handler_builder)
            .unwrap()
            .take(7)
            .collect()
            .wait()
            .unwrap();
        assert_eq!(resp_vec.len(), 7);
        assert!(counter.load(atomic::Ordering::SeqCst) < 14);
    }

    #[test]
    fn test_handle_time() {
        use tikv_util::config::ReadableDuration;

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

        let engine = TestEngineBuilder::new().build().unwrap();

        let read_pool = build_read_pool_for_test(
            &CoprReadPoolConfig {
                low_concurrency: 1,
                normal_concurrency: 1,
                high_concurrency: 1,
                ..CoprReadPoolConfig::default_for_test()
            },
            engine,
        );

        let mut config = Config::default();
        config.end_point_request_max_handle_duration =
            ReadableDuration::millis((PAYLOAD_SMALL + PAYLOAD_LARGE) as u64 * 2);

        let cop = Endpoint::<RocksEngine>::new(&config, read_pool);

        let (tx, rx) = std::sync::mpsc::channel();

        // A request that requests execution details.
        let mut req_with_exec_detail = ReqContext::default_for_test();
        req_with_exec_detail.context.set_handle_time(true);

        {
            let mut wait_time: i64 = 0;

            // Request 1: Unary, success response.
            let handler_builder = Box::new(|_, _: &_| {
                Ok(UnaryFixture::new_with_duration(
                    Ok(coppb::Response::default()),
                    PAYLOAD_SMALL as u64,
                )
                .into_boxed())
            });
            let resp_future_1 = cop
                .handle_unary_request(req_with_exec_detail.clone(), handler_builder)
                .unwrap();
            let sender = tx.clone();
            thread::spawn(move || sender.send(vec![resp_future_1.wait().unwrap()]).unwrap());
            // Sleep a while to make sure that thread is spawn and snapshot is taken.
            thread::sleep(Duration::from_millis(SNAPSHOT_DURATION_MS as u64));

            // Request 2: Unary, error response.
            let handler_builder = Box::new(|_, _: &_| {
                Ok(
                    UnaryFixture::new_with_duration(Err(box_err!("foo")), PAYLOAD_LARGE as u64)
                        .into_boxed(),
                )
            });
            let resp_future_2 = cop
                .handle_unary_request(req_with_exec_detail.clone(), handler_builder)
                .unwrap();
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
            let handler_builder = Box::new(|_, _: &_| {
                Ok(UnaryFixture::new_with_duration(
                    Ok(coppb::Response::default()),
                    PAYLOAD_LARGE as u64,
                )
                .into_boxed())
            });
            let resp_future_1 = cop
                .handle_unary_request(req_with_exec_detail.clone(), handler_builder)
                .unwrap();
            let sender = tx.clone();
            thread::spawn(move || sender.send(vec![resp_future_1.wait().unwrap()]).unwrap());
            // Sleep a while to make sure that thread is spawn and snapshot is taken.
            thread::sleep(Duration::from_millis(SNAPSHOT_DURATION_MS as u64));

            // Request 2: Stream.
            let handler_builder = Box::new(|_, _: &_| {
                Ok(StreamFixture::new_with_duration(
                    vec![
                        Ok(coppb::Response::default()),
                        Err(box_err!("foo")),
                        Ok(coppb::Response::default()),
                    ],
                    vec![
                        PAYLOAD_SMALL as u64,
                        PAYLOAD_LARGE as u64,
                        PAYLOAD_SMALL as u64,
                    ],
                )
                .into_boxed())
            });
            let resp_future_3 = cop
                .handle_stream_request(req_with_exec_detail.clone(), handler_builder)
                .unwrap();
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
