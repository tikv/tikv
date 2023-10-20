// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

mod storage_impl;

use std::{marker::PhantomData, sync::Arc};

use api_version::KvFormat;
use async_trait::async_trait;
use kvproto::coprocessor::{KeyRange, Response};
use protobuf::Message;
use tidb_query_common::{execute_stats::ExecSummary, storage::IntervalRange};
use tikv_alloc::trace::MemoryTraceGuard;
use tipb::{DagRequest, SelectResponse, StreamResponse};

pub use self::storage_impl::TikvStorage;
use crate::{
    coprocessor::{metrics::*, Deadline, RequestHandler, Result},
    storage::{Statistics, Store},
    tikv_util::quota_limiter::QuotaLimiter,
};

pub struct DagHandlerBuilder<S: Store + 'static, F: KvFormat> {
    req: DagRequest,
    ranges: Vec<KeyRange>,
    store: S,
    data_version: Option<u64>,
    deadline: Deadline,
    batch_row_limit: usize,
    is_streaming: bool,
    is_cache_enabled: bool,
    paging_size: Option<u64>,
    quota_limiter: Arc<QuotaLimiter>,
    _phantom: PhantomData<F>,
}

impl<S: Store + 'static, F: KvFormat> DagHandlerBuilder<S, F> {
    pub fn new(
        req: DagRequest,
        ranges: Vec<KeyRange>,
        store: S,
        deadline: Deadline,
        batch_row_limit: usize,
        is_streaming: bool,
        is_cache_enabled: bool,
        paging_size: Option<u64>,
        quota_limiter: Arc<QuotaLimiter>,
    ) -> Self {
        DagHandlerBuilder {
            req,
            ranges,
            store,
            data_version: None,
            deadline,
            batch_row_limit,
            is_streaming,
            is_cache_enabled,
            paging_size,
            quota_limiter,
            _phantom: PhantomData,
        }
    }

    #[must_use]
    pub fn data_version(mut self, data_version: Option<u64>) -> Self {
        self.data_version = data_version;
        self
    }

    pub fn build(self) -> Result<Box<dyn RequestHandler>> {
        COPR_DAG_REQ_COUNT.with_label_values(&["batch"]).inc();
        Ok(BatchDagHandler::new::<_, F>(
            self.req,
            self.ranges,
            self.store,
            self.data_version,
            self.deadline,
            self.is_cache_enabled,
            self.batch_row_limit,
            self.is_streaming,
            self.paging_size,
            self.quota_limiter,
        )?
        .into_boxed())
    }
}

pub struct BatchDagHandler {
    runner: tidb_query_executors::runner::BatchExecutorsRunner<Statistics>,
    data_version: Option<u64>,
}

impl BatchDagHandler {
    pub fn new<S: Store + 'static, F: KvFormat>(
        req: DagRequest,
        ranges: Vec<KeyRange>,
        store: S,
        data_version: Option<u64>,
        deadline: Deadline,
        is_cache_enabled: bool,
        streaming_batch_limit: usize,
        is_streaming: bool,
        paging_size: Option<u64>,
        quota_limiter: Arc<QuotaLimiter>,
    ) -> Result<Self> {
        Ok(Self {
            runner: tidb_query_executors::runner::BatchExecutorsRunner::from_request::<_, F>(
                req,
                ranges,
                TikvStorage::new(store, is_cache_enabled),
                deadline,
                streaming_batch_limit,
                is_streaming,
                paging_size,
                quota_limiter,
            )?,
            data_version,
        })
    }
}

#[async_trait]
impl RequestHandler for BatchDagHandler {
    async fn handle_request(&mut self) -> Result<MemoryTraceGuard<Response>> {
        let result = self.runner.handle_request().await;
        handle_qe_response(result, self.runner.can_be_cached(), self.data_version).map(|x| x.into())
    }

    async fn handle_streaming_request(&mut self) -> Result<(Option<Response>, bool)> {
        handle_qe_stream_response(self.runner.handle_streaming_request().await)
    }

    fn collect_scan_statistics(&mut self, dest: &mut Statistics) {
        self.runner.collect_storage_stats(dest);
    }

    fn collect_scan_summary(&mut self, dest: &mut ExecSummary) {
        self.runner.collect_scan_summary(dest);
    }
}

fn handle_qe_response(
    result: tidb_query_common::Result<(SelectResponse, Option<IntervalRange>)>,
    can_be_cached: bool,
    data_version: Option<u64>,
) -> Result<Response> {
    use tidb_query_common::error::{ErrorInner, EvaluateError};

    use crate::coprocessor::Error;

    match result {
        Ok((sel_resp, range)) => {
            let mut resp = Response::default();
            if let Some(range) = range {
                resp.mut_range().set_start(range.lower_inclusive);
                resp.mut_range().set_end(range.upper_exclusive);
            }
            resp.set_data(box_try!(sel_resp.write_to_bytes()));
            resp.set_can_be_cached(can_be_cached);
            resp.set_is_cache_hit(false);
            if let Some(v) = data_version {
                resp.set_cache_last_version(v);
            }
            Ok(resp)
        }
        Err(err) => match *err.0 {
            ErrorInner::Storage(err) => Err(err.into()),
            ErrorInner::Evaluate(EvaluateError::DeadlineExceeded) => Err(Error::DeadlineExceeded),
            ErrorInner::Evaluate(err) => {
                let mut resp = Response::default();
                let mut sel_resp = SelectResponse::default();
                sel_resp.mut_error().set_code(err.code());
                sel_resp.mut_error().set_msg(err.to_string());
                resp.set_data(box_try!(sel_resp.write_to_bytes()));
                resp.set_can_be_cached(can_be_cached);
                resp.set_is_cache_hit(false);
                Ok(resp)
            }
        },
    }
}

fn handle_qe_stream_response(
    result: tidb_query_common::Result<(Option<(StreamResponse, IntervalRange)>, bool)>,
) -> Result<(Option<Response>, bool)> {
    use tidb_query_common::error::{ErrorInner, EvaluateError};

    use crate::coprocessor::Error;

    match result {
        Ok((Some((s_resp, range)), finished)) => {
            let mut resp = Response::default();
            resp.set_data(box_try!(s_resp.write_to_bytes()));
            resp.mut_range().set_start(range.lower_inclusive);
            resp.mut_range().set_end(range.upper_exclusive);
            Ok((Some(resp), finished))
        }
        Ok((None, finished)) => Ok((None, finished)),
        Err(err) => match *err.0 {
            ErrorInner::Storage(err) => Err(err.into()),
            ErrorInner::Evaluate(EvaluateError::DeadlineExceeded) => Err(Error::DeadlineExceeded),
            ErrorInner::Evaluate(err) => {
                let mut resp = Response::default();
                let mut s_resp = StreamResponse::default();
                s_resp.mut_error().set_code(err.code());
                s_resp.mut_error().set_msg(err.to_string());
                resp.set_data(box_try!(s_resp.write_to_bytes()));
                Ok((Some(resp), true))
            }
        },
    }
}

#[cfg(test)]
mod tests {
    use anyhow::anyhow;
    use protobuf::Message;
    use tidb_query_common::error::{Error as CommonError, EvaluateError, StorageError};

    use super::*;
    use crate::coprocessor::Error;

    #[test]
    fn test_handle_qe_response() {
        // Ok Response
        let ok_res = Ok((SelectResponse::default(), None));
        let res = handle_qe_response(ok_res, true, Some(1)).unwrap();
        assert!(res.can_be_cached);
        assert_eq!(res.get_cache_last_version(), 1);
        let mut select_res = SelectResponse::new();
        Message::merge_from_bytes(&mut select_res, res.get_data()).unwrap();
        assert!(!select_res.has_error());

        // Storage Error
        let storage_err = CommonError::from(StorageError(anyhow!("unknown")));
        let res = handle_qe_response(Err(storage_err), false, None);
        assert!(matches!(res, Err(Error::Other(_))));

        // Evaluate Error
        let err = CommonError::from(EvaluateError::DeadlineExceeded);
        let res = handle_qe_response(Err(err), false, None);
        assert!(matches!(res, Err(Error::DeadlineExceeded)));

        let err = CommonError::from(EvaluateError::InvalidCharacterString {
            charset: "test".into(),
        });
        let res = handle_qe_response(Err(err), false, None).unwrap();
        let mut select_res = SelectResponse::new();
        Message::merge_from_bytes(&mut select_res, res.get_data()).unwrap();
        assert_eq!(select_res.get_error().get_code(), 1300);
    }
}
