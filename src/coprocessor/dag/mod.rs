// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

mod storage_impl;

use std::{marker::PhantomData, sync::Arc};

use api_version::KvFormat;
use async_trait::async_trait;
use kvproto::{
    coprocessor::{KeyRange, Response},
    metapb,
};
use protobuf::Message;
use raftstore::store::util;
use tidb_query_common::{
    execute_stats::ExecSummary, storage::IntervalRange, util::convert_to_prefix_next,
};
use tidb_query_datatype::{
    codec::{
        data_type::LogicalRows,
        table::{RECORD_PREFIX_SEP, TABLE_PREFIX},
    },
    expr::{EvalContext, EvalWarnings},
    FieldTypeTp,
};
use tidb_query_executors::interface::BatchExecuteResult;
use tikv_alloc::trace::MemoryTraceGuard;
use tikv_util::codec::number::NumberEncoder;
use tipb::{Chunk, DagRequest, FieldType, SelectResponse, StreamResponse};
use txn_types::Key;

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
        let extra_table_id = self
            .req
            .has_extra_table_id()
            .then(|| self.req.get_extra_table_id());
        if extra_table_id.is_some() {
            Ok(IndexLookupBatchDagHandler::new::<_, F>(
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
                extra_table_id.unwrap(),
            )?
            .into_boxed())
        } else {
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

pub struct IndexLookupBatchDagHandler {
    index_runner: tidb_query_executors::runner::BatchExecutorsRunner<Statistics>,
    index_data_version: Option<u64>,
    extra_table_id: i64,

    index_results: Option<IndexScanResults>,
}

#[async_trait]
impl RequestHandler for IndexLookupBatchDagHandler {
    async fn handle_request(&mut self) -> Result<MemoryTraceGuard<Response>> {
        let result = self.internal_handle_request().await;
        handle_qe_response(
            result,
            self.index_runner.can_be_cached(),
            self.index_data_version,
        )
        .map(|x| x.into())
    }

    async fn handle_streaming_request(&mut self) -> Result<(Option<Response>, bool)> {
        handle_qe_stream_response(self.index_runner.handle_streaming_request().await)
    }

    fn collect_scan_statistics(&mut self, dest: &mut Statistics) {
        self.index_runner.collect_storage_stats(dest);
    }

    fn collect_scan_summary(&mut self, dest: &mut ExecSummary) {
        self.index_runner.collect_scan_summary(dest);
    }

    fn build_extra_executor(
        mut self: Box<Self>,
        locate_key: fn(key: &[u8]) -> Option<(Arc<metapb::Region>, u64, u64)>,
    ) -> Result<Option<ExtraExecutor>> {
        match self.index_results {
            Some(ref mut results) => {
                let mut ctx = EvalContext::new(self.index_runner.config.clone());
                let schema = self.index_runner.schema();
                let table_id = self.extra_table_id;
                let tasks = Self::build_extra_tasks_internal(
                    &mut ctx,
                    &schema,
                    &mut results.results,
                    results.record_len,
                    table_id,
                    locate_key,
                )?;

                let index_results = self.index_results.take().unwrap();
                Ok(Some(ExtraExecutor {
                    index_runner: self.index_runner,
                    index_results: Some(index_results),
                    tasks,
                }))
            }
            _ => return Ok(None),
        }
    }
}

impl IndexLookupBatchDagHandler {
    pub fn new<S: Store + 'static, F: KvFormat>(
        dag_req: DagRequest,
        ranges: Vec<KeyRange>,
        store: S,
        data_version: Option<u64>,
        deadline: Deadline,
        is_cache_enabled: bool,
        streaming_batch_limit: usize,
        is_streaming: bool,
        paging_size: Option<u64>,
        quota_limiter: Arc<QuotaLimiter>,
        extra_table_id: i64,
    ) -> Result<Self> {
        Ok(Self {
            index_runner: tidb_query_executors::runner::BatchExecutorsRunner::from_request::<_, F>(
                dag_req,
                ranges,
                TikvStorage::new(store, is_cache_enabled),
                deadline,
                streaming_batch_limit,
                is_streaming,
                paging_size,
                quota_limiter,
            )?,
            index_data_version: data_version,
            extra_table_id,
            index_results: None,
        })
    }
    async fn internal_handle_request(
        &mut self,
    ) -> tidb_query_common::Result<(SelectResponse, Option<IntervalRange>)> {
        let (results, record_len, warnings, range) = self.index_runner.run_request().await?;
        let mut ctx = EvalContext::new(self.index_runner.config.clone());
        let schema = self.index_runner.schema();
        if record_len == 0 || schema.len() != 1 {
            return self.build_response(&mut ctx, results, None, warnings, range);
        }
        self.index_results = Some(IndexScanResults {
            results,
            record_len,
        });
        return self.build_response(&mut ctx, vec![], None, warnings, range);
    }
    fn build_extra_tasks_internal(
        ctx: &mut EvalContext,
        schema: &[FieldType],
        results: &mut Vec<BatchExecuteResult>,
        record_len: usize,
        table_id: i64,
        locate_key: fn(key: &[u8]) -> Option<(Arc<metapb::Region>, u64, u64)>,
    ) -> Result<Vec<ExtraExecutorTask>> {
        let mut extra_tasks = Vec::new();
        if record_len == 0 || schema.len() != 1 {
            return Ok(extra_tasks);
        }

        let begin = std::time::Instant::now();
        let tp = schema[0].get_tp();
        let tp = FieldTypeTp::from_i32(tp).unwrap_or(FieldTypeTp::Unspecified);
        if matches!(tp, FieldTypeTp::Long | FieldTypeTp::LongLong) {
            let mut table_prefix = vec![];
            table_prefix.extend(TABLE_PREFIX);
            table_prefix.encode_i64(table_id).expect("encode i64 succ");
            table_prefix.extend(RECORD_PREFIX_SEP);

            let offset = 0;
            let mut keys = Vec::with_capacity(record_len);
            for result in results {
                result.physical_columns[offset].ensure_decoded(
                    ctx,
                    &schema[offset],
                    LogicalRows::from_slice(&result.logical_rows),
                )?;
                let handle_values = result.physical_columns[offset].decoded().to_int_vec();
                for handle in handle_values.iter() {
                    info!("decode handle for extra task"; "handle" => handle);
                    if handle.is_some() {
                        let mut key;
                        let handle = handle.unwrap();
                        key = table_prefix.clone();
                        key.encode_i64(handle).expect("encode i64 succ");
                        keys.push((key, handle, keys.len()));
                    }
                }
            }

            keys.sort_by(|a, b| a.0.cmp(&b.0));
            let mut ranges: Vec<KeyRange> = Vec::new();
            let mut last_region: Option<(Arc<metapb::Region>, u64, u64)> = None;
            fn add_point_range(key: Vec<u8>, ranges: &mut Vec<KeyRange>) {
                let mut r = KeyRange::new();
                r.set_start(key);
                r.set_end(r.get_start().to_vec());
                convert_to_prefix_next(r.mut_end());
                ranges.push(r);
            }
            fn append_last_range(mut key: Vec<u8>, ranges: &mut Vec<KeyRange>) {
                convert_to_prefix_next(&mut key);
                let last_idx = ranges.len() - 1;
                ranges.get_mut(last_idx).unwrap().set_end(key);
            }
            let mut index_not_located_task = ExtraExecutorTask::default();
            let mut ranges_index_pointers = Vec::new();
            let mut last_handle = None;
            for (raw_key, handle, i) in keys {
                let key = Key::from_raw(&raw_key);
                if let Some((region, peer_id, term)) = &last_region {
                    if util::check_key_in_region(key.as_encoded(), &region).is_ok() {
                        ranges_index_pointers.push(i);
                        match last_handle {
                            Some(last) if last == handle - 1 => {
                                append_last_range(raw_key.clone(), &mut ranges);
                            }
                            _ => {
                                add_point_range(raw_key.clone(), &mut ranges);
                            }
                        };
                        last_handle = Some(handle);
                        continue;
                    } else {
                        if ranges.len() > 0 && ranges_index_pointers.len() > 0 {
                            extra_tasks.push(ExtraExecutorTask {
                                ranges: ranges.clone(),
                                region: region.clone(),
                                peer_id: *peer_id,
                                term: *term,
                                index_pointers: ranges_index_pointers.clone(),
                            });
                        }
                        ranges.clear();
                        ranges_index_pointers.clear();
                    }
                }

                if let Some((region, peer_id, term)) = locate_key(key.as_encoded()) {
                    last_region = Some((region.clone(), peer_id, term));
                    add_point_range(raw_key.clone(), &mut ranges);
                    last_handle = Some(handle);
                    ranges_index_pointers.push(i);
                } else {
                    info!("index lookup not locate key"; "key" => ?key, "handle" => handle);
                    index_not_located_task.index_pointers.push(i);
                }
            }
            if let Some((region, peer_id, term)) = &last_region {
                if ranges.len() > 0 {
                    extra_tasks.push(ExtraExecutorTask {
                        ranges: ranges.clone(),
                        region: region.clone(),
                        peer_id: *peer_id,
                        term: *term,
                        index_pointers: ranges_index_pointers.clone(),
                    });
                }
            }
            extra_tasks.push(index_not_located_task);
            let build_cost = begin.elapsed().as_secs_f64();
            info!("build extra task range cost";
                "build_cost" => build_cost,
                "extra_task_count" => extra_tasks.len(),
            );
        }
        Ok(extra_tasks)
    }

    #[inline]
    pub fn build_response(
        &mut self,
        ctx: &mut EvalContext,
        results: Vec<BatchExecuteResult>,
        extra_chunks: Option<Vec<Chunk>>,
        warnings: EvalWarnings,
        range: Option<IntervalRange>,
    ) -> tidb_query_common::Result<(SelectResponse, Option<IntervalRange>)> {
        let chunks = self.index_runner.encode_to_chunks(ctx, false, results)?;
        let mut resp = self.index_runner.build_response(chunks, warnings)?;
        if let Some(extra_chunks) = extra_chunks {
            resp.set_extra_chunks(extra_chunks.into());
        }
        Ok((resp, range))
    }
}

pub struct IndexScanResults {
    pub results: Vec<BatchExecuteResult>,
    pub record_len: usize,
}

pub struct ExtraExecutor {
    pub index_runner: tidb_query_executors::runner::BatchExecutorsRunner<Statistics>,
    pub index_results: Option<IndexScanResults>,
    pub tasks: Vec<ExtraExecutorTask>,
}

#[derive(Default, Debug)]
pub struct ExtraExecutorTask {
    pub ranges: Vec<KeyRange>,
    pub region: Arc<metapb::Region>,
    pub peer_id: u64,
    pub term: u64,
    pub index_pointers: Vec<usize>,
}

impl ExtraExecutor {
    #[inline]
    pub fn encode_index_results_to_chunks(
        &mut self,
        results: Vec<BatchExecuteResult>,
    ) -> tidb_query_common::Result<Vec<Chunk>> {
        let mut ctx = EvalContext::new(self.index_runner.config.clone());
        self.index_runner.encode_to_chunks(&mut ctx, false, results)
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
