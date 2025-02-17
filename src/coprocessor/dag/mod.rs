// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

mod storage_impl;

use std::{future::Future, marker::PhantomData, sync::Arc};

use api_version::KvFormat;
use async_trait::async_trait;
use kvproto::{
    coprocessor as coppb,
    coprocessor::{KeyRange, Response},
    metapb,
};
use protobuf::{CodedInputStream, Message};
use raftstore::store::util;
use tidb_query_common::{
    error::{ErrorInner, EvaluateError},
    execute_stats::ExecSummary,
    storage::IntervalRange,
    util::convert_to_prefix_next,
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
use tikv_kv::{with_tls_engine, Engine};
use tikv_util::codec::number::NumberEncoder;
use tipb::{Chunk, DagRequest, FieldType, SelectResponse, StreamResponse, TableScan};
use tracker::{set_tls_tracker_token, RequestInfo, RequestType, GLOBAL_TRACKERS};
use txn_types::Key;

pub use self::storage_impl::TikvStorage;
use crate::{
    coprocessor::{metrics::*, Deadline, Endpoint, Error, RequestHandler, Result},
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
    extra_table_id: Option<i64>,
}

impl BatchDagHandler {
    pub fn new<S: Store + 'static, F: KvFormat>(
        mut req: DagRequest,
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
        let extra_table_id = req.has_extra_table_id().then(|| req.get_extra_table_id());
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
            extra_table_id,
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

    fn index_lookup(&self) -> Option<(Vec<FieldType>, i64)> {
        self.extra_table_id
            .map(|extra_table_id| (self.runner.schema().to_vec(), extra_table_id))
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
        // this can't be cached?
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

    fn has_extra_executor(&self) -> bool {
        self.index_results.is_some()
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
                    handler: self,
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
        let (mut results, record_len, warnings, range) = self.index_runner.run_request().await?;

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
                for (idx, handle) in handle_values.iter().enumerate() {
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
            fn add_point_range(
                key: Vec<u8>,
                region: Arc<metapb::Region>,
                peer_id: u64,
                term: u64,
                ranges: &mut Vec<KeyRange>,
            ) {
                // info!("index lookup locate key"; "key" => ?key,
                //                             "region" => region.id,
                //                             "peer" => peer_id,
                //                             "term" => term);
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
                                add_point_range(
                                    raw_key.clone(),
                                    region.clone(),
                                    *peer_id,
                                    *term,
                                    &mut ranges,
                                );
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
                    add_point_range(raw_key.clone(), region, peer_id, term, &mut ranges);
                    last_handle = Some(handle);
                    ranges_index_pointers.push(i);
                } else {
                    info!("index lookup not locate key"; "key" => ?key,
    "handle" => handle);
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
                // "start_ts" => start_ts,
                "build_cost" => build_cost,
                "extra_task_count" => extra_tasks.len(),
            );
        }
        Ok(extra_tasks)
    }

    // async fn handle_extra_requests(
    //     copr: Arc<Endpoint<E>>,
    //     req: coppb::Request,
    //     results: &mut Vec<BatchExecuteResult>,
    //     extra_tasks: Vec<ExtraExecutorTask>,
    //     peer: Option<String>,
    // ) -> tidb_query_common::Result<Vec<Chunk>> {
    //     let mut result_futures = Vec::new();
    //     let mut result_futures_batch = Vec::new();
    //     let mut keep_index = Vec::new();
    //     let mut handle_extra_request_cost: f64 = 0.0;
    //     let mut total_extra_task = 0;
    //     for task in extra_tasks.iter() {
    //         if task.ranges.len() == 0 {
    //             if task.index_pointers.len() > 0 {
    //                 keep_index.extend_from_slice(&task.index_pointers);
    //             }
    //             continue;
    //         }
    //         let begin = std::time::Instant::now();
    //         let range_result = Self::handle_extra_request(
    //             copr.clone(),
    //             req.clone(),
    //             task.ranges.clone(),
    //             peer.clone(),
    //             task.region.clone(),
    //             task.peer_id,
    //             task.term,
    //         );
    //         handle_extra_request_cost += begin.elapsed().as_secs_f64();
    //         total_extra_task += 1;
    //         result_futures_batch.push(range_result);
    //         if result_futures_batch.len() > 100 {
    //             result_futures.push(result_futures_batch);
    //             result_futures_batch = Vec::new();
    //         }
    //     }
    //     if result_futures_batch.len() > 0 {
    //         result_futures.push(result_futures_batch);
    //     }
    //
    //     if result_futures.len() == 0 {
    //         // this may print many log
    //         info!("no extra task need to do");
    //         // return Ok(resp);
    //         panic!("xxxx");
    //     }
    //     let begin = std::time::Instant::now();
    //     // let mut total_chunks = sel.take_extra_chunks();
    //     let mut extra_chunks = Vec::new();
    //
    //     let mut batch_res: Vec<Option<MemoryTraceGuard<coppb::Response>>> =
    // vec![];     let mut handled_count = 0;
    //     for furs in result_futures {
    //         let group_res: Vec<Option<MemoryTraceGuard<coppb::Response>>> =
    //             futures::future::join_all(furs).await;
    //         handled_count += group_res.len();
    //         batch_res.extend(group_res);
    //         let wait_cost: f64 = begin.elapsed().as_secs_f64();
    //         info!("handle groups extra_requests cost";
    //             // "start_ts" => start_ts,
    //             "wait_group_tasks_cost" => wait_cost,
    //             "total_extra_task" => total_extra_task,
    //             "handled_count" => handled_count,
    //         );
    //     }
    //     for (i, extra_resp) in batch_res.iter().enumerate() {
    //         let mut extra_sel = SelectResponse::default();
    //         if let Some(extra_resp) = extra_resp {
    //             if extra_sel.merge_from_bytes(extra_resp.get_data()).is_ok() {
    //                 let chunks = extra_sel.take_chunks().to_vec();
    //                 for chk in chunks {
    //                     extra_chunks.push(chk);
    //                 }
    //             } else {
    //                 keep_index.extend_from_slice(&extra_tasks[i].index_pointers);
    //             }
    //         } else {
    //             keep_index.extend_from_slice(&extra_tasks[i].index_pointers);
    //         }
    //     }
    //
    //     let wait_extra_task_resp_cost: f64 = begin.elapsed().as_secs_f64();
    //     info!("handle all extra_requests cost";
    //         // "start_ts" => start_ts,
    //         "handle_extra_request_cost" => handle_extra_request_cost,
    //         "wait_extra_task_resp_cost" => wait_extra_task_resp_cost,
    //         "total_extra_task" => total_extra_task,
    //     );
    //     // sel.set_extra_chunks(total_chunks);
    //     if keep_index.len() == 0 {
    //         // info!("no need keep index data since all have extra task");
    //         // sel.clear_chunks();
    //         results.clear();
    //     } else {
    //         keep_index.sort();
    //         info!("some index data need keep"; "keep_index_idxs" => ?keep_index);
    //         for result in results.iter_mut() {
    //             let mut new_logical_rows = Vec::new();
    //             for (i, row) in result.logical_rows.iter().enumerate() {
    //                 if !keep_index.contains(&i) {
    //                     new_logical_rows.push(*row);
    //                 }
    //             }
    //             result.logical_rows = new_logical_rows;
    //         }
    //     }
    //     Ok(extra_chunks)
    // }

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

    // fn handle_extra_request(
    //     copr: Arc<Endpoint<E>>,
    //     req: coppb::Request,
    //     ranges: Vec<KeyRange>,
    //     peer: Option<String>,
    //     region: Arc<metapb::Region>,
    //     peer_id: u64,
    //     term: u64,
    // ) -> impl Future<Output = Option<MemoryTraceGuard<Response>>> {
    //     let begin = std::time::Instant::now();
    //     let mut req = req.clone();
    //     req.set_ranges(ranges.into());
    //     let new_context = req.mut_context();
    //     let old_region_id = new_context.get_region_id();
    //     new_context.set_region_id(region.id);
    //     new_context.set_region_epoch(region.get_region_epoch().clone());
    //     new_context.set_term(term);
    //     new_context.set_replica_read(false);
    //     new_context.set_stale_read(false);
    //     for p in &region.peers {
    //         if p.id == peer_id {
    //             new_context.set_peer(p.clone());
    //             break;
    //         }
    //     }
    //     let mut dag = DagRequest::default();
    //     let data = req.take_data();
    //     let mut input = CodedInputStream::from_bytes(&data);
    //     dag.merge_from(&mut input).expect("decode dag failed");
    //     let extra_executor = dag.take_extra_executors();
    //     let extra_output_offset = dag.take_extra_output_offsets();
    //     dag.set_output_offsets(extra_output_offset);
    //     dag.set_executors(extra_executor);
    //     dag.clear_extra_table_id();
    //     req.set_data(dag.write_to_bytes().expect(
    //         "dag write to byte
    //     failed",
    //     ));
    //     let request_info = RequestInfo::new(req.get_context(),
    // RequestType::Unknown, req.start_ts);     let cur_tracker =
    // GLOBAL_TRACKERS.insert(::tracker::Tracker::new(request_info));
    //     set_tls_tracker_token(cur_tracker);
    //     let result_of_future = copr
    //         .parse_request_and_check_memory_locks(req, peer.clone(), false)
    //         .map(|(handler_builder, req_ctx)| copr.handle_unary_request(req_ctx,
    // handler_builder));
    //
    //     async move {
    //         let build_cost = begin.elapsed().as_secs_f64();
    //         let begin = std::time::Instant::now();
    //         let handle_fut = match result_of_future {
    //             Err(e) => return None,
    //             Ok(handle_fut) => handle_fut,
    //         };
    //         let (mut resp, extra_task) = match handle_fut.await {
    //             Err(e) => return None,
    //             Ok(response) => response,
    //         };
    //         let wait_resp_cost = begin.elapsed().as_secs_f64();
    //         let td = resp.get_exec_details_v2().get_time_detail_v2();
    //         let sd = resp.get_exec_details_v2().get_scan_detail_v2();
    //         let process_wall_time = (td.process_wall_time_ns as f64) /
    // 1_000_000_000.0;         let wait_wall_time = (td.wait_wall_time_ns as
    // f64) / 1_000_000_000.0;         let process_suspend_wall_time =
    //             (td.process_suspend_wall_time_ns as f64) / 1_000_000_000.0;
    //         let kv_read_wall_time = (td.kv_read_wall_time_ns as f64) /
    // 1_000_000_000.0;         let get_snapshot_time =
    // (sd.get_get_snapshot_nanos() as f64) / 1_000_000_000.0;         let
    // rocksdb_block_read_time =             (sd.get_rocksdb_block_read_nanos()
    // as f64) / 1_000_000_000.0;         info!("handle_extra_request cost";
    //             // "start_ts" => start_ts,
    //             "index_region" => old_region_id,
    //             "row_region" => region.id,
    //             "build_cost" => build_cost,
    //             "wait_resp_cost" => wait_resp_cost,
    //             "total_cost" => build_cost + wait_resp_cost,
    //             "process_wall_time" => process_wall_time,
    //             "wait_wall_time" => wait_wall_time,
    //             "process_suspend_wall_time" => process_suspend_wall_time,
    //             "kv_read_wall_time" => kv_read_wall_time,
    //             "get_snapshot_time" => get_snapshot_time,
    //             "rocksdb_block_read" => rocksdb_block_read_time,
    //         );
    //
    //         GLOBAL_TRACKERS.remove(cur_tracker);
    //         Some(resp)
    //     }
    // }
}

pub struct IndexScanResults {
    pub results: Vec<BatchExecuteResult>,
    pub record_len: usize,
}

pub struct ExtraExecutor {
    pub handler: Box<IndexLookupBatchDagHandler>,
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
        let mut ctx = EvalContext::new(self.handler.index_runner.config.clone());
        self.handler
            .index_runner
            .encode_to_chunks(&mut ctx, false, results)
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
