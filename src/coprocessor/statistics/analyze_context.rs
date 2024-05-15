// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

use std::{cmp::Reverse, collections::BinaryHeap, marker::PhantomData, sync::Arc};

use api_version::{keyspace::KvPair, KvFormat};
use async_trait::async_trait;
use kvproto::coprocessor::{KeyRange, Response};
use protobuf::Message;
use tidb_query_common::storage::{
    scanner::{RangesScanner, RangesScannerOptions},
    Range,
};
use tidb_query_datatype::codec::{datum::split_datum, table};
use tidb_query_executors::interface::BatchExecutor;
use tikv_alloc::trace::MemoryTraceGuard;
use tikv_util::quota_limiter::QuotaLimiter;
use tipb::{self, AnalyzeIndexReq, AnalyzeReq, AnalyzeType};

use super::{cmsketch::CmSketch, fmsketch::FmSketch, histogram::Histogram};
use crate::{
    coprocessor::{
        dag::TikvStorage,
        statistics::analyze::{
            AnalyzeIndexResult, AnalyzeMixedResult, RowSampleBuilder, SampleBuilder,
        },
        MEMTRACE_ANALYZE, *,
    },
    storage::{Snapshot, SnapshotStore, Statistics},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AnalyzeVersion {
    V1,
    V2,
}

impl From<i32> for AnalyzeVersion {
    fn from(v: i32) -> Self {
        match v {
            1 => AnalyzeVersion::V1,
            2 => AnalyzeVersion::V2,
            _ => panic!("Unknown analyze version: {}", v),
        }
    }
}

/// Used to handle analyze request.
pub struct AnalyzeContext<S: Snapshot, F: KvFormat> {
    req: AnalyzeReq,
    storage: Option<TikvStorage<SnapshotStore<S>>>,
    ranges: Vec<KeyRange>,
    storage_stats: Statistics,
    quota_limiter: Arc<QuotaLimiter>,
    // is_auto_analyze is used to indicate whether the analyze request is sent by TiDB itself.
    is_auto_analyze: bool,
    _phantom: PhantomData<F>,
}

impl<S: Snapshot, F: KvFormat> AnalyzeContext<S, F> {
    pub fn new(
        req: AnalyzeReq,
        ranges: Vec<KeyRange>,
        start_ts: u64,
        snap: S,
        req_ctx: &ReqContext,
        quota_limiter: Arc<QuotaLimiter>,
    ) -> Result<Self> {
        let store = SnapshotStore::new(
            snap,
            start_ts.into(),
            req_ctx.context.get_isolation_level(),
            !req_ctx.context.get_not_fill_cache(),
            req_ctx.bypass_locks.clone(),
            req_ctx.access_locks.clone(),
            false,
            false,
        );
        let is_auto_analyze = req.get_flags() & REQ_FLAG_TIDB_SYSSESSION > 0;

        Ok(Self {
            req,
            storage: Some(TikvStorage::new(store, false)),
            ranges,
            storage_stats: Statistics::default(),
            quota_limiter,
            is_auto_analyze,
            _phantom: PhantomData,
        })
    }

    // handle_column is used to process `AnalyzeColumnsReq`
    // it would build a histogram for the primary key(if needed) and
    // collectors for each column value.
    async fn handle_column(builder: &mut SampleBuilder<S, F>) -> Result<Vec<u8>> {
        let (col_res, _) = builder.collect_columns_stats().await?;

        let res_data = {
            let res: tipb::AnalyzeColumnsResp = col_res.into();
            box_try!(res.write_to_bytes())
        };
        Ok(res_data)
    }

    // Handle mixed request, it would build histograms for common handle and columns
    // by scan table rows once.
    async fn handle_mixed(builder: &mut SampleBuilder<S, F>) -> Result<Vec<u8>> {
        let (col_res, idx_res) = builder.collect_columns_stats().await?;

        let res_data = {
            let resp: tipb::AnalyzeMixedResp = AnalyzeMixedResult::new(
                col_res,
                idx_res.ok_or_else(|| {
                    Error::Other("Mixed analyze type should have index response.".into())
                })?,
            )
            .into();
            box_try!(resp.write_to_bytes())
        };
        Ok(res_data)
    }

    async fn handle_full_sampling(builder: &mut RowSampleBuilder<S, F>) -> Result<Vec<u8>> {
        let sample_res = builder.collect_column_stats().await?;
        let res_data = {
            let res: tipb::AnalyzeColumnsResp = sample_res.into();
            box_try!(res.write_to_bytes())
        };
        Ok(res_data)
    }

    // handle_index is used to handle `AnalyzeIndexReq`,
    // it would build a histogram and count-min sketch of index values.
    async fn handle_index(
        req: AnalyzeIndexReq,
        scanner: &mut RangesScanner<TikvStorage<SnapshotStore<S>>, F>,
        is_common_handle: bool,
    ) -> Result<Vec<u8>> {
        let mut hist = Histogram::new(req.get_bucket_size() as usize);
        let mut cms = CmSketch::new(
            req.get_cmsketch_depth() as usize,
            req.get_cmsketch_width() as usize,
        );
        let mut fms = FmSketch::new(req.get_sketch_size() as usize);
        let mut topn_heap = BinaryHeap::new();
        // cur_val recording the current value's data and its counts when iterating
        // index's rows. Once we met a new value, the old value will be pushed
        // into the topn_heap to maintain the top-n information.
        let mut cur_val: (u32, Vec<u8>) = (0, vec![]);
        let top_n_size = req.get_top_n_size() as usize;
        let stats_version = if req.has_version() {
            req.get_version().into()
        } else {
            AnalyzeVersion::V1
        };
        while let Some(row) = scanner.next().await? {
            let mut key = row.key();
            if is_common_handle {
                table::check_record_key(key)?;
                key = &key[table::PREFIX_LEN..];
            } else {
                table::check_index_key(key)?;
                key = &key[table::PREFIX_LEN + table::ID_LEN..];
            }
            let mut datums = key;
            let mut data = Vec::with_capacity(key.len());
            for i in 0..req.get_num_columns() as usize {
                if datums.is_empty() {
                    return Err(box_err!(
                        "{}th column is missing in datum buffer: {}",
                        i,
                        log_wrappers::Value::key(key)
                    ));
                }
                let (column, remaining) = split_datum(datums, false)?;
                datums = remaining;
                data.extend_from_slice(column);
                if let Some(cms) = cms.as_mut() {
                    cms.insert(&data);
                }
            }
            fms.insert(&data);
            if stats_version == AnalyzeVersion::V2 {
                hist.append(&data, true);
                if cur_val.1 == data {
                    cur_val.0 += 1;
                } else {
                    if cur_val.0 > 0 {
                        topn_heap.push(Reverse(cur_val));
                    }
                    if topn_heap.len() > top_n_size {
                        topn_heap.pop();
                    }
                    cur_val = (1, data);
                }
            } else {
                hist.append(&data, false);
            }
        }

        if stats_version == AnalyzeVersion::V2 {
            if cur_val.0 > 0 {
                topn_heap.push(Reverse(cur_val));
                if topn_heap.len() > top_n_size {
                    topn_heap.pop();
                }
            }
            if let Some(c) = cms.as_mut() {
                for heap_item in topn_heap {
                    c.sub(&(heap_item.0).1, (heap_item.0).0);
                    c.push_to_top_n((heap_item.0).1, (heap_item.0).0 as u64);
                }
            }
        }

        let res: tipb::AnalyzeIndexResp = AnalyzeIndexResult::new(hist, cms, Some(fms)).into();
        let dt = box_try!(res.write_to_bytes());
        Ok(dt)
    }
}

#[async_trait]
impl<S: Snapshot, F: KvFormat> RequestHandler for AnalyzeContext<S, F> {
    async fn handle_request(&mut self) -> Result<MemoryTraceGuard<Response>> {
        let ret = match self.req.get_tp() {
            AnalyzeType::TypeIndex | AnalyzeType::TypeCommonHandle => {
                let req = self.req.take_idx_req();
                let ranges = std::mem::take(&mut self.ranges);
                table::check_table_ranges::<F>(&ranges)?;
                let mut scanner = RangesScanner::<_, F>::new(RangesScannerOptions {
                    storage: self.storage.take().unwrap(),
                    ranges: ranges
                        .into_iter()
                        .map(|r| Range::from_pb_range(r, false))
                        .collect(),
                    scan_backward_in_range: false,
                    is_key_only: true,
                    is_scanned_range_aware: false,
                });
                let res = AnalyzeContext::handle_index(
                    req,
                    &mut scanner,
                    self.req.get_tp() == AnalyzeType::TypeCommonHandle,
                )
                .await;
                scanner.collect_storage_stats(&mut self.storage_stats);
                res
            }

            AnalyzeType::TypeColumn => {
                let col_req = self.req.take_col_req();
                let storage = self.storage.take().unwrap();
                let ranges = std::mem::take(&mut self.ranges);
                let mut builder = SampleBuilder::<_, F>::new(col_req, None, storage, ranges)?;
                let res = AnalyzeContext::handle_column(&mut builder).await;
                builder.data.collect_storage_stats(&mut self.storage_stats);
                res
            }

            // Type mixed is analyze common handle and columns by scan table rows once.
            AnalyzeType::TypeMixed => {
                let col_req = self.req.take_col_req();
                let idx_req = self.req.take_idx_req();
                let storage = self.storage.take().unwrap();
                let ranges = std::mem::take(&mut self.ranges);
                let mut builder =
                    SampleBuilder::<_, F>::new(col_req, Some(idx_req), storage, ranges)?;
                let res = AnalyzeContext::handle_mixed(&mut builder).await;
                builder.data.collect_storage_stats(&mut self.storage_stats);
                res
            }

            AnalyzeType::TypeFullSampling => {
                let col_req = self.req.take_col_req();
                let storage = self.storage.take().unwrap();
                let ranges = std::mem::take(&mut self.ranges);

                let mut builder = RowSampleBuilder::<_, F>::new(
                    col_req,
                    storage,
                    ranges,
                    self.quota_limiter.clone(),
                    self.is_auto_analyze,
                )?;

                let res = AnalyzeContext::handle_full_sampling(&mut builder).await;
                builder.data.collect_storage_stats(&mut self.storage_stats);
                res
            }

            AnalyzeType::TypeSampleIndex => Err(Error::Other(
                "Analyze of this kind not implemented".to_string(),
            )),
        };
        match ret {
            Ok(data) => {
                let memory_size = data.capacity();
                let mut resp = Response::default();
                resp.set_data(data);
                Ok(MEMTRACE_ANALYZE.trace_guard(resp, memory_size))
            }
            Err(Error::Other(e)) => {
                let mut resp = Response::default();
                resp.set_other_error(e);
                Ok(resp.into())
            }
            Err(e) => Err(e),
        }
    }
}
