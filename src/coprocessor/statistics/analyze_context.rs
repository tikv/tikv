// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

use std::{cmp::Reverse, collections::BinaryHeap, marker::PhantomData, sync::Arc};

use api_version::{KvFormat, keyspace::KvPairEntry};
use async_trait::async_trait;
use engine_traits::{CF_WRITE, DATA_KEY_PREFIX_LEN, IterOptions};
use kvproto::coprocessor::{KeyRange, Response};
use protobuf::Message;
use tidb_query_common::storage::{
    Range,
    scanner::{RangesScanner, RangesScannerOptions},
};
use tidb_query_datatype::codec::{datum::split_datum, table};
use tidb_query_executors::interface::BatchExecutor;
use tikv_alloc::trace::MemoryTraceGuard;
use tikv_util::quota_limiter::QuotaLimiter;
use tipb::{self, AnalyzeIndexReq, AnalyzeReq, AnalyzeType};

use super::{cmsketch::CmSketch, fmsketch::FmSketch, histogram::Histogram};
use crate::{
    coprocessor::{
        MEMTRACE_ANALYZE,
        dag::TikvStorage,
        statistics::analyze::{
            AnalyzeIndexResult, AnalyzeMixedResult, RowSampleBuilder, SampleBuilder,
        },
        *,
    },
    storage::{Iterator as _, Snapshot, SnapshotStore, Statistics},
};

const SST_RANGE_SAMPLE_SPLIT_COUNT: usize = 200;

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
pub struct AnalyzeContext<E: crate::storage::Engine, S: Snapshot, F: KvFormat> {
    req: AnalyzeReq,
    snapshot: S,
    storage: Option<TikvStorage<SnapshotStore<S>>>,
    ranges: Vec<KeyRange>,
    storage_stats: Statistics,
    quota_limiter: Arc<QuotaLimiter>,
    // is_auto_analyze is used to indicate whether the analyze request is sent by TiDB itself.
    is_auto_analyze: bool,
    _phantom: PhantomData<(E, F)>,
}

impl<E: crate::storage::Engine, S: Snapshot, F: KvFormat> AnalyzeContext<E, S, F> {
    pub fn new(
        req: AnalyzeReq,
        ranges: Vec<KeyRange>,
        start_ts: u64,
        snap: S,
        req_ctx: &ReqContext,
        quota_limiter: Arc<QuotaLimiter>,
    ) -> Result<Self> {
        let snapshot = snap.clone();
        let store = SnapshotStore::new(
            snap,
            start_ts.into(),
            req_ctx.context.get_isolation_level(),
            !req_ctx.context.get_not_fill_cache(),
            req_ctx.bypass_locks.clone(),
            req_ctx.access_locks.clone(),
            false,
        );
        let is_auto_analyze = req.get_flags() & REQ_FLAG_TIDB_SYSSESSION > 0;

        Ok(Self {
            req,
            snapshot,
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
    // NOTE: Mixed requests are only sent when the statistics version is set to 1.
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
impl<E: crate::storage::Engine, S: Snapshot, F: KvFormat> RequestHandler
    for AnalyzeContext<E, S, F>
{
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
                    load_commit_ts: false,
                });
                let res = AnalyzeContext::<E, S, F>::handle_index(
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
                let res = AnalyzeContext::<E, S, F>::handle_column(&mut builder).await;
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
                let res = AnalyzeContext::<E, S, F>::handle_mixed(&mut builder).await;
                builder.data.collect_storage_stats(&mut self.storage_stats);
                res
            }

            AnalyzeType::TypeFullSampling => {
                let col_req = self.req.take_col_req();
                let storage = self.storage.take().unwrap();
                let ranges = std::mem::take(&mut self.ranges);

                let req_ndv_rate = col_req.get_ndv_rate();
                let range_sample_rate = if req_ndv_rate > 0.0 && req_ndv_rate < 1.0 {
                    req_ndv_rate
                } else {
                    1.0
                };

                // SST seek-probe range sampling is driven by NDVRATE. Keep the split
                // count as a code constant so this path has one clear tuning surface.
                let pre_selected: Option<(Vec<KeyRange>, f64)> = if range_sample_rate < 1.0 {
                    let start = ranges.first().map(|r| r.get_start().to_vec());
                    let end = ranges.last().map(|r| r.get_end().to_vec());
                    if let (Some(s), Some(e)) = (start, end) {
                        let split_keys =
                            crate::coprocessor::statistics::analyze::interpolate_split_keys(
                                &s,
                                &e,
                                SST_RANGE_SAMPLE_SPLIT_COUNT,
                            );
                        if split_keys.is_empty() {
                            None
                        } else {
                            let mut boundaries = vec![s.clone()];
                            boundaries.extend(split_keys.iter().cloned());
                            boundaries.push(e.clone());

                            let encoded_bounds: Vec<Vec<u8>> = boundaries
                                .iter()
                                .map(|b| txn_types::Key::from_raw(b).into_encoded())
                                .collect();
                            let mut iter_opts = IterOptions::default();
                            iter_opts.set_vec_upper_bound(
                                encoded_bounds[encoded_bounds.len() - 1].clone(),
                                DATA_KEY_PREFIX_LEN,
                            );
                            if let Ok(mut iter) = self.snapshot.iter(CF_WRITE, iter_opts) {
                                let mut non_empty: Vec<KeyRange> = Vec::new();
                                for i in 0..boundaries.len() - 1 {
                                    let valid = iter.seek(&txn_types::Key::from_encoded(
                                        encoded_bounds[i].clone(),
                                    ));
                                    if matches!(valid, Ok(true))
                                        && iter.key() < encoded_bounds[i + 1].as_slice()
                                    {
                                        let mut kr = KeyRange::default();
                                        kr.set_start(boundaries[i].clone());
                                        kr.set_end(boundaries[i + 1].clone());
                                        non_empty.push(kr);
                                    }
                                }

                                let total = boundaries.len() - 1;
                                let non_empty_count = non_empty.len();
                                if non_empty_count == 0 {
                                    None
                                } else {
                                    let select_count = std::cmp::max(
                                        1,
                                        (non_empty_count as f64 * range_sample_rate).round()
                                            as usize,
                                    );
                                    let scale = non_empty_count as f64 / select_count as f64;
                                    use rand::seq::SliceRandom;
                                    let mut rng = rand::thread_rng();
                                    let mut indices: Vec<usize> = (0..non_empty_count).collect();
                                    indices.shuffle(&mut rng);
                                    indices.truncate(select_count);
                                    indices.sort();
                                    let selected: Vec<KeyRange> = indices
                                        .into_iter()
                                        .map(|idx| non_empty[idx].clone())
                                        .collect();
                                    info!("analyze seek-probe";
                                        "total_sub" => total,
                                        "non_empty" => non_empty_count,
                                        "selected" => selected.len(),
                                        "range_sample_rate" => range_sample_rate,
                                        "scale" => scale,
                                    );
                                    Some((selected, scale))
                                }
                            } else {
                                None
                            }
                        }
                    } else {
                        None
                    }
                } else {
                    None
                };

                let mut builder = RowSampleBuilder::<_, F>::new_with_range_sampling(
                    col_req,
                    storage,
                    ranges,
                    self.quota_limiter.clone(),
                    self.is_auto_analyze,
                    pre_selected,
                )?;

                let res = AnalyzeContext::<E, S, F>::handle_full_sampling(&mut builder).await;
                builder.merge_storage_stats_into(&mut self.storage_stats);
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

    fn collect_scan_statistics(&mut self, dest: &mut Statistics) {
        dest.add(&self.storage_stats);
        self.storage_stats = Statistics::default();
    }
}
