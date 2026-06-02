// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

use std::{cmp::Reverse, collections::BinaryHeap, marker::PhantomData, sync::Arc};

use api_version::{KvFormat, keyspace::KvPairEntry};
use async_trait::async_trait;
use engine_traits::{CF_WRITE, DATA_KEY_PREFIX_LEN, IterOptions};
use kvproto::{
    coprocessor::{KeyRange, Response},
    kvrpcpb::IsolationLevel,
};
use mur3::murmurhash3_x64_128;
use protobuf::Message;
use tidb_query_common::storage::{
    Range,
    scanner::{RangesScanner, RangesScannerOptions},
};
use tidb_query_datatype::codec::{datum::split_datum, table};
use tidb_query_executors::interface::BatchExecutor;
use tikv_alloc::trace::MemoryTraceGuard;
use tikv_util::{keybuilder::KeyBuilder, quota_limiter::QuotaLimiter};
use tipb::{self, AnalyzeIndexReq, AnalyzeReq, AnalyzeType};
use txn_types::{Key, TimeStamp, TsSet, WriteRef, WriteType};

use super::{cmsketch::CmSketch, fmsketch::FmSketch, histogram::Histogram};
use crate::{
    coprocessor::{
        MEMTRACE_ANALYZE,
        dag::TikvStorage,
        statistics::analyze::{
            AnalyzeIndexResult, AnalyzeMixedResult, PointRowSampleBuilder, RowSampleBuilder,
            SampleBuilder, effective_bernoulli_sample_rate_after_preselection,
        },
        *,
    },
    storage::{Iterator as _, Snapshot, SnapshotStore, Statistics},
};

const HASH_SAMPLE_RAW_KEY_BATCH_SIZE: usize = 8192;

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
    // Kept alongside `storage` so TypeFullSampling can build row-key samples
    // against the same MVCC snapshot before scanning selected point ranges.
    snapshot: S,
    // Read timestamp used by the row-key sampler to ignore writes a snapshot read
    // would not see.
    start_ts: TimeStamp,
    isolation_level: IsolationLevel,
    fill_cache: bool,
    bypass_locks: TsSet,
    access_locks: TsSet,
    storage: Option<TikvStorage<SnapshotStore<S>>>,
    ranges: Vec<KeyRange>,
    storage_stats: Statistics,
    quota_limiter: Arc<QuotaLimiter>,
    // is_auto_analyze is used to indicate whether the analyze request is sent by TiDB itself.
    is_auto_analyze: bool,
    // Keeps AnalyzeContext tied to the engine and key format types used by the
    // coprocessor endpoint.
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
        let read_ts: TimeStamp = start_ts.into();
        let isolation_level = req_ctx.context.get_isolation_level();
        let fill_cache = !req_ctx.context.get_not_fill_cache();
        let bypass_locks = req_ctx.bypass_locks.clone();
        let access_locks = req_ctx.access_locks.clone();
        let store = SnapshotStore::new(
            snap,
            read_ts,
            isolation_level,
            fill_cache,
            bypass_locks.clone(),
            access_locks.clone(),
            false,
        );
        let is_auto_analyze = req.get_flags() & REQ_FLAG_TIDB_SYSSESSION > 0;

        Ok(Self {
            req,
            snapshot,
            start_ts: read_ts,
            isolation_level,
            fill_cache,
            bypass_locks,
            access_locks,
            storage: Some(TikvStorage::new(store, false)),
            ranges,
            storage_stats: Statistics::default(),
            quota_limiter,
            is_auto_analyze,
            _phantom: PhantomData,
        })
    }

    fn build_tikv_storage(&self) -> TikvStorage<SnapshotStore<S>> {
        let store = SnapshotStore::new(
            self.snapshot.clone(),
            self.start_ts,
            self.isolation_level,
            self.fill_cache,
            self.bypass_locks.clone(),
            self.access_locks.clone(),
            false,
        );
        TikvStorage::new(store, false)
    }

    fn hash_sample_threshold(sample_rate: f64) -> Option<u64> {
        if sample_rate <= 0.0 {
            return None;
        }
        if sample_rate >= 1.0 {
            return Some(u64::MAX);
        }
        Some((sample_rate * u64::MAX as f64).round() as u64)
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

    async fn handle_full_sampling_result(&mut self) -> Result<AnalyzeSamplingResult> {
        if self.req.get_tp() != AnalyzeType::TypeFullSampling {
            return Err(Error::Other(
                "typed analyze result is only supported for full sampling".to_owned(),
            ));
        }
        let col_req = self.req.take_col_req();
        let ranges = std::mem::take(&mut self.ranges);

        let req_ndv_rate = col_req.get_ndv_rate();
        let row_sample_rate = if req_ndv_rate > 0.0 && req_ndv_rate < 1.0 {
            req_ndv_rate
        } else {
            1.0
        };

        if row_sample_rate < 1.0 {
            return self
                .handle_hash_sampled_full_sampling_result(col_req, ranges, row_sample_rate)
                .await;
        }

        let storage = self.storage.take().unwrap();
        let mut builder = RowSampleBuilder::<_, F>::new_with_preselected_ranges(
            col_req,
            storage,
            ranges,
            self.quota_limiter.clone(),
            self.is_auto_analyze,
            None,
        )?;

        let res = builder.collect_column_stats().await;
        builder.merge_storage_stats_into(&mut self.storage_stats);
        res
    }

    async fn handle_hash_sampled_full_sampling_result(
        &mut self,
        mut col_req: tipb::AnalyzeColumnsReq,
        ranges: Vec<KeyRange>,
        row_sample_rate: f64,
    ) -> Result<AnalyzeSamplingResult> {
        let sample_threshold = Self::hash_sample_threshold(row_sample_rate).ok_or_else(|| {
            Error::Other(format!(
                "invalid analyze NDV sample rate: {}",
                row_sample_rate
            ))
        })?;
        if col_req.get_sample_size() == 0 {
            let sample_rate = effective_bernoulli_sample_rate_after_preselection(
                col_req.get_sample_rate(),
                1.0 / row_sample_rate,
            );
            col_req.set_sample_rate(sample_rate);
        }

        let mut sampler =
            HashSamplingRangeBatcher::new(&self.snapshot, &ranges, sample_threshold, self.start_ts);
        let mut builder = PointRowSampleBuilder::<_, F>::new(
            col_req,
            self.build_tikv_storage(),
            self.quota_limiter.clone(),
            self.is_auto_analyze,
        )?;
        let mut range_batch_count = 0_u64;
        let mut max_raw_key_batch_size = 0_usize;
        while let Some(raw_keys) = sampler.next_batch(HASH_SAMPLE_RAW_KEY_BATCH_SIZE)? {
            max_raw_key_batch_size = max_raw_key_batch_size.max(raw_keys.len());
            range_batch_count += 1;
            builder.collect_raw_key_batch(raw_keys).await?;
        }

        let visible_rows = sampler.visible_rows();
        let selected_rows = sampler.selected_rows();
        builder.rescale_pre_sampled_rows(visible_rows, selected_rows);
        builder.merge_storage_stats_into(&mut self.storage_stats);
        let scale = if selected_rows > 0 {
            visible_rows as f64 / selected_rows as f64
        } else {
            1.0
        };
        info!("analyze hidden sampling index prototype";
            "visible_rows" => visible_rows,
            "selected" => selected_rows,
            "sample_rate" => row_sample_rate,
            "scale" => scale,
            "raw_key_batches" => range_batch_count,
            "max_raw_key_batch_size" => max_raw_key_batch_size,
            "selection" => "row_key_hash_bernoulli",
            "source" => "cf_write_scan",
            "scan" => "streaming_point_get",
        );
        Ok(builder.into_result())
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

struct HashSamplingRangeBatcher<'a, S: Snapshot> {
    snapshot: &'a S,
    ranges: &'a [KeyRange],
    range_index: usize,
    iter: Option<S::Iter>,
    end_encoded: Option<Vec<u8>>,
    sample_threshold: u64,
    read_ts: TimeStamp,
    visible_rows: u64,
    selected_rows: u64,
    min_hash_key: Option<(u64, Vec<u8>)>,
    fallback_emitted: bool,
}

impl<'a, S: Snapshot> HashSamplingRangeBatcher<'a, S> {
    fn new(
        snapshot: &'a S,
        ranges: &'a [KeyRange],
        sample_threshold: u64,
        read_ts: TimeStamp,
    ) -> Self {
        Self {
            snapshot,
            ranges,
            range_index: 0,
            iter: None,
            end_encoded: None,
            sample_threshold,
            read_ts,
            visible_rows: 0,
            selected_rows: 0,
            min_hash_key: None,
            fallback_emitted: false,
        }
    }

    fn visible_rows(&self) -> u64 {
        self.visible_rows
    }

    fn selected_rows(&self) -> u64 {
        self.selected_rows
    }

    fn next_batch(&mut self, limit: usize) -> Result<Option<Vec<Vec<u8>>>> {
        assert!(limit > 0);
        let mut raw_keys = Vec::with_capacity(limit);
        while raw_keys.len() < limit {
            if self.iter.is_none() && !self.open_next_range()? {
                break;
            }
            if !self.consume_current_write_key(&mut raw_keys)? {
                self.iter = None;
                self.end_encoded = None;
            }
        }
        if !raw_keys.is_empty() {
            return Ok(Some(raw_keys));
        }
        if self.iter.is_none()
            && self.range_index >= self.ranges.len()
            && self.visible_rows > 0
            && self.selected_rows == 0
            && !self.fallback_emitted
        {
            self.fallback_emitted = true;
            if let Some((_, key)) = self.min_hash_key.take() {
                let raw_key = Key::from_encoded_slice(&key)
                    .into_raw()
                    .map_err(|err| Error::Other(err.to_string()))?;
                self.selected_rows = 1;
                return Ok(Some(vec![raw_key]));
            }
        }
        Ok(None)
    }

    fn open_next_range(&mut self) -> Result<bool> {
        while self.range_index < self.ranges.len() {
            let range = &self.ranges[self.range_index];
            self.range_index += 1;
            let start = range.get_start();
            let end = range.get_end();
            if !end.is_empty() && start >= end {
                continue;
            }

            let lower = KeyBuilder::from_vec(
                Key::from_raw(start).as_encoded().clone(),
                DATA_KEY_PREFIX_LEN,
                0,
            );
            let upper = if end.is_empty() {
                None
            } else {
                Some(KeyBuilder::from_vec(
                    Key::from_raw(end).as_encoded().clone(),
                    DATA_KEY_PREFIX_LEN,
                    0,
                ))
            };
            let iter_opt = IterOptions::new(Some(lower), upper, false);
            let mut iter = self
                .snapshot
                .iter(CF_WRITE, iter_opt)
                .map_err(|err| Error::Other(err.to_string()))?;
            if iter
                .seek(&Key::from_raw(start))
                .map_err(|err| Error::Other(err.to_string()))?
            {
                self.end_encoded = if end.is_empty() {
                    None
                } else {
                    Some(Key::from_raw(end).as_encoded().clone())
                };
                self.iter = Some(iter);
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn consume_current_write_key(&mut self, raw_keys: &mut Vec<Vec<u8>>) -> Result<bool> {
        let iter = self
            .iter
            .as_mut()
            .expect("hash sampling iterator must be opened");
        if !iter.valid().map_err(|err| Error::Other(err.to_string()))? {
            return Ok(false);
        }
        let (user_key, _) =
            Key::split_on_ts_for(iter.key()).map_err(|err| Error::Other(err.to_string()))?;
        let user_key = user_key.to_vec();
        if self
            .end_encoded
            .as_ref()
            .is_some_and(|end| user_key.as_slice() >= end.as_slice())
        {
            return Ok(false);
        }

        let mut found_visible_put = false;
        loop {
            let (current_key, commit_ts) =
                Key::split_on_ts_for(iter.key()).map_err(|err| Error::Other(err.to_string()))?;
            if current_key != user_key.as_slice() {
                break;
            }
            if commit_ts <= self.read_ts {
                let write =
                    WriteRef::parse(iter.value()).map_err(|err| Error::Other(err.to_string()))?;
                match write.write_type {
                    WriteType::Put => {
                        found_visible_put = true;
                        break;
                    }
                    WriteType::Delete => break,
                    WriteType::Lock | WriteType::Rollback => {}
                }
            }
            if !iter.next().map_err(|err| Error::Other(err.to_string()))?
                || !iter.valid().map_err(|err| Error::Other(err.to_string()))?
            {
                break;
            }
        }

        if found_visible_put {
            self.visible_rows += 1;
            let hash = murmurhash3_x64_128(&user_key, 0).0;
            if self
                .min_hash_key
                .as_ref()
                .is_none_or(|(min_hash, _)| hash < *min_hash)
            {
                self.min_hash_key = Some((hash, user_key.clone()));
            }
            if hash <= self.sample_threshold {
                let raw_key = Key::from_encoded_slice(&user_key)
                    .into_raw()
                    .map_err(|err| Error::Other(err.to_string()))?;
                raw_keys.push(raw_key);
                self.selected_rows += 1;
            }
        }

        skip_current_write_key(iter, &user_key)
    }
}

fn skip_current_write_key<I: crate::storage::Iterator>(
    iter: &mut I,
    user_key: &[u8],
) -> Result<bool> {
    let skip_target = Key::from_encoded_slice(user_key).append_ts(TimeStamp::zero());
    if !iter
        .seek(&skip_target)
        .map_err(|err| Error::Other(err.to_string()))?
    {
        return Ok(false);
    }
    if !iter.valid().map_err(|err| Error::Other(err.to_string()))? {
        return Ok(false);
    }
    let (landed_key, _) =
        Key::split_on_ts_for(iter.key()).map_err(|err| Error::Other(err.to_string()))?;
    if landed_key == user_key && !iter.next().map_err(|err| Error::Other(err.to_string()))? {
        return Ok(false);
    }
    Ok(true)
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
                let res = Self::handle_index(
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
                let res = Self::handle_column(&mut builder).await;
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
                let res = Self::handle_mixed(&mut builder).await;
                builder.data.collect_storage_stats(&mut self.storage_stats);
                res
            }

            AnalyzeType::TypeFullSampling => {
                let res = self.handle_full_sampling_result().await;
                res.and_then(AnalyzeSamplingResult::write_to_bytes)
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

    async fn handle_analyze_full_sampling(&mut self) -> Result<AnalyzeSamplingResult> {
        self.handle_full_sampling_result().await
    }

    fn collect_scan_statistics(&mut self, dest: &mut Statistics) {
        dest.add(&self.storage_stats);
        self.storage_stats = Statistics::default();
    }
}
