// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

use std::{cmp::Reverse, collections::BinaryHeap, marker::PhantomData, sync::Arc};

use api_version::{KvFormat, keyspace::KvPairEntry};
use async_trait::async_trait;
use engine_traits::{CF_WRITE, DATA_KEY_PREFIX_LEN, IterOptions};
use kvproto::coprocessor::{KeyRange, Response};
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
use txn_types::{Key, TimeStamp, WriteRef, WriteType};

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
        let store = SnapshotStore::new(
            snap,
            read_ts,
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
            start_ts: read_ts,
            storage: Some(TikvStorage::new(store, false)),
            ranges,
            storage_stats: Statistics::default(),
            quota_limiter,
            is_auto_analyze,
            _phantom: PhantomData,
        })
    }

    fn point_range_for_raw_key(raw_key: &[u8]) -> KeyRange {
        let mut range = KeyRange::default();
        range.set_start(raw_key.to_vec());
        let mut end = raw_key.to_vec();
        end.push(0);
        range.set_end(end);
        range
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

    fn skip_current_write_key(iter: &mut S::Iter, user_key: &[u8]) -> Option<bool> {
        let skip_target = Key::from_encoded_slice(user_key).append_ts(TimeStamp::zero());
        if !iter.seek(&skip_target).ok()? {
            return Some(false);
        }
        if !iter.valid().ok()? {
            return Some(false);
        }
        let (landed_key, _) = Key::split_on_ts_for(iter.key()).ok()?;
        if landed_key == user_key && !iter.next().ok()? {
            return Some(false);
        }
        Some(true)
    }

    fn sample_visible_row_keys_in_range(
        snapshot: &S,
        range: &KeyRange,
        sample_threshold: u64,
        read_ts: TimeStamp,
        selected: &mut Vec<KeyRange>,
    ) -> Option<(u64, Option<(u64, Vec<u8>)>)> {
        let start = range.get_start();
        let end = range.get_end();
        if !end.is_empty() && start >= end {
            return Some((0, None));
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
        let end_encoded = if end.is_empty() {
            None
        } else {
            Some(Key::from_raw(end).as_encoded().clone())
        };
        let iter_opt = IterOptions::new(Some(lower), upper, false);
        let mut iter = snapshot.iter(CF_WRITE, iter_opt).ok()?;
        if !iter.seek(&Key::from_raw(start)).ok()? {
            return Some((0, None));
        }

        let mut visible_rows = 0_u64;
        let mut min_hash_key = None;
        while iter.valid().ok()? {
            let (user_key, _) = Key::split_on_ts_for(iter.key()).ok()?;
            let user_key = user_key.to_vec();
            if end_encoded
                .as_ref()
                .is_some_and(|end| user_key.as_slice() >= end.as_slice())
            {
                break;
            }

            let mut found_visible_put = false;
            loop {
                let (current_key, commit_ts) = Key::split_on_ts_for(iter.key()).ok()?;
                if current_key != user_key.as_slice() {
                    break;
                }

                if commit_ts <= read_ts {
                    let write = WriteRef::parse(iter.value()).ok()?;
                    match write.write_type {
                        WriteType::Put => {
                            found_visible_put = true;
                            break;
                        }
                        WriteType::Delete => break,
                        WriteType::Lock | WriteType::Rollback => {}
                    }
                }

                if !iter.next().ok()? || !iter.valid().ok()? {
                    break;
                }
            }

            if found_visible_put {
                visible_rows += 1;
                let hash = murmurhash3_x64_128(&user_key, 0).0;
                if min_hash_key
                    .as_ref()
                    .is_none_or(|(min_hash, _)| hash < *min_hash)
                {
                    min_hash_key = Some((hash, user_key.clone()));
                }
                if hash <= sample_threshold {
                    let raw_key = Key::from_encoded_slice(&user_key).into_raw().ok()?;
                    selected.push(Self::point_range_for_raw_key(&raw_key));
                }
            }

            if !Self::skip_current_write_key(&mut iter, &user_key)? {
                break;
            }
        }

        Some((visible_rows, min_hash_key))
    }

    /// Prototype hidden sampling index behavior by selecting live row keys via
    /// a deterministic hash and then scanning those rows as point ranges.
    ///
    /// A real hidden sampling index would maintain keys ordered by
    /// hash(row-key), so analyze could seek directly into hash space. This POC
    /// still scans CF_WRITE once to discover visible row keys, but it uses the
    /// same sampling unit the hidden index would expose: snapshot-visible rows,
    /// not physical SST/range chunks.
    fn pre_select_ranges_via_hash_sampling_index(
        snapshot: &S,
        ranges: &[KeyRange],
        sample_rate: f64,
        read_ts: TimeStamp,
    ) -> Option<(Vec<KeyRange>, f64)> {
        let sample_threshold = Self::hash_sample_threshold(sample_rate)?;
        let mut visible_rows = 0_u64;
        let mut selected = Vec::new();
        let mut min_hash_key = None;
        for range in ranges {
            let (range_visible_rows, range_min_hash_key) = Self::sample_visible_row_keys_in_range(
                snapshot,
                range,
                sample_threshold,
                read_ts,
                &mut selected,
            )?;
            visible_rows += range_visible_rows;
            if let Some((hash, key)) = range_min_hash_key {
                if min_hash_key
                    .as_ref()
                    .is_none_or(|(min_hash, _)| hash < *min_hash)
                {
                    min_hash_key = Some((hash, key));
                }
            }
        }

        if selected.is_empty() {
            let (_, key) = min_hash_key?;
            let raw_key = Key::from_encoded_slice(&key).into_raw().ok()?;
            selected.push(Self::point_range_for_raw_key(&raw_key));
        }

        if visible_rows == 0 {
            return None;
        }
        selected.sort_by(|a, b| a.get_start().cmp(b.get_start()));
        let scale = visible_rows as f64 / selected.len() as f64;
        info!("analyze hidden sampling index prototype";
            "visible_rows" => visible_rows,
            "selected" => selected.len(),
            "sample_rate" => sample_rate,
            "scale" => scale,
            "selection" => "row_key_hash_bernoulli",
            "source" => "cf_write_scan",
        );
        Some((selected, scale))
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
        let storage = self.storage.take().unwrap();
        let ranges = std::mem::take(&mut self.ranges);

        let req_ndv_rate = col_req.get_ndv_rate();
        let row_sample_rate = if req_ndv_rate > 0.0 && req_ndv_rate < 1.0 {
            req_ndv_rate
        } else {
            1.0
        };

        let pre_selected = if row_sample_rate < 1.0 {
            // Emulate a hidden sampling index by selecting point ranges keyed by
            // hash(row-key). Unlike SST range sampling, this makes the sampling
            // unit a row rather than a physical key interval.
            Self::pre_select_ranges_via_hash_sampling_index(
                &self.snapshot,
                &ranges,
                row_sample_rate,
                self.start_ts,
            )
        } else {
            None
        };

        let mut builder = RowSampleBuilder::<_, F>::new_with_preselected_ranges(
            col_req,
            storage,
            ranges,
            self.quota_limiter.clone(),
            self.is_auto_analyze,
            pre_selected,
        )?;

        let res = builder.collect_column_stats().await;
        builder.merge_storage_stats_into(&mut self.storage_stats);
        res
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
