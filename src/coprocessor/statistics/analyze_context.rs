// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

use std::{cmp::Reverse, collections::BinaryHeap, marker::PhantomData, sync::Arc};

use api_version::{KvFormat, keyspace::KvPairEntry};
use async_trait::async_trait;
use engine_traits::{
    CF_WRITE, DATA_KEY_PREFIX_LEN, IterOptions, Range as EngineRange, RangePropertiesExt,
};
use kvproto::coprocessor::{KeyRange, Response};
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
    storage::{Iterator as _, Snapshot, SnapshotStore, Statistics, kv::with_tls_engine},
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
    // Kept alongside `storage` so TypeFullSampling can run the seek-probe
    // range sampler against the same MVCC snapshot.
    snapshot: S,
    // Read timestamp used by the seek-probe to ignore writes a snapshot read
    // would not see.
    start_ts: TimeStamp,
    storage: Option<TikvStorage<SnapshotStore<S>>>,
    ranges: Vec<KeyRange>,
    storage_stats: Statistics,
    quota_limiter: Arc<QuotaLimiter>,
    // is_auto_analyze is used to indicate whether the analyze request is sent by TiDB itself.
    is_auto_analyze: bool,
    // `E` is only used to reach the thread-local KvEngine for SST
    // RangeProperties when building density-aware sample sub-ranges.
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

    /// Build split keys from CF_WRITE SST RangeProperties.
    ///
    /// This reads SST metadata only. If properties are unavailable, the caller
    /// falls back to key-space interpolation. The returned keys are raw user
    /// keys suitable for KeyRange boundaries.
    fn range_properties_split_keys(start_raw: &[u8], end_raw: &[u8], count: usize) -> Vec<Vec<u8>> {
        let lower = keys::data_key(Key::from_raw(start_raw).as_encoded());
        let upper = keys::data_key(Key::from_raw(end_raw).as_encoded());

        // The coprocessor read pool installs a thread-local engine for request
        // handlers. The actual row probe below still uses the safe snapshot
        // iterator; this is only for SST metadata.
        let data_keys = unsafe {
            with_tls_engine::<E, _, _>(|engine| {
                engine.kv_engine().and_then(|db| {
                    db.get_range_approximate_split_keys_cf(
                        CF_WRITE,
                        EngineRange::new(&lower, &upper),
                        count,
                    )
                    .ok()
                })
            })
        };
        let Some(data_keys) = data_keys else {
            return Vec::new();
        };

        let mut raw = Vec::with_capacity(data_keys.len());
        for data_key in &data_keys {
            if !keys::validate_data_key(data_key) {
                continue;
            }
            let encoded = keys::origin_key(data_key);
            let Ok(no_ts) = Key::from_encoded_slice(encoded).truncate_ts() else {
                continue;
            };
            let Ok(user_key) = no_ts.into_raw() else {
                continue;
            };
            if user_key.as_slice() > start_raw && user_key.as_slice() < end_raw {
                raw.push(user_key);
            }
        }
        raw.dedup();
        raw
    }

    /// Split the request range into `SST_RANGE_SAMPLE_SPLIT_COUNT` sub-ranges,
    /// drop the ones with no live row visible at `read_ts`, then randomly
    /// sample `range_sample_rate` of the rest. Returns the selected sub-ranges
    /// plus a scale factor that compensates for the down-sampling. Any
    /// iterator failure falls back to `None`, which signals the caller to scan
    /// the full range.
    ///
    /// A sub-range is "non-empty" iff at least one user key's latest visible
    /// version at `read_ts` is a `Put`. CF_WRITE versions are iterated
    /// newest-first within each user key, so once a visible `Delete` is seen
    /// for a user key, older versions of that key are shadowed and ignored.
    /// `Lock` and `Rollback` entries do not determine row state, so the probe
    /// keeps walking.
    ///
    /// `gc_fence` and overlapping-rollback subtleties are intentionally
    /// ignored: a fenced stale `Put` can still mark the sub-range
    /// non-empty. That is conservative because it may over-include a
    /// sub-range, but it should not under-include live rows.
    fn pre_select_ranges_via_seek_probe(
        snapshot: &S,
        ranges: &[KeyRange],
        range_sample_rate: f64,
        read_ts: TimeStamp,
    ) -> Option<(Vec<KeyRange>, f64)> {
        let start = ranges.first().map(|r| r.get_start().to_vec())?;
        let end = ranges.last().map(|r| r.get_end().to_vec())?;

        let mut split_source = "range_properties";
        let mut split_keys =
            Self::range_properties_split_keys(&start, &end, SST_RANGE_SAMPLE_SPLIT_COUNT);
        if split_keys.is_empty() {
            split_source = "interpolate";
            split_keys = crate::coprocessor::statistics::analyze::interpolate_split_keys(
                &start,
                &end,
                SST_RANGE_SAMPLE_SPLIT_COUNT,
            );
        }
        if split_keys.is_empty() {
            return None;
        }

        let mut boundaries = vec![start];
        boundaries.extend(split_keys);
        boundaries.push(end);

        let bound_keys: Vec<Key> = boundaries.iter().map(|b| Key::from_raw(b)).collect();
        let lower =
            KeyBuilder::from_vec(bound_keys[0].as_encoded().clone(), DATA_KEY_PREFIX_LEN, 0);
        let upper = KeyBuilder::from_vec(
            bound_keys[bound_keys.len() - 1].as_encoded().clone(),
            DATA_KEY_PREFIX_LEN,
            0,
        );
        // Cannot use key-only mode: the probe needs each Write record's first
        // byte to distinguish Put from Delete/Lock/Rollback.
        let iter_opt = IterOptions::new(Some(lower), Some(upper), false);
        let mut iter = snapshot.iter(CF_WRITE, iter_opt).ok()?;

        let mut current_user_key = Vec::new();
        let mut non_empty = Vec::new();
        for i in 0..bound_keys.len() - 1 {
            let upper_encoded = bound_keys[i + 1].as_encoded().as_slice();
            if !iter.seek(&bound_keys[i]).ok()? {
                continue;
            }

            current_user_key.clear();
            let mut current_user_key_dead = false;
            let mut found_live_put = false;

            while iter.valid().ok()? && iter.key() < upper_encoded {
                let (user_key, commit_ts) = Key::split_on_ts_for(iter.key()).ok()?;
                if user_key != current_user_key.as_slice() {
                    current_user_key.clear();
                    current_user_key.extend_from_slice(user_key);
                    current_user_key_dead = false;
                }

                if !current_user_key_dead && commit_ts <= read_ts {
                    let write = WriteRef::parse(iter.value()).ok()?;
                    match write.write_type {
                        WriteType::Put => {
                            found_live_put = true;
                            break;
                        }
                        WriteType::Delete => {
                            current_user_key_dead = true;
                            // Seek past every older version of this user key.
                            // `append_ts(0)` builds the largest suffix for the
                            // current key, so the seek lands at the oldest
                            // version of this key or directly at the next key.
                            let skip_target = Key::from_encoded_slice(&current_user_key)
                                .append_ts(TimeStamp::zero());
                            if !iter.seek(&skip_target).ok()? {
                                break;
                            }
                            if iter.valid().ok()? {
                                let (landed_key, _) = Key::split_on_ts_for(iter.key()).ok()?;
                                if landed_key == current_user_key.as_slice() && !iter.next().ok()? {
                                    break;
                                }
                            }
                            continue;
                        }
                        WriteType::Lock | WriteType::Rollback => {}
                    }
                }

                if !iter.next().ok()? {
                    break;
                }
            }

            if found_live_put {
                let mut kr = KeyRange::default();
                kr.set_start(boundaries[i].clone());
                kr.set_end(boundaries[i + 1].clone());
                non_empty.push(kr);
            }
        }

        let total = boundaries.len() - 1;
        let non_empty_count = non_empty.len();
        if non_empty_count == 0 {
            return None;
        }
        let select_count = std::cmp::max(
            1,
            (non_empty_count as f64 * range_sample_rate).round() as usize,
        );
        let scale = non_empty_count as f64 / select_count as f64;
        use rand::seq::SliceRandom;
        let mut rng = rand::thread_rng();
        let mut indices: Vec<usize> = (0..non_empty_count).collect();
        indices.shuffle(&mut rng);
        indices.truncate(select_count);
        indices.sort();
        let selected = indices
            .into_iter()
            .map(|idx| non_empty[idx].clone())
            .collect::<Vec<_>>();
        info!("analyze seek-probe";
            "split_source" => split_source,
            "total_sub" => total,
            "non_empty" => non_empty_count,
            "selected" => selected.len(),
            "range_sample_rate" => range_sample_rate,
            "scale" => scale,
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
                    Self::pre_select_ranges_via_seek_probe(
                        &self.snapshot,
                        &ranges,
                        range_sample_rate,
                        self.start_ts,
                    )
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

                let res = Self::handle_full_sampling(&mut builder).await;
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
