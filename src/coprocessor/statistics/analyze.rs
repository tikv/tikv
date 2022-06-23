// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

use std::{cmp::Reverse, collections::BinaryHeap, mem, sync::Arc};

use async_trait::async_trait;
use kvproto::coprocessor::{KeyRange, Response};
use protobuf::Message;
use rand::{rngs::StdRng, Rng};
use tidb_query_common::storage::{
    scanner::{RangesScanner, RangesScannerOptions},
    Range,
};
use tidb_query_datatype::{
    codec::{
        datum::{
            encode_value, split_datum, Datum, DatumDecoder, DURATION_FLAG, INT_FLAG, NIL_FLAG,
            UINT_FLAG,
        },
        table,
    },
    def::Collation,
    expr::{EvalConfig, EvalContext},
    FieldTypeAccessor,
};
use tidb_query_executors::{
    interface::BatchExecutor, runner::MAX_TIME_SLICE, BatchTableScanExecutor,
};
use tidb_query_expr::BATCH_MAX_SIZE;
use tikv_alloc::trace::{MemoryTraceGuard, TraceEvent};
use tikv_util::{
    metrics::{ThrottleType, NON_TXN_COMMAND_THROTTLE_TIME_COUNTER_VEC_STATIC},
    quota_limiter::QuotaLimiter,
    time::Instant,
};
use tipb::{self, AnalyzeColumnsReq, AnalyzeIndexReq, AnalyzeReq, AnalyzeType};
use yatp::task::future::reschedule;

use super::{cmsketch::CmSketch, fmsketch::FmSketch, histogram::Histogram};
use crate::{
    coprocessor::{dag::TiKvStorage, MEMTRACE_ANALYZE, *},
    storage::{Snapshot, SnapshotStore, Statistics},
};

const ANALYZE_VERSION_V1: i32 = 1;
const ANALYZE_VERSION_V2: i32 = 2;

// `AnalyzeContext` is used to handle `AnalyzeReq`
pub struct AnalyzeContext<S: Snapshot> {
    req: AnalyzeReq,
    storage: Option<TiKvStorage<SnapshotStore<S>>>,
    ranges: Vec<KeyRange>,
    storage_stats: Statistics,
    quota_limiter: Arc<QuotaLimiter>,
    is_auto_analyze: bool,
}

impl<S: Snapshot> AnalyzeContext<S> {
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
        );
        let is_auto_analyze = req.get_flags() & REQ_FLAG_TIDB_SYSSESSION > 0;

        Ok(Self {
            req,
            storage: Some(TiKvStorage::new(store, false)),
            ranges,
            storage_stats: Statistics::default(),
            quota_limiter,
            is_auto_analyze,
        })
    }

    // handle_column is used to process `AnalyzeColumnsReq`
    // it would build a histogram for the primary key(if needed) and
    // collectors for each column value.
    async fn handle_column(builder: &mut SampleBuilder<S>) -> Result<Vec<u8>> {
        let (col_res, _) = builder.collect_columns_stats().await?;

        let res_data = {
            let res = col_res.into_proto();
            box_try!(res.write_to_bytes())
        };
        Ok(res_data)
    }

    async fn handle_mixed(builder: &mut SampleBuilder<S>) -> Result<Vec<u8>> {
        let (col_res, idx_res) = builder.collect_columns_stats().await?;

        let res_data = {
            let resp = AnalyzeMixedResult::new(
                col_res,
                idx_res.ok_or_else(|| {
                    Error::Other("Mixed analyze type should have index response.".into())
                })?,
            )
            .into_proto();
            box_try!(resp.write_to_bytes())
        };
        Ok(res_data)
    }

    async fn handle_full_sampling(builder: &mut RowSampleBuilder<S>) -> Result<Vec<u8>> {
        let sample_res = builder.collect_column_stats().await?;
        let res_data = {
            let res = sample_res.into_proto();
            box_try!(res.write_to_bytes())
        };
        Ok(res_data)
    }

    // handle_index is used to handle `AnalyzeIndexReq`,
    // it would build a histogram and count-min sketch of index values.
    async fn handle_index(
        req: AnalyzeIndexReq,
        scanner: &mut RangesScanner<TiKvStorage<SnapshotStore<S>>>,
        is_common_handle: bool,
    ) -> Result<Vec<u8>> {
        let mut hist = Histogram::new(req.get_bucket_size() as usize);
        let mut cms = CmSketch::new(
            req.get_cmsketch_depth() as usize,
            req.get_cmsketch_width() as usize,
        );
        let mut fms = FmSketch::new(req.get_sketch_size() as usize);
        let mut row_count = 0;
        let mut time_slice_start = Instant::now();
        let mut topn_heap = BinaryHeap::new();
        // cur_val recording the current value's data and its counts when iterating index's rows.
        // Once we met a new value, the old value will be pushed into the topn_heap to maintain the
        // top-n information.
        let mut cur_val: (u32, Vec<u8>) = (0, vec![]);
        let top_n_size = req.get_top_n_size() as usize;
        let stats_version = if req.has_version() {
            req.get_version()
        } else {
            ANALYZE_VERSION_V1
        };
        while let Some((key, _)) = scanner.next()? {
            row_count += 1;
            if row_count >= BATCH_MAX_SIZE {
                if time_slice_start.saturating_elapsed() > MAX_TIME_SLICE {
                    reschedule().await;
                    time_slice_start = Instant::now();
                }
                row_count = 0;
            }
            let mut key = &key[..];
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
            if stats_version == ANALYZE_VERSION_V2 {
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

        if stats_version == ANALYZE_VERSION_V2 {
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

        let res = AnalyzeIndexResult::new(hist, cms, Some(fms)).into_proto();
        let dt = box_try!(res.write_to_bytes());
        Ok(dt)
    }
}

#[async_trait]
impl<S: Snapshot> RequestHandler for AnalyzeContext<S> {
    async fn handle_request(&mut self) -> Result<MemoryTraceGuard<Response>> {
        let ret = match self.req.get_tp() {
            AnalyzeType::TypeIndex | AnalyzeType::TypeCommonHandle => {
                let req = self.req.take_idx_req();
                let ranges = std::mem::take(&mut self.ranges);
                table::check_table_ranges(&ranges)?;
                let mut scanner = RangesScanner::new(RangesScannerOptions {
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
                let mut builder = SampleBuilder::new(col_req, None, storage, ranges)?;
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
                let mut builder = SampleBuilder::new(col_req, Some(idx_req), storage, ranges)?;
                let res = AnalyzeContext::handle_mixed(&mut builder).await;
                builder.data.collect_storage_stats(&mut self.storage_stats);
                res
            }

            AnalyzeType::TypeFullSampling => {
                let col_req = self.req.take_col_req();
                let storage = self.storage.take().unwrap();
                let ranges = std::mem::take(&mut self.ranges);

                let mut builder = RowSampleBuilder::new(
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

    fn collect_scan_statistics(&mut self, dest: &mut Statistics) {
        dest.add(&self.storage_stats);
        self.storage_stats = Statistics::default();
    }
}

struct RowSampleBuilder<S: Snapshot> {
    data: BatchTableScanExecutor<TiKvStorage<SnapshotStore<S>>>,

    max_sample_size: usize,
    max_fm_sketch_size: usize,
    sample_rate: f64,
    columns_info: Vec<tipb::ColumnInfo>,
    column_groups: Vec<tipb::AnalyzeColumnGroup>,
    quota_limiter: Arc<QuotaLimiter>,
    is_quota_auto_tune: bool,
}

impl<S: Snapshot> RowSampleBuilder<S> {
    fn new(
        mut req: AnalyzeColumnsReq,
        storage: TiKvStorage<SnapshotStore<S>>,
        ranges: Vec<KeyRange>,
        quota_limiter: Arc<QuotaLimiter>,
        is_quota_auto_tune: bool,
    ) -> Result<Self> {
        let columns_info: Vec<_> = req.take_columns_info().into();
        if columns_info.is_empty() {
            return Err(box_err!("empty columns_info"));
        }
        let common_handle_ids = req.take_primary_column_ids();
        let table_scanner = BatchTableScanExecutor::new(
            storage,
            Arc::new(EvalConfig::default()),
            columns_info.clone(),
            ranges,
            common_handle_ids,
            false,
            false, // Streaming mode is not supported in Analyze request, always false here
            req.take_primary_prefix_column_ids(),
        )?;
        Ok(Self {
            data: table_scanner,
            max_sample_size: req.get_sample_size() as usize,
            max_fm_sketch_size: req.get_sketch_size() as usize,
            sample_rate: req.get_sample_rate(),
            columns_info,
            column_groups: req.take_column_groups().into(),
            quota_limiter,
            is_quota_auto_tune,
        })
    }

    fn new_collector(&mut self) -> Box<dyn RowSampleCollector> {
        if self.max_sample_size > 0 {
            return Box::new(ReservoirRowSampleCollector::new(
                self.max_sample_size,
                self.max_fm_sketch_size,
                self.columns_info.len() + self.column_groups.len(),
            ));
        }
        Box::new(BernoulliRowSampleCollector::new(
            self.sample_rate,
            self.max_fm_sketch_size,
            self.columns_info.len() + self.column_groups.len(),
        ))
    }

    async fn collect_column_stats(&mut self) -> Result<AnalyzeSamplingResult> {
        use tidb_query_datatype::{codec::collation::Collator, match_template_collator};

        let mut is_drained = false;
        let mut time_slice_start = Instant::now();
        let mut collector = self.new_collector();
        while !is_drained {
            let time_slice_elapsed = time_slice_start.saturating_elapsed();
            if time_slice_elapsed > MAX_TIME_SLICE {
                reschedule().await;
                time_slice_start = Instant::now();
            }

            let mut sample = self.quota_limiter.new_sample();
            {
                let _guard = sample.observe_cpu();
                let result = self.data.next_batch(BATCH_MAX_SIZE);
                is_drained = result.is_drained?;

                let columns_slice = result.physical_columns.as_slice();

                for logical_row in &result.logical_rows {
                    let mut column_vals: Vec<Vec<u8>> = Vec::new();
                    let mut collation_key_vals: Vec<Vec<u8>> = Vec::new();
                    for i in 0..self.columns_info.len() {
                        let mut val = vec![];
                        columns_slice[i].encode(
                            *logical_row,
                            &self.columns_info[i],
                            &mut EvalContext::default(),
                            &mut val,
                        )?;
                        if self.columns_info[i].as_accessor().is_string_like() {
                            let sorted_val = match_template_collator! {
                                TT, match self.columns_info[i].as_accessor().collation()? {
                                    Collation::TT => {
                                        let mut mut_val = &val[..];
                                        let decoded_val = table::decode_col_value(&mut mut_val, &mut EvalContext::default(), &self.columns_info[i])?;
                                        if decoded_val == Datum::Null {
                                            val.clone()
                                        } else {
                                            // Only if the `decoded_val` is Datum::Null, `decoded_val` is a Ok(None).
                                            // So it is safe the unwrap the Ok value.
                                            let decoded_sorted_val = TT::sort_key(&decoded_val.as_string()?.unwrap().into_owned())?;
                                            decoded_sorted_val
                                        }
                                    }
                                }
                            };
                            collation_key_vals.push(sorted_val);
                        } else {
                            collation_key_vals.push(Vec::new());
                        }
                        column_vals.push(val);
                    }
                    collector.mut_base().count += 1;
                    collector.collect_column_group(
                        &column_vals,
                        &collation_key_vals,
                        &self.columns_info,
                        &self.column_groups,
                    );
                    collector.collect_column(column_vals, collation_key_vals, &self.columns_info);
                }
            }

            // Don't let analyze bandwidth limit the quota limiter, this is already limited in rate limiter.
            let quota_delay = {
                if !self.is_quota_auto_tune {
                    self.quota_limiter.consume_sample(sample, true).await
                } else {
                    self.quota_limiter.consume_sample(sample, false).await
                }
            };

            if !quota_delay.is_zero() {
                NON_TXN_COMMAND_THROTTLE_TIME_COUNTER_VEC_STATIC
                    .get(ThrottleType::analyze_full_sampling)
                    .inc_by(quota_delay.as_micros() as u64);
            }
        }
        Ok(AnalyzeSamplingResult::new(collector))
    }
}

trait RowSampleCollector: Send {
    fn mut_base(&mut self) -> &mut BaseRowSampleCollector;
    fn collect_column_group(
        &mut self,
        columns_val: &[Vec<u8>],
        collation_keys_val: &[Vec<u8>],
        columns_info: &[tipb::ColumnInfo],
        column_groups: &[tipb::AnalyzeColumnGroup],
    );
    fn collect_column(
        &mut self,
        columns_val: Vec<Vec<u8>>,
        collation_keys_val: Vec<Vec<u8>>,
        columns_info: &[tipb::ColumnInfo],
    );
    fn sampling(&mut self, data: Vec<Vec<u8>>);
    fn to_proto(&mut self) -> tipb::RowSampleCollector;
    fn get_reported_memory_usage(&mut self) -> usize {
        self.mut_base().reported_memory_usage
    }
    fn get_memory_usage(&mut self) -> usize {
        self.mut_base().memory_usage
    }
}

#[derive(Clone)]
struct BaseRowSampleCollector {
    null_count: Vec<i64>,
    count: u64,
    fm_sketches: Vec<FmSketch>,
    rng: StdRng,
    total_sizes: Vec<i64>,
    row_buf: Vec<u8>,
    memory_usage: usize,
    reported_memory_usage: usize,
}

impl Default for BaseRowSampleCollector {
    fn default() -> Self {
        BaseRowSampleCollector {
            null_count: vec![],
            count: 0,
            fm_sketches: vec![],
            rng: StdRng::from_entropy(),
            total_sizes: vec![],
            row_buf: Vec::new(),
            memory_usage: 0,
            reported_memory_usage: 0,
        }
    }
}

impl BaseRowSampleCollector {
    fn new(max_fm_sketch_size: usize, col_and_group_len: usize) -> BaseRowSampleCollector {
        BaseRowSampleCollector {
            null_count: vec![0; col_and_group_len],
            count: 0,
            fm_sketches: vec![FmSketch::new(max_fm_sketch_size); col_and_group_len],
            rng: StdRng::from_entropy(),
            total_sizes: vec![0; col_and_group_len],
            row_buf: Vec::new(),
            memory_usage: 0,
            reported_memory_usage: 0,
        }
    }
    pub fn collect_column_group(
        &mut self,
        columns_val: &[Vec<u8>],
        collation_keys_val: &[Vec<u8>],
        columns_info: &[tipb::ColumnInfo],
        column_groups: &[tipb::AnalyzeColumnGroup],
    ) {
        let col_len = columns_val.len();
        for i in 0..column_groups.len() {
            self.row_buf.clear();
            let offsets = column_groups[i].get_column_offsets();
            let mut has_null = true;
            for j in offsets {
                if columns_val[*j as usize][0] == NIL_FLAG {
                    continue;
                }
                has_null = false;
                self.total_sizes[col_len + i] += columns_val[*j as usize].len() as i64 - 1
            }
            // We only maintain the null count for single column case.
            if has_null && offsets.len() == 1 {
                self.null_count[col_len + i] += 1;
                continue;
            }
            // Use a in place murmur3 to replace this memory copy.
            for j in offsets {
                if columns_info[*j as usize].as_accessor().is_string_like() {
                    self.row_buf
                        .extend_from_slice(&collation_keys_val[*j as usize]);
                } else {
                    self.row_buf.extend_from_slice(&columns_val[*j as usize]);
                }
            }
            self.fm_sketches[col_len + i].insert(&self.row_buf);
        }
    }

    pub fn collect_column(
        &mut self,
        columns_val: &[Vec<u8>],
        collation_keys_val: Vec<Vec<u8>>,
        columns_info: &[tipb::ColumnInfo],
    ) {
        for i in 0..columns_val.len() {
            if columns_val[i][0] == NIL_FLAG {
                self.null_count[i] += 1;
                continue;
            }
            if columns_info[i].as_accessor().is_string_like() {
                self.fm_sketches[i].insert(&collation_keys_val[i]);
            } else {
                self.fm_sketches[i].insert(&columns_val[i]);
            }
            self.total_sizes[i] += columns_val[i].len() as i64 - 1;
        }
    }

    pub fn fill_proto(&mut self, proto_collector: &mut tipb::RowSampleCollector) {
        proto_collector.set_null_counts(self.null_count.clone());
        proto_collector.set_count(self.count as i64);
        let pb_fm_sketches = mem::take(&mut self.fm_sketches)
            .into_iter()
            .map(|fm_sketch| fm_sketch.into_proto())
            .collect();
        proto_collector.set_fm_sketch(pb_fm_sketches);
        proto_collector.set_total_size(self.total_sizes.clone());
    }

    fn report_memory_usage(&mut self, on_finish: bool) {
        let diff = self.memory_usage as isize - self.reported_memory_usage as isize;
        if on_finish || diff.abs() > 1024 * 1024 {
            let event = if diff >= 0 {
                TraceEvent::Add(diff as usize)
            } else {
                TraceEvent::Sub(-diff as usize)
            };
            MEMTRACE_ANALYZE.trace(event);
            self.reported_memory_usage = self.memory_usage;
        }
    }
}

#[derive(Clone)]
struct BernoulliRowSampleCollector {
    base: BaseRowSampleCollector,
    samples: Vec<Vec<Vec<u8>>>,
    sample_rate: f64,
}

impl BernoulliRowSampleCollector {
    fn new(
        sample_rate: f64,
        max_fm_sketch_size: usize,
        col_and_group_len: usize,
    ) -> BernoulliRowSampleCollector {
        BernoulliRowSampleCollector {
            base: BaseRowSampleCollector::new(max_fm_sketch_size, col_and_group_len),
            samples: Vec::new(),
            sample_rate,
        }
    }
}

impl Default for BernoulliRowSampleCollector {
    fn default() -> Self {
        BernoulliRowSampleCollector {
            base: Default::default(),
            samples: Vec::new(),
            sample_rate: 0.0,
        }
    }
}

impl RowSampleCollector for BernoulliRowSampleCollector {
    fn mut_base(&mut self) -> &mut BaseRowSampleCollector {
        &mut self.base
    }
    fn collect_column_group(
        &mut self,
        columns_val: &[Vec<u8>],
        collation_keys_val: &[Vec<u8>],
        columns_info: &[tipb::ColumnInfo],
        column_groups: &[tipb::AnalyzeColumnGroup],
    ) {
        self.base.collect_column_group(
            columns_val,
            collation_keys_val,
            columns_info,
            column_groups,
        );
    }
    fn collect_column(
        &mut self,
        columns_val: Vec<Vec<u8>>,
        collation_keys_val: Vec<Vec<u8>>,
        columns_info: &[tipb::ColumnInfo],
    ) {
        self.base
            .collect_column(&columns_val, collation_keys_val, columns_info);
        self.sampling(columns_val);
    }
    fn sampling(&mut self, data: Vec<Vec<u8>>) {
        let cur_rng = self.base.rng.gen_range(0.0, 1.0);
        if cur_rng >= self.sample_rate {
            return;
        }
        self.base.memory_usage += data.iter().map(|x| x.capacity()).sum::<usize>();
        self.base.report_memory_usage(false);
        self.samples.push(data);
    }
    fn to_proto(&mut self) -> tipb::RowSampleCollector {
        self.base.memory_usage = 0;
        self.base.report_memory_usage(true);
        let mut s = tipb::RowSampleCollector::default();
        let samples = mem::take(&mut self.samples)
            .into_iter()
            .map(|row| {
                let mut pb_sample = tipb::RowSample::default();
                pb_sample.set_row(row.into());
                pb_sample
            })
            .collect();
        s.set_samples(samples);
        self.base.fill_proto(&mut s);
        s
    }
}

#[derive(Clone, Default)]
struct ReservoirRowSampleCollector {
    base: BaseRowSampleCollector,
    samples: BinaryHeap<Reverse<(i64, Vec<Vec<u8>>)>>,
    max_sample_size: usize,
}

impl ReservoirRowSampleCollector {
    fn new(
        max_sample_size: usize,
        max_fm_sketch_size: usize,
        col_and_group_len: usize,
    ) -> ReservoirRowSampleCollector {
        ReservoirRowSampleCollector {
            base: BaseRowSampleCollector::new(max_fm_sketch_size, col_and_group_len),
            samples: BinaryHeap::new(),
            max_sample_size,
        }
    }
}

impl RowSampleCollector for ReservoirRowSampleCollector {
    fn mut_base(&mut self) -> &mut BaseRowSampleCollector {
        &mut self.base
    }
    fn collect_column_group(
        &mut self,
        columns_val: &[Vec<u8>],
        collation_keys_val: &[Vec<u8>],
        columns_info: &[tipb::ColumnInfo],
        column_groups: &[tipb::AnalyzeColumnGroup],
    ) {
        self.base.collect_column_group(
            columns_val,
            collation_keys_val,
            columns_info,
            column_groups,
        );
    }

    fn collect_column(
        &mut self,
        columns_val: Vec<Vec<u8>>,
        collation_keys_val: Vec<Vec<u8>>,
        columns_info: &[tipb::ColumnInfo],
    ) {
        self.base
            .collect_column(&columns_val, collation_keys_val, columns_info);
        self.sampling(columns_val);
    }

    fn sampling(&mut self, data: Vec<Vec<u8>>) {
        let mut need_push = false;
        let cur_rng = self.base.rng.gen_range(0, i64::MAX);
        if self.samples.len() < self.max_sample_size {
            need_push = true;
        } else if self.samples.peek().unwrap().0.0 < cur_rng {
            need_push = true;
            let (_, evicted) = self.samples.pop().unwrap().0;
            self.base.memory_usage -= evicted.iter().map(|x| x.capacity()).sum::<usize>();
        }

        if need_push {
            self.base.memory_usage += data.iter().map(|x| x.capacity()).sum::<usize>();
            self.samples.push(Reverse((cur_rng, data)));
            self.base.report_memory_usage(false);
        }
    }

    fn to_proto(&mut self) -> tipb::RowSampleCollector {
        self.base.memory_usage = 0;
        self.base.report_memory_usage(true);
        let mut s = tipb::RowSampleCollector::default();
        let samples = mem::take(&mut self.samples)
            .into_iter()
            .map(|r_tuple| {
                let mut pb_sample = tipb::RowSample::default();
                pb_sample.set_row(r_tuple.0.1.into());
                pb_sample.set_weight(r_tuple.0.0);
                pb_sample
            })
            .collect();
        s.set_samples(samples);
        self.base.fill_proto(&mut s);
        s
    }
}

impl Drop for BaseRowSampleCollector {
    fn drop(&mut self) {
        self.memory_usage = 0;
        self.report_memory_usage(true);
    }
}

struct SampleBuilder<S: Snapshot> {
    data: BatchTableScanExecutor<TiKvStorage<SnapshotStore<S>>>,

    max_bucket_size: usize,
    max_sample_size: usize,
    max_fm_sketch_size: usize,
    cm_sketch_depth: usize,
    cm_sketch_width: usize,
    stats_version: i32,
    top_n_size: usize,
    columns_info: Vec<tipb::ColumnInfo>,
    analyze_common_handle: bool,
    common_handle_col_ids: Vec<i64>,
}

/// `SampleBuilder` is used to analyze columns. It collects sample from
/// the result set using Reservoir Sampling algorithm, estimates NDVs
/// using FM Sketch during the collecting process, and builds count-min sketch.
impl<S: Snapshot> SampleBuilder<S> {
    fn new(
        mut req: AnalyzeColumnsReq,
        common_handle_req: Option<tipb::AnalyzeIndexReq>,
        storage: TiKvStorage<SnapshotStore<S>>,
        ranges: Vec<KeyRange>,
    ) -> Result<Self> {
        let columns_info: Vec<_> = req.take_columns_info().into();
        if columns_info.is_empty() {
            return Err(box_err!("empty columns_info"));
        }
        let common_handle_ids = req.take_primary_column_ids();
        let table_scanner = BatchTableScanExecutor::new(
            storage,
            Arc::new(EvalConfig::default()),
            columns_info.clone(),
            ranges,
            common_handle_ids.clone(),
            false,
            false, // Streaming mode is not supported in Analyze request, always false here
            req.take_primary_prefix_column_ids(),
        )?;
        Ok(Self {
            data: table_scanner,
            max_bucket_size: req.get_bucket_size() as usize,
            max_fm_sketch_size: req.get_sketch_size() as usize,
            max_sample_size: req.get_sample_size() as usize,
            cm_sketch_depth: req.get_cmsketch_depth() as usize,
            cm_sketch_width: req.get_cmsketch_width() as usize,
            stats_version: common_handle_req.as_ref().map_or_else(
                || ANALYZE_VERSION_V1,
                |req| match req.has_version() {
                    true => req.get_version(),
                    _ => ANALYZE_VERSION_V1,
                },
            ),
            top_n_size: common_handle_req
                .as_ref()
                .map_or_else(|| 0_usize, |req| req.get_top_n_size() as usize),
            common_handle_col_ids: common_handle_ids,
            columns_info,
            analyze_common_handle: common_handle_req != None,
        })
    }

    // `collect_columns_stats` returns the sample collectors which contain total count,
    // null count, distinct values count and count-min sketch. And it also returns the statistic
    // builder for PK which contains the histogram. When PK is common handle, it returns index stats
    // for PK.
    // See https://en.wikipedia.org/wiki/Reservoir_sampling
    async fn collect_columns_stats(
        &mut self,
    ) -> Result<(AnalyzeColumnsResult, Option<AnalyzeIndexResult>)> {
        use tidb_query_datatype::{codec::collation::Collator, match_template_collator};
        let columns_without_handle_len =
            self.columns_info.len() - self.columns_info[0].get_pk_handle() as usize;

        // The number of columns need to be sampled is `columns_without_handle_len`.
        // It equals to `columns_info.len()` if the first column doesn't contain a handle.
        // Otherwise, it equals to `columns_info.len() - 1`.
        let mut pk_builder = Histogram::new(self.max_bucket_size);
        let mut collectors = vec![
            SampleCollector::new(
                self.max_sample_size,
                self.max_fm_sketch_size,
                self.cm_sketch_depth,
                self.cm_sketch_width,
            );
            columns_without_handle_len
        ];
        let mut is_drained = false;
        let mut time_slice_start = Instant::now();
        let mut common_handle_hist = Histogram::new(self.max_bucket_size);
        let mut common_handle_cms = CmSketch::new(self.cm_sketch_depth, self.cm_sketch_width);
        let mut common_handle_fms = FmSketch::new(self.max_fm_sketch_size);
        while !is_drained {
            let time_slice_elapsed = time_slice_start.saturating_elapsed();
            if time_slice_elapsed > MAX_TIME_SLICE {
                reschedule().await;
                time_slice_start = Instant::now();
            }
            let result = self.data.next_batch(BATCH_MAX_SIZE);
            is_drained = result.is_drained?;

            let mut columns_slice = result.physical_columns.as_slice();
            let mut columns_info = &self.columns_info[..];
            if columns_without_handle_len + 1 == columns_slice.len() {
                for logical_row in &result.logical_rows {
                    let mut data = vec![];
                    columns_slice[0].encode(
                        *logical_row,
                        &columns_info[0],
                        &mut EvalContext::default(),
                        &mut data,
                    )?;
                    pk_builder.append(&data, false);
                }
                columns_slice = &columns_slice[1..];
                columns_info = &columns_info[1..];
            }

            if self.analyze_common_handle {
                // cur_val recording the current value's data and its counts when iterating index's rows.
                // Once we met a new value, the old value will be pushed into the topn_heap to maintain the
                // top-n information.
                let mut cur_val: (u32, Vec<u8>) = (0, vec![]);
                let mut topn_heap = BinaryHeap::new();
                for logical_row in &result.logical_rows {
                    let mut data = vec![];
                    for i in 0..self.common_handle_col_ids.len() {
                        let mut handle_col_val = vec![];
                        columns_slice[i].encode(
                            *logical_row,
                            &columns_info[i],
                            &mut EvalContext::default(),
                            &mut handle_col_val,
                        )?;
                        data.extend_from_slice(&handle_col_val);
                        if let Some(common_handle_cms) = common_handle_cms.as_mut() {
                            common_handle_cms.insert(&data);
                        }
                    }
                    common_handle_fms.insert(&data);
                    if self.stats_version == ANALYZE_VERSION_V2 {
                        common_handle_hist.append(&data, true);
                        if cur_val.1 == data {
                            cur_val.0 += 1;
                        } else {
                            if cur_val.0 > 0 {
                                topn_heap.push(Reverse(cur_val));
                            }
                            if topn_heap.len() > self.top_n_size {
                                topn_heap.pop();
                            }
                            cur_val = (1, data);
                        }
                    } else {
                        common_handle_hist.append(&data, false)
                    }
                }
                if self.stats_version == ANALYZE_VERSION_V2 {
                    if cur_val.0 > 0 {
                        topn_heap.push(Reverse(cur_val));
                        if topn_heap.len() > self.top_n_size {
                            topn_heap.pop();
                        }
                    }
                    if let Some(c) = common_handle_cms.as_mut() {
                        for heap_item in topn_heap {
                            c.sub(&(heap_item.0).1, (heap_item.0).0);
                            c.push_to_top_n((heap_item.0).1, (heap_item.0).0 as u64);
                        }
                    }
                }
            }

            for (i, collector) in collectors.iter_mut().enumerate() {
                for logical_row in &result.logical_rows {
                    let mut val = vec![];
                    columns_slice[i].encode(
                        *logical_row,
                        &columns_info[i],
                        &mut EvalContext::default(),
                        &mut val,
                    )?;

                    // This is a workaround for different encoding methods used by TiDB and TiKV for CM Sketch.
                    // We need this because we must ensure we are using the same encoding method when we are querying values from
                    //   CM Sketch (in TiDB) and inserting values into CM Sketch (here).
                    // We are inserting raw bytes from TableScanExecutor into CM Sketch here and query CM Sketch using bytes
                    //   encoded by tablecodec.EncodeValue() in TiDB. Their results are different after row format becomes ver 2.
                    //
                    // Here we (1) convert INT bytes to VAR_INT bytes, (2) convert UINT bytes to VAR_UINT bytes,
                    //   and (3) "flatten" the duration value from DURATION bytes into i64 value, then convert it to VAR_INT bytes.
                    // These are the only 3 cases we need to care about according to TiDB's tablecodec.EncodeValue() and
                    //   TiKV's V1CompatibleEncoder::write_v2_as_datum().
                    val = match val[0] {
                        INT_FLAG | UINT_FLAG | DURATION_FLAG => {
                            let mut mut_val = &val[..];
                            let decoded_val = mut_val.read_datum()?;
                            let flattened =
                                table::flatten(&mut EvalContext::default(), decoded_val)?;
                            encode_value(&mut EvalContext::default(), &[flattened])?
                        }
                        _ => val,
                    };

                    if columns_info[i].as_accessor().is_string_like() {
                        let sorted_val = match_template_collator! {
                            TT, match columns_info[i].as_accessor().collation()? {
                                Collation::TT => {
                                    let mut mut_val = &val[..];
                                    let decoded_val = table::decode_col_value(&mut mut_val, &mut EvalContext::default(), &columns_info[i])?;
                                    if decoded_val == Datum::Null {
                                        val
                                    } else {
                                        // Only if the `decoded_val` is Datum::Null, `decoded_val` is a Ok(None).
                                        // So it is safe the unwrap the Ok value.
                                        let decoded_sorted_val = TT::sort_key(&decoded_val.as_string()?.unwrap().into_owned())?;
                                        encode_value(&mut EvalContext::default(), &[Datum::Bytes(decoded_sorted_val)])?
                                    }
                                }
                            }
                        };
                        collector.collect(sorted_val);
                        continue;
                    }
                    collector.collect(val);
                }
            }
        }
        let idx_res = if self.analyze_common_handle {
            Some(AnalyzeIndexResult::new(
                common_handle_hist,
                common_handle_cms,
                Some(common_handle_fms),
            ))
        } else {
            None
        };
        Ok((AnalyzeColumnsResult::new(collectors, pk_builder), idx_res))
    }
}

/// `SampleCollector` will collect Samples and calculate the count, ndv and total size of an attribute.
#[derive(Clone)]
struct SampleCollector {
    samples: Vec<Vec<u8>>,
    null_count: u64,
    count: u64,
    max_sample_size: usize,
    fm_sketch: FmSketch,
    cm_sketch: Option<CmSketch>,
    rng: StdRng,
    total_size: u64,
}

impl SampleCollector {
    fn new(
        max_sample_size: usize,
        max_fm_sketch_size: usize,
        cm_sketch_depth: usize,
        cm_sketch_width: usize,
    ) -> SampleCollector {
        SampleCollector {
            samples: Default::default(),
            null_count: 0,
            count: 0,
            max_sample_size,
            fm_sketch: FmSketch::new(max_fm_sketch_size),
            cm_sketch: CmSketch::new(cm_sketch_depth, cm_sketch_width),
            rng: StdRng::from_entropy(),
            total_size: 0,
        }
    }

    fn into_proto(self) -> tipb::SampleCollector {
        let mut s = tipb::SampleCollector::default();
        s.set_null_count(self.null_count as i64);
        s.set_count(self.count as i64);
        s.set_fm_sketch(self.fm_sketch.into_proto());
        s.set_samples(self.samples.into());
        if let Some(c) = self.cm_sketch {
            s.set_cm_sketch(c.into_proto())
        }
        s.set_total_size(self.total_size as i64);
        s
    }

    pub fn collect(&mut self, data: Vec<u8>) {
        if data[0] == NIL_FLAG {
            self.null_count += 1;
            return;
        }
        self.count += 1;
        self.fm_sketch.insert(&data);
        if let Some(c) = self.cm_sketch.as_mut() {
            c.insert(&data);
        }
        self.total_size += data.len() as u64 - 1;
        if self.samples.len() < self.max_sample_size {
            self.samples.push(data);
            return;
        }
        if self.rng.gen_range(0, self.count) < self.max_sample_size as u64 {
            let idx = self.rng.gen_range(0, self.max_sample_size);
            // https://github.com/pingcap/tidb/blob/master/statistics/sample.go#L173
            self.samples.remove(idx);
            self.samples.push(data);
        }
    }
}

struct AnalyzeSamplingResult {
    row_sample_collector: Box<dyn RowSampleCollector>,
}

impl AnalyzeSamplingResult {
    fn new(row_sample_collector: Box<dyn RowSampleCollector>) -> AnalyzeSamplingResult {
        AnalyzeSamplingResult {
            row_sample_collector,
        }
    }

    fn into_proto(mut self) -> tipb::AnalyzeColumnsResp {
        let pb_collector = self.row_sample_collector.to_proto();
        let mut res = tipb::AnalyzeColumnsResp::default();
        res.set_row_collector(pb_collector);
        res
    }
}

impl Default for AnalyzeSamplingResult {
    fn default() -> Self {
        AnalyzeSamplingResult::new(Box::new(ReservoirRowSampleCollector::default()))
    }
}

/// `AnalyzeColumnsResult` collect the result of analyze columns request.
#[derive(Default)]
struct AnalyzeColumnsResult {
    sample_collectors: Vec<SampleCollector>,
    pk_hist: Histogram,
}

impl AnalyzeColumnsResult {
    fn new(sample_collectors: Vec<SampleCollector>, pk_hist: Histogram) -> AnalyzeColumnsResult {
        AnalyzeColumnsResult {
            sample_collectors,
            pk_hist,
        }
    }

    fn into_proto(self) -> tipb::AnalyzeColumnsResp {
        let hist = self.pk_hist.into_proto();
        let cols: Vec<tipb::SampleCollector> = self
            .sample_collectors
            .into_iter()
            .map(|col| col.into_proto())
            .collect();
        let mut res = tipb::AnalyzeColumnsResp::default();
        res.set_collectors(cols.into());
        res.set_pk_hist(hist);
        res
    }
}

/// `AnalyzeIndexResult` collect the result of analyze index request.
#[derive(Default)]
struct AnalyzeIndexResult {
    hist: Histogram,
    cms: Option<CmSketch>,
    fms: Option<FmSketch>,
}

impl AnalyzeIndexResult {
    fn new(hist: Histogram, cms: Option<CmSketch>, fms: Option<FmSketch>) -> AnalyzeIndexResult {
        AnalyzeIndexResult { hist, cms, fms }
    }

    fn into_proto(self) -> tipb::AnalyzeIndexResp {
        let mut res = tipb::AnalyzeIndexResp::default();
        res.set_hist(self.hist.into_proto());
        if let Some(c) = self.cms {
            res.set_cms(c.into_proto());
        }
        if let Some(f) = self.fms {
            let mut s = tipb::SampleCollector::default();
            s.set_fm_sketch(f.into_proto());
            res.set_collector(s);
        }
        res
    }
}

/// `AnalyzeMixedResult` collect the result of analyze mixed request.
#[derive(Default)]
struct AnalyzeMixedResult {
    col_res: AnalyzeColumnsResult,
    idx_res: AnalyzeIndexResult,
}

impl AnalyzeMixedResult {
    fn new(col_res: AnalyzeColumnsResult, idx_res: AnalyzeIndexResult) -> AnalyzeMixedResult {
        AnalyzeMixedResult { col_res, idx_res }
    }

    fn into_proto(self) -> tipb::AnalyzeMixedResp {
        let mut res = tipb::AnalyzeMixedResp::default();
        res.set_index_resp(self.idx_res.into_proto());
        res.set_columns_resp(self.col_res.into_proto());
        res
    }
}

#[cfg(test)]
mod tests {
    use ::std::collections::HashMap;
    use tidb_query_datatype::codec::{datum, datum::Datum};

    use super::*;

    #[test]
    fn test_sample_collector() {
        let max_sample_size = 3;
        let max_fm_sketch_size = 10;
        let cm_sketch_depth = 2;
        let cm_sketch_width = 16;
        let mut sample = SampleCollector::new(
            max_sample_size,
            max_fm_sketch_size,
            cm_sketch_depth,
            cm_sketch_width,
        );
        let cases = vec![Datum::I64(1), Datum::Null, Datum::I64(2), Datum::I64(5)];

        for data in cases {
            sample.collect(datum::encode_value(&mut EvalContext::default(), &[data]).unwrap());
        }
        assert_eq!(sample.samples.len(), max_sample_size);
        assert_eq!(sample.null_count, 1);
        assert_eq!(sample.count, 3);
        assert_eq!(sample.cm_sketch.unwrap().count(), 3);
        assert_eq!(sample.total_size, 3)
    }

    #[test]
    fn test_row_reservoir_sample_collector() {
        let sample_num = 20;
        let row_num = 100;
        let loop_cnt = 1000;
        let mut item_cnt: HashMap<Vec<u8>, usize> = HashMap::new();
        let mut nums: Vec<Vec<u8>> = Vec::with_capacity(row_num);
        for i in 0..row_num {
            nums.push(
                datum::encode_value(&mut EvalContext::default(), &[Datum::I64(i as i64)]).unwrap(),
            );
        }
        for loop_i in 0..loop_cnt {
            let mut collector = ReservoirRowSampleCollector::new(sample_num, 1000, 1);
            for row in &nums {
                collector.sampling([row.clone()].to_vec());
            }
            assert_eq!(collector.samples.len(), sample_num);
            for sample in &collector.samples {
                *item_cnt.entry(sample.0.1[0].clone()).or_insert(0) += 1;
            }

            // Test memory usage tracing is correct.
            collector.mut_base().report_memory_usage(true);
            assert_eq!(
                collector.get_reported_memory_usage(),
                collector.get_memory_usage()
            );
            if loop_i % 2 == 0 {
                collector.to_proto();
                assert_eq!(collector.get_memory_usage(), 0);
                assert_eq!(MEMTRACE_ANALYZE.sum(), 0);
            }
            drop(collector);
            assert_eq!(MEMTRACE_ANALYZE.sum(), 0);
        }

        let exp_freq = sample_num as f64 * loop_cnt as f64 / row_num as f64;
        let delta = 0.5;
        for (_, v) in item_cnt.into_iter() {
            assert!(
                v as f64 >= exp_freq / (1.0 + delta) && v as f64 <= exp_freq * (1.0 + delta),
                "v: {}",
                v
            );
        }
    }

    #[test]
    fn test_row_bernoulli_sample_collector() {
        let sample_num = 20;
        let row_num = 100;
        let loop_cnt = 1000;
        let mut item_cnt: HashMap<Vec<u8>, usize> = HashMap::new();
        let mut nums: Vec<Vec<u8>> = Vec::with_capacity(row_num);
        for i in 0..row_num {
            nums.push(
                datum::encode_value(&mut EvalContext::default(), &[Datum::I64(i as i64)]).unwrap(),
            );
        }
        for loop_i in 0..loop_cnt {
            let mut collector =
                BernoulliRowSampleCollector::new(sample_num as f64 / row_num as f64, 1000, 1);
            for row in &nums {
                collector.sampling([row.clone()].to_vec());
            }
            for sample in &collector.samples {
                *item_cnt.entry(sample[0].clone()).or_insert(0) += 1;
            }

            // Test memory usage tracing is correct.
            collector.mut_base().report_memory_usage(true);
            assert_eq!(
                collector.get_reported_memory_usage(),
                collector.get_memory_usage()
            );
            if loop_i % 2 == 0 {
                collector.to_proto();
                assert_eq!(collector.get_memory_usage(), 0);
                assert_eq!(MEMTRACE_ANALYZE.sum(), 0);
            }
            drop(collector);
            assert_eq!(MEMTRACE_ANALYZE.sum(), 0);
        }

        let exp_freq = sample_num as f64 * loop_cnt as f64 / row_num as f64;
        let delta = 0.5;
        for (_, v) in item_cnt.into_iter() {
            assert!(
                v as f64 >= exp_freq / (1.0 + delta) && v as f64 <= exp_freq * (1.0 + delta),
                "v: {}",
                v
            );
        }
    }
}
