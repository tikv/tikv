// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

use std::{cmp::Reverse, collections::BinaryHeap, hash::Hasher, mem, sync::Arc};

use api_version::KvFormat;
use kvproto::coprocessor::KeyRange;
use mur3::Hasher128;
use rand::{rngs::StdRng, Rng};
use tidb_query_datatype::{
    codec::{
        datum::{encode_value, Datum, DatumDecoder, DURATION_FLAG, INT_FLAG, NIL_FLAG, UINT_FLAG},
        table,
    },
    def::Collation,
    expr::{EvalConfig, EvalContext},
    FieldTypeAccessor,
};
use tidb_query_executors::{interface::BatchExecutor, BatchTableScanExecutor};
use tidb_query_expr::BATCH_MAX_SIZE;
use tikv_alloc::trace::TraceEvent;
use tikv_util::{
    metrics::{ThrottleType, NON_TXN_COMMAND_THROTTLE_TIME_COUNTER_VEC_STATIC},
    quota_limiter::QuotaLimiter,
};
use tipb::{self, AnalyzeColumnsReq};

use super::{cmsketch::CmSketch, fmsketch::FmSketch, histogram::Histogram};
use crate::{
    coprocessor::{
        dag::TikvStorage, statistics::analyze_context::AnalyzeVersion, MEMTRACE_ANALYZE, *,
    },
    storage::{Snapshot, SnapshotStore},
};

pub(crate) struct RowSampleBuilder<S: Snapshot, F: KvFormat> {
    pub(crate) data: BatchTableScanExecutor<TikvStorage<SnapshotStore<S>>, F>,

    max_sample_size: usize,
    max_fm_sketch_size: usize,
    sample_rate: f64,
    columns_info: Vec<tipb::ColumnInfo>,
    column_groups: Vec<tipb::AnalyzeColumnGroup>,
    quota_limiter: Arc<QuotaLimiter>,
    is_auto_analyze: bool,
}

impl<S: Snapshot, F: KvFormat> RowSampleBuilder<S, F> {
    pub(crate) fn new(
        mut req: AnalyzeColumnsReq,
        storage: TikvStorage<SnapshotStore<S>>,
        ranges: Vec<KeyRange>,
        quota_limiter: Arc<QuotaLimiter>,
        is_auto_analyze: bool,
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
            is_auto_analyze,
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

    pub(crate) async fn collect_column_stats(&mut self) -> Result<AnalyzeSamplingResult> {
        use tidb_query_datatype::{codec::collation::Collator, match_template_collator};

        let mut is_drained = false;
        let mut collector = self.new_collector();
        let mut ctx = EvalContext::default();
        while !is_drained {
            let mut sample = self.quota_limiter.new_sample(!self.is_auto_analyze);
            let mut read_size: usize = 0;
            {
                let result = {
                    let (duration, res) = sample
                        .observe_cpu_async(self.data.next_batch(BATCH_MAX_SIZE))
                        .await;
                    sample.add_cpu_time(duration);
                    res
                };
                let _guard = sample.observe_cpu();
                is_drained = result.is_drained?.stop();

                let columns_slice = result.physical_columns.as_slice();
                let mut column_vals: Vec<Vec<u8>> = vec![vec![]; self.columns_info.len()];
                let mut collation_key_vals: Vec<Vec<u8>> = vec![vec![]; self.columns_info.len()];
                for logical_row in &result.logical_rows {
                    for i in 0..self.columns_info.len() {
                        column_vals[i].clear();
                        collation_key_vals[i].clear();
                        columns_slice[i].encode(
                            *logical_row,
                            &self.columns_info[i],
                            &mut ctx,
                            &mut column_vals[i],
                        )?;
                        if self.columns_info[i].as_accessor().is_string_like() {
                            match_template_collator! {
                                TT, match self.columns_info[i].as_accessor().collation()? {
                                    Collation::TT => {
                                        let mut mut_val = &column_vals[i][..];
                                        let decoded_val = table::decode_col_value(&mut mut_val, &mut ctx, &self.columns_info[i])?;
                                        if decoded_val == Datum::Null {
                                            collation_key_vals[i].clone_from(&column_vals[i]);
                                        } else {
                                            // Only if the `decoded_val` is Datum::Null, `decoded_val` is a Ok(None).
                                            // So it is safe the unwrap the Ok value.
                                            TT::write_sort_key(&mut collation_key_vals[i], &decoded_val.as_string()?.unwrap())?;
                                        }
                                    }
                                }
                            };
                        }
                        read_size += column_vals[i].len();
                    }
                    collector.mut_base().count += 1;
                    collector.collect_column_group(
                        &column_vals,
                        &collation_key_vals,
                        &self.columns_info,
                        &self.column_groups,
                    );
                    collector.collect_column(&column_vals, &collation_key_vals, &self.columns_info);
                }
            }

            sample.add_read_bytes(read_size);
            // Don't let analyze bandwidth limit the quota limiter, this is already limited
            // in rate limiter.
            let quota_delay = {
                if !self.is_auto_analyze {
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
        for i in 0..self.column_groups.len() {
            let offsets = self.column_groups[i].get_column_offsets();
            if offsets.len() != 1 {
                continue;
            }
            // For the single-column group, its fm_sketch is the same as that of the
            // corresponding column. Hence, we don't maintain its fm_sketch in
            // collect_column_group. We just copy the corresponding column's fm_sketch after
            // iterating all rows. Also, we can directly copy total_size and null_count.
            let col_pos = offsets[0] as usize;
            let col_group_pos = self.columns_info.len() + i;
            collector.mut_base().fm_sketches[col_group_pos] =
                collector.mut_base().fm_sketches[col_pos].clone();
            collector.mut_base().null_count[col_group_pos] =
                collector.mut_base().null_count[col_pos];
            collector.mut_base().total_sizes[col_group_pos] =
                collector.mut_base().total_sizes[col_pos];
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
        columns_val: &[Vec<u8>],
        collation_keys_val: &[Vec<u8>],
        columns_info: &[tipb::ColumnInfo],
    );
    fn sampling(&mut self, data: &[Vec<u8>]);
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
            let offsets = column_groups[i].get_column_offsets();
            if offsets.len() == 1 {
                // For the single-column group, its fm_sketch is the same as that of the
                // corresponding column. Hence, we don't need to maintain its
                // fm_sketch. We just copy the corresponding column's fm_sketch after iterating
                // all rows. Also, we can directly copy total_size and null_count.
                continue;
            }
            // We don't maintain the null count information for the multi-column group.
            for j in offsets {
                if columns_val[*j as usize][0] == NIL_FLAG {
                    continue;
                }
                self.total_sizes[col_len + i] += columns_val[*j as usize].len() as i64 - 1
            }
            let mut hasher = Hasher128::with_seed(0);
            for j in offsets {
                if columns_info[*j as usize].as_accessor().is_string_like() {
                    hasher.write(&collation_keys_val[*j as usize]);
                } else {
                    hasher.write(&columns_val[*j as usize]);
                }
            }
            self.fm_sketches[col_len + i].insert_hash_value(hasher.finish());
        }
    }

    pub fn collect_column(
        &mut self,
        columns_val: &[Vec<u8>],
        collation_keys_val: &[Vec<u8>],
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
            .map(|fm_sketch| fm_sketch.into())
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
        columns_val: &[Vec<u8>],
        collation_keys_val: &[Vec<u8>],
        columns_info: &[tipb::ColumnInfo],
    ) {
        self.base
            .collect_column(columns_val, collation_keys_val, columns_info);
        self.sampling(columns_val);
    }
    fn sampling(&mut self, data: &[Vec<u8>]) {
        let cur_rng = self.base.rng.gen_range(0.0, 1.0);
        if cur_rng >= self.sample_rate {
            return;
        }
        let sample = data.to_vec();
        self.base.memory_usage += sample.iter().map(|x| x.capacity()).sum::<usize>();
        self.base.report_memory_usage(false);
        self.samples.push(sample);
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
        columns_val: &[Vec<u8>],
        collation_keys_val: &[Vec<u8>],
        columns_info: &[tipb::ColumnInfo],
    ) {
        self.base
            .collect_column(columns_val, collation_keys_val, columns_info);
        self.sampling(columns_val);
    }

    fn sampling(&mut self, data: &[Vec<u8>]) {
        // We should tolerate the abnormal case => `self.max_sample_size == 0`.
        if self.max_sample_size == 0 {
            return;
        }
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
            let sample = data.to_vec();
            self.base.memory_usage += sample.iter().map(|x| x.capacity()).sum::<usize>();
            self.base.report_memory_usage(false);
            self.samples.push(Reverse((cur_rng, sample)));
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

pub(crate) struct SampleBuilder<S: Snapshot, F: KvFormat> {
    pub(crate) data: BatchTableScanExecutor<TikvStorage<SnapshotStore<S>>, F>,

    max_bucket_size: usize,
    max_sample_size: usize,
    max_fm_sketch_size: usize,
    cm_sketch_depth: usize,
    cm_sketch_width: usize,
    columns_info: Vec<tipb::ColumnInfo>,
    analyze_common_handle: bool,
    common_handle_col_ids: Vec<i64>,
}

/// `SampleBuilder` is used to analyze columns. It collects sample from
/// the result set using Reservoir Sampling algorithm, estimates NDVs
/// using FM Sketch during the collecting process, and builds count-min sketch.
impl<S: Snapshot, F: KvFormat> SampleBuilder<S, F> {
    pub(crate) fn new(
        mut req: AnalyzeColumnsReq,
        common_handle_req: Option<tipb::AnalyzeIndexReq>,
        storage: TikvStorage<SnapshotStore<S>>,
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
            common_handle_col_ids: common_handle_ids,
            columns_info,
            analyze_common_handle: common_handle_req.is_some(),
        })
    }

    // `collect_columns_stats` returns the sample collectors which contain total
    // count, null count, distinct values count and count-min sketch. And it
    // also returns the statistic builder for PK which contains the histogram.
    // When PK is common handle, it returns index stats for PK.
    // See https://en.wikipedia.org/wiki/Reservoir_sampling
    pub(crate) async fn collect_columns_stats(
        &mut self,
    ) -> Result<(AnalyzeColumnsResult, Option<AnalyzeIndexResult>)> {
        use tidb_query_datatype::{codec::collation::Collator, match_template_collator};
        // The number of columns need to be sampled is `columns_without_handle_len`.
        // It equals to `columns_info.len()` if the first column doesn't contain a
        // handle. Otherwise, it equals to `columns_info.len() - 1`.
        let columns_without_handle_len =
            self.columns_info.len() - self.columns_info[0].get_pk_handle() as usize;

        let mut pk_hist = Histogram::new(self.max_bucket_size);
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
        let mut common_handle_hist = Histogram::new(self.max_bucket_size);
        let mut common_handle_cms = CmSketch::new(self.cm_sketch_depth, self.cm_sketch_width);
        let mut common_handle_fms = FmSketch::new(self.max_fm_sketch_size);
        let mut ctx = EvalContext::default();
        while !is_drained {
            let result = self.data.next_batch(BATCH_MAX_SIZE).await;
            is_drained = result.is_drained?.stop();

            let mut columns_slice = result.physical_columns.as_slice();
            let mut columns_info = &self.columns_info[..];
            if columns_without_handle_len + 1 == columns_slice.len() {
                for logical_row in &result.logical_rows {
                    let mut data = vec![];
                    columns_slice[0].encode(*logical_row, &columns_info[0], &mut ctx, &mut data)?;
                    pk_hist.append(&data, false);
                }
                columns_slice = &columns_slice[1..];
                columns_info = &columns_info[1..];
            }

            if self.analyze_common_handle {
                for logical_row in &result.logical_rows {
                    let mut data = vec![];
                    for i in 0..self.common_handle_col_ids.len() {
                        let mut handle_col_val = vec![];
                        columns_slice[i].encode(
                            *logical_row,
                            &columns_info[i],
                            &mut ctx,
                            &mut handle_col_val,
                        )?;
                        data.extend_from_slice(&handle_col_val);
                        if let Some(common_handle_cms) = common_handle_cms.as_mut() {
                            common_handle_cms.insert(&data);
                        }
                    }
                    common_handle_fms.insert(&data);
                    common_handle_hist.append(&data, false)
                }
            }

            for (i, collector) in collectors.iter_mut().enumerate() {
                for logical_row in &result.logical_rows {
                    let mut val = vec![];
                    columns_slice[i].encode(*logical_row, &columns_info[i], &mut ctx, &mut val)?;

                    // This is a workaround for different encoding methods used by TiDB and TiKV for
                    // CM Sketch. We need this because we must ensure we are using the same encoding
                    // method when we are querying values from CM Sketch (in TiDB) and inserting
                    // values into CM Sketch (here).
                    // We are inserting raw bytes from TableScanExecutor into CM Sketch here and
                    // query CM Sketch using bytes encoded by tablecodec.EncodeValue() in TiDB.
                    // Their results are different after row format becomes ver 2.
                    //
                    // Here we:
                    // - convert INT bytes to VAR_INT bytes
                    // - convert UINT bytes to VAR_UINT bytes
                    // - "flatten" the duration value from DURATION bytes into i64 value, then
                    //   convert it to VAR_INT bytes.
                    // These are the only 3 cases we need to care about according to TiDB's
                    // tablecodec.EncodeValue() and TiKV's V1CompatibleEncoder::write_v2_as_datum().
                    val = match val[0] {
                        INT_FLAG | UINT_FLAG | DURATION_FLAG => {
                            let mut mut_val = &val[..];
                            let decoded_val = mut_val.read_datum()?;
                            let flattened = table::flatten(&mut ctx, decoded_val)?;
                            encode_value(&mut ctx, &[flattened])?
                        }
                        _ => val,
                    };

                    if columns_info[i].as_accessor().is_string_like() {
                        let sorted_val = match_template_collator! {
                            TT, match columns_info[i].as_accessor().collation()? {
                                Collation::TT => {
                                    let mut mut_val = &val[..];
                                    let decoded_val = table::decode_col_value(&mut mut_val, &mut ctx, &columns_info[i])?;
                                    if decoded_val == Datum::Null {
                                        val
                                    } else {
                                        // Only if the `decoded_val` is Datum::Null, `decoded_val` is a Ok(None).
                                        // So it is safe the unwrap the Ok value.
                                        let decoded_sorted_val = TT::sort_key(&decoded_val.as_string()?.unwrap().into_owned())?;
                                        encode_value(&mut ctx, &[Datum::Bytes(decoded_sorted_val)])?
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
        Ok((AnalyzeColumnsResult::new(collectors, pk_hist), idx_res))
    }
}

/// `SampleCollector` will collect Samples and calculate the count, ndv and
/// total size of an attribute.
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

impl From<SampleCollector> for tipb::SampleCollector {
    fn from(collector: SampleCollector) -> tipb::SampleCollector {
        let mut s = tipb::SampleCollector::default();
        s.set_null_count(collector.null_count as i64);
        s.set_count(collector.count as i64);
        s.set_fm_sketch(collector.fm_sketch.into());
        s.set_samples(collector.samples.into());
        if let Some(c) = collector.cm_sketch {
            s.set_cm_sketch(c.into())
        }
        s.set_total_size(collector.total_size as i64);
        s
    }
}

pub(crate) struct AnalyzeSamplingResult {
    row_sample_collector: Box<dyn RowSampleCollector>,
}

impl AnalyzeSamplingResult {
    fn new(row_sample_collector: Box<dyn RowSampleCollector>) -> AnalyzeSamplingResult {
        AnalyzeSamplingResult {
            row_sample_collector,
        }
    }
}

impl From<AnalyzeSamplingResult> for tipb::AnalyzeColumnsResp {
    fn from(mut result: AnalyzeSamplingResult) -> tipb::AnalyzeColumnsResp {
        let pb_collector = result.row_sample_collector.to_proto();
        let mut res = tipb::AnalyzeColumnsResp::default();
        res.set_row_collector(pb_collector);
        res
    }
}

impl Default for AnalyzeSamplingResult {
    fn default() -> Self {
        AnalyzeSamplingResult::new(Box::<ReservoirRowSampleCollector>::default())
    }
}

/// `AnalyzeColumnsResult` collect the result of analyze columns request.
#[derive(Default)]
pub(crate) struct AnalyzeColumnsResult {
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
}

impl From<AnalyzeColumnsResult> for tipb::AnalyzeColumnsResp {
    fn from(result: AnalyzeColumnsResult) -> tipb::AnalyzeColumnsResp {
        let hist = result.pk_hist.into();
        let cols: Vec<tipb::SampleCollector> = result
            .sample_collectors
            .into_iter()
            .map(|col| col.into())
            .collect();
        let mut res = tipb::AnalyzeColumnsResp::default();
        res.set_collectors(cols.into());
        res.set_pk_hist(hist);
        res
    }
}

/// `AnalyzeIndexResult` collect the result of analyze index request.
#[derive(Default)]
pub(crate) struct AnalyzeIndexResult {
    hist: Histogram,
    cms: Option<CmSketch>,
    fms: Option<FmSketch>,
}

impl AnalyzeIndexResult {
    pub(crate) fn new(
        hist: Histogram,
        cms: Option<CmSketch>,
        fms: Option<FmSketch>,
    ) -> AnalyzeIndexResult {
        AnalyzeIndexResult { hist, cms, fms }
    }
}

impl From<AnalyzeIndexResult> for tipb::AnalyzeIndexResp {
    fn from(result: AnalyzeIndexResult) -> tipb::AnalyzeIndexResp {
        let mut res = tipb::AnalyzeIndexResp::default();
        res.set_hist(result.hist.into());
        if let Some(c) = result.cms {
            res.set_cms(c.into());
        }
        if let Some(f) = result.fms {
            let mut s = tipb::SampleCollector::default();
            s.set_fm_sketch(f.into());
            res.set_collector(s);
        }
        res
    }
}

/// `AnalyzeMixedResult` collect the result of analyze mixed request.
#[derive(Default)]
pub(crate) struct AnalyzeMixedResult {
    col_res: AnalyzeColumnsResult,
    idx_res: AnalyzeIndexResult,
}

impl AnalyzeMixedResult {
    pub(crate) fn new(
        col_res: AnalyzeColumnsResult,
        idx_res: AnalyzeIndexResult,
    ) -> AnalyzeMixedResult {
        AnalyzeMixedResult { col_res, idx_res }
    }
}

impl From<AnalyzeMixedResult> for tipb::AnalyzeMixedResp {
    fn from(result: AnalyzeMixedResult) -> tipb::AnalyzeMixedResp {
        let mut res = tipb::AnalyzeMixedResp::default();
        res.set_index_resp(result.idx_res.into());
        res.set_columns_resp(result.col_res.into());
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

        let mut ctx = EvalContext::default();
        for data in cases {
            sample.collect(datum::encode_value(&mut ctx, &[data]).unwrap());
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
        let mut ctx = EvalContext::default();
        for i in 0..row_num {
            nums.push(datum::encode_value(&mut ctx, &[Datum::I64(i as i64)]).unwrap());
        }
        for loop_i in 0..loop_cnt {
            let mut collector = ReservoirRowSampleCollector::new(sample_num, 1000, 1);
            for row in &nums {
                collector.sampling(&[row.clone()]);
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
        let mut ctx = EvalContext::default();
        for i in 0..row_num {
            nums.push(datum::encode_value(&mut ctx, &[Datum::I64(i as i64)]).unwrap());
        }
        for loop_i in 0..loop_cnt {
            let mut collector =
                BernoulliRowSampleCollector::new(sample_num as f64 / row_num as f64, 1000, 1);
            for row in &nums {
                collector.sampling(&[row.clone()]);
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

    #[test]
    fn test_abnormal_sampling() {
        let sample_num = 0; // abnormal.
        let row_num = 100;
        let mut nums: Vec<Vec<u8>> = Vec::with_capacity(row_num);
        let mut ctx = EvalContext::default();
        for i in 0..row_num {
            nums.push(datum::encode_value(&mut ctx, &[Datum::I64(i as i64)]).unwrap());
        }
        {
            // Test for ReservoirRowSampleCollector
            let mut collector = ReservoirRowSampleCollector::new(sample_num, 1000, 1);
            for row in &nums {
                collector.sampling(&[row.clone()]);
            }
            assert_eq!(collector.samples.len(), 0);
        }
        {
            // Test for BernoulliRowSampleCollector
            let mut collector =
                BernoulliRowSampleCollector::new(sample_num as f64 / row_num as f64, 1000, 1);
            for row in &nums {
                collector.sampling(&[row.clone()]);
            }
            assert_eq!(collector.samples.len(), 0);
        }
    }
}

#[cfg(test)]
mod benches {
    use tidb_query_datatype::{
        codec::{
            batch::LazyBatchColumn,
            collation::{collator::CollatorUtf8Mb4Bin, Collator},
        },
        EvalType, FieldTypeTp,
    };

    use super::*;

    fn prepare_arguments() -> (
        Vec<Vec<u8>>,
        Vec<Vec<u8>>,
        Vec<tipb::ColumnInfo>,
        Vec<tipb::AnalyzeColumnGroup>,
    ) {
        let mut columns_info = Vec::new();
        for i in 1..4 {
            let mut col_info = tipb::ColumnInfo::default();
            col_info.set_column_id(i as i64);
            col_info.as_mut_accessor().set_tp(FieldTypeTp::VarChar);
            col_info
                .as_mut_accessor()
                .set_collation(Collation::Utf8Mb4Bin);
            columns_info.push(col_info);
        }
        let mut columns_slice = Vec::new();
        for _ in 0..3 {
            let mut col = LazyBatchColumn::decoded_with_capacity_and_tp(1, EvalType::Bytes);
            col.mut_decoded().push_bytes(Some(b"abcdefghijklmnopqrstuvwxyz0123456789abcdefghijklmnopqrstuvwxyz0123456789abcdefghijklmnopqrstuvwxyz0123456789abcdefghijklmnopqrstuvwxyz0123456789abcdefghijklmnopqrstuvwxyz0123456789".to_vec()));
            columns_slice.push(col)
        }
        let mut column_vals = Vec::new();
        let mut collation_key_vals = Vec::new();
        let mut ctx = EvalContext::default();
        for i in 0..columns_info.len() {
            let mut val = vec![];
            columns_slice[i]
                .encode(0, &columns_info[i], &mut ctx, &mut val)
                .unwrap();
            if columns_info[i].as_accessor().is_string_like() {
                let mut mut_val = &val[..];
                let decoded_val =
                    table::decode_col_value(&mut mut_val, &mut ctx, &columns_info[i]).unwrap();
                let decoded_sorted_val =
                    CollatorUtf8Mb4Bin::sort_key(&decoded_val.as_string().unwrap().unwrap())
                        .unwrap();
                collation_key_vals.push(decoded_sorted_val);
            } else {
                collation_key_vals.push(Vec::new());
            }
            column_vals.push(val);
        }
        let mut column_group = tipb::AnalyzeColumnGroup::default();
        column_group.set_column_offsets(vec![0, 1, 2]);
        column_group.set_prefix_lengths(vec![-1, -1, -1]);
        let column_groups = vec![column_group];
        (column_vals, collation_key_vals, columns_info, column_groups)
    }

    #[bench]
    fn bench_collect_column(b: &mut test::Bencher) {
        let mut collector = BaseRowSampleCollector::new(10000, 4);
        let (column_vals, collation_key_vals, columns_info, _) = prepare_arguments();
        b.iter(|| {
            collector.collect_column(&column_vals, &collation_key_vals, &columns_info);
        })
    }

    #[bench]
    fn bench_collect_column_group(b: &mut test::Bencher) {
        let mut collector = BaseRowSampleCollector::new(10000, 4);
        let (column_vals, collation_key_vals, columns_info, column_groups) = prepare_arguments();
        b.iter(|| {
            collector.collect_column_group(
                &column_vals,
                &collation_key_vals,
                &columns_info,
                &column_groups,
            );
        })
    }
}
