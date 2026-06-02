// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    cmp::Reverse,
    collections::BinaryHeap,
    hash::Hasher,
    mem,
    sync::Arc,
    time::{Duration, Instant},
};

use api_version::KvFormat;
use collections::HashSet;
use kvproto::coprocessor::KeyRange;
use mur3::{Hasher128, murmurhash3_x64_128};
use protobuf::Message;
use rand::{Rng, SeedableRng, rngs::StdRng};
use tidb_query_common::execute_stats::ExecuteStats;
use tidb_query_datatype::{
    FieldTypeAccessor,
    codec::{
        datum::{DURATION_FLAG, Datum, DatumDecoder, INT_FLAG, NIL_FLAG, UINT_FLAG, encode_value},
        table,
    },
    def::Collation,
    expr::{EvalConfig, EvalContext},
};
use tidb_query_executors::{
    BatchTablePointScanExecutor, BatchTableScanExecutor, interface::BatchExecutor,
};
use tidb_query_expr::BATCH_MAX_SIZE;
use tikv_alloc::trace::TraceEvent;
use tikv_util::{
    metrics::{NON_TXN_COMMAND_THROTTLE_TIME_COUNTER_VEC_STATIC, ThrottleType},
    quota_limiter::QuotaLimiter,
};
use tipb::{self, AnalyzeColumnsReq};

use super::{
    cmsketch::CmSketch,
    fmsketch::FmSketch,
    histogram::Histogram,
    hll::{DEFAULT_HLL_PRECISION, Hll},
};
use crate::{
    coprocessor::{MEMTRACE_ANALYZE, dag::TikvStorage, metrics, *},
    storage::{Snapshot, SnapshotStore, Statistics},
};

fn duration_ms(duration: Duration) -> u64 {
    duration.as_millis() as u64
}

pub(crate) fn effective_bernoulli_sample_rate_after_preselection(
    sample_rate: f64,
    pre_sample_scale: f64,
) -> f64 {
    if pre_sample_scale <= 1.0 {
        return sample_rate;
    }
    (sample_rate * pre_sample_scale).min(1.0)
}

pub(crate) struct RowSampleBuilder<S: Snapshot, F: KvFormat> {
    pub(crate) data: BatchTableScanExecutor<TikvStorage<SnapshotStore<S>>, F>,
    /// Accumulated storage statistics for this request. Filled per batch so
    /// that collect_scan_statistics can report request-scoped stats (see
    /// merge_storage_stats_into).
    accumulated_storage_stats: Statistics,

    max_sample_size: usize,
    max_fm_sketch_size: usize,
    sample_rate: f64,
    /// Scale factor for rescaling count/null_count/total_sizes after
    /// pre-selecting a subset of row key ranges. 1.0 means no pre-selection
    /// was done.
    pre_sample_scale: f64,
    // Whether to build per-row singleton sketches (only needed when ndv_rate < 1).
    build_singletons: bool,
    columns_info: Vec<tipb::ColumnInfo>,
    column_groups: Vec<tipb::AnalyzeColumnGroup>,
    quota_limiter: Arc<QuotaLimiter>,
    #[allow(dead_code)] // kept for future use (e.g. priority or reporting)
    is_auto_analyze: bool,
}

impl<S: Snapshot, F: KvFormat> RowSampleBuilder<S, F> {
    pub(crate) fn new_with_preselected_ranges(
        mut req: AnalyzeColumnsReq,
        storage: TikvStorage<SnapshotStore<S>>,
        ranges: Vec<KeyRange>,
        quota_limiter: Arc<QuotaLimiter>,
        is_auto_analyze: bool,
        pre_selected_ranges: Option<(Vec<KeyRange>, f64)>,
    ) -> Result<Self> {
        let columns_info: Vec<_> = req.take_columns_info().into();
        if columns_info.is_empty() {
            return Err(box_err!("empty columns_info"));
        }
        let max_sample_size = req.get_sample_size() as usize;
        let mut sample_rate = req.get_sample_rate();
        // `ndv_rate` controls whether TiDB needs singleton sketches for NDV
        // extrapolation. Do not apply it as another row-level filter here:
        // pre-selected row-key ranges already carry the intended NDV sampling
        // budget.
        let ndv_rate = req.get_ndv_rate();
        let ndv_rate = if ndv_rate > 0.0 { ndv_rate } else { 1.0 };
        let build_singletons = ndv_rate < 1.0;

        let (selected_ranges, scale) = if let Some((ranges, scale)) = pre_selected_ranges {
            (ranges, scale)
        } else {
            (ranges, 1.0)
        };
        if scale > 1.0 && max_sample_size == 0 {
            // Row-key pre-selection is the NDV sampling filter. Bernoulli row
            // samples should still keep the original marginal SAMPLERATE, so
            // sample from the already pre-selected rows with sample_rate /
            // selected_fraction. If SAMPLERATE exceeds that selected fraction,
            // every pre-selected row becomes a row sample; reservoir sampling is
            // already target-size based and does not need this adjustment.
            sample_rate = effective_bernoulli_sample_rate_after_preselection(sample_rate, scale);
        }

        let common_handle_ids = req.take_primary_column_ids();
        let table_scanner = BatchTableScanExecutor::new(
            storage,
            Arc::new(EvalConfig::default()),
            columns_info.clone(),
            selected_ranges,
            common_handle_ids,
            false,
            false, // Streaming mode is not supported in Analyze request, always false here
            req.take_primary_prefix_column_ids(),
        )?;
        Ok(Self {
            data: table_scanner,
            accumulated_storage_stats: Statistics::default(),
            max_sample_size,
            max_fm_sketch_size: req.get_sketch_size() as usize,
            sample_rate,
            pre_sample_scale: scale,
            build_singletons,
            columns_info,
            column_groups: req.take_column_groups().into(),
            quota_limiter,
            is_auto_analyze,
        })
    }

    fn new_collector(&mut self) -> Box<dyn RowSampleCollector> {
        new_row_sample_collector(
            self.max_sample_size,
            self.max_fm_sketch_size,
            self.columns_info.len() + self.column_groups.len(),
            self.build_singletons,
            self.sample_rate,
        )
    }

    /// Merges accumulated storage statistics into `dest`. Used by the context
    /// so that collect_scan_statistics gets request-scoped stats (from the
    /// Scanner), consistent with other handlers (e.g. DAG / checksum).
    pub(crate) fn merge_storage_stats_into(&mut self, dest: &mut Statistics) {
        dest.add(&mem::take(&mut self.accumulated_storage_stats));
        // Collect potential trailing scanner stats that were generated after
        // the last per-batch collection.
        self.data.collect_storage_stats(dest);
    }

    pub(crate) async fn collect_column_stats(&mut self) -> Result<AnalyzeSamplingResult> {
        use tidb_query_datatype::{codec::collation::Collator, match_template_collator};

        let mut is_drained = false;
        let mut collector = self.new_collector();
        let mut ctx = EvalContext::default();
        let scan_start = Instant::now();
        let mut batch_count = 0_u64;
        let mut read_bytes = 0_usize;
        let mut next_batch_elapsed = Duration::ZERO;
        let mut encode_collect_elapsed = Duration::ZERO;
        let mut quota_consume_elapsed = Duration::ZERO;
        let mut quota_delay_total = Duration::ZERO;
        while !is_drained {
            // Use background limiters for both manual and auto analyze so that iops_limiter
            // (and other background quotas) apply to manual analyze as well.
            let mut sample = self.quota_limiter.new_sample(false);
            let mut read_size: usize = 0;
            {
                let next_batch_start = Instant::now();
                let result = {
                    let (duration, res) = sample
                        .observe_cpu_async(self.data.next_batch(BATCH_MAX_SIZE))
                        .await;
                    sample.add_cpu_time(duration);
                    res
                };
                next_batch_elapsed += next_batch_start.elapsed();
                batch_count += 1;

                // Use request-scoped storage stats for IOPS (like collect_scan_statistics),
                // and count only RocksDB block reads as an approximation of disk IOPS.
                // PerfContext is thread-local; across an await other tasks can run on the same
                // thread and pollute it, so we use the Scanner's statistics instead.
                let mut batch_stats = Statistics::default();
                self.data.collect_storage_stats(&mut batch_stats);
                let batch_iops = batch_stats.data.block_read_count
                    + batch_stats.lock.block_read_count
                    + batch_stats.write.block_read_count;
                let batch_total_ops = batch_stats.data.total_op_count()
                    + batch_stats.lock.total_op_count()
                    + batch_stats.write.total_op_count();
                sample.add_iops(batch_iops);
                self.accumulated_storage_stats.add(&batch_stats);

                metrics::ANALYZE_METRICS_STATIC
                    .get(metrics::AnalyzeMetricKind::read_iops)
                    .inc_by(batch_iops as u64);
                metrics::ANALYZE_METRICS_STATIC
                    .get(metrics::AnalyzeMetricKind::read_total_op_count)
                    .inc_by(batch_total_ops as u64);
                if batch_total_ops > 0 {
                    metrics::ANALYZE_IOPS_PER_TOTAL_OP_HISTOGRAM
                        .observe(batch_iops as f64 / batch_total_ops as f64);
                }
                metrics::ANALYZE_METRICS_STATIC
                    .get(metrics::AnalyzeMetricKind::next_batch_count)
                    .inc();
                let _guard = sample.observe_cpu();
                is_drained = result.is_drained?.stop();
                let mut exec_stats = ExecuteStats::new(0);
                self.data.collect_exec_stats(&mut exec_stats);
                collector.mut_base().count +=
                    exec_stats.scanned_rows_per_range.into_iter().sum::<usize>() as u64;

                let encode_collect_start = Instant::now();
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
                    collector.mut_base().sketch_sample_count += 1;
                    collector.collect_column_group(
                        &column_vals,
                        &collation_key_vals,
                        &self.columns_info,
                        &self.column_groups,
                    );
                    collector.collect_column(&column_vals, &collation_key_vals, &self.columns_info);
                }
                encode_collect_elapsed += encode_collect_start.elapsed();
            }

            sample.add_read_bytes(read_size);
            read_bytes += read_size;
            // Don't let analyze bandwidth limit the quota limiter, this is already limited
            // in rate limiter.
            let quota_consume_start = Instant::now();
            let quota_delay = {
                // Use background limiters for both manual and auto analyze so that iops_limiter
                // applies to manual analyze as well.
                self.quota_limiter.consume_sample(sample, false).await
            };
            quota_consume_elapsed += quota_consume_start.elapsed();
            quota_delay_total += quota_delay;

            if !quota_delay.is_zero() {
                NON_TXN_COMMAND_THROTTLE_TIME_COUNTER_VEC_STATIC
                    .get(ThrottleType::analyze_full_sampling)
                    .inc_by(quota_delay.as_micros() as u64);
            }
        }
        debug!("analyze full sampling trace";
            "component" => "tikv",
            "phase" => "tikv.scan_full_sampling_table",
            "event" => "finish",
            "elapsed_ms" => scan_start.elapsed().as_millis() as u64,
            "next_batch_elapsed_ms" => duration_ms(next_batch_elapsed),
            "encode_collect_elapsed_ms" => duration_ms(encode_collect_elapsed),
            "quota_consume_elapsed_ms" => duration_ms(quota_consume_elapsed),
            "quota_delay_ms" => duration_ms(quota_delay_total),
            "batch_count" => batch_count,
            "scanned_rows" => collector.mut_base().count,
            "read_bytes" => read_bytes,
            "columns" => self.columns_info.len(),
            "column_groups" => self.column_groups.len(),
            "max_sample_size" => self.max_sample_size,
            "sample_rate" => self.sample_rate,
            "is_auto_analyze" => self.is_auto_analyze,
        );
        // No-op unless a future row-level NDV sub-sample makes
        // sketch_sample_count smaller than count. Keep this before the
        // single-column-group copy so derived columns see corrected values.
        collector.mut_base().rescale_null_count_and_total_sizes();
        let column_group_fixup_start = Instant::now();
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
            // Copy whichever sketch type this mode populated; the other Vec is
            // empty (see BaseRowSampleCollector::new).
            if collector.mut_base().build_singletons {
                collector.mut_base().singleton_sketches[col_group_pos] =
                    collector.mut_base().singleton_sketches[col_pos].clone();
            } else {
                collector.mut_base().fm_sketches[col_group_pos] =
                    collector.mut_base().fm_sketches[col_pos].clone();
            }
            collector.mut_base().null_count[col_group_pos] =
                collector.mut_base().null_count[col_pos];
            collector.mut_base().total_sizes[col_group_pos] =
                collector.mut_base().total_sizes[col_pos];
        }
        debug!("analyze full sampling trace";
            "component" => "tikv",
            "phase" => "tikv.column_group_fixup",
            "event" => "finish",
            "elapsed_ms" => duration_ms(column_group_fixup_start.elapsed()),
            "column_groups" => self.column_groups.len(),
        );
        // Rescale count/null_count/total_sizes if row-key pre-sampling was
        // used. FM/singleton sketches describe only the selected rows and stay
        // unchanged.
        if self.pre_sample_scale > 1.0 {
            let total_rows =
                (collector.mut_base().count as f64 * self.pre_sample_scale).round() as u64;
            rescale_pre_sampled_base(collector.mut_base(), total_rows, self.pre_sample_scale);
        }
        Ok(AnalyzeSamplingResult::new(collector))
    }
}

fn new_row_sample_collector(
    max_sample_size: usize,
    max_fm_sketch_size: usize,
    col_and_group_len: usize,
    build_singletons: bool,
    sample_rate: f64,
) -> Box<dyn RowSampleCollector> {
    if max_sample_size > 0 {
        return Box::new(ReservoirRowSampleCollector::new(
            max_sample_size,
            max_fm_sketch_size,
            col_and_group_len,
            build_singletons,
        ));
    }
    Box::new(BernoulliRowSampleCollector::new(
        sample_rate,
        max_fm_sketch_size,
        col_and_group_len,
        build_singletons,
    ))
}

pub(crate) struct PointRowSampleBuilder<S: Snapshot, F: KvFormat> {
    data: BatchTablePointScanExecutor<TikvStorage<SnapshotStore<S>>, F>,
    accumulated_storage_stats: Statistics,
    collector: Box<dyn RowSampleCollector>,
    columns_info: Vec<tipb::ColumnInfo>,
    column_groups: Vec<tipb::AnalyzeColumnGroup>,
    quota_limiter: Arc<QuotaLimiter>,
    max_sample_size: usize,
    sample_rate: f64,
    is_auto_analyze: bool,
}

impl<S: Snapshot, F: KvFormat> PointRowSampleBuilder<S, F> {
    pub(crate) fn new(
        mut req: AnalyzeColumnsReq,
        storage: TikvStorage<SnapshotStore<S>>,
        quota_limiter: Arc<QuotaLimiter>,
        is_auto_analyze: bool,
    ) -> Result<Self> {
        let columns_info: Vec<_> = req.take_columns_info().into();
        if columns_info.is_empty() {
            return Err(box_err!("empty columns_info"));
        }
        let max_sample_size = req.get_sample_size() as usize;
        let max_fm_sketch_size = req.get_sketch_size() as usize;
        let sample_rate = req.get_sample_rate();
        let ndv_rate = req.get_ndv_rate();
        let ndv_rate = if ndv_rate > 0.0 { ndv_rate } else { 1.0 };
        let build_singletons = ndv_rate < 1.0;
        let column_groups: Vec<_> = req.take_column_groups().into();
        let collector = new_row_sample_collector(
            max_sample_size,
            max_fm_sketch_size,
            columns_info.len() + column_groups.len(),
            build_singletons,
            sample_rate,
        );
        let data = BatchTablePointScanExecutor::new(
            storage,
            Arc::new(EvalConfig::default()),
            columns_info.clone(),
            Vec::new(),
            req.take_primary_column_ids(),
            req.take_primary_prefix_column_ids(),
        )?;
        Ok(Self {
            data,
            accumulated_storage_stats: Statistics::default(),
            collector,
            columns_info,
            column_groups,
            quota_limiter,
            max_sample_size,
            sample_rate,
            is_auto_analyze,
        })
    }

    pub(crate) async fn collect_raw_key_batch(&mut self, raw_keys: Vec<Vec<u8>>) -> Result<()> {
        if raw_keys.is_empty() {
            return Ok(());
        }
        self.data.reset_raw_keys(raw_keys);
        collect_column_stats_from_executor(
            &mut self.data,
            self.collector.as_mut(),
            &self.columns_info,
            &self.column_groups,
            &self.quota_limiter,
            self.max_sample_size,
            self.sample_rate,
            self.is_auto_analyze,
            &mut self.accumulated_storage_stats,
        )
        .await
    }

    pub(crate) fn rescale_pre_sampled_rows(&mut self, total_rows: u64, selected_rows: u64) {
        if total_rows == 0 || selected_rows == 0 || total_rows == selected_rows {
            return;
        }
        let scale = total_rows as f64 / selected_rows as f64;
        rescale_pre_sampled_base(self.collector.mut_base(), total_rows, scale);
    }

    pub(crate) fn into_result(self) -> AnalyzeSamplingResult {
        AnalyzeSamplingResult::new(self.collector)
    }

    pub(crate) fn merge_storage_stats_into(&mut self, dest: &mut Statistics) {
        dest.add(&mem::take(&mut self.accumulated_storage_stats));
        self.data.collect_storage_stats(dest);
    }
}

#[allow(clippy::too_many_arguments)]
async fn collect_column_stats_from_executor<D>(
    data: &mut D,
    collector: &mut dyn RowSampleCollector,
    columns_info: &[tipb::ColumnInfo],
    column_groups: &[tipb::AnalyzeColumnGroup],
    quota_limiter: &Arc<QuotaLimiter>,
    max_sample_size: usize,
    sample_rate: f64,
    is_auto_analyze: bool,
    accumulated_storage_stats: &mut Statistics,
) -> Result<()>
where
    D: BatchExecutor<StorageStats = Statistics>,
{
    use tidb_query_datatype::{codec::collation::Collator, match_template_collator};

    let mut is_drained = false;
    let mut ctx = EvalContext::default();
    let scan_start = Instant::now();
    let mut batch_count = 0_u64;
    let mut read_bytes = 0_usize;
    let mut next_batch_elapsed = Duration::ZERO;
    let mut encode_collect_elapsed = Duration::ZERO;
    let mut quota_consume_elapsed = Duration::ZERO;
    let mut quota_delay_total = Duration::ZERO;
    while !is_drained {
        // Use background limiters for both manual and auto analyze so that iops_limiter
        // (and other background quotas) apply to manual analyze as well.
        let mut sample = quota_limiter.new_sample(false);
        let mut read_size: usize = 0;
        {
            let next_batch_start = Instant::now();
            let result = {
                let (duration, res) = sample
                    .observe_cpu_async(data.next_batch(BATCH_MAX_SIZE))
                    .await;
                sample.add_cpu_time(duration);
                res
            };
            next_batch_elapsed += next_batch_start.elapsed();
            batch_count += 1;

            // Use request-scoped storage stats for IOPS (like collect_scan_statistics),
            // and count only RocksDB block reads as an approximation of disk IOPS.
            // PerfContext is thread-local; across an await other tasks can run on the same
            // thread and pollute it, so we use the Scanner's statistics instead.
            let mut batch_stats = Statistics::default();
            data.collect_storage_stats(&mut batch_stats);
            let batch_iops = batch_stats.data.block_read_count
                + batch_stats.lock.block_read_count
                + batch_stats.write.block_read_count;
            let batch_total_ops = batch_stats.data.total_op_count()
                + batch_stats.lock.total_op_count()
                + batch_stats.write.total_op_count();
            sample.add_iops(batch_iops);
            accumulated_storage_stats.add(&batch_stats);

            metrics::ANALYZE_METRICS_STATIC
                .get(metrics::AnalyzeMetricKind::read_iops)
                .inc_by(batch_iops as u64);
            metrics::ANALYZE_METRICS_STATIC
                .get(metrics::AnalyzeMetricKind::read_total_op_count)
                .inc_by(batch_total_ops as u64);
            if batch_total_ops > 0 {
                metrics::ANALYZE_IOPS_PER_TOTAL_OP_HISTOGRAM
                    .observe(batch_iops as f64 / batch_total_ops as f64);
            }
            metrics::ANALYZE_METRICS_STATIC
                .get(metrics::AnalyzeMetricKind::next_batch_count)
                .inc();
            let _guard = sample.observe_cpu();
            is_drained = result.is_drained?.stop();
            let mut exec_stats = ExecuteStats::new(0);
            data.collect_exec_stats(&mut exec_stats);
            collector.mut_base().count +=
                exec_stats.scanned_rows_per_range.into_iter().sum::<usize>() as u64;

            let encode_collect_start = Instant::now();
            let columns_slice = result.physical_columns.as_slice();
            let mut column_vals: Vec<Vec<u8>> = vec![vec![]; columns_info.len()];
            let mut collation_key_vals: Vec<Vec<u8>> = vec![vec![]; columns_info.len()];
            for logical_row in &result.logical_rows {
                for i in 0..columns_info.len() {
                    column_vals[i].clear();
                    collation_key_vals[i].clear();
                    columns_slice[i].encode(
                        *logical_row,
                        &columns_info[i],
                        &mut ctx,
                        &mut column_vals[i],
                    )?;
                    if columns_info[i].as_accessor().is_string_like() {
                        match_template_collator! {
                            TT, match columns_info[i].as_accessor().collation()? {
                                Collation::TT => {
                                    let mut mut_val = &column_vals[i][..];
                                    let decoded_val = table::decode_col_value(&mut mut_val, &mut ctx, &columns_info[i])?;
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
                collector.mut_base().sketch_sample_count += 1;
                collector.collect_column_group(
                    &column_vals,
                    &collation_key_vals,
                    columns_info,
                    column_groups,
                );
                collector.collect_column(&column_vals, &collation_key_vals, columns_info);
            }
            encode_collect_elapsed += encode_collect_start.elapsed();
        }

        sample.add_read_bytes(read_size);
        read_bytes += read_size;
        // Don't let analyze bandwidth limit the quota limiter, this is already limited
        // in rate limiter.
        let quota_consume_start = Instant::now();
        let quota_delay = {
            // Use background limiters for both manual and auto analyze so that iops_limiter
            // applies to manual analyze as well.
            quota_limiter.consume_sample(sample, false).await
        };
        quota_consume_elapsed += quota_consume_start.elapsed();
        quota_delay_total += quota_delay;

        if !quota_delay.is_zero() {
            NON_TXN_COMMAND_THROTTLE_TIME_COUNTER_VEC_STATIC
                .get(ThrottleType::analyze_full_sampling)
                .inc_by(quota_delay.as_micros() as u64);
        }
    }
    debug!("analyze full sampling trace";
        "component" => "tikv",
        "phase" => "tikv.scan_full_sampling_table",
        "event" => "finish",
        "elapsed_ms" => scan_start.elapsed().as_millis() as u64,
        "next_batch_elapsed_ms" => duration_ms(next_batch_elapsed),
        "encode_collect_elapsed_ms" => duration_ms(encode_collect_elapsed),
        "quota_consume_elapsed_ms" => duration_ms(quota_consume_elapsed),
        "quota_delay_ms" => duration_ms(quota_delay_total),
        "batch_count" => batch_count,
        "scanned_rows" => collector.mut_base().count,
        "read_bytes" => read_bytes,
        "columns" => columns_info.len(),
        "column_groups" => column_groups.len(),
        "max_sample_size" => max_sample_size,
        "sample_rate" => sample_rate,
        "is_auto_analyze" => is_auto_analyze,
    );
    // No-op unless a future row-level NDV sub-sample makes sketch_sample_count
    // smaller than count. Keep this before the single-column-group copy so
    // derived columns see corrected values.
    collector.mut_base().rescale_null_count_and_total_sizes();
    let column_group_fixup_start = Instant::now();
    for i in 0..column_groups.len() {
        let offsets = column_groups[i].get_column_offsets();
        if offsets.len() != 1 {
            continue;
        }
        // For the single-column group, its sketch/null_count/total_size is the
        // same as the corresponding column. We do not collect it in
        // collect_column_group, so copy it after iterating rows.
        let col_pos = offsets[0] as usize;
        let col_group_pos = columns_info.len() + i;
        let base = collector.mut_base();
        if base.build_singletons {
            let sketch = base.singleton_sketches[col_pos].clone();
            base.singleton_sketches[col_group_pos] = sketch;
        } else {
            let sketch = base.fm_sketches[col_pos].clone();
            base.fm_sketches[col_group_pos] = sketch;
        }
        let null_count = base.null_count[col_pos];
        base.null_count[col_group_pos] = null_count;
        let total_size = base.total_sizes[col_pos];
        base.total_sizes[col_group_pos] = total_size;
    }
    debug!("analyze full sampling trace";
        "component" => "tikv",
        "phase" => "tikv.column_group_fixup",
        "event" => "finish",
        "elapsed_ms" => duration_ms(column_group_fixup_start.elapsed()),
        "column_groups" => column_groups.len(),
    );
    Ok(())
}

fn rescale_pre_sampled_base(base: &mut BaseRowSampleCollector, total_rows: u64, scale: f64) {
    let scanned_count = base.count;
    base.count = total_rows;
    for nc in &mut base.null_count {
        *nc = (*nc as f64 * scale).round() as i64;
    }
    for ts in &mut base.total_sizes {
        *ts = (*ts as f64 * scale).round() as i64;
    }
    info!("analyze pre-sampled rows rescale";
        "scanned_count" => scanned_count,
        "rescaled_count" => base.count,
        "sketch_sample_count" => base.sketch_sample_count,
        "scale_factor" => scale,
    );
}

type BernoulliSamples = Vec<Vec<Vec<u8>>>;
type ReservoirSamples = BinaryHeap<Reverse<(i64, Vec<Vec<u8>>)>>;

trait RowSampleCollector: Send {
    fn mut_base(&mut self) -> &mut BaseRowSampleCollector;
    fn take_base(&mut self) -> BaseRowSampleCollector;
    fn merge_collector(&mut self, other: Box<dyn RowSampleCollector>) -> Result<()>;
    fn take_bernoulli_samples(&mut self) -> Option<BernoulliSamples> {
        None
    }
    fn take_reservoir_samples(&mut self) -> Option<ReservoirSamples> {
        None
    }
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
    #[allow(dead_code)]
    fn get_reported_memory_usage(&mut self) -> usize {
        self.mut_base().reported_memory_usage
    }
    #[allow(dead_code)]
    fn get_memory_usage(&mut self) -> usize {
        self.mut_base().memory_usage
    }
}

// SingletonSketch tracks which hashes have been seen exactly once (`once`)
// versus more than once (`multi`) so into_hlls can build a "seen exactly once"
// HLL. Both sets are transient per scan and bounded by the sampled distinct
// count.
#[derive(Clone)]
struct SingletonSketch {
    once: HashSet<u64>,
    multi: HashSet<u64>,
}

impl SingletonSketch {
    fn new() -> SingletonSketch {
        SingletonSketch {
            once: HashSet::with_capacity_and_hasher(0, Default::default()),
            multi: HashSet::with_capacity_and_hasher(0, Default::default()),
        }
    }

    fn insert(&mut self, bytes: &[u8]) {
        let hash = murmurhash3_x64_128(bytes, 0).0;
        self.insert_hash_value(hash);
    }

    fn insert_hash_value(&mut self, hash_val: u64) {
        if self.multi.contains(&hash_val) {
            return;
        }
        if self.once.remove(&hash_val) {
            self.multi.insert(hash_val);
        } else {
            self.once.insert(hash_val);
        }
    }

    fn merge_from(&mut self, other: SingletonSketch) {
        for hash in other.once {
            self.insert_hash_value(hash);
        }
        for hash in other.multi {
            self.once.remove(&hash);
            self.multi.insert(hash);
        }
    }

    /// Builds this region's NDV and singleton HyperLogLog sketches. The NDV
    /// sketch covers every distinct hash (once + multi); the singleton sketch
    /// covers only hashes seen exactly once. TiDB unions these across regions
    /// to estimate the global singleton (f1) count via a leave-one-out.
    fn into_hlls(self, precision: u8) -> (tipb::HllSketch, tipb::HllSketch) {
        let mut ndv = Hll::new(precision);
        let mut singleton = Hll::new(precision);
        for &hash in &self.once {
            ndv.insert_hash_value(hash);
            singleton.insert_hash_value(hash);
        }
        for &hash in &self.multi {
            ndv.insert_hash_value(hash);
        }
        (ndv.into(), singleton.into())
    }
}

#[derive(Clone)]
struct BaseRowSampleCollector {
    null_count: Vec<i64>,
    count: u64,
    sketch_sample_count: u64,
    // fm_sketches and singleton_sketches are mutually exclusive by build_singletons:
    // the FM sketch backs the exact NDV at full rate, the HLLs back the GEE f1
    // estimate under NDV sub-sampling. Only the active one is allocated (see new).
    fm_sketches: Vec<FmSketch>,
    singleton_sketches: Vec<SingletonSketch>,
    build_singletons: bool,
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
            sketch_sample_count: 0,
            fm_sketches: vec![],
            singleton_sketches: vec![],
            build_singletons: false,
            rng: StdRng::from_entropy(),
            total_sizes: vec![],
            memory_usage: 0,
            reported_memory_usage: 0,
        }
    }
}

/// Scales `sampled` (accumulated over `sample_count` rows) into an estimate
/// over `total_row_count` rows using round-half-up integer division. Returns
/// 0 when `sampled` or `sample_count` is non-positive. A `u128` intermediate
/// avoids overflow in `sampled * total_row_count` for large tables.
fn rescale_sampled_value(sampled: i64, total_row_count: u64, sample_count: u64) -> i64 {
    if sampled <= 0 || sample_count == 0 {
        return 0;
    }
    let sampled = sampled as u128;
    let total_row_count = total_row_count as u128;
    let sample_count = sample_count as u128;
    // Round-half-up integer division: floor(a*b/c + 1/2).
    let scaled = (sampled * total_row_count + sample_count / 2) / sample_count;
    scaled.min(i64::MAX as u128) as i64
}

fn add_i64_repeated(dst: &mut Vec<i64>, src: &[i64]) {
    if dst.len() < src.len() {
        dst.resize(src.len(), 0);
    }
    for (idx, value) in src.iter().enumerate() {
        dst[idx] += value;
    }
}

fn row_sample_memory_usage(row: &[Vec<u8>]) -> usize {
    row.iter().map(Vec::capacity).sum()
}

fn bernoulli_samples_memory_usage(samples: &[Vec<Vec<u8>>]) -> usize {
    samples.iter().map(|row| row_sample_memory_usage(row)).sum()
}

fn release_reported_memory_usage(base: &mut BaseRowSampleCollector) {
    base.memory_usage = 0;
    base.report_memory_usage(true);
}

impl BaseRowSampleCollector {
    fn new(
        max_fm_sketch_size: usize,
        col_and_group_len: usize,
        build_singletons: bool,
    ) -> BaseRowSampleCollector {
        BaseRowSampleCollector {
            null_count: vec![0; col_and_group_len],
            count: 0,
            sketch_sample_count: 0,
            // NDV sub-sampling derives the column NDV from the singleton sketches'
            // HLLs (see SingletonSketch::into_hlls), so the FM sketch is dead weight
            // there; allocate only the sketch type this mode actually fills.
            fm_sketches: if build_singletons {
                vec![]
            } else {
                vec![FmSketch::new(max_fm_sketch_size); col_and_group_len]
            },
            singleton_sketches: if build_singletons {
                vec![SingletonSketch::new(); col_and_group_len]
            } else {
                vec![]
            },
            build_singletons,
            rng: StdRng::from_entropy(),
            total_sizes: vec![0; col_and_group_len],
            memory_usage: 0,
            reported_memory_usage: 0,
        }
    }

    fn merge_from(&mut self, other: &mut BaseRowSampleCollector) {
        self.count += other.count;
        self.sketch_sample_count += other.sketch_sample_count;
        add_i64_repeated(&mut self.null_count, &other.null_count);
        add_i64_repeated(&mut self.total_sizes, &other.total_sizes);

        for (idx, other_sketch) in mem::take(&mut other.fm_sketches).into_iter().enumerate() {
            if idx >= self.fm_sketches.len() {
                self.fm_sketches.push(other_sketch);
            } else {
                self.fm_sketches[idx].merge(&other_sketch);
            }
        }

        for (idx, other_sketch) in mem::take(&mut other.singleton_sketches)
            .into_iter()
            .enumerate()
        {
            if idx >= self.singleton_sketches.len() {
                self.singleton_sketches.push(other_sketch);
            } else {
                self.singleton_sketches[idx].merge_from(other_sketch);
            }
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
            let hash = hasher.finish();
            // See collect_column: only the sketch type this mode allocated is filled.
            if self.build_singletons {
                self.singleton_sketches[col_len + i].insert_hash_value(hash);
            } else {
                self.fm_sketches[col_len + i].insert_hash_value(hash);
            }
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
            let key: &[u8] = if columns_info[i].as_accessor().is_string_like() {
                &collation_keys_val[i]
            } else {
                &columns_val[i]
            };
            // FM and singleton sketches are mutually exclusive: only the one this
            // mode allocated is filled (see BaseRowSampleCollector::new).
            if self.build_singletons {
                self.singleton_sketches[i].insert(key);
            } else {
                self.fm_sketches[i].insert(key);
            }
            self.total_sizes[i] += columns_val[i].len() as i64 - 1;
        }
    }

    /// Converts per-column null counts and total sizes gathered from the NDV
    /// sub-sample into estimates over the full row population. `count` is the
    /// exact scanned row count (reported via `scanned_rows_per_range`) and is
    /// only read here as the scaling divisor — it is never modified. No-op
    /// when no sub-sampling occurred (`sketch_sample_count == 0` or every
    /// scanned row was sampled).
    fn rescale_null_count_and_total_sizes(&mut self) {
        let sample_count = self.sketch_sample_count;
        let total_row_count = self.count;
        // sample_count > total_row_count would scale stats *down* and corrupt
        // them — that's an invariant violation, not a real input.
        debug_assert!(
            total_row_count >= sample_count,
            "total_row_count ({}) must be >= sample_count ({})",
            total_row_count,
            sample_count,
        );
        // No sub-sampling: values are already exact.
        if sample_count == 0 {
            return;
        }
        // Scaling factor is 1.0; nothing to do.
        if total_row_count == sample_count {
            return;
        }
        for nc in &mut self.null_count {
            *nc = rescale_sampled_value(*nc, total_row_count, sample_count);
        }
        for ts in &mut self.total_sizes {
            *ts = rescale_sampled_value(*ts, total_row_count, sample_count);
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
        // Only build the per-region HLL sketches when NDV sub-sampling is on;
        // otherwise leave the fields empty so TiDB skips the f1 estimate. Each
        // region contributes an NDV HLL (all distinct values) and a singleton HLL
        // (values seen exactly once), which TiDB unions across regions. HLL is
        // fixed-size, so TiDB can retain one per region without O(regions) blowup.
        if self.build_singletons {
            let mut pb_hll_ndv = Vec::with_capacity(self.singleton_sketches.len());
            let mut pb_hll_singleton = Vec::with_capacity(self.singleton_sketches.len());
            for sketch in mem::take(&mut self.singleton_sketches) {
                let (ndv, singleton) = sketch.into_hlls(DEFAULT_HLL_PRECISION);
                pb_hll_ndv.push(ndv);
                pb_hll_singleton.push(singleton);
            }
            proto_collector.set_hll_ndv_sketch(pb_hll_ndv.into_iter().collect());
            proto_collector.set_hll_singleton_sketch(pb_hll_singleton.into_iter().collect());
        }
        proto_collector.set_sketch_sample_count(self.sketch_sample_count as i64);
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
        build_singletons: bool,
    ) -> BernoulliRowSampleCollector {
        BernoulliRowSampleCollector {
            base: BaseRowSampleCollector::new(
                max_fm_sketch_size,
                col_and_group_len,
                build_singletons,
            ),
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

    fn take_base(&mut self) -> BaseRowSampleCollector {
        mem::take(&mut self.base)
    }

    fn merge_collector(&mut self, mut other: Box<dyn RowSampleCollector>) -> Result<()> {
        let mut samples = other.take_bernoulli_samples().ok_or_else(|| {
            Error::Other("cannot merge reservoir samples into bernoulli samples".to_owned())
        })?;
        let sample_memory_usage = bernoulli_samples_memory_usage(&samples);
        self.samples.append(&mut samples);
        self.base.memory_usage += sample_memory_usage;
        self.base.report_memory_usage(false);

        let mut other_base = other.take_base();
        release_reported_memory_usage(&mut other_base);
        self.base.merge_from(&mut other_base);
        Ok(())
    }

    fn take_bernoulli_samples(&mut self) -> Option<BernoulliSamples> {
        Some(mem::take(&mut self.samples))
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
        build_singletons: bool,
    ) -> ReservoirRowSampleCollector {
        ReservoirRowSampleCollector {
            base: BaseRowSampleCollector::new(
                max_fm_sketch_size,
                col_and_group_len,
                build_singletons,
            ),
            samples: BinaryHeap::new(),
            max_sample_size,
        }
    }

    fn should_keep_weight(&self, weight: i64) -> bool {
        if self.max_sample_size == 0 {
            return false;
        }
        self.samples.len() < self.max_sample_size || self.samples.peek().unwrap().0.0 < weight
    }

    fn push_weighted_sample(&mut self, weight: i64, sample: Vec<Vec<u8>>) {
        if self.samples.len() >= self.max_sample_size {
            let (_, evicted) = self.samples.pop().unwrap().0;
            self.base.memory_usage -= row_sample_memory_usage(&evicted);
        }
        self.base.memory_usage += row_sample_memory_usage(&sample);
        self.samples.push(Reverse((weight, sample)));
    }
}

impl RowSampleCollector for ReservoirRowSampleCollector {
    fn mut_base(&mut self) -> &mut BaseRowSampleCollector {
        &mut self.base
    }

    fn take_base(&mut self) -> BaseRowSampleCollector {
        mem::take(&mut self.base)
    }

    fn merge_collector(&mut self, mut other: Box<dyn RowSampleCollector>) -> Result<()> {
        let samples = other.take_reservoir_samples().ok_or_else(|| {
            Error::Other("cannot merge bernoulli samples into reservoir samples".to_owned())
        })?;
        for Reverse((weight, sample)) in samples {
            if self.should_keep_weight(weight) {
                self.push_weighted_sample(weight, sample);
            }
        }
        self.base.report_memory_usage(false);

        let mut other_base = other.take_base();
        release_reported_memory_usage(&mut other_base);
        self.base.merge_from(&mut other_base);
        Ok(())
    }

    fn take_reservoir_samples(&mut self) -> Option<ReservoirSamples> {
        Some(mem::take(&mut self.samples))
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
        let cur_rng = self.base.rng.gen_range(0, i64::MAX);
        if self.should_keep_weight(cur_rng) {
            self.push_weighted_sample(cur_rng, data.to_vec());
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

pub(crate) struct SampleBuilder<S: Snapshot, F: KvFormat> {
    pub(crate) data: BatchTableScanExecutor<TikvStorage<SnapshotStore<S>>, F>,

    max_bucket_size: usize,
    max_sample_size: usize,
    max_fm_sketch_size: usize,
    cm_sketch_depth: usize,
    cm_sketch_width: usize,
    columns_info: Vec<tipb::ColumnInfo>,
    // NOTE: The field is currently used only when mixed analyze requests are received,
    // which happens exclusively when the statistics version is 1.
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

pub struct AnalyzeSamplingResult {
    row_sample_collector: Box<dyn RowSampleCollector>,
}

impl AnalyzeSamplingResult {
    fn new(row_sample_collector: Box<dyn RowSampleCollector>) -> AnalyzeSamplingResult {
        AnalyzeSamplingResult {
            row_sample_collector,
        }
    }

    pub(crate) fn merge_from(&mut self, other: AnalyzeSamplingResult) -> Result<()> {
        self.row_sample_collector
            .merge_collector(other.row_sample_collector)
    }

    pub(crate) fn write_to_bytes(self) -> Result<Vec<u8>> {
        let resp: tipb::AnalyzeColumnsResp = self.into();
        Ok(box_try!(resp.write_to_bytes()))
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
    fn test_singleton_sketch_into_hlls() {
        use super::super::hll::{DEFAULT_HLL_PRECISION, Hll};
        // HLL needs a uniformly distributed hash; mix sequential keys with splitmix64.
        fn mix(x: u64) -> u64 {
            let mut z = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
            z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
            z ^ (z >> 31)
        }
        let mut sketch = SingletonSketch::new();
        // 1000 values seen once (singletons) and 1000 seen twice (not singletons).
        for i in 0..1000u64 {
            sketch.insert_hash_value(mix(i));
        }
        for i in 1000..2000u64 {
            let h = mix(i);
            sketch.insert_hash_value(h);
            sketch.insert_hash_value(h);
        }
        let (ndv_proto, singleton_proto) = sketch.into_hlls(DEFAULT_HLL_PRECISION);
        // Singleton values are a subset of all distinct values, so the singleton
        // sketch is dominated register-by-register by the NDV sketch.
        assert_eq!(
            ndv_proto.get_registers().len(),
            singleton_proto.get_registers().len()
        );
        for (n, s) in ndv_proto
            .get_registers()
            .iter()
            .zip(singleton_proto.get_registers())
        {
            assert!(s <= n);
        }
        // NDV covers ~2000 distinct values; singletons ~1000.
        let ndv = Hll::from(&ndv_proto).count() as f64;
        let singleton = Hll::from(&singleton_proto).count() as f64;
        assert!((ndv - 2000.0).abs() / 2000.0 < 0.05, "ndv={ndv}");
        assert!(
            (singleton - 1000.0).abs() / 1000.0 < 0.05,
            "singleton={singleton}"
        );
    }

    #[test]
    fn test_rescale_sampled_value() {
        // Non-positive inputs short-circuit to 0.
        assert_eq!(rescale_sampled_value(0, 100, 10), 0);
        assert_eq!(rescale_sampled_value(-3, 100, 10), 0);
        assert_eq!(rescale_sampled_value(5, 100, 0), 0);
        // Exact scaling: 3 out of 10 sampled rows ⇒ 30 out of 100.
        assert_eq!(rescale_sampled_value(3, 100, 10), 30);
        // Round-half-up: (2*7 + 5/2) / 5 = 16/5 = 3 (vs truncated 2).
        assert_eq!(rescale_sampled_value(2, 7, 5), 3);
        // sample_count == total_row_count is normally short-circuited by the
        // caller, but the helper still returns the input unchanged.
        assert_eq!(rescale_sampled_value(5, 5, 5), 5);
    }

    #[test]
    fn test_effective_bernoulli_sample_rate_after_preselection() {
        // No row-key pre-selection: keep SAMPLERATE unchanged.
        assert_eq!(
            effective_bernoulli_sample_rate_after_preselection(0.1, 1.0),
            0.1
        );
        // NDVRATE selected 10% of rows. SAMPLERATE is also 10%, so every
        // pre-selected row should be sent as a row sample rather than sampled
        // again at 10%, which would double-sample down to 1%.
        assert_eq!(
            effective_bernoulli_sample_rate_after_preselection(0.1, 10.0),
            1.0
        );
        // NDVRATE selected 10% of rows but SAMPLERATE asks for 5%. Sampling half
        // of the pre-selected rows preserves a 5% marginal row-sample rate.
        assert_eq!(
            effective_bernoulli_sample_rate_after_preselection(0.05, 10.0),
            0.5
        );
        // If SAMPLERATE exceeds the NDV-selected fraction, the NDV pre-filter is
        // the cap: all selected rows become row samples.
        assert_eq!(
            effective_bernoulli_sample_rate_after_preselection(0.5, 10.0),
            1.0
        );
    }

    #[test]
    fn test_rescale_null_count_and_total_sizes() {
        let mut base = BaseRowSampleCollector::new(8, 2, true);
        base.count = 100;
        base.sketch_sample_count = 10;
        base.null_count = vec![2, 0];
        base.total_sizes = vec![50, 30];
        base.rescale_null_count_and_total_sizes();
        // Scaling factor 10x.
        assert_eq!(base.null_count, vec![20, 0]);
        assert_eq!(base.total_sizes, vec![500, 300]);

        // sketch_sample_count == 0 ⇒ no-op (values exact already).
        let mut base = BaseRowSampleCollector::new(8, 1, true);
        base.count = 100;
        base.null_count = vec![7];
        base.total_sizes = vec![42];
        base.rescale_null_count_and_total_sizes();
        assert_eq!(base.null_count, vec![7]);
        assert_eq!(base.total_sizes, vec![42]);

        // sketch_sample_count == count ⇒ no-op (scale factor 1.0).
        let mut base = BaseRowSampleCollector::new(8, 1, true);
        base.count = 50;
        base.sketch_sample_count = 50;
        base.null_count = vec![3];
        base.total_sizes = vec![17];
        base.rescale_null_count_and_total_sizes();
        assert_eq!(base.null_count, vec![3]);
        assert_eq!(base.total_sizes, vec![17]);
    }

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
            let mut collector = ReservoirRowSampleCollector::new(sample_num, 1000, 1, true);
            for row in &nums {
                collector.sampling(std::slice::from_ref(row));
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
                BernoulliRowSampleCollector::new(sample_num as f64 / row_num as f64, 1000, 1, true);
            for row in &nums {
                collector.sampling(std::slice::from_ref(row));
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
            let mut collector = ReservoirRowSampleCollector::new(sample_num, 1000, 1, true);
            for row in &nums {
                collector.sampling(std::slice::from_ref(row));
            }
            assert_eq!(collector.samples.len(), 0);
        }
        {
            // Test for BernoulliRowSampleCollector
            let mut collector =
                BernoulliRowSampleCollector::new(sample_num as f64 / row_num as f64, 1000, 1, true);
            for row in &nums {
                collector.sampling(std::slice::from_ref(row));
            }
            assert_eq!(collector.samples.len(), 0);
        }
    }

    #[test]
    fn test_build_singletons_gate() {
        // ndv_rate >= 1 maps to build_singletons = false: only the FM sketch is
        // built; the per-region HLL fields stay empty so the work and bytes are saved.
        let mut base = BaseRowSampleCollector::new(1000, 1, false);
        base.fm_sketches[0].insert(b"x");
        let mut proto = tipb::RowSampleCollector::default();
        base.fill_proto(&mut proto);
        assert_eq!(proto.get_fm_sketch().len(), 1);
        assert!(proto.get_hll_ndv_sketch().is_empty());
        assert!(proto.get_hll_singleton_sketch().is_empty());

        // ndv_rate < 1 maps to build_singletons = true: HLL sketches are emitted
        // (one NDV + one singleton per column) and no FM sketch is built.
        let mut base = BaseRowSampleCollector::new(1000, 1, true);
        base.singleton_sketches[0].insert(b"x");
        let mut proto = tipb::RowSampleCollector::default();
        base.fill_proto(&mut proto);
        assert!(proto.get_fm_sketch().is_empty());
        assert_eq!(proto.get_hll_ndv_sketch().len(), 1);
        assert_eq!(proto.get_hll_singleton_sketch().len(), 1);
    }

    fn sorted_hashset(sketch: &tipb::FmSketch) -> Vec<u64> {
        let mut hashes = sketch.get_hashset().to_vec();
        hashes.sort_unstable();
        hashes
    }

    fn test_sampling_result(
        count: u64,
        null_count: i64,
        total_size: i64,
        sample_weights: &[i64],
        ndv_hashes: &[u64],
    ) -> AnalyzeSamplingResult {
        let mut collector = ReservoirRowSampleCollector::new(2, 1000, 1, false);
        collector.base.count = count;
        collector.base.null_count[0] = null_count;
        collector.base.total_sizes[0] = total_size;
        for hash in ndv_hashes {
            collector.base.fm_sketches[0].insert_hash_value(*hash);
        }
        for weight in sample_weights {
            collector.push_weighted_sample(*weight, vec![vec![*weight as u8]]);
        }
        AnalyzeSamplingResult::new(Box::new(collector))
    }

    fn test_singleton_sampling_result(
        count: u64,
        sketch_sample_count: u64,
        once_hashes: &[u64],
        multi_hashes: &[u64],
    ) -> AnalyzeSamplingResult {
        let mut collector = ReservoirRowSampleCollector::new(2, 1000, 1, true);
        collector.base.count = count;
        collector.base.sketch_sample_count = sketch_sample_count;
        for hash in once_hashes {
            collector.base.singleton_sketches[0].insert_hash_value(*hash);
        }
        for hash in multi_hashes {
            collector.base.singleton_sketches[0].insert_hash_value(*hash);
            collector.base.singleton_sketches[0].insert_hash_value(*hash);
        }
        AnalyzeSamplingResult::new(Box::new(collector))
    }

    #[test]
    fn test_analyze_sampling_result_merge() {
        let a = 10;
        let b = 20;
        let c = 30;
        let mut result = test_sampling_result(2, 1, 10, &[1, 3], &[a, b]);
        result
            .merge_from(test_sampling_result(2, 2, 20, &[4], &[a, c]))
            .unwrap();

        let resp: tipb::AnalyzeColumnsResp = result.into();
        let collector = resp.get_row_collector();
        assert_eq!(collector.get_count(), 4);
        assert_eq!(collector.get_null_counts(), &[3]);
        assert_eq!(collector.get_total_size(), &[30]);

        let mut sample_weights: Vec<_> = collector
            .get_samples()
            .iter()
            .map(|sample| sample.get_weight())
            .collect();
        sample_weights.sort_unstable();
        assert_eq!(sample_weights, vec![3, 4]);
        assert_eq!(sorted_hashset(&collector.get_fm_sketch()[0]), vec![a, b, c]);
    }

    #[test]
    fn test_analyze_sampling_result_merge_singleton_sketches() {
        // Pick hashes with distinct HLL register indexes under p=14 so the
        // tiny-set linear-count estimates are exact.
        let a = 1u64 << 50;
        let b = 2u64 << 50;
        let c = 3u64 << 50;
        let d = 4u64 << 50;

        let mut result = test_singleton_sampling_result(2, 2, &[a, b], &[]);
        result
            .merge_from(test_singleton_sampling_result(3, 3, &[a, c], &[d]))
            .unwrap();

        let resp: tipb::AnalyzeColumnsResp = result.into();
        let collector = resp.get_row_collector();
        assert_eq!(collector.get_count(), 5);
        assert_eq!(collector.get_sketch_sample_count(), 5);
        assert!(collector.get_fm_sketch().is_empty());
        assert_eq!(collector.get_hll_ndv_sketch().len(), 1);
        assert_eq!(collector.get_hll_singleton_sketch().len(), 1);

        // NDV is {a,b,c,d}; singleton values in the merged batch scope are
        // {b,c}. `a` appears once in each input and `d` appears multiple times
        // in the second input, so neither is a singleton after merge.
        assert_eq!(Hll::from(&collector.get_hll_ndv_sketch()[0]).count(), 4);
        assert_eq!(
            Hll::from(&collector.get_hll_singleton_sketch()[0]).count(),
            2
        );
    }
}

#[cfg(test)]
mod benches {
    use tidb_query_datatype::{
        EvalType, FieldTypeTp,
        codec::{
            batch::LazyBatchColumn,
            collation::{Collator, collator::CollatorUtf8Mb4Bin},
        },
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
        let mut collector = BaseRowSampleCollector::new(10000, 4, true);
        let (column_vals, collation_key_vals, columns_info, _) = prepare_arguments();
        b.iter(|| {
            collector.collect_column(&column_vals, &collation_key_vals, &columns_info);
        })
    }

    #[bench]
    fn bench_collect_column_group(b: &mut test::Bencher) {
        let mut collector = BaseRowSampleCollector::new(10000, 4, true);
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
