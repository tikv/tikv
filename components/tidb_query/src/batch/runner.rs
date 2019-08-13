// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use kvproto::coprocessor::KeyRange;
use tipb::{self, ExecType, ExecutorExecutionSummary};
use tipb::{Chunk, DagRequest, SelectResponse};

use tikv_util::deadline::Deadline;

use super::executors::*;
use super::interface::{BatchExecutor, ExecuteStats};
use crate::execute_stats::*;
use crate::expr::EvalConfig;
use crate::metrics::*;
use crate::storage::Storage;
use crate::Result;

// TODO: The value is chosen according to some very subjective experience, which is not tuned
// carefully. We need to benchmark to find a best value. Also we may consider accepting this value
// from TiDB side.
const BATCH_INITIAL_SIZE: usize = 32;

// TODO: This value is chosen based on MonetDB/X100's research without our own benchmarks.
pub const BATCH_MAX_SIZE: usize = 1024;

// TODO: Maybe there can be some better strategy. Needs benchmarks and tunes.
const BATCH_GROW_FACTOR: usize = 2;

pub struct BatchExecutorsRunner<SS> {
    /// The deadline of this handler. For each check point (e.g. each iteration) we need to check
    /// whether or not the deadline is exceeded and break the process if so.
    // TODO: Deprecate it using a better deadline mechanism.
    deadline: Deadline,

    out_most_executor: Box<dyn BatchExecutor<StorageStats = SS>>,

    /// The offset of the columns need to be outputted. For example, TiDB may only needs a subset
    /// of the columns in the result so that unrelated columns don't need to be encoded and
    /// returned back.
    output_offsets: Vec<u32>,

    config: Arc<EvalConfig>,

    /// Whether or not execution summary need to be collected.
    collect_exec_summary: bool,

    exec_stats: ExecuteStats,
}

// We assign a dummy type `()` so that we can omit the type when calling `check_supported`.
impl BatchExecutorsRunner<()> {
    /// Given a list of executor descriptors and checks whether all executor descriptors can
    /// be used to build batch executors.
    pub fn check_supported(exec_descriptors: &[tipb::Executor]) -> Result<()> {
        for ed in exec_descriptors {
            match ed.get_tp() {
                ExecType::TypeTableScan => {
                    let descriptor = ed.get_tbl_scan();
                    BatchTableScanExecutor::check_supported(&descriptor)
                        .map_err(|e| other_err!("BatchTableScanExecutor: {}", e))?;
                }
                ExecType::TypeIndexScan => {
                    let descriptor = ed.get_idx_scan();
                    BatchIndexScanExecutor::check_supported(&descriptor)
                        .map_err(|e| other_err!("BatchIndexScanExecutor: {}", e))?;
                }
                ExecType::TypeSelection => {
                    let descriptor = ed.get_selection();
                    BatchSelectionExecutor::check_supported(&descriptor)
                        .map_err(|e| other_err!("BatchSelectionExecutor: {}", e))?;
                }
                ExecType::TypeAggregation | ExecType::TypeStreamAgg
                    if ed.get_aggregation().get_group_by().is_empty() =>
                {
                    let descriptor = ed.get_aggregation();
                    BatchSimpleAggregationExecutor::check_supported(&descriptor)
                        .map_err(|e| other_err!("BatchSimpleAggregationExecutor: {}", e))?;
                }
                ExecType::TypeAggregation => {
                    let descriptor = ed.get_aggregation();
                    if BatchFastHashAggregationExecutor::check_supported(&descriptor).is_err() {
                        BatchSlowHashAggregationExecutor::check_supported(&descriptor)
                            .map_err(|e| other_err!("BatchSlowHashAggregationExecutor: {}", e))?;
                    }
                }
                ExecType::TypeStreamAgg => {
                    // Note: We won't check whether the source of stream aggregation is in order.
                    //       It is undefined behavior if the source is unordered.
                    let descriptor = ed.get_aggregation();
                    BatchStreamAggregationExecutor::check_supported(&descriptor)
                        .map_err(|e| other_err!("BatchStreamAggregationExecutor: {}", e))?;
                }
                ExecType::TypeLimit => {}
                ExecType::TypeTopN => {
                    let descriptor = ed.get_top_n();
                    BatchTopNExecutor::check_supported(&descriptor)
                        .map_err(|e| other_err!("BatchTopNExecutor: {}", e))?;
                }
            }
        }

        Ok(())
    }
}

pub fn build_executors<S: Storage + 'static, C: ExecSummaryCollector + 'static>(
    executor_descriptors: Vec<tipb::Executor>,
    storage: S,
    ranges: Vec<KeyRange>,
    config: Arc<EvalConfig>,
) -> Result<Box<dyn BatchExecutor<StorageStats = S::Statistics>>> {
    let mut executor_descriptors = executor_descriptors.into_iter();
    let mut first_ed = executor_descriptors
        .next()
        .ok_or_else(|| other_err!("No executors"))?;

    let mut executor: Box<dyn BatchExecutor<StorageStats = S::Statistics>>;
    let mut summary_slot_index = 0;

    match first_ed.get_tp() {
        ExecType::TypeTableScan => {
            // TODO: Use static metrics.
            COPR_EXECUTOR_COUNT
                .with_label_values(&["batch_table_scan"])
                .inc();

            let mut descriptor = first_ed.take_tbl_scan();
            let columns_info = descriptor.take_columns().into();
            executor = Box::new(
                BatchTableScanExecutor::new(
                    storage,
                    config.clone(),
                    columns_info,
                    ranges,
                    descriptor.get_desc(),
                )?
                .with_summary_collector(C::new(summary_slot_index)),
            );
        }
        ExecType::TypeIndexScan => {
            COPR_EXECUTOR_COUNT
                .with_label_values(&["batch_index_scan"])
                .inc();

            let mut descriptor = first_ed.take_idx_scan();
            let columns_info = descriptor.take_columns().into();
            executor = Box::new(
                BatchIndexScanExecutor::new(
                    storage,
                    config.clone(),
                    columns_info,
                    ranges,
                    descriptor.get_desc(),
                    descriptor.get_unique(),
                )?
                .with_summary_collector(C::new(summary_slot_index)),
            );
        }
        _ => {
            return Err(other_err!(
                "Unexpected first executor {:?}",
                first_ed.get_tp()
            ));
        }
    }

    for mut ed in executor_descriptors {
        summary_slot_index += 1;

        let new_executor: Box<dyn BatchExecutor<StorageStats = S::Statistics>> = match ed.get_tp() {
            ExecType::TypeSelection => {
                COPR_EXECUTOR_COUNT
                    .with_label_values(&["batch_selection"])
                    .inc();

                Box::new(
                    BatchSelectionExecutor::new(
                        config.clone(),
                        executor,
                        ed.take_selection().take_conditions().into(),
                    )?
                    .with_summary_collector(C::new(summary_slot_index)),
                )
            }
            ExecType::TypeAggregation | ExecType::TypeStreamAgg
                if ed.get_aggregation().get_group_by().is_empty() =>
            {
                COPR_EXECUTOR_COUNT
                    .with_label_values(&["batch_simple_aggr"])
                    .inc();

                Box::new(
                    BatchSimpleAggregationExecutor::new(
                        config.clone(),
                        executor,
                        ed.mut_aggregation().take_agg_func().into(),
                    )?
                    .with_summary_collector(C::new(summary_slot_index)),
                )
            }
            ExecType::TypeAggregation => {
                if BatchFastHashAggregationExecutor::check_supported(&ed.get_aggregation()).is_ok()
                {
                    COPR_EXECUTOR_COUNT
                        .with_label_values(&["batch_fast_hash_aggr"])
                        .inc();

                    Box::new(
                        BatchFastHashAggregationExecutor::new(
                            config.clone(),
                            executor,
                            ed.mut_aggregation().take_group_by().into(),
                            ed.mut_aggregation().take_agg_func().into(),
                        )?
                        .with_summary_collector(C::new(summary_slot_index)),
                    )
                } else {
                    COPR_EXECUTOR_COUNT
                        .with_label_values(&["batch_slow_hash_aggr"])
                        .inc();

                    Box::new(
                        BatchSlowHashAggregationExecutor::new(
                            config.clone(),
                            executor,
                            ed.mut_aggregation().take_group_by().into(),
                            ed.mut_aggregation().take_agg_func().into(),
                        )?
                        .with_summary_collector(C::new(summary_slot_index)),
                    )
                }
            }
            ExecType::TypeStreamAgg => {
                COPR_EXECUTOR_COUNT
                    .with_label_values(&["batch_stream_aggr"])
                    .inc();

                Box::new(
                    BatchStreamAggregationExecutor::new(
                        config.clone(),
                        executor,
                        ed.mut_aggregation().take_group_by().into(),
                        ed.mut_aggregation().take_agg_func().into(),
                    )?
                    .with_summary_collector(C::new(summary_slot_index)),
                )
            }
            ExecType::TypeLimit => {
                COPR_EXECUTOR_COUNT
                    .with_label_values(&["batch_limit"])
                    .inc();

                Box::new(
                    BatchLimitExecutor::new(executor, ed.get_limit().get_limit() as usize)?
                        .with_summary_collector(C::new(summary_slot_index)),
                )
            }
            ExecType::TypeTopN => {
                COPR_EXECUTOR_COUNT
                    .with_label_values(&["batch_top_n"])
                    .inc();

                let mut d = ed.take_top_n();
                let order_bys = d.get_order_by().len();
                let mut order_exprs_def = Vec::with_capacity(order_bys);
                let mut order_is_desc = Vec::with_capacity(order_bys);
                for mut item in d.take_order_by().into_iter() {
                    order_exprs_def.push(item.take_expr());
                    order_is_desc.push(item.get_desc());
                }

                Box::new(
                    BatchTopNExecutor::new(
                        config.clone(),
                        executor,
                        order_exprs_def,
                        order_is_desc,
                        d.get_limit() as usize,
                    )?
                    .with_summary_collector(C::new(summary_slot_index)),
                )
            }
            _ => {
                return Err(other_err!(
                    "Unexpected non-first executor {:?}",
                    ed.get_tp()
                ));
            }
        };
        executor = new_executor;
    }

    Ok(executor)
}

impl<SS: 'static> BatchExecutorsRunner<SS> {
    pub fn from_request<S: Storage<Statistics = SS> + 'static>(
        mut req: DagRequest,
        ranges: Vec<KeyRange>,
        storage: S,
        deadline: Deadline,
    ) -> Result<Self> {
        let executors_len = req.get_executors().len();
        let collect_exec_summary = req.get_collect_execution_summaries();
        let config = Arc::new(EvalConfig::from_request(&req)?);

        let out_most_executor = if collect_exec_summary {
            build_executors::<_, ExecSummaryCollectorEnabled>(
                req.take_executors().into(),
                storage,
                ranges,
                config.clone(),
            )?
        } else {
            build_executors::<_, ExecSummaryCollectorDisabled>(
                req.take_executors().into(),
                storage,
                ranges,
                config.clone(),
            )?
        };

        // Check output offsets
        let output_offsets = req.take_output_offsets();
        let schema_len = out_most_executor.schema().len();
        for offset in &output_offsets {
            if (*offset as usize) >= schema_len {
                return Err(other_err!(
                    "Invalid output offset (schema has {} columns, access index {})",
                    schema_len,
                    offset
                ));
            }
        }

        let exec_stats = ExecuteStats::new(if collect_exec_summary {
            executors_len
        } else {
            0 // Avoid allocation for executor summaries when it is not needed
        });

        Ok(Self {
            deadline,
            out_most_executor,
            output_offsets,
            config,
            collect_exec_summary,
            exec_stats,
        })
    }

    pub fn handle_request(&mut self) -> Result<SelectResponse> {
        let mut chunks = vec![];
        let mut batch_size = BATCH_INITIAL_SIZE;
        let mut warnings = self.config.new_eval_warnings();

        loop {
            self.deadline.check()?;

            let mut result = self.out_most_executor.next_batch(batch_size);

            let is_drained;

            // Check error first, because it means that we should directly respond error.
            match result.is_drained {
                Err(e) => return Err(e),
                Ok(f) => is_drained = f,
            }

            // We will only get warnings limited by max_warning_count. Note that in future we
            // further want to ignore warnings from unused rows. See TODOs in the `result.warnings`
            // field.
            warnings.merge(&mut result.warnings);

            // Notice that logical rows len == 0 doesn't mean that it is drained.
            if !result.logical_rows.is_empty() {
                assert_eq!(
                    result.physical_columns.columns_len(),
                    self.out_most_executor.schema().len()
                );
                let mut chunk = Chunk::default();
                {
                    let data = chunk.mut_rows_data();
                    data.reserve(
                        result
                            .physical_columns
                            .maximum_encoded_size(&result.logical_rows, &self.output_offsets)?,
                    );
                    // Although `schema()` can be deeply nested, it is ok since we process data in
                    // batch.
                    result.physical_columns.encode(
                        &result.logical_rows,
                        &self.output_offsets,
                        self.out_most_executor.schema(),
                        data,
                    )?;
                }
                chunks.push(chunk);
            }

            if is_drained {
                self.out_most_executor
                    .collect_exec_stats(&mut self.exec_stats);

                let mut sel_resp = SelectResponse::default();
                sel_resp.set_chunks(chunks.into());
                // TODO: output_counts should not be i64. Let's fix it in Coprocessor DAG V2.
                sel_resp.set_output_counts(
                    self.exec_stats
                        .scanned_rows_per_range
                        .iter()
                        .map(|v| *v as i64)
                        .collect(),
                );

                if self.collect_exec_summary {
                    let summaries = self
                        .exec_stats
                        .summary_per_executor
                        .iter()
                        .map(|summary| {
                            let mut ret = ExecutorExecutionSummary::default();
                            ret.set_num_iterations(summary.num_iterations as u64);
                            ret.set_num_produced_rows(summary.num_produced_rows as u64);
                            ret.set_time_processed_ns(summary.time_processed_ns as u64);
                            ret
                        })
                        .collect::<Vec<_>>();
                    sel_resp.set_execution_summaries(summaries.into());
                }

                sel_resp.set_warnings(warnings.warnings.into());
                sel_resp.set_warning_count(warnings.warning_cnt as i64);

                // In case of this function is called multiple times.
                self.exec_stats.clear();

                return Ok(sel_resp);
            }

            // Grow batch size
            if batch_size < BATCH_MAX_SIZE {
                batch_size *= BATCH_GROW_FACTOR;
                if batch_size > BATCH_MAX_SIZE {
                    batch_size = BATCH_MAX_SIZE
                }
            }
        }
    }

    pub fn collect_storage_stats(&mut self, dest: &mut SS) {
        self.out_most_executor.collect_storage_stats(dest);
    }
}
