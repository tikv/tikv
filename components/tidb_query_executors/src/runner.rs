// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::{convert::TryFrom, iter, sync::Arc};

use api_version::KvFormat;
use fail::fail_point;
use itertools::Itertools;
use kvproto::coprocessor::KeyRange;
use protobuf::Message;
use tidb_query_common::{
    Result,
    execute_stats::ExecSummary,
    metrics::*,
    storage::{IntervalRange, RegionStorageAccessor, Storage},
};
use tidb_query_datatype::{
    EvalType, FieldTypeAccessor,
    codec::table::IntHandle,
    expr::{EvalConfig, EvalContext, EvalWarnings},
};
use tikv_util::{
    deadline::Deadline,
    metrics::{NON_TXN_COMMAND_THROTTLE_TIME_COUNTER_VEC_STATIC, ThrottleType},
    quota_limiter::QuotaLimiter,
};
use tipb::{
    self, Chunk, DagRequest, EncodeType, ExecType, ExecutorExecutionSummary, FieldType,
    SelectResponse, StreamResponse,
};

use super::{
    interface::{BatchExecIsDrain, BatchExecutor, ExecuteStats},
    *,
};

// TODO: The value is chosen according to some very subjective experience, which
// is not tuned carefully. We need to benchmark to find a best value. Also we
// may consider accepting this value from TiDB side.
const BATCH_INITIAL_SIZE: usize = 32;

// TODO: This value is chosen based on MonetDB/X100's research without our own
// benchmarks.
pub use tidb_query_expr::types::BATCH_MAX_SIZE;

use crate::{index_lookup_executor::build_index_lookup_executor, interface::BatchExecuteResult};

// TODO: Maybe there can be some better strategy. Needs benchmarks and tunes.
const BATCH_GROW_FACTOR: usize = 2;

#[derive(Clone, Debug, PartialEq)]
struct IntermediateOutputChannel {
    encode_type: EncodeType,
    output_offsets: Vec<u32>,
    schema: Vec<FieldType>,
}

pub struct BatchExecutorsRunner<SS> {
    /// The deadline of this handler. For each check point (e.g. each iteration)
    /// we need to check whether or not the deadline is exceeded and break
    /// the process if so.
    // TODO: Deprecate it using a better deadline mechanism.
    deadline: Deadline,

    out_most_executor: Box<dyn BatchExecutor<StorageStats = SS>>,

    /// The offset of the columns need to be outputted. For example, TiDB may
    /// only needs a subset of the columns in the result so that unrelated
    /// columns don't need to be encoded and returned back.
    output_offsets: Vec<u32>,

    config: Arc<EvalConfig>,

    /// Whether or not execution summary need to be collected.
    collect_exec_summary: bool,

    exec_stats: ExecuteStats,

    /// Maximum rows to return in batch stream mode.
    stream_row_limit: usize,

    /// The encoding method for the response.
    /// Possible encoding methods are:
    /// 1. default: result is encoded row by row using datum format.
    /// 2. chunk: result is encoded column by column using chunk format.
    encode_type: EncodeType,

    /// If it's a paging request, paging_size indicates to the required size for
    /// current page.
    paging_size: Option<u64>,

    quota_limiter: Arc<QuotaLimiter>,

    /// Information for intermediate output channels.
    /// For example, the `BatchIndexLookupExecutor` may output the index scan
    /// results if the primary rows cannot be founded in the local store.
    intermediate_channels: Vec<IntermediateOutputChannel>,
    /// Reserved for intermediate results, used to avoid re-allocation.
    /// It is None when BatchExecutorsRunner is created
    /// and will be set to Some by the inner code by demand
    reserved_intermediate_results: Option<Vec<Vec<BatchExecuteResult>>>,
}

// We assign a dummy type `()` so that we can omit the type when calling
// `check_supported`.
impl BatchExecutorsRunner<()> {
    /// Given a list of executor descriptors and checks whether all executor
    /// descriptors can be used to build batch executors.
    pub fn check_supported(exec_descriptors: &[tipb::Executor]) -> Result<()> {
        for ed in exec_descriptors {
            match ed.get_tp() {
                ExecType::TypeTableScan => {
                    let descriptor = ed.get_tbl_scan();
                    BatchTableScanExecutor::check_supported(descriptor)
                        .map_err(|e| other_err!("BatchTableScanExecutor: {}", e))?;
                }
                ExecType::TypeIndexScan => {
                    let descriptor = ed.get_idx_scan();
                    BatchIndexScanExecutor::check_supported(descriptor)
                        .map_err(|e| other_err!("BatchIndexScanExecutor: {}", e))?;
                }
                ExecType::TypeSelection => {
                    let descriptor = ed.get_selection();
                    BatchSelectionExecutor::check_supported(descriptor)
                        .map_err(|e| other_err!("BatchSelectionExecutor: {}", e))?;
                }
                ExecType::TypeAggregation | ExecType::TypeStreamAgg
                    if ed.get_aggregation().get_group_by().is_empty() =>
                {
                    let descriptor = ed.get_aggregation();
                    BatchSimpleAggregationExecutor::check_supported(descriptor)
                        .map_err(|e| other_err!("BatchSimpleAggregationExecutor: {}", e))?;
                }
                ExecType::TypeAggregation => {
                    let descriptor = ed.get_aggregation();
                    if BatchFastHashAggregationExecutor::check_supported(descriptor).is_err() {
                        BatchSlowHashAggregationExecutor::check_supported(descriptor)
                            .map_err(|e| other_err!("BatchSlowHashAggregationExecutor: {}", e))?;
                    }
                }
                ExecType::TypeStreamAgg => {
                    // Note: We won't check whether the source of stream aggregation is in order.
                    //       It is undefined behavior if the source is unordered.
                    let descriptor = ed.get_aggregation();
                    BatchStreamAggregationExecutor::check_supported(descriptor)
                        .map_err(|e| other_err!("BatchStreamAggregationExecutor: {}", e))?;
                }
                ExecType::TypeLimit => {}
                ExecType::TypeTopN => {
                    let descriptor = ed.get_top_n();
                    BatchTopNExecutor::check_supported(descriptor)
                        .map_err(|e| other_err!("BatchTopNExecutor: {}", e))?;
                }
                ExecType::TypeProjection => {
                    let descriptor = ed.get_projection();
                    BatchProjectionExecutor::check_supported(descriptor)
                        .map_err(|e| other_err!("BatchProjectionExecutor: {}", e))?;
                }
                ExecType::TypeIndexLookUp => {
                    let descriptor = ed.get_index_lookup();
                    BatchIndexLookUpExecutor::check_supported(descriptor)
                        .map_err(|e| other_err!("BatchIndexLookUpExecutor: {}", e))?;
                }
                ExecType::TypeJoin => {
                    return Err(other_err!("Join executor not implemented"));
                }
                ExecType::TypeKill => {
                    return Err(other_err!("Kill executor not implemented"));
                }
                ExecType::TypeExchangeSender => {
                    return Err(other_err!("ExchangeSender executor not implemented"));
                }
                ExecType::TypeExchangeReceiver => {
                    return Err(other_err!("ExchangeReceiver executor not implemented"));
                }
                ExecType::TypePartitionTableScan => {
                    return Err(other_err!("PartitionTableScan executor not implemented"));
                }
                ExecType::TypeSort => {
                    return Err(other_err!("Sort executor not implemented"));
                }
                ExecType::TypeWindow => {
                    return Err(other_err!("Window executor not implemented"));
                }
                ExecType::TypeExpand => {
                    return Err(other_err!("Expand executor not implemented"));
                }
                ExecType::TypeExpand2 => {
                    return Err(other_err!("Expand2 executor not implemented"));
                }
                ExecType::TypeBroadcastQuery => {
                    return Err(other_err!("TypeBroadcastQuery executor not implemented"));
                }
                ExecType::TypeCteSink => {
                    return Err(other_err!("TypeCteSink executor not implemented"));
                }
                ExecType::TypeCteSource => {
                    return Err(other_err!("TypeCteSource executor not implemented"));
                }
            }
        }

        Ok(())
    }
}

#[inline]
fn is_arrow_encodable<'a>(mut schema: impl Iterator<Item = &'a FieldType>) -> bool {
    schema.all(|schema| EvalType::try_from(schema.as_accessor().tp()).is_ok())
}

#[allow(clippy::explicit_counter_loop)]
pub fn build_executors<S: Storage + 'static, F: KvFormat>(
    executor_descriptors: Vec<tipb::Executor>,
    intermediate_output_descriptors: &[tipb::IntermediateOutputChannel],
    storage: S,
    extra_storage_accessor: Option<impl RegionStorageAccessor<Storage = S> + 'static>,
    ranges: Vec<KeyRange>,
    config: Arc<EvalConfig>,
    is_scanned_range_aware: bool,
) -> Result<Box<dyn BatchExecutor<StorageStats = S::Statistics>>> {
    let executors_len = executor_descriptors.len();
    let mut executor_descriptors = executor_descriptors.into_iter();
    let mut first_ed = executor_descriptors
        .next()
        .ok_or_else(|| other_err!("No executors"))?;

    let mut summary_slot_index = 0;
    // Limit executor use this flag to check if its src is table/index scan.
    // Performance enhancement for plan like: limit 1 -> table/index scan.
    let mut is_src_scan_executor = true;

    let mut executor: Box<dyn BatchExecutor<StorageStats = S::Statistics>> = match first_ed.get_tp()
    {
        ExecType::TypeTableScan => {
            EXECUTOR_COUNT_METRICS.batch_table_scan.inc();

            let mut descriptor = first_ed.take_tbl_scan();
            let columns_info = descriptor.take_columns().into();
            let primary_column_ids = descriptor.take_primary_column_ids();
            let primary_prefix_column_ids = descriptor.take_primary_prefix_column_ids();

            Box::new(
                BatchTableScanExecutor::<_, F>::new(
                    storage,
                    config.clone(),
                    columns_info,
                    ranges,
                    primary_column_ids,
                    descriptor.get_desc(),
                    is_scanned_range_aware,
                    primary_prefix_column_ids,
                )?
                .collect_summary(summary_slot_index),
            )
        }
        ExecType::TypeIndexScan => {
            EXECUTOR_COUNT_METRICS.batch_index_scan.inc();

            let mut descriptor = first_ed.take_idx_scan();
            let columns_info = descriptor.take_columns().into();
            let primary_column_ids_len = descriptor.take_primary_column_ids().len();
            Box::new(
                BatchIndexScanExecutor::<_, F>::new(
                    storage,
                    config.clone(),
                    columns_info,
                    ranges,
                    primary_column_ids_len,
                    descriptor.get_desc(),
                    descriptor.get_unique(),
                    is_scanned_range_aware,
                )?
                .collect_summary(summary_slot_index),
            )
        }
        _ => {
            return Err(other_err!(
                "Unexpected first executor {:?}",
                first_ed.get_tp()
            ));
        }
    };

    let mut next_parent_index = if first_ed.get_parent_idx() > 0 {
        first_ed.get_parent_idx() as usize
    } else {
        1
    };

    let mut children = vec![];
    for item in executor_descriptors.enumerate() {
        summary_slot_index += 1;
        if summary_slot_index != next_parent_index {
            if children.is_empty() {
                children.reserve(next_parent_index - summary_slot_index);
            }
            children.push(item);
            continue;
        }

        let mut ed = item.1;

        executor = match ed.get_tp() {
            ExecType::TypeSelection => {
                EXECUTOR_COUNT_METRICS.batch_selection.inc();

                Box::new(
                    BatchSelectionExecutor::new(
                        config.clone(),
                        executor,
                        ed.take_selection().take_conditions().into(),
                    )?
                    .collect_summary(summary_slot_index),
                )
            }
            ExecType::TypeProjection => {
                EXECUTOR_COUNT_METRICS.batch_projection.inc();

                Box::new(
                    BatchProjectionExecutor::new(
                        config.clone(),
                        executor,
                        ed.take_projection().take_exprs().into(),
                    )?
                    .collect_summary(summary_slot_index),
                )
            }
            ExecType::TypeAggregation | ExecType::TypeStreamAgg
                if ed.get_aggregation().get_group_by().is_empty() =>
            {
                EXECUTOR_COUNT_METRICS.batch_simple_aggr.inc();

                Box::new(
                    BatchSimpleAggregationExecutor::new(
                        config.clone(),
                        executor,
                        ed.mut_aggregation().take_agg_func().into(),
                    )?
                    .collect_summary(summary_slot_index),
                )
            }
            ExecType::TypeAggregation => {
                if BatchFastHashAggregationExecutor::check_supported(ed.get_aggregation()).is_ok() {
                    EXECUTOR_COUNT_METRICS.batch_fast_hash_aggr.inc();

                    Box::new(
                        BatchFastHashAggregationExecutor::new(
                            config.clone(),
                            executor,
                            ed.mut_aggregation().take_group_by().into(),
                            ed.mut_aggregation().take_agg_func().into(),
                        )?
                        .collect_summary(summary_slot_index),
                    )
                } else {
                    EXECUTOR_COUNT_METRICS.batch_slow_hash_aggr.inc();

                    Box::new(
                        BatchSlowHashAggregationExecutor::new(
                            config.clone(),
                            executor,
                            ed.mut_aggregation().take_group_by().into(),
                            ed.mut_aggregation().take_agg_func().into(),
                        )?
                        .collect_summary(summary_slot_index),
                    )
                }
            }
            ExecType::TypeStreamAgg => {
                EXECUTOR_COUNT_METRICS.batch_stream_aggr.inc();

                Box::new(
                    BatchStreamAggregationExecutor::new(
                        config.clone(),
                        executor,
                        ed.mut_aggregation().take_group_by().into(),
                        ed.mut_aggregation().take_agg_func().into(),
                    )?
                    .collect_summary(summary_slot_index),
                )
            }
            ExecType::TypeLimit => {
                EXECUTOR_COUNT_METRICS.batch_limit.inc();

                let mut d = ed.take_limit();

                // If there is partition_by field in Limit, we treat it as a
                // partitionTopN without order_by.
                // todo: refine those logics.
                let partition_by = d
                    .take_partition_by()
                    .into_iter()
                    .map(|mut item| item.take_expr())
                    .collect_vec();

                if partition_by.is_empty() {
                    Box::new(
                        BatchLimitExecutor::new(
                            executor,
                            d.get_limit() as usize,
                            is_src_scan_executor,
                        )?
                        .collect_summary(summary_slot_index),
                    )
                } else {
                    Box::new(
                        BatchPartitionTopNExecutor::new(
                            config.clone(),
                            executor,
                            partition_by,
                            vec![],
                            vec![],
                            d.get_limit() as usize,
                        )?
                        .collect_summary(summary_slot_index),
                    )
                }
            }
            ExecType::TypeTopN => {
                EXECUTOR_COUNT_METRICS.batch_top_n.inc();

                let mut d = ed.take_top_n();
                let order_bys = d.get_order_by().len();
                let mut order_exprs_def = Vec::with_capacity(order_bys);
                let mut order_is_desc = Vec::with_capacity(order_bys);
                for mut item in d.take_order_by().into_iter() {
                    order_exprs_def.push(item.take_expr());
                    order_is_desc.push(item.get_desc());
                }
                let partition_by = d
                    .take_partition_by()
                    .into_iter()
                    .map(|mut item| item.take_expr())
                    .collect_vec();

                if partition_by.is_empty() {
                    Box::new(
                        BatchTopNExecutor::new(
                            config.clone(),
                            executor,
                            order_exprs_def,
                            order_is_desc,
                            d.get_limit() as usize,
                        )?
                        .collect_summary(summary_slot_index),
                    )
                } else {
                    Box::new(
                        BatchPartitionTopNExecutor::new(
                            config.clone(),
                            executor,
                            partition_by,
                            order_exprs_def,
                            order_is_desc,
                            d.get_limit() as usize,
                        )?
                        .collect_summary(summary_slot_index),
                    )
                }
            }
            ExecType::TypeIndexLookUp => {
                EXECUTOR_COUNT_METRICS.batch_index_lookup.inc();
                if children.len() != 1 {
                    return Err(other_err!(
                        "IndexLookUp should have one child executor, but got {}",
                        children.len()
                    ));
                }

                let (_, mut child) = children.pop().unwrap();
                if child.get_tp() != ExecType::TypeTableScan {
                    return Err(other_err!(
                        "IndexLookup's child should be TableScan, but got {:?}",
                        child.get_tp()
                    ));
                }

                let channel_ids = intermediate_output_descriptors
                    .iter()
                    .enumerate()
                    .filter(|(_, ch)| {
                        ch.has_executor_idx() && ch.get_executor_idx() == summary_slot_index as u32
                    })
                    .map(|(i, _)| i)
                    .collect_vec();
                if channel_ids.len() != 1 {
                    return Err(other_err!(
                        "IndexLookup should have exactly one intermediate output channel, but got {}, {:?}",
                        channel_ids.len(),
                        channel_ids
                    ));
                }

                let tbl_scan = child.take_tbl_scan();
                if !tbl_scan.get_primary_column_ids().is_empty() {
                    return Err(other_err!("Common handle is not supported in index lookup"));
                } else {
                    let e = build_index_lookup_executor::<_, IntHandle, F>(
                        config.clone(),
                        executor,
                        ed.take_index_lookup(),
                        tbl_scan,
                        extra_storage_accessor.clone(),
                        channel_ids[0],
                        summary_slot_index - 1,
                    )?;
                    Box::new(e.collect_summary(summary_slot_index))
                }
            }
            _ => {
                return Err(other_err!(
                    "Unexpected non-first executor {:?}",
                    ed.get_tp()
                ));
            }
        };

        if !children.is_empty() {
            return Err(other_err!(
                "Children not handled for executor: {:?}, index: {}",
                ed.get_tp(),
                summary_slot_index
            ));
        }

        next_parent_index = match ed.get_parent_idx() as usize {
            // 0 means the natural next executor.
            0 => summary_slot_index + 1,
            parent_idx if parent_idx > summary_slot_index && parent_idx < executors_len => {
                parent_idx
            }
            parent_idx => {
                return Err(other_err!(
                    "Invalid parent index: {} for executor at index {}",
                    parent_idx,
                    summary_slot_index
                ));
            }
        };

        is_src_scan_executor = false;
    }

    Ok(executor)
}

impl<SS: 'static> BatchExecutorsRunner<SS> {
    pub fn from_request<S: Storage<Statistics = SS> + 'static, F: KvFormat>(
        mut req: DagRequest,
        ranges: Vec<KeyRange>,
        storage: S,
        extra_storage_accessor: Option<impl RegionStorageAccessor<Storage = S> + 'static>,
        deadline: Deadline,
        stream_row_limit: usize,
        is_streaming: bool,
        paging_size: Option<u64>,
        quota_limiter: Arc<QuotaLimiter>,
    ) -> Result<Self> {
        let executors_len = req.get_executors().len();
        let collect_exec_summary = req.get_collect_execution_summaries();
        let mut config = EvalConfig::from_request(&req)?;
        config.paging_size = paging_size;
        let config = Arc::new(config);
        let intermediate_output_descriptors = req.take_intermediate_output_channels();
        let out_most_executor = build_executors::<_, F>(
            req.take_executors().into(),
            &intermediate_output_descriptors,
            storage,
            extra_storage_accessor,
            ranges,
            config.clone(),
            is_streaming || paging_size.is_some(), /* For streaming and paging request,
                                                    * executors will continue scan from range
                                                    * end where last scan is finished */
        )?;

        let req_encode_type = req.get_encode_type();
        let get_encode_type_with_check =
            |schema: &[FieldType], output_offsets: &[u32]| -> Result<EncodeType> {
                // Check output offsets
                let schema_len = schema.len();
                for offset in output_offsets {
                    if (*offset as usize) >= schema_len {
                        return Err(other_err!(
                            "Invalid output offset (schema has {} columns, access index {})",
                            schema_len,
                            offset
                        ));
                    }
                }

                // Only check output schema field types
                let new_schema = output_offsets.iter().map(|&i| &schema[i as usize]);
                Ok(if !is_arrow_encodable(new_schema) {
                    EncodeType::TypeDefault
                } else {
                    req_encode_type
                })
            };

        let output_offsets = req.take_output_offsets();
        let encode_type = get_encode_type_with_check(out_most_executor.schema(), &output_offsets)?;
        let exec_stats = ExecuteStats::new(executors_len);
        let intermediate_channels = if !intermediate_output_descriptors.is_empty() {
            let mut channels = Vec::with_capacity(intermediate_output_descriptors.len());
            for (idx, mut ch) in intermediate_output_descriptors.into_iter().enumerate() {
                if !ch.has_executor_idx() || ch.get_executor_idx() >= executors_len as u32 {
                    return Err(other_err!(
                        "Invalid intermediate output channel idx: {}, executors len: {}",
                        if ch.has_executor_idx() {
                            format!("{}", ch.get_executor_idx())
                        } else {
                            "None".to_string()
                        },
                        executors_len
                    ));
                }

                let schema = out_most_executor.intermediate_schema(idx)?.to_vec();
                let encode_type = get_encode_type_with_check(&schema, ch.get_output_offsets())?;
                let output_offsets = ch.take_output_offsets();
                channels.push(IntermediateOutputChannel {
                    encode_type,
                    output_offsets,
                    schema,
                });
            }
            channels
        } else {
            vec![]
        };

        Ok(Self {
            deadline,
            out_most_executor,
            output_offsets,
            config,
            collect_exec_summary,
            exec_stats,
            stream_row_limit,
            encode_type,
            paging_size,
            quota_limiter,
            intermediate_channels,
            reserved_intermediate_results: None,
        })
    }

    fn batch_initial_size() -> usize {
        fail_point!("copr_batch_initial_size", |r| r
            .map_or(1, |e| e.parse().unwrap()));
        BATCH_INITIAL_SIZE
    }

    #[inline]
    fn set_response_intermediate_outputs(
        &self,
        sel_resp: &mut SelectResponse,
        chunks: Vec<Vec<Chunk>>,
    ) {
        sel_resp.set_intermediate_outputs(
            self.intermediate_channels
                .iter()
                .zip(chunks)
                .map(|(ch, chunks)| {
                    let mut inter_output = tipb::IntermediateOutput::default();
                    inter_output.set_encode_type(ch.encode_type);
                    inter_output.set_chunks(chunks.into());
                    inter_output
                })
                .collect(),
        )
    }

    /// handle_request returns the response of selection and an optional range,
    /// only paging request will return Some(IntervalRange),
    /// this should be used when calculating ranges of the next batch.
    /// IntervalRange records whole range scanned though there are gaps in multi
    /// ranges. e.g.: [(k1 -> k2), (k4 -> k5)] may got response (k1, k2, k4)
    /// with IntervalRange like (k1, k4).
    pub async fn handle_request(&mut self) -> Result<(SelectResponse, Option<IntervalRange>)> {
        let mut chunks = vec![];
        let mut batch_size = Self::batch_initial_size();
        let mut warnings = self.config.new_eval_warnings();
        let mut ctx = EvalContext::new(self.config.clone());
        let mut record_all = 0;
        let mut intermediate_output_chunks = if self.intermediate_channels.is_empty() {
            vec![]
        } else {
            iter::repeat_with(Vec::new)
                .take(self.intermediate_channels.len())
                .collect_vec()
        };

        loop {
            let mut chunk = Chunk::default();
            let mut sample = self.quota_limiter.new_sample(true);
            let (drained, record_len, read_bytes_len) = {
                let (cpu_time, res) = sample
                    .observe_cpu_async(self.internal_handle_request(
                        false,
                        batch_size,
                        &mut chunk,
                        &mut intermediate_output_chunks,
                        &mut warnings,
                        &mut ctx,
                    ))
                    .await;
                sample.add_cpu_time(cpu_time);
                res?
            };

            if read_bytes_len > 0 {
                sample.add_read_bytes(read_bytes_len);
            }

            let quota_delay = self.quota_limiter.consume_sample(sample, true).await;
            if !quota_delay.is_zero() {
                NON_TXN_COMMAND_THROTTLE_TIME_COUNTER_VEC_STATIC
                    .get(ThrottleType::dag)
                    .inc_by(quota_delay.as_micros() as u64);
            }

            if record_len > 0 {
                record_all += record_len;
            }

            if chunk.has_rows_data() {
                chunks.push(chunk);
            }

            if drained.stop() || self.paging_size.is_some_and(|p| record_all >= p as usize) {
                self.out_most_executor
                    .collect_exec_stats(&mut self.exec_stats);
                let range = if drained == BatchExecIsDrain::Drain {
                    None
                } else {
                    // It's not allowed to stop paging when BatchExecIsDrain::PagingDrain.
                    self.paging_size
                        .map(|_| self.out_most_executor.take_scanned_range())
                };

                let mut sel_resp = SelectResponse::default();
                sel_resp.set_chunks(chunks.into());
                sel_resp.set_encode_type(self.encode_type);
                if !self.intermediate_channels.is_empty() {
                    self.set_response_intermediate_outputs(
                        &mut sel_resp,
                        intermediate_output_chunks,
                    );
                }

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
                return Ok((sel_resp, range));
            }

            // Grow batch size
            grow_batch_size(&mut batch_size);
        }
    }

    pub async fn handle_streaming_request(
        &mut self,
    ) -> Result<(Option<(StreamResponse, IntervalRange)>, bool)> {
        if !self.intermediate_channels.is_empty() {
            return Err(other_err!(
                "Intermediate result is not supported in streaming request"
            ));
        }

        let mut warnings = self.config.new_eval_warnings();

        let (mut record_len, mut is_drained) = (0, false);
        let mut chunk = Chunk::default();
        let mut ctx = EvalContext::new(self.config.clone());
        let batch_size = self.stream_row_limit.min(BATCH_MAX_SIZE);

        // record count less than batch size and is not drained
        while record_len < self.stream_row_limit && !is_drained {
            let mut current_chunk = Chunk::default();
            // TODO: Streaming coprocessor on TiKV is just not enabled in TiDB now.
            let (drained, len, _) = self
                .internal_handle_request(
                    true,
                    batch_size.min(self.stream_row_limit - record_len),
                    &mut current_chunk,
                    &mut [],
                    &mut warnings,
                    &mut ctx,
                )
                .await?;
            chunk
                .mut_rows_data()
                .extend_from_slice(current_chunk.get_rows_data());
            record_len += len;
            is_drained = drained.stop();
        }

        if !is_drained || record_len > 0 {
            let range = self.out_most_executor.take_scanned_range();
            return self
                .make_stream_response(chunk, warnings)
                .map(|r| (Some((r, range)), is_drained));
        }
        Ok((None, true))
    }

    pub fn collect_storage_stats(&mut self, dest: &mut SS) {
        self.out_most_executor.collect_storage_stats(dest);
    }

    pub fn can_be_cached(&self) -> bool {
        self.out_most_executor.can_be_cached()
    }

    pub fn collect_scan_summary(&mut self, dest: &mut ExecSummary) {
        // Get the first executor which is always the scan executor
        if let Some(exec_stat) = self.exec_stats.summary_per_executor.first() {
            dest.clone_from(exec_stat);
        }
    }

    #[inline]
    fn consume_and_encode_intermediate_results(
        &mut self,
        chunks: &mut [Vec<Chunk>],
        ctx: &mut EvalContext,
        is_streaming: bool,
    ) -> Result<(usize, usize)> {
        if self.reserved_intermediate_results.is_none() {
            self.reserved_intermediate_results = Some(
                iter::repeat_with(Vec::new)
                    .take(self.intermediate_channels.len())
                    .collect(),
            );
        }

        let reserved_intermediate_results = self.reserved_intermediate_results.as_mut().unwrap();
        let mut intermediate_record_len = 0;
        let mut chk_byte_size = 0;
        self.out_most_executor
            .consume_and_fill_intermediate_results(reserved_intermediate_results)?;
        for (i, results) in reserved_intermediate_results.iter_mut().enumerate() {
            if results.is_empty() {
                continue;
            }

            let ch = &self.intermediate_channels[i];
            for mut r in results.drain(..) {
                let local_rows_len = r.logical_rows.len();
                if local_rows_len > 0 {
                    let mut chk = Chunk::default();
                    encode_result_to_chunk(
                        ctx,
                        is_streaming,
                        ch.encode_type,
                        &ch.schema,
                        &ch.output_offsets,
                        &mut r,
                        &mut chk,
                    )?;
                    intermediate_record_len += local_rows_len;
                    chk_byte_size += chk.get_rows_data().len();
                    chunks[i].push(chk);
                }
            }
        }
        Ok((intermediate_record_len, chk_byte_size))
    }

    async fn internal_handle_request(
        &mut self,
        is_streaming: bool,
        batch_size: usize,
        chunk: &mut Chunk,
        intermediate_chunks: &mut [Vec<Chunk>],
        warnings: &mut EvalWarnings,
        ctx: &mut EvalContext,
    ) -> Result<(BatchExecIsDrain, usize, usize)> {
        let mut record_len = 0;
        let mut read_bytes_len = 0;

        self.deadline.check()?;

        let mut result = self.out_most_executor.next_batch(batch_size).await;
        let is_drained = match result.is_drained {
            Err(err) => return Err(err),
            Ok(is_drained) => is_drained,
        };

        if !result.logical_rows.is_empty() {
            assert_eq!(
                result.physical_columns.columns_len(),
                self.out_most_executor.schema().len()
            );
            encode_result_to_chunk(
                ctx,
                is_streaming,
                self.encode_type,
                self.out_most_executor.schema(),
                &self.output_offsets,
                &mut result,
                chunk,
            )?;
            record_len += result.logical_rows.len();
            if chunk.has_rows_data() {
                read_bytes_len += chunk.get_rows_data().len();
            }
        }

        warnings.merge(&mut result.warnings);
        if !self.intermediate_channels.is_empty() {
            let (r_len, bytes_len) = self.consume_and_encode_intermediate_results(
                intermediate_chunks,
                ctx,
                is_streaming,
            )?;
            record_len += r_len;
            read_bytes_len += bytes_len;
        }
        Ok((is_drained, record_len, read_bytes_len))
    }

    fn make_stream_response(
        &mut self,
        chunk: Chunk,
        warnings: EvalWarnings,
    ) -> Result<StreamResponse> {
        self.out_most_executor
            .collect_exec_stats(&mut self.exec_stats);

        let mut s_resp = StreamResponse::default();
        s_resp.set_data(box_try!(chunk.write_to_bytes()));

        s_resp.set_output_counts(
            self.exec_stats
                .scanned_rows_per_range
                .iter()
                .map(|v| *v as i64)
                .collect(),
        );

        s_resp.set_warnings(warnings.warnings.into());
        s_resp.set_warning_count(warnings.warning_cnt as i64);

        self.exec_stats.clear();

        Ok(s_resp)
    }
}

#[inline]
fn encode_result_to_chunk(
    ctx: &mut EvalContext,
    is_streaming: bool,
    encode_type: EncodeType,
    schema: &[FieldType],
    output_offsets: &[u32],
    result: &mut BatchExecuteResult,
    chunk: &mut Chunk,
) -> Result<()> {
    let data = chunk.mut_rows_data();
    // Although `schema()` can be deeply nested, it is ok since we process data in
    // batch.
    if is_streaming || encode_type == EncodeType::TypeDefault {
        data.reserve(
            result
                .physical_columns
                .maximum_encoded_size(&result.logical_rows, output_offsets),
        );
        result
            .physical_columns
            .encode(&result.logical_rows, output_offsets, schema, data, ctx)?;
    } else {
        data.reserve(
            result
                .physical_columns
                .maximum_encoded_size_chunk(&result.logical_rows, output_offsets),
        );
        result.physical_columns.encode_chunk(
            &result.logical_rows,
            output_offsets,
            schema,
            data,
            ctx,
        )?;
    }

    Ok(())
}

#[inline]
fn batch_grow_factor() -> usize {
    fail_point!("copr_batch_grow_size", |r| r
        .map_or(1, |e| e.parse().unwrap()));
    BATCH_GROW_FACTOR
}

#[inline]
fn grow_batch_size(batch_size: &mut usize) {
    if *batch_size < BATCH_MAX_SIZE {
        *batch_size *= batch_grow_factor();
        if *batch_size > BATCH_MAX_SIZE {
            *batch_size = BATCH_MAX_SIZE
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{any::type_name, time::Duration};

    use api_version::ApiV1;
    use futures::executor::block_on;
    use kvproto::metapb::Region;
    use tidb_query_common::execute_stats::ExecSummaryCollectorEnabled;
    use tidb_query_datatype::{
        FieldTypeTp,
        codec::{
            Datum,
            batch::LazyBatchColumnVec,
            chunk,
            data_type::{Real, VectorValue},
            datum,
            mysql::{Time, TimeType},
        },
        expr::{Flag, SqlMode},
    };
    use tipb::*;
    use tipb_helper::ExprDefBuilder;

    use super::*;
    use crate::{
        index_lookup_executor::{AccessorTableTaskIterBuilder, IndexLayout, TableScanParams},
        interface::WithSummaryCollector,
        util::{
            mock_executor::{MockExecutor, MockRegionStorageAccessor, MockStorage},
            scan_executor::field_type_from_column_info,
        },
    };

    fn check_chunk(
        ctx: &mut EvalContext,
        expect_rows: Vec<Vec<Datum>>,
        chk: Chunk,
        field_types: &[FieldType],
        encode_type: EncodeType,
    ) {
        let mut expect_rows_iter = expect_rows.into_iter();
        let mut rows_data = chk.get_rows_data();
        assert!(!rows_data.is_empty());
        match encode_type {
            EncodeType::TypeDefault => {
                let mut datum_iter = datum::decode(&mut rows_data).unwrap().into_iter();
                for expect_row in expect_rows_iter {
                    let mut actual_row = Vec::with_capacity(field_types.len());
                    for field_type in field_types {
                        let mut datum = datum_iter.next().unwrap();
                        if field_type.tp() == FieldTypeTp::Date
                            || field_type.tp() == FieldTypeTp::DateTime
                            || field_type.tp() == FieldTypeTp::Timestamp
                        {
                            if let Datum::U64(n) = &mut datum {
                                let t = Time::from_packed_u64(
                                    ctx,
                                    *n,
                                    TimeType::try_from(field_type.tp()).unwrap(),
                                    0,
                                )
                                .unwrap();
                                datum = Datum::Time(t);
                            } else {
                                panic!("expect u64, got {:?}", datum);
                            }
                        }
                        actual_row.push(datum);
                    }
                    assert_eq!(expect_row, actual_row);
                }
                assert!(datum_iter.next().is_none());
            }
            EncodeType::TypeChunk => {
                let rows = chunk::Chunk::decode_for_test(&mut rows_data, field_types).unwrap();
                let row_iter = chunk::RowIterator::new(&rows);
                for row in row_iter {
                    assert_eq!(row.len(), field_types.len());
                    let actual_row = (0..field_types.len())
                        .map(|i| row.get_datum(i, &field_types[i]).unwrap())
                        .collect_vec();
                    let expect_row = expect_rows_iter.next().unwrap();
                    assert_eq!(expect_row, actual_row);
                }
                assert!(expect_rows_iter.next().is_none());
            }
            _ => {
                panic!("unsupported encode type {:?}", encode_type);
            }
        }
    }

    type BoxedExecutor = Box<dyn BatchExecutor<StorageStats = ()>>;

    type IndexScanExecutor = BatchIndexScanExecutor<MockStorage, ApiV1>;

    fn index_scan_descriptor(
        table_id: i64,
        index_id: i64,
        columns_info: Vec<ColumnInfo>,
    ) -> Executor {
        let mut exec = Executor::default();
        exec.set_tp(ExecType::TypeIndexScan);
        let mut idx_scan = IndexScan::default();
        idx_scan.set_table_id(table_id);
        idx_scan.set_index_id(index_id);
        idx_scan.set_columns(columns_info.into());
        exec.set_idx_scan(idx_scan);
        exec
    }

    fn table_scan_descriptor(table_id: i64, columns_info: Vec<ColumnInfo>) -> Executor {
        let mut exec = Executor::default();
        exec.set_tp(ExecType::TypeTableScan);
        let mut tbl_scan = TableScan::default();
        tbl_scan.set_table_id(table_id);
        tbl_scan.set_columns(columns_info.into());
        exec.set_tbl_scan(tbl_scan);
        exec
    }

    type IndexLookupExecutor = BatchIndexLookUpExecutor<
        MockStorage,
        BoxedExecutor,
        AccessorTableTaskIterBuilder<MockRegionStorageAccessor, IntHandle>,
        ApiV1,
    >;

    fn index_lookup_descriptor(index_handle_offsets: Vec<u32>) -> Executor {
        use tipb::{ExecType, IndexLookUp};
        let mut exec = Executor::default();
        exec.set_tp(ExecType::TypeIndexLookUp);
        let mut idx_lookup = IndexLookUp::default();
        idx_lookup.set_index_handle_offsets(index_handle_offsets);
        exec.set_index_lookup(idx_lookup);
        exec
    }

    type LimitExecutor = BatchLimitExecutor<BoxedExecutor>;

    fn limit_descriptor(limit: u64) -> Executor {
        let mut exec = Executor::default();
        exec.set_tp(ExecType::TypeLimit);
        let mut limit_exec = Limit::default();
        limit_exec.set_limit(limit);
        exec.set_limit(limit_exec);
        exec
    }

    type SelectionExecutor = BatchSelectionExecutor<BoxedExecutor>;

    fn selection_descriptor(conditions: Vec<Expr>) -> Executor {
        let mut exec = Executor::default();
        exec.set_tp(ExecType::TypeSelection);
        let mut selection = Selection::default();
        selection.set_conditions(conditions.into());
        exec.set_selection(selection);
        exec
    }

    type ProjectionExecutor = BatchProjectionExecutor<BoxedExecutor>;

    fn projection_descriptor(exprs: Vec<Expr>) -> Executor {
        let mut exec = Executor::default();
        exec.set_tp(ExecType::TypeProjection);
        let mut projection = Projection::default();
        projection.set_exprs(exprs.into());
        exec.set_projection(projection);
        exec
    }

    fn column_with_type(col_id: i64, tp: FieldTypeTp) -> ColumnInfo {
        let mut col = ColumnInfo::default();
        col.set_column_id(col_id);
        col.set_tp(tp as i32);
        col
    }

    fn cast_as_summary<T: BatchExecutor<StorageStats = ()>>(
        executor: Box<dyn BatchExecutor<StorageStats = ()>>,
    ) -> Box<WithSummaryCollector<ExecSummaryCollectorEnabled, T>> {
        unsafe {
            let exec = Box::into_raw(executor)
                as *mut WithSummaryCollector<ExecSummaryCollectorEnabled, T>;
            let summary = Box::from_raw(exec);
            assert_eq!(type_name::<T>(), summary.inner_type);
            summary
        }
    }

    fn build_runner_for_test(req: DagRequest) -> Result<BatchExecutorsRunner<()>> {
        BatchExecutorsRunner::from_request::<_, ApiV1>(
            req,
            vec![],
            MockStorage(Region::default(), vec![]),
            Some(MockRegionStorageAccessor::with_expect_mode()),
            Deadline::from_now(Duration::from_secs(300)),
            1024,
            false,
            None,
            Arc::new(QuotaLimiter::default()),
        )
    }

    fn build_simple_runner_for_test() -> BatchExecutorsRunner<()> {
        let mut dag = DagRequest::default();
        dag.set_executors(
            vec![table_scan_descriptor(
                1,
                vec![column_with_type(1, FieldTypeTp::Long)],
            )]
            .into(),
        );
        dag.set_output_offsets(vec![0]);
        dag.set_encode_type(EncodeType::TypeChunk);
        build_runner_for_test(dag).unwrap()
    }

    fn build_simple_result_for_test(cols: Vec<VectorValue>) -> BatchExecuteResult {
        let logical_rows = if cols.is_empty() {
            vec![]
        } else {
            (0..cols[0].len()).collect()
        };
        BatchExecuteResult {
            physical_columns: LazyBatchColumnVec::from(cols),
            logical_rows,
            warnings: EvalWarnings::default(),
            is_drained: Ok(BatchExecIsDrain::Remain),
        }
    }

    fn build_int_result_for_test(vals: Vec<i64>) -> BatchExecuteResult {
        build_simple_result_for_test(vec![VectorValue::Int(
            vals.iter().map(|&v| Some(v)).collect_vec().into(),
        )])
    }

    fn build_n_rows_int_result(base: i64, n: i64) -> BatchExecuteResult {
        build_int_result_for_test((0..n).map(|v| v + base).collect())
    }

    #[test]
    fn test_build_index_lookup_executors() {
        let index_columns = vec![
            column_with_type(2, FieldTypeTp::String),
            column_with_type(1, FieldTypeTp::LongLong),
        ];
        let table_columns = vec![
            column_with_type(1, FieldTypeTp::LongLong),
            column_with_type(2, FieldTypeTp::String),
        ];
        let descriptors = vec![
            index_scan_descriptor(123, 456, index_columns.clone()),
            {
                let mut descriptor = limit_descriptor(128);
                descriptor.set_parent_idx(3);
                descriptor
            },
            table_scan_descriptor(456, table_columns.clone()),
            index_lookup_descriptor(vec![1]),
            selection_descriptor(vec![
                ExprDefBuilder::column_ref(0, FieldTypeTp::LongLong).build(),
            ]),
            projection_descriptor(vec![
                ExprDefBuilder::column_ref(0, FieldTypeTp::LongLong).build(),
            ]),
        ];

        let intermediate_output_channels = vec![
            {
                let mut ch = tipb::IntermediateOutputChannel::default();
                ch.set_executor_idx(2);
                ch.set_output_offsets(vec![2, 3]);
                ch
            },
            {
                let mut ch = tipb::IntermediateOutputChannel::default();
                ch.set_executor_idx(3);
                ch.set_output_offsets(vec![2, 3]);
                ch
            },
        ];

        let mut cfg = EvalConfig::default();
        cfg.set_sql_mode(SqlMode::NO_ZERO_DATE | SqlMode::INVALID_DATES);
        cfg.set_flag(Flag::IN_UPDATE_OR_DELETE_STMT);
        cfg.set_div_precision_incr(1);

        fn build_executors_for_test(
            cfg: &EvalConfig,
            descriptors: Vec<Executor>,
            intermediate_output_channels: &[tipb::IntermediateOutputChannel],
        ) -> Result<Box<dyn BatchExecutor<StorageStats = ()>>> {
            build_executors::<_, ApiV1>(
                descriptors,
                intermediate_output_channels,
                MockStorage(Region::default(), vec![]),
                Some(MockRegionStorageAccessor::with_expect_mode()),
                vec![],
                Arc::new(cfg.clone()),
                false,
            )
        }

        let mut executor =
            build_executors_for_test(&cfg, descriptors.clone(), &intermediate_output_channels)
                .unwrap();

        // intermediate_schema 1 should work
        assert_eq!(
            executor.intermediate_schema(1).unwrap(),
            index_columns
                .iter()
                .map(field_type_from_column_info)
                .collect_vec()
                .as_slice()
        );

        // other intermediate_schema should return error
        let err = executor.intermediate_schema(0).unwrap_err();
        assert!(
            err.to_string()
                .contains("The intermediate schema is not found until root executor, index: 0")
        );
        let err = executor.intermediate_schema(2).unwrap_err();
        assert!(
            err.to_string()
                .contains("The intermediate schema is not found until root executor, index: 2")
        );

        let check_index_lookup_executor =
            |executor: &IndexLookupExecutor, table_scan_child_idx: usize| {
                // schema should use the table scan's schema
                assert_eq!(
                    executor.schema(),
                    table_columns
                        .iter()
                        .map(field_type_from_column_info)
                        .collect_vec()
                        .as_slice()
                );

                // cfg should equal
                let config = executor.config();
                assert_eq!(config.flag, cfg.flag);
                assert_eq!(config.div_precision_increment, cfg.div_precision_increment);
                assert_eq!(config.sql_mode, cfg.sql_mode);

                // check table scan params
                assert_eq!(
                    &TableScanParams {
                        columns_info: table_columns.clone(),
                        primary_column_ids: vec![],
                        primary_prefix_column_ids: vec![],
                    },
                    executor.get_table_scan_params()
                );

                // check table task iter builder
                let builder = executor.get_table_task_builder().unwrap();
                assert_eq!(456, builder.table_id());
                assert_eq!(
                    &IndexLayout {
                        handle_types: vec![field_type_from_column_info(
                            index_columns.get(1).unwrap()
                        )],
                        handle_offsets: vec![1],
                    },
                    builder.get_index_layout()
                );

                // check intermediate channel index
                assert_eq!(1, executor.intermediate_channel_index());

                // check the lookup executor's index
                assert_eq!(table_scan_child_idx, executor.table_scan_child_index());
            };

        // check the executors from top to down:
        // Projection <- Selection <- IndexLookUp <- Limit <- IndexScan
        let summary = cast_as_summary::<ProjectionExecutor>(executor);
        assert_eq!(5, summary.summary_collector.get_output_index());
        executor = summary.into_inner().into_child();
        let summary = cast_as_summary::<SelectionExecutor>(executor);
        assert_eq!(4, summary.summary_collector.get_output_index());
        executor = summary.into_inner().into_child();
        let summary = cast_as_summary::<IndexLookupExecutor>(executor);
        assert_eq!(3, summary.summary_collector.get_output_index());
        let index_lookup = summary.into_inner();
        check_index_lookup_executor(&index_lookup, 2);
        executor = index_lookup.into_index_child();
        let summary = cast_as_summary::<LimitExecutor>(executor);
        assert_eq!(1, summary.summary_collector.get_output_index());
        executor = summary.into_inner().into_child();
        let summary = cast_as_summary::<IndexScanExecutor>(executor);
        assert_eq!(0, summary.summary_collector.get_output_index());

        // intermediate channels don't match
        assert!(
            build_executors_for_test(&cfg, descriptors.clone(), &[])
                .err()
                .unwrap()
                .to_string()
                .contains("IndexLookup should have exactly one intermediate output channel")
        );
        assert!(
            build_executors_for_test(
                &cfg,
                descriptors.clone(),
                &[intermediate_output_channels[0].clone()],
            )
            .err()
            .unwrap()
            .to_string()
            .contains("IndexLookup should have exactly one intermediate output channel")
        );
        assert!(
            build_executors_for_test(
                &cfg,
                descriptors.clone(),
                &[intermediate_output_channels[1].clone(), {
                    let mut ch = tipb::IntermediateOutputChannel::default();
                    ch.set_executor_idx(3);
                    ch.set_output_offsets(vec![2, 3]);
                    ch
                }],
            )
            .err()
            .unwrap()
            .to_string()
            .contains("IndexLookup should have exactly one intermediate output channel")
        );

        // the IndexLookup's probe side has more than 1 executor
        let err = build_executors_for_test(
            &cfg,
            vec![
                index_scan_descriptor(123, 456, index_columns.clone()),
                {
                    let mut descriptor = limit_descriptor(128);
                    descriptor.set_parent_idx(4);
                    descriptor
                },
                table_scan_descriptor(456, table_columns.clone()),
                limit_descriptor(10),
                index_lookup_descriptor(vec![1]),
                selection_descriptor(vec![
                    ExprDefBuilder::column_ref(0, FieldTypeTp::LongLong).build(),
                ]),
            ],
            &intermediate_output_channels,
        )
        .err()
        .unwrap();
        assert!(
            err.to_string()
                .contains("IndexLookUp should have one child executor, but got 2")
        );

        // the IndexLookup's probe side executor is not TableScan
        let mut err_descriptors = descriptors.clone();
        err_descriptors[2] = index_scan_descriptor(123, 123, index_columns.clone());
        assert!(
            build_executors_for_test(&cfg, err_descriptors, &intermediate_output_channels)
                .err()
                .unwrap()
                .to_string()
                .contains("IndexLookup's child should be TableScan, but got TypeIndexScan")
        );
    }

    #[test]
    fn test_build_runner_with_index_lookup() {
        let index_columns = vec![
            column_with_type(3, FieldTypeTp::String),
            column_with_type(1, FieldTypeTp::LongLong),
            column_with_type(5, FieldTypeTp::Geometry),
        ];
        let table_columns = vec![
            column_with_type(1, FieldTypeTp::LongLong),
            column_with_type(2, FieldTypeTp::Float),
            column_with_type(3, FieldTypeTp::String),
            column_with_type(4, FieldTypeTp::Timestamp),
            column_with_type(5, FieldTypeTp::Geometry),
        ];
        let descriptors = vec![
            {
                let mut descriptor = index_scan_descriptor(123, 456, index_columns.clone());
                descriptor.set_parent_idx(2);
                descriptor
            },
            table_scan_descriptor(456, table_columns.clone()),
            index_lookup_descriptor(vec![1]),
        ];

        let mut req = DagRequest::default();
        req.set_executors(descriptors.clone().into());
        req.set_output_offsets(vec![0, 3]);
        req.set_intermediate_output_channels(
            vec![{
                let mut ch = tipb::IntermediateOutputChannel::default();
                ch.set_executor_idx(2);
                ch.set_output_offsets(vec![1, 0]);
                ch
            }]
            .into(),
        );
        req.set_encode_type(EncodeType::TypeChunk);
        // normal build
        let runner = build_runner_for_test(req.clone()).unwrap();
        assert_eq!(runner.encode_type, EncodeType::TypeChunk);
        assert_eq!(runner.output_offsets, vec![0, 3]);
        let mut expected_intermediate_channels = vec![runner::IntermediateOutputChannel {
            encode_type: EncodeType::TypeChunk,
            output_offsets: vec![1, 0],
            schema: index_columns
                .iter()
                .map(field_type_from_column_info)
                .collect(),
        }];
        assert_eq!(runner.intermediate_channels, expected_intermediate_channels);
        assert!(runner.reserved_intermediate_results.is_none());

        // main output falls back to default encode
        req.set_output_offsets(vec![0, 4]);
        let runner = build_runner_for_test(req.clone()).unwrap();
        assert_eq!(runner.encode_type, EncodeType::TypeDefault);
        assert_eq!(runner.output_offsets, vec![0, 4]);
        assert_eq!(runner.intermediate_channels, expected_intermediate_channels);
        assert!(runner.reserved_intermediate_results.is_none());

        // intermediate output falls back to default encode
        req.set_output_offsets(vec![0, 3]);
        req.mut_intermediate_output_channels()
            .get_mut(0)
            .unwrap()
            .set_output_offsets(vec![2, 0]);
        let runner = build_runner_for_test(req.clone()).unwrap();
        assert_eq!(runner.encode_type, EncodeType::TypeChunk);
        assert_eq!(runner.output_offsets, vec![0, 3]);
        expected_intermediate_channels[0].encode_type = EncodeType::TypeDefault;
        expected_intermediate_channels[0].output_offsets = vec![2, 0];
        assert_eq!(runner.intermediate_channels, expected_intermediate_channels);
        assert!(runner.reserved_intermediate_results.is_none());

        // both fall back to default encode
        req.set_output_offsets(vec![0, 4]);
        let runner = build_runner_for_test(req.clone()).unwrap();
        assert_eq!(runner.encode_type, EncodeType::TypeDefault);
        assert_eq!(runner.output_offsets, vec![0, 4]);
        assert_eq!(runner.intermediate_channels, expected_intermediate_channels);
        assert!(runner.reserved_intermediate_results.is_none());

        // an intermediate channel doesn't refer to any executor
        req.mut_intermediate_output_channels().push({
            let mut ch = tipb::IntermediateOutputChannel::default();
            ch.set_executor_idx(3);
            ch.set_output_offsets(vec![0]);
            ch
        });
        let err = build_runner_for_test(req.clone()).err().unwrap();
        assert!(
            err.to_string()
                .contains("Invalid intermediate output channel idx: 3")
        );
        req.mut_intermediate_output_channels()[1].clear_executor_idx();
        let err = build_runner_for_test(req.clone()).err().unwrap();
        assert!(
            err.to_string()
                .contains("Invalid intermediate output channel idx: None")
        );
    }

    fn parse_timestamp(s: &str) -> Time {
        Time::parse_timestamp(&mut EvalContext::default(), s, 0, false).unwrap()
    }

    #[test]
    fn test_encode_result_to_chunk() {
        let mut cfg = EvalConfig::default();
        // set the time zone to test the time zone conversion
        cfg.set_time_zone_by_offset(3 * 3600).unwrap();
        let mut ctx = EvalContext::new(Arc::new(cfg));

        let field_types: Vec<FieldType> = vec![
            FieldTypeTp::LongLong.into(),
            FieldTypeTp::String.into(),
            FieldTypeTp::LongLong.into(),
            FieldTypeTp::Timestamp.into(),
        ];
        let output_offsets = vec![3, 0, 1];
        let output_field_types = output_offsets
            .iter()
            .map(|&i| field_types[i as usize].clone())
            .collect_vec();

        let mut result = BatchExecuteResult {
            physical_columns: LazyBatchColumnVec::from(vec![
                VectorValue::Int(vec![Some(1), Some(2), Some(3), Some(4), Some(5)].into()),
                VectorValue::Bytes(
                    vec![
                        Some(b"1a".to_vec()),
                        Some(b"2b".to_vec()),
                        Some(b"3c".to_vec()),
                        Some(b"4d".to_vec()),
                        Some(b"5e".to_vec()),
                    ]
                    .into(),
                ),
                VectorValue::Int(vec![Some(11), Some(21), Some(31), Some(14), Some(15)].into()),
                VectorValue::DateTime(
                    vec![
                        Some(parse_timestamp("2021-07-01 01:02:01")),
                        Some(parse_timestamp("2021-07-01 01:02:02")),
                        Some(parse_timestamp("2021-07-01 01:02:03")),
                        Some(parse_timestamp("2021-07-01 01:02:04")),
                        Some(parse_timestamp("2021-07-01 01:02:05")),
                    ]
                    .into(),
                ),
            ]),
            logical_rows: vec![4, 1, 2],
            warnings: EvalWarnings::default(),
            is_drained: Ok(BatchExecIsDrain::Remain),
        };

        let expected_rows = vec![
            vec![
                Datum::Time(parse_timestamp("2021-07-01 01:02:05")),
                Datum::I64(5),
                Datum::Bytes(b"5e".to_vec()),
            ],
            vec![
                Datum::Time(parse_timestamp("2021-07-01 01:02:02")),
                Datum::I64(2),
                Datum::Bytes(b"2b".to_vec()),
            ],
            vec![
                Datum::Time(parse_timestamp("2021-07-01 01:02:03")),
                Datum::I64(3),
                Datum::Bytes(b"3c".to_vec()),
            ],
        ];

        let mut check_encode_with_type = |tp| {
            let mut chk = Chunk::default();
            encode_result_to_chunk(
                &mut ctx,
                false,
                tp,
                &field_types,
                &output_offsets,
                &mut result,
                &mut chk,
            )
            .unwrap();
            check_chunk(
                &mut ctx,
                expected_rows.clone(),
                chk,
                &output_field_types,
                tp,
            )
        };

        check_encode_with_type(EncodeType::TypeDefault);
        check_encode_with_type(EncodeType::TypeChunk);
    }

    #[test]
    fn test_consume_and_encode_intermediate_results() {
        let mut runner = build_simple_runner_for_test();
        runner.intermediate_channels = vec![
            runner::IntermediateOutputChannel {
                encode_type: EncodeType::TypeDefault,
                output_offsets: vec![0, 2],
                schema: vec![
                    FieldTypeTp::Long.into(),
                    FieldTypeTp::Float.into(),
                    FieldTypeTp::String.into(),
                ],
            },
            runner::IntermediateOutputChannel {
                encode_type: EncodeType::TypeChunk,
                output_offsets: vec![2, 1],
                schema: vec![
                    FieldTypeTp::String.into(),
                    FieldTypeTp::Long.into(),
                    FieldTypeTp::Timestamp.into(),
                ],
            },
        ];

        let output_types = [
            vec![FieldTypeTp::Long.into(), FieldTypeTp::String.into()],
            vec![FieldTypeTp::Timestamp.into(), FieldTypeTp::Long.into()],
        ];

        let mut executor1 = MockExecutor::new(
            vec![FieldTypeTp::Long.into()],
            vec![BatchExecuteResult {
                physical_columns: LazyBatchColumnVec::empty(),
                logical_rows: vec![],
                warnings: EvalWarnings::default(),
                is_drained: Ok(BatchExecIsDrain::Remain),
            }],
        );
        executor1.intermediate_schema = Some((1, runner.intermediate_channels[1].schema.clone()));
        executor1.set_next_intermediate_results(vec![build_simple_result_for_test(vec![
            VectorValue::Bytes(
                vec![
                    Some(b"10a".to_vec()),
                    Some(b"20b".to_vec()),
                    Some(b"30c".to_vec()),
                ]
                .into(),
            ),
            VectorValue::Int(vec![Some(101), Some(102), Some(103)].into()),
            VectorValue::DateTime(
                vec![
                    Some(parse_timestamp("2025-07-01 01:02:01")),
                    Some(parse_timestamp("2025-07-01 01:02:02")),
                    Some(parse_timestamp("2025-07-01 01:02:03")),
                ]
                .into(),
            ),
        ])]);

        let mut executor0 = MockExecutor::new_with_child(executor1);
        executor0.intermediate_schema = Some((0, runner.intermediate_channels[0].schema.clone()));
        executor0.set_next_intermediate_results(vec![
            build_simple_result_for_test(vec![
                VectorValue::Int(vec![Some(1), Some(2), Some(3)].into()),
                VectorValue::Real(
                    vec![
                        Real::new(2.1).ok(),
                        Real::new(2.2).ok(),
                        Real::new(2.3).ok(),
                    ]
                    .into(),
                ),
                VectorValue::Bytes(
                    vec![
                        Some(b"1a".to_vec()),
                        Some(b"2b".to_vec()),
                        Some(b"3c".to_vec()),
                    ]
                    .into(),
                ),
            ]),
            // empty result should not encode to chunk
            build_simple_result_for_test(vec![]),
            build_simple_result_for_test(vec![
                VectorValue::Int(vec![Some(11), Some(12), Some(13)].into()),
                VectorValue::Real(
                    vec![
                        Real::new(12.1).ok(),
                        Real::new(12.2).ok(),
                        Real::new(12.3).ok(),
                    ]
                    .into(),
                ),
                VectorValue::Bytes(
                    vec![
                        Some(b"1d".to_vec()),
                        Some(b"2e".to_vec()),
                        Some(b"3f".to_vec()),
                    ]
                    .into(),
                ),
            ]),
        ]);

        let mut exist_chk = Chunk::default();
        exist_chk.mut_rows_data().push(97);
        runner.out_most_executor = Box::new(executor0);
        let mut ctx = EvalContext::default();
        let mut chunks = vec![vec![exist_chk], vec![]];
        let (record_len, bytes_len) = runner
            .consume_and_encode_intermediate_results(&mut chunks, &mut ctx, false)
            .unwrap();

        assert_eq!(record_len, 9);
        assert_eq!(
            bytes_len,
            chunks[0][1].get_rows_data().len()
                + chunks[0][2].get_rows_data().len()
                + chunks[1][0].get_rows_data().len()
        );
        assert_eq!(chunks[0].len(), 3);
        assert_eq!(chunks[1].len(), 1);

        // exist chunk should not change
        assert_eq!(chunks[0][0].get_rows_data(), &[97]);
        check_chunk(
            &mut ctx,
            vec![
                vec![Datum::I64(1), Datum::Bytes(b"1a".to_vec())],
                vec![Datum::I64(2), Datum::Bytes(b"2b".to_vec())],
                vec![Datum::I64(3), Datum::Bytes(b"3c".to_vec())],
            ],
            chunks[0][1].clone(),
            &output_types[0],
            runner.intermediate_channels[0].encode_type,
        );
        check_chunk(
            &mut ctx,
            vec![
                vec![Datum::I64(11), Datum::Bytes(b"1d".to_vec())],
                vec![Datum::I64(12), Datum::Bytes(b"2e".to_vec())],
                vec![Datum::I64(13), Datum::Bytes(b"3f".to_vec())],
            ],
            chunks[0][2].clone(),
            &output_types[0],
            runner.intermediate_channels[0].encode_type,
        );
        check_chunk(
            &mut ctx,
            vec![
                vec![
                    Datum::Time(parse_timestamp("2025-07-01 01:02:01")),
                    Datum::I64(101),
                ],
                vec![
                    Datum::Time(parse_timestamp("2025-07-01 01:02:02")),
                    Datum::I64(102),
                ],
                vec![
                    Datum::Time(parse_timestamp("2025-07-01 01:02:03")),
                    Datum::I64(103),
                ],
            ],
            chunks[1][0].clone(),
            &output_types[1],
            runner.intermediate_channels[1].encode_type,
        );
        // reserved_intermediate_results should be cleared
        runner
            .reserved_intermediate_results
            .unwrap()
            .iter()
            .for_each(|r| {
                assert!(r.is_empty());
            });
    }

    #[test]
    fn test_set_response_intermediate_outputs() {
        let mut runner = build_simple_runner_for_test();
        runner.intermediate_channels = vec![
            runner::IntermediateOutputChannel {
                encode_type: EncodeType::TypeDefault,
                output_offsets: vec![1, 0],
                schema: vec![FieldTypeTp::Long.into(), FieldTypeTp::Float.into()],
            },
            runner::IntermediateOutputChannel {
                encode_type: EncodeType::TypeChunk,
                output_offsets: vec![0],
                schema: vec![FieldTypeTp::Timestamp.into()],
            },
        ];

        let mut resp = SelectResponse::default();
        let chunks = vec![
            vec![
                {
                    let mut chk = Chunk::default();
                    chk.mut_rows_data().extend_from_slice(&[1, 2, 3]);
                    chk
                },
                {
                    let mut chk = Chunk::default();
                    chk.mut_rows_data().extend_from_slice(&[11, 12, 13, 14]);
                    chk
                },
            ],
            vec![{
                let mut chk = Chunk::default();
                chk.mut_rows_data().extend_from_slice(&[21, 22]);
                chk
            }],
        ];
        runner.set_response_intermediate_outputs(&mut resp, chunks);
        assert_eq!(2, resp.get_intermediate_outputs().len());
        let output0 = &resp.get_intermediate_outputs()[0];
        assert_eq!(EncodeType::TypeDefault, output0.get_encode_type());
        assert_eq!(vec![1, 2, 3], output0.get_chunks()[0].get_rows_data());
        assert_eq!(
            vec![11, 12, 13, 14],
            output0.get_chunks()[1].get_rows_data()
        );
        let output1 = &resp.get_intermediate_outputs()[1];
        assert_eq!(EncodeType::TypeChunk, output1.get_encode_type());
        assert_eq!(vec![21, 22], output1.get_chunks()[0].get_rows_data());
    }

    #[test]
    fn test_handle_request() {
        let mut runner = build_simple_runner_for_test();
        let mut ctx = EvalContext {
            cfg: runner.config.clone(),
            ..Default::default()
        };
        let field_types = vec![FieldTypeTp::Long.into()];
        let mut mock_executor = MockExecutor::new(
            field_types.clone(),
            vec![
                // first loop, empty main result, some intermediate results
                build_simple_result_for_test(vec![]),
                // second loop, main result with 3 rows, some intermediate results
                build_n_rows_int_result(0, 3),
                // third loop, main result with 2 rows, no intermediate result
                {
                    let mut r = build_n_rows_int_result(10, 2);
                    r.is_drained = Ok(BatchExecIsDrain::Drain);
                    r
                },
            ],
        );
        mock_executor.intermediate_schema = Some((0, field_types.clone()));
        mock_executor.intermediate_results = vec![
            // first loop, 2 intermediate result with 1 and 2 rows
            vec![
                build_n_rows_int_result(1000, 1),
                build_n_rows_int_result(1010, 2),
            ],
            // second loop, 1 intermediate result with 3 rows
            vec![build_n_rows_int_result(1100, 3)],
            // third loop, no intermediate result
        ]
        .into_iter();

        runner.out_most_executor = Box::new(mock_executor);
        runner.intermediate_channels = vec![runner::IntermediateOutputChannel {
            encode_type: EncodeType::TypeChunk,
            output_offsets: vec![0],
            schema: field_types.clone(),
        }];
        let (mut resp, range) = block_on(runner.handle_request()).unwrap();
        // no paging
        assert!(range.is_none());
        // check the main result
        let main_chunks = resp.take_chunks();
        assert_eq!(2, main_chunks.len());
        check_chunk(
            &mut ctx,
            vec![
                vec![Datum::I64(0)],
                vec![Datum::I64(1)],
                vec![Datum::I64(2)],
            ],
            main_chunks[0].clone(),
            &field_types,
            resp.get_encode_type(),
        );
        check_chunk(
            &mut ctx,
            vec![vec![Datum::I64(10)], vec![Datum::I64(11)]],
            main_chunks[1].clone(),
            &field_types,
            resp.get_encode_type(),
        );
        // check the intermediate result
        assert_eq!(1, resp.get_intermediate_outputs().len());
        let mut outputs = resp.take_intermediate_outputs().to_vec();
        let chunks = outputs[0].take_chunks().to_vec();
        let encode_type = outputs[0].get_encode_type();
        assert_eq!(3, chunks.len());
        check_chunk(
            &mut ctx,
            vec![vec![Datum::I64(1000)]],
            chunks[0].clone(),
            &field_types,
            encode_type,
        );
        check_chunk(
            &mut ctx,
            vec![vec![Datum::I64(1010)], vec![Datum::I64(1011)]],
            chunks[1].clone(),
            &field_types,
            encode_type,
        );
        check_chunk(
            &mut ctx,
            vec![
                vec![Datum::I64(1100)],
                vec![Datum::I64(1101)],
                vec![Datum::I64(1102)],
            ],
            chunks[2].clone(),
            &field_types,
            encode_type,
        );

        // test paging
        let mut paging_runner = build_simple_runner_for_test();
        paging_runner.paging_size = Some(5);
        let mut mock_executor = MockExecutor::new(
            field_types.clone(),
            vec![
                // first loop, 1 row
                build_n_rows_int_result(0, 1),
                // second loop, 1 row
                build_n_rows_int_result(10, 1),
                // third loop, 1 row
                build_n_rows_int_result(100, 1),
            ],
        );
        let expected_range: IntervalRange = (b"a".to_vec(), b"c".to_vec()).into();
        mock_executor.scanned_range = Some(expected_range.clone());
        mock_executor.intermediate_schema = Some((0, field_types.clone()));
        mock_executor.intermediate_results = vec![
            // first loop, 1 intermediate result with 1 row
            vec![build_n_rows_int_result(1000, 1)],
            // second loop, 2 intermediate result with 2 rows
            vec![
                build_n_rows_int_result(1100, 1),
                build_n_rows_int_result(1200, 1),
            ],
            // third loop, 1 intermediate result with 1 row
            vec![build_n_rows_int_result(1300, 1)],
        ]
        .into_iter();
        paging_runner.out_most_executor = Box::new(mock_executor);
        paging_runner.intermediate_channels = vec![runner::IntermediateOutputChannel {
            encode_type: EncodeType::TypeChunk,
            output_offsets: vec![0],
            schema: field_types.clone(),
        }];
        let (mut resp, range) = block_on(paging_runner.handle_request()).unwrap();
        assert_eq!(Some(expected_range), range);
        let main_chunks = resp.take_chunks();
        // the results should stop for paging in the second loop
        assert_eq!(2, main_chunks.len());
        check_chunk(
            &mut ctx,
            vec![vec![Datum::I64(0)]],
            main_chunks[0].clone(),
            &field_types,
            resp.get_encode_type(),
        );
        check_chunk(
            &mut ctx,
            vec![vec![Datum::I64(10)]],
            main_chunks[1].clone(),
            &field_types,
            resp.get_encode_type(),
        );
        let mut outputs = resp.take_intermediate_outputs();
        assert_eq!(1, outputs.len());
        let chunks = outputs[0].take_chunks().to_vec();
        let encode_type = outputs[0].get_encode_type();
        assert_eq!(3, chunks.len());
        check_chunk(
            &mut ctx,
            vec![vec![Datum::I64(1000)]],
            chunks[0].clone(),
            &field_types,
            encode_type,
        );
        check_chunk(
            &mut ctx,
            vec![vec![Datum::I64(1100)]],
            chunks[1].clone(),
            &field_types,
            encode_type,
        );
        check_chunk(
            &mut ctx,
            vec![vec![Datum::I64(1200)]],
            chunks[2].clone(),
            &field_types,
            encode_type,
        );
    }
}
