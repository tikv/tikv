// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::{convert::TryFrom, future::Future, marker::PhantomData, mem, pin::Pin, sync::Arc};

use api_version::KvFormat;
use fail::fail_point;
use itertools::Itertools;
use kvproto::{coprocessor::KeyRange, metapb};
use protobuf::Message;
use tidb_query_common::{
    execute_stats::ExecSummary,
    metrics::*,
    storage::{IntervalRange, Storage},
    Result,
};
use tidb_query_datatype::{
    expr::{EvalConfig, EvalContext, EvalWarnings}, EvalType,
    FieldTypeAccessor,
};
use tikv_util::{
    deadline::Deadline,
    metrics::{ThrottleType, NON_TXN_COMMAND_THROTTLE_TIME_COUNTER_VEC_STATIC},
    quota_limiter::QuotaLimiter,
};
use tipb::{
    self, Chunk, DagRequest, EncodeType, ExecType, ExecutorExecutionSummary, FieldType,
    PartialOutput, SelectResponse, StreamResponse,
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

use crate::{
    index_lookup_executor::{build_index_lookup_probe_ranges, BatchIndexLookupExecutor},
    interface::{AsyncRegion, BatchExecuteResult, FnLocateRegionKey},
    runner::SrcExecutorRanges::Simple,
};

// TODO: Maybe there can be some better strategy. Needs benchmarks and tunes.
const BATCH_GROW_FACTOR: usize = 2;

struct ExecutorsWithOutput<SS> {
    out_most_executor: Box<dyn BatchExecutor<StorageStats = SS>>,
    output_offsets: Vec<u32>,
    encode_type: EncodeType,
}

impl<SS> ExecutorsWithOutput<SS> {
    fn build<S: Storage<Statistics = SS> + 'static, F: KvFormat>(
        executor_descriptors: Vec<tipb::Executor>,
        ranges: SrcExecutorRanges<S>,
        config: Arc<EvalConfig>,
        is_scanned_range_aware: bool,
        output_offsets: Vec<u32>,
        encode_type: EncodeType,
    ) -> Result<Self> {
        let out_most_executor =
            build_executors::<_, F>(executor_descriptors, ranges, config, is_scanned_range_aware)?;

        // Check output offsets
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

        // Only check output schema field types
        let new_schema = output_offsets
            .iter()
            .map(|&i| &out_most_executor.schema()[i as usize]);
        let encode_type = if !is_arrow_encodable(new_schema) {
            EncodeType::TypeDefault
        } else {
            encode_type
        };

        Ok(Self {
            out_most_executor,
            output_offsets,
            encode_type,
        })
    }

    fn encode_result_to_trunk(
        &self,
        ctx: &mut EvalContext,
        chunk: &mut Chunk,
        result: &BatchExecuteResult,
        encode_type: Option<EncodeType>,
    ) -> Result<()> {
        if result.logical_rows.is_empty() {
            return Ok(());
        }
        assert_eq!(
            result.physical_columns.columns_len(),
            self.out_most_executor.schema().len()
        );
        let data = chunk.mut_rows_data();
        let encode_type = encode_type.unwrap_or(self.encode_type);
        // Although `schema()` can be deeply nested, it is ok since we process data in
        // batch.
        if encode_type == EncodeType::TypeDefault {
            data.reserve(
                result
                    .physical_columns
                    .maximum_encoded_size(&result.logical_rows, &self.output_offsets),
            );
            result.physical_columns.encode(
                &result.logical_rows,
                &self.output_offsets,
                self.out_most_executor.schema(),
                data,
                ctx,
            )?;
        } else {
            data.reserve(
                result
                    .physical_columns
                    .maximum_encoded_size_chunk(&result.logical_rows, &self.output_offsets),
            );
            result.physical_columns.encode_chunk(
                &result.logical_rows,
                &self.output_offsets,
                self.out_most_executor.schema(),
                data,
                ctx,
            )?;
        }
        Ok(())
    }
}

struct ExecutorDescriptors {
    executor_descriptors: Vec<tipb::Executor>,
    output_offsets: Vec<u32>,
    encode_type: EncodeType,
}

impl ExecutorDescriptors {
    fn new(
        executor_descriptors: Vec<tipb::Executor>,
        output_offsets: Vec<u32>,
        encode_type: EncodeType,
    ) -> Self {
        Self {
            executor_descriptors,
            output_offsets,
            encode_type,
        }
    }

    async fn build_instant<S: Storage<Statistics = SS> + 'static, SS: 'static, F: KvFormat>(
        self,
        ctx: &mut EvalContext,
        build_side_results: &mut Vec<BatchExecuteResult>,
        result_schema: &[FieldType],
        locate_key: &FnLocateRegionKey<S>,
    ) -> Result<ExecutorsWithOutput<SS>> {
        if self.executor_descriptors.len() == 0 {
            return Err(other_err!("No executors"));
        }

        let descriptor = &self.executor_descriptors[0];
        let tp = descriptor.get_tp();
        match tp {
            ExecType::TypeIndexLookup => {
                let ranges = build_index_lookup_probe_ranges(
                    ctx,
                    descriptor.get_index_lookup(),
                    build_side_results,
                    result_schema,
                    locate_key,
                )
                .await?;

                ExecutorsWithOutput::build::<_, F>(
                    self.executor_descriptors,
                    SrcExecutorRanges::Probes(ranges),
                    ctx.cfg.clone(),
                    false,
                    self.output_offsets,
                    self.encode_type,
                )
            }
            _ => Err(other_err!(
                "unsupported first type: {:?} to generate probe side ranges",
                tp
            )),
        }
    }
}

enum ExecutionGroup<SS> {
    Instant(ExecutorsWithOutput<SS>),
    Descriptors(ExecutorDescriptors),
}

impl<SS> ExecutionGroup<SS> {
    fn mut_instant(&mut self) -> Result<&mut ExecutorsWithOutput<SS>> {
        match self {
            ExecutionGroup::Instant(ref mut instant) => Ok(instant),
            ExecutionGroup::Descriptors(_) => Err(other_err!("instant has not built yet")),
        }
    }

    fn instant(&self) -> Result<&ExecutorsWithOutput<SS>> {
        match self {
            ExecutionGroup::Instant(ref instant) => Ok(instant),
            ExecutionGroup::Descriptors(_) => Err(other_err!("instant has not built yet")),
        }
    }
}

impl<SS: 'static> ExecutionGroup<SS> {
    async fn build_instant<S: Storage<Statistics = SS> + 'static, F: KvFormat>(
        &mut self,
        ctx: &mut EvalContext,
        build_side_results: &mut Vec<BatchExecuteResult>,
        result_schema: &[FieldType],
        locate_key: &FnLocateRegionKey<S>,
    ) -> Result<&mut ExecutorsWithOutput<SS>> {
        match self {
            ExecutionGroup::Instant(_) => Err(other_err!("instant has already been built")),
            ExecutionGroup::Descriptors(desc) => {
                let mut descriptors = ExecutorDescriptors {
                    executor_descriptors: vec![],
                    output_offsets: vec![],
                    encode_type: EncodeType::TypeDefault,
                };
                mem::swap(desc, &mut descriptors);
                *self = Self::Instant(
                    descriptors
                        .build_instant::<_, _, F>(
                            ctx,
                            build_side_results,
                            result_schema,
                            locate_key,
                        )
                        .await?,
                );
                self.mut_instant()
            }
        }
    }
}

pub struct BatchExecutorsRunner<S, SS, F> {
    /// The deadline of this handler. For each check point (e.g. each iteration)
    /// we need to check whether or not the deadline is exceeded and break
    /// the process if so.
    // TODO: Deprecate it using a better deadline mechanism.
    deadline: Deadline,

    locate_key: FnLocateRegionKey<S>,

    execution_groups: Vec<ExecutionGroup<SS>>,

    config: Arc<EvalConfig>,

    /// Whether or not execution summary need to be collected.
    collect_exec_summary: bool,

    exec_stats: ExecuteStats,

    /// Maximum rows to return in batch stream mode.
    stream_row_limit: usize,

    /// If it's a paging request, paging_size indicates to the required size for
    /// current page.
    paging_size: Option<u64>,

    quota_limiter: Arc<QuotaLimiter>,

    _phantom: PhantomData<F>,
}

// We assign a dummy type `()` so that we can omit the type when calling
// `check_supported`.
impl BatchExecutorsRunner<(), (), ()> {
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
                ExecType::TypeIndexLookup => {
                    let descriptor = ed.get_index_lookup();
                    BatchIndexLookupExecutor::check_supported(descriptor)
                        .map_err(|e| other_err!("BatchLookupExecutor: {}", e))?;
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
            }
        }

        Ok(())
    }
}

#[inline]
fn is_arrow_encodable<'a>(mut schema: impl Iterator<Item = &'a FieldType>) -> bool {
    schema.all(|schema| EvalType::try_from(schema.as_accessor().tp()).is_ok())
}

pub enum SrcExecutorRanges<S: Storage> {
    Simple((S, Vec<KeyRange>)),
    Probes(Vec<(S, Vec<KeyRange>)>),
}

impl<S: Storage> SrcExecutorRanges<S> {
    fn into_simple_ranges(self) -> Result<(S, Vec<KeyRange>)> {
        match self {
            SrcExecutorRanges::Simple(ranges) => Ok(ranges),
            SrcExecutorRanges::Probes(_) => Err(other_err!("simple ranges expected")),
        }
    }

    fn into_probe_side_ranges(self) -> Result<Vec<(S, Vec<KeyRange>)>> {
        match self {
            SrcExecutorRanges::Simple(_) => Err(other_err!("probes ranges expected")),
            SrcExecutorRanges::Probes(ranges) => Ok(ranges),
        }
    }
}

#[allow(clippy::explicit_counter_loop)]
pub fn build_executors<S: Storage + 'static, F: KvFormat>(
    executor_descriptors: Vec<tipb::Executor>,
    src_scan_ranges: SrcExecutorRanges<S>,
    config: Arc<EvalConfig>,
    is_scanned_range_aware: bool,
) -> Result<Box<dyn BatchExecutor<StorageStats = S::Statistics>>> {
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
            let (storage, ranges) = src_scan_ranges.into_simple_ranges().map_err(|_| {
                other_err!(
                    "only simple ranges are allowed for src executor: {:?}",
                    first_ed.get_tp()
                )
            })?;
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
            let (storage, ranges) = src_scan_ranges.into_simple_ranges().map_err(|_| {
                other_err!(
                    "only simple ranges are allowed for src executor: {:?}",
                    first_ed.get_tp()
                )
            })?;
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
        ExecType::TypeIndexLookup => {
            EXECUTOR_COUNT_METRICS.batch_index_lookup.inc();
            let mut descriptor = first_ed.take_index_lookup();
            let probe_side_ranges = src_scan_ranges.into_probe_side_ranges().map_err(|_| {
                other_err!(
                    "only probe ranges are allowed for src executor: {:?}",
                    first_ed.get_tp()
                )
            })?;
            Box::new(BatchIndexLookupExecutor::<_, F>::new(
                config.clone(),
                descriptor.take_columns().into(),
                descriptor.take_primary_column_ids(),
                descriptor.take_primary_prefix_column_ids(),
                probe_side_ranges,
            )?)
        }
        _ => {
            return Err(other_err!(
                "Unexpected first executor {:?}",
                first_ed.get_tp()
            ));
        }
    };

    for mut ed in executor_descriptors {
        summary_slot_index += 1;

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
            _ => {
                return Err(other_err!(
                    "Unexpected non-first executor {:?}",
                    ed.get_tp()
                ));
            }
        };
        is_src_scan_executor = false;
    }

    Ok(executor)
}

impl<S: Storage<Statistics = SS> + 'static, SS: 'static, F: KvFormat>
    BatchExecutorsRunner<S, SS, F>
{
    pub fn from_request(
        req: DagRequest,
        ranges: Vec<KeyRange>,
        storage: S,
        deadline: Deadline,
        stream_row_limit: usize,
        is_streaming: bool,
        paging_size: Option<u64>,
        quota_limiter: Arc<QuotaLimiter>,
        locate_key: FnLocateRegionKey<S>,
    ) -> Result<Self> {
        let executors_len = req.get_executors().len();
        let collect_exec_summary = req.get_collect_execution_summaries();
        let mut config = EvalConfig::from_request(&req)?;
        config.paging_size = paging_size;
        let config = Arc::new(config);
        let exec_stats = ExecuteStats::new(executors_len);
        let execution_groups = Self::split_execution_groups(
            req,
            storage,
            ranges,
            config.clone(),
            is_streaming || paging_size.is_some(), /* For streaming and paging request,
                                                    * executors will continue scan from range
                                                    * end where last scan is finished */
        )?;

        Ok(Self {
            deadline,
            locate_key,
            execution_groups,
            config,
            collect_exec_summary,
            exec_stats,
            stream_row_limit,
            paging_size,
            quota_limiter,
            _phantom: PhantomData,
        })
    }

    fn split_execution_groups(
        mut req: DagRequest,
        storage: S,
        first_scan_ranges: Vec<KeyRange>,
        config: Arc<EvalConfig>,
        is_scanned_range_aware: bool,
    ) -> Result<Vec<ExecutionGroup<SS>>> {
        let executors = req.take_executors().to_vec();
        let mut barriers = req.take_parital_output_barriers();
        if barriers.len() == 0 {
            return Ok(vec![ExecutionGroup::Instant(ExecutorsWithOutput::build::<
                _,
                F,
            >(
                executors,
                Simple((storage, first_scan_ranges)),
                config,
                is_scanned_range_aware,
                req.take_output_offsets().into(),
                req.get_encode_type(),
            )?)]);
        }

        let mut groups = Vec::with_capacity(barriers.len() + 1);
        let first_barrier = barriers.first_mut().unwrap();
        let last_group_end = first_barrier.get_position() as usize;
        if last_group_end <= 0 {
            return Err(other_err!(
                "invalid barrier position: {} at index: 0",
                last_group_end
            ));
        }

        groups.push(ExecutionGroup::Instant(ExecutorsWithOutput::build::<_, F>(
            executors[..last_group_end].to_vec(),
            Simple((storage, first_scan_ranges)),
            config.clone(),
            is_scanned_range_aware,
            first_barrier.take_output_offsets(),
            first_barrier.get_encode_type(),
        )?));

        let barriers_len = barriers.len();
        for i in 1..=barriers_len {
            let (group_end, output_offsets, encode_type) = if i == barriers_len {
                (
                    executors.len(),
                    req.take_output_offsets(),
                    req.get_encode_type(),
                )
            } else {
                let barrier = &mut barriers[i];
                (
                    barrier.get_position() as usize,
                    barrier.take_output_offsets(),
                    barrier.get_encode_type(),
                )
            };

            if group_end <= last_group_end || group_end > executors.len() {
                return Err(other_err!(
                    "invalid barrier position: {} at index: {}",
                    group_end,
                    i + 1
                ));
            }

            groups.push(ExecutionGroup::Descriptors(ExecutorDescriptors::new(
                executors[last_group_end..group_end].to_vec(),
                output_offsets,
                encode_type,
            )));
        }

        Ok(groups)
    }

    fn batch_initial_size() -> usize {
        fail_point!("copr_batch_initial_size", |r| r
            .map_or(1, |e| e.parse().unwrap()));
        BATCH_INITIAL_SIZE
    }

    /// handle_request returns the response of selection and an optional range,
    /// only paging request will return Some(IntervalRange),
    /// this should be used when calculating ranges of the next batch.
    /// IntervalRange records whole range scanned though there are gaps in multi
    /// ranges. e.g.: [(k1 -> k2), (k4 -> k5)] may got response (k1, k2, k4)
    /// with IntervalRange like (k1, k4).
    pub async fn handle_request(&mut self) -> Result<(SelectResponse, Option<IntervalRange>)> {
        let mut warnings = self.config.new_eval_warnings();
        let mut ctx = EvalContext::new(self.config.clone());

        let groups_len = self.execution_groups.len();
        let mut sel_resp = SelectResponse::default();
        let mut interval_range_resp = None;
        for i in 0..groups_len {
            let (mut write_chunks, mut write_results) = if i < groups_len - 1 {
                (None, Some(vec![]))
            } else {
                (Some(vec![]), None)
            };
            let paging = self.paging_size.is_some() && i == 0;
            let internal_range = self
                .handle_one_executor_group(
                    i,
                    &mut warnings,
                    &mut ctx,
                    paging,
                    &mut write_chunks,
                    &mut write_results,
                )
                .await?;

            if paging {
                interval_range_resp = internal_range;
            }

            let (group, next_groups) = self.execution_groups[i..].split_first_mut().unwrap();
            if let Some(chunks) = write_chunks {
                sel_resp.set_chunks(chunks.into());
                sel_resp.set_encode_type(group.instant()?.encode_type);
            } else if let Some(results) = write_results.as_mut() {
                let group = group.mut_instant()?;
                let next_group = next_groups.iter_mut().next().unwrap();
                next_group
                    .build_instant::<_, F>(
                        &mut ctx,
                        results,
                        &group.out_most_executor.schema(),
                        &self.locate_key,
                    )
                    .await?;
                let mut chunks = Vec::with_capacity(results.len());
                for result in results {
                    if !result.logical_rows.is_empty() {
                        let mut chunk = Chunk::default();
                        group.encode_result_to_trunk(&mut ctx, &mut chunk, &result, None)?;
                        chunks.push(chunk);
                    }
                }

                let mut partial_output = PartialOutput::default();
                partial_output.set_chunks(chunks.into());
                partial_output.set_encode_type(group.encode_type);
                sel_resp.mut_partial_outputs().push(partial_output);
            }
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
        Ok((sel_resp, interval_range_resp))
    }

    pub async fn handle_streaming_request(
        &mut self,
    ) -> Result<(Option<(StreamResponse, IntervalRange)>, bool)> {
        if self.execution_groups.len() != 1 {
            return Err(other_err!("not supported"));
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
            let (drained, result) = self
                .internal_handle_request(
                    0,
                    Some(EncodeType::TypeDefault),
                    batch_size.min(self.stream_row_limit - record_len),
                    Some(&mut current_chunk),
                    &mut warnings,
                    &mut ctx,
                )
                .await?;
            chunk
                .mut_rows_data()
                .extend_from_slice(current_chunk.get_rows_data());
            record_len += result.logical_rows.len();
            is_drained = drained.stop();
        }

        if !is_drained || record_len > 0 {
            let range = self.execution_groups[0]
                .mut_instant()?
                .out_most_executor
                .take_scanned_range();
            return self
                .make_stream_response(chunk, warnings)
                .map(|r| (Some((r, range)), is_drained));
        }
        Ok((None, true))
    }

    pub fn collect_storage_stats(&mut self, dest: &mut SS) {
        for group in &mut self.execution_groups {
            if let ExecutionGroup::Instant(group) = group {
                group.out_most_executor.collect_storage_stats(dest);
            }
        }
    }

    pub fn can_be_cached(&self) -> bool {
        self.execution_groups.len() == 1
            && self.execution_groups.first().map_or(false, |group| {
                if let ExecutionGroup::Instant(group) = group {
                    group.out_most_executor.can_be_cached()
                } else {
                    false
                }
            })
    }

    pub fn collect_scan_summary(&mut self, dest: &mut ExecSummary) {
        // Get the first executor which is always the scan executor
        if let Some(exec_stat) = self.exec_stats.summary_per_executor.first() {
            dest.clone_from(exec_stat);
        }
    }

    async fn handle_one_executor_group(
        &mut self,
        group_idx: usize,
        warnings: &mut EvalWarnings,
        ctx: &mut EvalContext,
        paging: bool,
        write_chunks: &mut Option<Vec<Chunk>>,
        append_results: &mut Option<Vec<BatchExecuteResult>>,
    ) -> Result<Option<IntervalRange>> {
        let mut record_all = 0;
        let mut batch_size = Self::batch_initial_size();
        loop {
            let mut chunk = Chunk::default();
            let mut sample = self.quota_limiter.new_sample(true);
            let (drained, result) = {
                let (cpu_time, res) = sample
                    .observe_cpu_async(self.internal_handle_request(
                        group_idx,
                        None,
                        batch_size,
                        write_chunks.is_some().then_some(&mut chunk),
                        warnings,
                        ctx,
                    ))
                    .await;
                sample.add_cpu_time(cpu_time);
                res?
            };

            if !result.logical_rows.is_empty() {
                record_all += result.logical_rows.len();
                if let Some(chunks) = write_chunks {
                    sample.add_read_bytes(chunk.get_rows_data().len());
                    chunks.push(chunk);
                }

                if let Some(append_results) = append_results {
                    append_results.push(result);
                }
            }

            let quota_delay = self.quota_limiter.consume_sample(sample, true).await;
            if !quota_delay.is_zero() {
                NON_TXN_COMMAND_THROTTLE_TIME_COUNTER_VEC_STATIC
                    .get(ThrottleType::dag)
                    .inc_by(quota_delay.as_micros() as u64);
            }

            if drained.stop()
                || (paging && self.paging_size.is_some_and(|p| record_all >= p as usize))
            {
                let group = self.execution_groups[group_idx].mut_instant()?;
                group
                    .out_most_executor
                    .collect_exec_stats(&mut self.exec_stats);
                let range = if drained == BatchExecIsDrain::Drain {
                    None
                } else {
                    // It's not allowed to stop paging when BatchExecIsDrain::PagingDrain.
                    self.paging_size
                        .map(|_| group.out_most_executor.take_scanned_range())
                };
                return Ok(range);
            }

            // Grow batch size
            grow_batch_size(&mut batch_size);
        }
    }

    async fn internal_handle_request(
        &mut self,
        group: usize,
        force_encode_type: Option<EncodeType>,
        batch_size: usize,
        write_chunk: Option<&mut Chunk>,
        warnings: &mut EvalWarnings,
        ctx: &mut EvalContext,
    ) -> Result<(BatchExecIsDrain, BatchExecuteResult)> {
        self.deadline.check()?;
        let group = self.execution_groups[group].mut_instant()?;
        let mut result = group.out_most_executor.next_batch(batch_size).await;
        match result.is_drained {
            Ok(is_drained) => {
                if let Some(chunk) = write_chunk {
                    group.encode_result_to_trunk(ctx, chunk, &result, force_encode_type)?;
                }
                warnings.merge(&mut result.warnings);
                Ok((is_drained, result))
            }
            Err(err) => Err(err),
        }
    }

    fn make_stream_response(
        &mut self,
        chunk: Chunk,
        warnings: EvalWarnings,
    ) -> Result<StreamResponse> {
        for group in self.execution_groups.iter_mut() {
            group
                .mut_instant()?
                .out_most_executor
                .collect_exec_stats(&mut self.exec_stats);
        }

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
