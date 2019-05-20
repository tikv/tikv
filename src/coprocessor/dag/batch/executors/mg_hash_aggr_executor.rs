// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use hashbrown::hash_map::Entry;
use smallvec::SmallVec;

use tikv_util::collections::HashMap;
use tipb::executor::Aggregation;
use tipb::expression::{Expr, FieldType};

use crate::coprocessor::codec::batch::{LazyBatchColumn, LazyBatchColumnVec};
use crate::coprocessor::dag::aggr_fn::*;
use crate::coprocessor::dag::batch::executors::util::aggr_executor::*;
use crate::coprocessor::dag::batch::executors::util::hash_aggr_helper::HashAggregationHelper;
use crate::coprocessor::dag::batch::interface::*;
use crate::coprocessor::dag::exec_summary::ExecSummaryCollectorDisabled;
use crate::coprocessor::dag::expr::EvalConfig;
use crate::coprocessor::dag::rpn_expr::{RpnExpression, RpnExpressionBuilder};
use crate::coprocessor::Result;

/// Multi Group Hash Aggregation Executor supports multiple groups but uses less efficient ways to
/// store group keys in hash tables.
///
/// FIXME: It is not correct to just store the serialized data as the group key.
/// See pingcap/tidb#10467.
pub struct BatchMultiGroupHashAggregationExecutor<C: ExecSummaryCollector, Src: BatchExecutor>(
    AggregationExecutor<C, Src, MultiGroupHashAggregationImpl>,
);

impl<C: ExecSummaryCollector, Src: BatchExecutor> BatchExecutor
    for BatchMultiGroupHashAggregationExecutor<C, Src>
{
    #[inline]
    fn schema(&self) -> &[FieldType] {
        self.0.schema()
    }

    #[inline]
    fn next_batch(&mut self, scan_rows: usize) -> BatchExecuteResult {
        self.0.next_batch(scan_rows)
    }

    #[inline]
    fn collect_statistics(&mut self, destination: &mut BatchExecuteStatistics) {
        self.0.collect_statistics(destination)
    }
}

impl<Src: BatchExecutor> BatchMultiGroupHashAggregationExecutor<ExecSummaryCollectorDisabled, Src> {
    #[cfg(test)]
    pub fn new_for_test(
        src: Src,
        group_by_exps: Vec<RpnExpression>,
        aggr_defs: Vec<Expr>,
        aggr_def_parser: impl AggrDefinitionParser,
    ) -> Self {
        Self::new_impl(
            ExecSummaryCollectorDisabled,
            Arc::new(EvalConfig::default()),
            src,
            group_by_exps,
            aggr_defs,
            aggr_def_parser,
        )
        .unwrap()
    }
}

impl BatchMultiGroupHashAggregationExecutor<ExecSummaryCollectorDisabled, Box<dyn BatchExecutor>> {
    /// Checks whether this executor can be used.
    #[inline]
    pub fn check_supported(descriptor: &Aggregation) -> Result<()> {
        let group_by_definitions = descriptor.get_group_by();
        assert!(group_by_definitions.len() > 1);
        for def in group_by_definitions {
            RpnExpressionBuilder::check_expr_tree_supported(def)?;
            if RpnExpressionBuilder::is_expr_eval_to_scalar(def)? {
                return Err(box_err!("Group by expression is not a column"));
            }
        }

        let aggr_definitions = descriptor.get_agg_func();
        for def in aggr_definitions {
            AllAggrDefinitionParser.check_supported(def)?;
        }
        Ok(())
    }
}

impl<C: ExecSummaryCollector, Src: BatchExecutor> BatchMultiGroupHashAggregationExecutor<C, Src> {
    pub fn new(
        summary_collector: C,
        config: Arc<EvalConfig>,
        src: Src,
        group_by_exp_defs: Vec<Expr>,
        aggr_defs: Vec<Expr>,
    ) -> Result<Self> {
        let schema_len = src.schema().len();
        let mut group_by_exps = Vec::with_capacity(group_by_exp_defs.len());
        for def in group_by_exp_defs {
            group_by_exps.push(RpnExpressionBuilder::build_from_expr_tree(
                def, &config.tz, schema_len,
            )?);
        }

        Self::new_impl(
            summary_collector,
            config,
            src,
            group_by_exps,
            aggr_defs,
            AllAggrDefinitionParser,
        )
    }

    #[inline]
    fn new_impl(
        summary_collector: C,
        config: Arc<EvalConfig>,
        src: Src,
        group_by_exps: Vec<RpnExpression>,
        aggr_defs: Vec<Expr>,
        aggr_def_parser: impl AggrDefinitionParser,
    ) -> Result<Self> {
        let aggr_impl = MultiGroupHashAggregationImpl {
            states: Vec::with_capacity(1024),
            groups: HashMap::default(),
            group_by_exps,
            group_keys_buffer: Vec::with_capacity(1024),
            group_keys_buffer_offset: Vec::with_capacity(128),
            states_offset_each_row: Vec::with_capacity(
                crate::coprocessor::dag::batch_handler::BATCH_MAX_SIZE,
            ),
        };

        Ok(Self(AggregationExecutor::new(
            summary_collector,
            aggr_impl,
            src,
            config,
            aggr_defs,
            aggr_def_parser,
        )?))
    }
}

pub struct MultiGroupHashAggregationImpl {
    states: Vec<Box<dyn AggrFunctionState>>,
    groups: HashMap<Vec<u8>, GroupInfo>,
    group_by_exps: Vec<RpnExpression>,

    /// Suppose that we have the following group columns:
    ///
    /// ```ignore
    /// Col1    Col2
    /// Aaa     D
    /// Bb      Eeeee
    /// Cccc    Ff
    /// ```
    ///
    /// `group_keys_buffer` will be `[AaaBbCcccDEeeeeFf   ]`
    /// `group_keys_offset` will be `[0, 3,5,  9,10, 15,17]`
    group_keys_buffer: Vec<u8>,
    group_keys_buffer_offset: Vec<usize>,

    states_offset_each_row: Vec<usize>,
}

#[derive(Debug)]
struct GroupInfo {
    /// The start offset of the states of this group stored in
    /// `MultiGroupHashAggregationImpl::states`.
    states_start_offset: usize,

    /// The offset of each GROUP BY column in the group key. There will be an additional offset
    /// at the end, which equals to the group key length.
    ///
    /// For example, if the group key `AaaFfffff` is composed by two group column `[Aaa, Ffffff]`,
    /// then the value of this field is `[0, 3, 9]`.
    ///
    /// Note 1:
    /// We want aggregation executor to provide each GROUP BY column independently, which will be
    /// useful in future executors (like Projection executor). However, currently this segmentation
    /// is not useful because we return data by row and columns will be always pasted together.
    /// Thus segmentation or not doesn't make any differences.
    ///
    /// Note 2:
    /// We use `SmallVec<[..; 6]>` so that when group by columns <= 5 SmallVec avoids allocation.
    /// We think the case that there are more than 5 group by columns are rare.
    ///
    /// Note 3:
    /// We use `SmallVec<[u32; ..]>` instead of `SmallVec<[usize; ..]>` because the length of the
    /// group key will not exceed `u32`. Using `u32` instead of `usize` makes this field small
    /// and can be fit into the cache line better.
    group_key_offsets: SmallVec<[u32; 6]>,
}

impl<Src: BatchExecutor> AggregationExecutorImpl<Src> for MultiGroupHashAggregationImpl {
    #[inline]
    fn prepare_entities(&mut self, entities: &mut Entities<Src>) {
        let src_schema = entities.src.schema();
        for group_by_exp in &self.group_by_exps {
            entities
                .schema
                .push(group_by_exp.ret_field_type(src_schema).clone());
        }
    }

    #[inline]
    fn process_batch_input(
        &mut self,
        entities: &mut Entities<Src>,
        mut input: LazyBatchColumnVec,
    ) -> Result<()> {
        let rows_len = input.rows_len();
        let group_by_exps_len = self.group_by_exps.len();

        // 1. Calculate which group each src row belongs to.
        self.states_offset_each_row.clear();
        self.group_keys_buffer.clear();
        self.group_keys_buffer_offset.clear();

        let src_schema = entities.src.schema();

        for group_by_exp in &self.group_by_exps {
            let group_by_result =
                group_by_exp.eval(&mut entities.context, rows_len, src_schema, &mut input)?;
            for row_index in 0..rows_len {
                // Unwrap is fine because we have verified the group by expression before.
                let group_column = group_by_result.vector_value().unwrap();
                self.group_keys_buffer_offset
                    .push(self.group_keys_buffer.len());
                group_column.encode(
                    row_index,
                    group_by_result.field_type(),
                    &mut self.group_keys_buffer,
                )?;
            }
        }

        // One extra offset, to be used as the end offset.
        self.group_keys_buffer_offset
            .push(self.group_keys_buffer.len());

        for row_index in 0..rows_len {
            // The group key of row i (i start from 0) is the combination of slots
            // [i, i + row_len, i + row_len * 2, ...] in `group_keys_buffer`, where slots are
            // indexed by `group_keys_buffer_offset`.

            // TODO: We still need to allocate `rows_len` group key each time.
            // TODO: We don't need to construct `group_key_offsets` when group_key exists in
            // hash map.
            let mut group_key = Vec::with_capacity(32);
            let mut group_key_offsets = SmallVec::new();
            for group_col_index in 0..group_by_exps_len {
                let slot_index = row_index + group_col_index * rows_len;
                let offset_begin = self.group_keys_buffer_offset[slot_index];
                let offset_end = self.group_keys_buffer_offset[slot_index + 1];
                group_key_offsets.push(group_key.len() as u32);
                group_key.extend_from_slice(&self.group_keys_buffer[offset_begin..offset_end]);
            }
            group_key_offsets.push(group_key.len() as u32);

            match self.groups.entry(group_key) {
                Entry::Vacant(entry) => {
                    let offset = self.states.len();
                    self.states_offset_each_row.push(offset);
                    entry.insert(GroupInfo {
                        states_start_offset: offset,
                        group_key_offsets,
                    });
                    for aggr_fn in &entities.each_aggr_fn {
                        self.states.push(aggr_fn.create_state());
                    }
                }
                Entry::Occupied(entry) => {
                    self.states_offset_each_row
                        .push(entry.get().states_start_offset);
                }
            }
        }

        // 2. Update states according to the group.
        HashAggregationHelper::update_each_row_states_by_offset(
            entities,
            &mut input,
            &mut self.states,
            &self.states_offset_each_row,
        )?;

        Ok(())
    }

    #[inline]
    fn iterate_each_group_for_aggregation(
        &mut self,
        entities: &mut Entities<Src>,
        mut iteratee: impl FnMut(&mut Entities<Src>, &[Box<dyn AggrFunctionState>]) -> Result<()>,
    ) -> Result<Vec<LazyBatchColumn>> {
        let number_of_groups = self.groups.len();
        let group_by_exps_len = self.group_by_exps.len();
        let mut group_by_columns: Vec<_> = self
            .group_by_exps
            .iter()
            .map(|_| LazyBatchColumn::raw_with_capacity(number_of_groups))
            .collect();
        let aggr_fns_len = entities.each_aggr_fn.len();

        let groups = std::mem::replace(&mut self.groups, HashMap::default());
        for (group_key, group_info) in groups {
            iteratee(
                entities,
                &self.states
                    [group_info.states_start_offset..group_info.states_start_offset + aggr_fns_len],
            )?;

            // Extract group column from group key for each group
            for group_index in 0..group_by_exps_len {
                let offset_begin = group_info.group_key_offsets[group_index] as usize;
                let offset_end = group_info.group_key_offsets[group_index + 1] as usize;
                group_by_columns[group_index].push_raw(&group_key[offset_begin..offset_end]);
            }
        }

        Ok(group_by_columns)
    }
}
