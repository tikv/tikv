// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::convert::TryFrom;
use std::sync::Arc;

use cop_datatype::{EvalType, FieldTypeAccessor};
use tikv_util::collections::HashMap;
use tipb::executor::Aggregation;
use tipb::expression::{Expr, FieldType};

use crate::coprocessor::codec::batch::{LazyBatchColumn, LazyBatchColumnVec};
use crate::coprocessor::codec::data_type::*;
use crate::coprocessor::dag::aggr_fn::*;
use crate::coprocessor::dag::batch::executors::util::aggr_executor::*;
use crate::coprocessor::dag::batch::interface::*;
use crate::coprocessor::dag::expr::{EvalConfig, EvalContext};
use crate::coprocessor::dag::rpn_expr::types::RpnStackNode;
use crate::coprocessor::dag::rpn_expr::{RpnExpression, RpnExpressionBuilder};
use crate::coprocessor::Result;

use smallvec::SmallVec;

pub struct BatchStreamAggregationExecutor<Src: BatchExecutor>(
    AggregationExecutor<Src, BatchStreamAggregationImpl>,
);

impl<Src: BatchExecutor> BatchExecutor for BatchStreamAggregationExecutor<Src> {
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

impl<Src: BatchExecutor> BatchStreamAggregationExecutor<Src> {
    #[cfg(test)]
    pub fn new_for_test(
        src: Src,
        group_by_exps: Vec<RpnExpression>,
        aggr_defs: Vec<Expr>,
        aggr_def_parser: impl AggrDefinitionParser,
    ) -> Self {
        Self::new_impl(
            Arc::new(EvalConfig::default()),
            src,
            group_by_exps,
            aggr_defs,
            aggr_def_parser,
        )
        .unwrap()
    }
}

impl BatchStreamAggregationExecutor<Box<dyn BatchExecutor>> {
    /// Checks whether this executor can be used.
    #[inline]
    pub fn check_supported(descriptor: &Aggregation) -> Result<()> {
        let group_by_definitions = descriptor.get_group_by();
        assert!(!group_by_definitions.is_empty());
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

pub struct BatchStreamAggregationImpl {
    group_by_exps: Vec<RpnExpression>,
    keys: Vec<ScalarValue>,
    states: Vec<Box<dyn AggrFunctionState>>,
}

impl<Src: BatchExecutor> BatchStreamAggregationExecutor<Src> {
    pub fn new(
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
            config,
            src,
            group_by_exps,
            aggr_defs,
            AllAggrDefinitionParser,
        )
    }

    #[inline]
    fn new_impl(
        config: Arc<EvalConfig>,
        src: Src,
        group_by_exps: Vec<RpnExpression>,
        aggr_defs: Vec<Expr>,
        aggr_def_parser: impl AggrDefinitionParser,
    ) -> Result<Self> {
        let aggr_impl = BatchStreamAggregationImpl {
            group_by_exps,
            keys: Vec::new(),
            states: Vec::new(),
        };

        Ok(Self(AggregationExecutor::new(
            aggr_impl,
            src,
            config,
            aggr_defs,
            aggr_def_parser,
        )?))
    }
}

impl<Src: BatchExecutor> AggregationExecutorImpl<Src> for BatchStreamAggregationImpl {
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
        let context = &mut entities.context;
        let src_schema = entities.src.schema();

        let rows_len = input.rows_len();
        let group_by_len = self.group_by_exps.len();
        let aggr_fn_len = entities.each_aggr_fn.len();

        ensure_columns_decoded(context, &self.group_by_exps, src_schema, &mut input)?;
        ensure_columns_decoded(context, &entities.each_aggr_exprs, src_schema, &mut input)?;
        let group_by_results = eval_exprs(context, &self.group_by_exps, src_schema, &input)?;
        let aggr_expr_results = eval_exprs(context, &entities.each_aggr_exprs, src_schema, &input)?;

        let mut group_key = Vec::with_capacity(group_by_len);
        let mut group_start_row = None;
        for row_index in 0..rows_len {
            for group_by_result in &group_by_results {
                // Unwrap is fine because we have verified the group by expression before.
                let group_column = group_by_result.vector_value().unwrap();
                group_key.push(group_column.get_unchecked(row_index));
            }
            match self.keys.rchunks_exact(group_by_len).next() {
                Some(current_key) if &group_key[..] == current_key => {
                    group_key.clear();
                }
                _ => {
                    if let Some(current_states) = self.states.rchunks_exact_mut(aggr_fn_len).next()
                    {
                        // if there is a group from the last batch, group_start_row will be None
                        let start_row = group_start_row.unwrap_or(0);
                        // update states
                        for (state, aggr_fn_input) in
                            current_states.iter_mut().zip(&aggr_expr_results)
                        {
                            match aggr_fn_input {
                                RpnStackNode::Scalar { value, .. } => {
                                    match_template_evaluable! {
                                        TT, match value {
                                            ScalarValue::TT(scalar_value) => {
                                                state.update_repeat(context, scalar_value, rows_len)?;
                                            },
                                        }
                                    }
                                }
                                RpnStackNode::Vector { value, .. } => {
                                    match_template_evaluable! {
                                        TT, match &**value {
                                            VectorValue::TT(vector_value) => {
                                                state.update_vector(context, vector_value)?;
                                            },
                                        }
                                    }
                                }
                            }
                        }
                    }

                    // new group
                    group_start_row = Some(row_index);
                    self.keys.extend(group_key.drain(..).map(Into::into));
                    for aggr_fn in &entities.each_aggr_fn {
                        self.states.push(aggr_fn.create_state());
                    }
                }
            }
        }

        Ok(())
    }

    #[inline]
    fn groups_len(&self) -> usize {
        self.keys
            .len()
            .checked_div(self.group_by_exps.len())
            .unwrap_or(0)
    }

    #[inline]
    fn iterate_each_group_for_aggregation(
        &mut self,
        entities: &mut Entities<Src>,
        mut iteratee: impl FnMut(&mut Entities<Src>, &[Box<dyn AggrFunctionState>]) -> Result<()>,
    ) -> Result<Vec<LazyBatchColumn>> {
        //        let number_of_groups = self.groups.len();
        //        let group_by_exps_len = self.group_by_exps.len();
        //        let mut group_by_columns: Vec<_> = self
        //            .group_by_exps
        //            .iter()
        //            .map(|_| LazyBatchColumn::raw_with_capacity(number_of_groups))
        //            .collect();
        //        let aggr_fns_len = entities.each_aggr_fn.len();
        //
        //        let groups = std::mem::replace(&mut self.groups, HashMap::default());
        //        for (group_key, group_info) in groups {
        //            iteratee(
        //                entities,
        //                &self.states
        //                    [group_info.states_start_offset..group_info.states_start_offset + aggr_fns_len],
        //            )?;
        //
        //            // Extract group column from group key for each group
        //            for group_index in 0..group_by_exps_len {
        //                let offset_begin = group_info.group_key_offsets[group_index] as usize;
        //                let offset_end = group_info.group_key_offsets[group_index + 1] as usize;
        //                group_by_columns[group_index].push_raw(&group_key[offset_begin..offset_end]);
        //            }
        //        }
        //
        //        Ok(group_by_columns)
        unimplemented!()
    }
}

fn ensure_columns_decoded(
    context: &mut EvalContext,
    exprs: &[RpnExpression],
    schema: &[FieldType],
    input: &mut LazyBatchColumnVec,
) -> Result<()> {
    for expr in exprs {
        expr.ensure_columns_decoded(context, schema, input)?;
    }
    Ok(())
}

fn eval_exprs<'a, 'b>(
    context: &'a mut EvalContext,
    exprs: &'b [RpnExpression],
    schema: &'b [FieldType],
    input: &'b LazyBatchColumnVec,
) -> Result<Vec<RpnStackNode<'b>>> {
    exprs
        .iter()
        .map(|expr| expr.eval_unchecked(context, input.len(), schema, &input))
        .collect()
}
