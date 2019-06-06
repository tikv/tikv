// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::convert::TryFrom;
use std::sync::Arc;

use cop_datatype::{EvalType, FieldTypeAccessor};
use tipb::executor::Aggregation;
use tipb::expression::{Expr, FieldType};

use crate::coprocessor::codec::batch::{LazyBatchColumn, LazyBatchColumnVec};
use crate::coprocessor::codec::data_type::*;
use crate::coprocessor::codec::mysql::time::Tz;
use crate::coprocessor::dag::aggr_fn::*;
use crate::coprocessor::dag::batch::executors::util::aggr_executor::*;
use crate::coprocessor::dag::batch::interface::*;
use crate::coprocessor::dag::expr::{EvalConfig, EvalContext};
use crate::coprocessor::dag::rpn_expr::types::RpnStackNode;
use crate::coprocessor::dag::rpn_expr::{RpnExpression, RpnExpressionBuilder};
use crate::coprocessor::Result;

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
            // Works for both vector and scalar. No need to check as other aggregation executor.
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
    /// used in the `iterate_each_group_for_aggregation` method
    group_by_exps_types: Vec<EvalType>,
    /// Stores all group keys for the current result partial.
    /// The last `group_by_exps.len()` elements are the keys of the last group.
    keys: Vec<ScalarValue>,
    /// Stores all group states for the current result partial.
    /// The last `each_aggr_fn.len()` elements are the states of the last group.
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
        let group_by_exps_types = group_by_exps
            .iter()
            .map(|exp| {
                // The unwrap is fine because aggregate function parser should never return an
                // eval type that we cannot process later. If we made a mistake there, then we
                // should panic.
                EvalType::try_from(exp.ret_field_type(src.schema()).tp()).unwrap()
            })
            .collect();

        let aggr_impl = BatchStreamAggregationImpl {
            group_by_exps,
            group_by_exps_types,
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

        // Decode columns with mutable input first, so subsequent access to input can be immutable
        // (and the borrow checker will be happy)
        ensure_columns_decoded(&context.cfg.tz, &self.group_by_exps, src_schema, &mut input)?;
        ensure_columns_decoded(
            &context.cfg.tz,
            &entities.each_aggr_exprs,
            src_schema,
            &mut input,
        )?;
        let group_by_results = eval_exprs(context, &self.group_by_exps, src_schema, &input)?;
        let aggr_expr_results = eval_exprs(context, &entities.each_aggr_exprs, src_schema, &input)?;

        // Stores input references, clone them when needed
        let mut group_key_ref = Vec::with_capacity(group_by_len);
        let mut group_start_row = 0;
        for row_index in 0..rows_len {
            for group_by_result in &group_by_results {
                group_key_ref.push(group_by_result.get_scalar_ref(row_index));
            }
            match self.keys.rchunks_exact(group_by_len).next() {
                Some(current_key) if &group_key_ref[..] == current_key => {
                    group_key_ref.clear();
                }
                _ => {
                    // Update the complete group
                    if row_index > 0 {
                        update_current_states(
                            context,
                            &mut self.states,
                            aggr_fn_len,
                            &aggr_expr_results,
                            group_start_row,
                            row_index,
                        )?;
                    }

                    // create a new group
                    group_start_row = row_index;
                    self.keys
                        .extend(group_key_ref.drain(..).map(ScalarValueRef::to_owned));
                    for aggr_fn in &entities.each_aggr_fn {
                        self.states.push(aggr_fn.create_state());
                    }
                }
            }
        }
        // Update the current group with the remaining data
        update_current_states(
            context,
            &mut self.states,
            aggr_fn_len,
            &aggr_expr_results,
            group_start_row,
            rows_len,
        )?;

        Ok(())
    }

    /// Note that the partial group is in count.
    #[inline]
    fn groups_len(&self) -> usize {
        self.keys
            .len()
            .checked_div(self.group_by_exps.len())
            .unwrap_or(0)
    }

    #[inline]
    fn iterate_available_groups(
        &mut self,
        entities: &mut Entities<Src>,
        src_is_drained: bool,
        mut iteratee: impl FnMut(&mut Entities<Src>, &[Box<dyn AggrFunctionState>]) -> Result<()>,
    ) -> Result<Vec<LazyBatchColumn>> {
        let number_of_groups = if src_is_drained {
            AggregationExecutorImpl::<Src>::groups_len(self)
        } else {
            // don't include the partial group
            AggregationExecutorImpl::<Src>::groups_len(self) - 1
        };

        let group_by_exps_len = self.group_by_exps.len();
        let mut group_by_columns: Vec<_> = self
            .group_by_exps_types
            .iter()
            .map(|tp| LazyBatchColumn::decoded_with_capacity_and_tp(number_of_groups, *tp))
            .collect();
        let aggr_fns_len = entities.each_aggr_fn.len();

        // key and state ranges of all available groups
        let keys_range = ..number_of_groups * group_by_exps_len;
        let states_range = ..number_of_groups * aggr_fns_len;

        if aggr_fns_len > 0 {
            for states in self.states[states_range].chunks_exact(aggr_fns_len) {
                iteratee(entities, states)?;
            }
            self.states.drain(states_range);
        }

        for (key, group_index) in self
            .keys
            .drain(keys_range)
            .zip((0..group_by_exps_len).cycle())
        {
            match_template_evaluable! {
                TT, match key {
                    ScalarValue::TT(key) => {
                        group_by_columns[group_index].mut_decoded().push(key);
                    }
                }
            }
        }

        Ok(group_by_columns)
    }

    /// We cannot ensure the last group is complete, so we can output partial results
    /// only if group count >= 2.
    #[inline]
    fn is_partial_results_ready(&self) -> bool {
        AggregationExecutorImpl::<Src>::groups_len(self) >= 2
    }
}

fn ensure_columns_decoded(
    tz: &Tz,
    exprs: &[RpnExpression],
    schema: &[FieldType],
    input: &mut LazyBatchColumnVec,
) -> Result<()> {
    for expr in exprs {
        expr.ensure_columns_decoded(tz, schema, input)?;
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
        .map(|expr| expr.eval_unchecked(context, input.rows_len(), schema, &input))
        .collect()
}

fn update_current_states(
    context: &mut EvalContext,
    states: &mut [Box<dyn AggrFunctionState>],
    aggr_fn_len: usize,
    aggr_expr_results: &[RpnStackNode<'_>],
    start_row: usize,
    end_row: usize,
) -> Result<()> {
    if aggr_fn_len == 0 {
        return Ok(());
    }

    if let Some(current_states) = states.rchunks_exact_mut(aggr_fn_len).next() {
        for (state, aggr_fn_input) in current_states.iter_mut().zip(aggr_expr_results) {
            match aggr_fn_input {
                RpnStackNode::Scalar { value, .. } => {
                    match_template_evaluable! {
                        TT, match value {
                            ScalarValue::TT(scalar_value) => {
                                state.update_repeat(context, scalar_value, end_row - start_row)?;
                            },
                        }
                    }
                }
                RpnStackNode::Vector { value, .. } => {
                    match_template_evaluable! {
                        TT, match &**value {
                            VectorValue::TT(vector_value) => {
                                state.update_vector(context, &vector_value[start_row..end_row])?;
                            },
                        }
                    }
                }
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    use cop_datatype::FieldTypeTp;
    use tipb::expression::ScalarFuncSig;

    use crate::coprocessor::dag::batch::executors::util::mock_executor::MockExecutor;
    use crate::coprocessor::dag::expr::EvalWarnings;
    use crate::coprocessor::dag::rpn_expr::impl_arithmetic::{RealPlus, RpnFnArithmetic};
    use crate::coprocessor::dag::rpn_expr::RpnExpressionBuilder;

    #[test]
    fn test_it_works_integration() {
        use tipb::expression::ExprType;
        use tipb_helper::ExprDefBuilder;

        // This test creates a stream aggregation executor with the following aggregate functions:
        // - COUNT(1)
        // - AVG(col_1 + 1.0)
        // And group by:
        // - col_0
        // - col_1 + 2.0

        let group_by_exps = vec![
            RpnExpressionBuilder::new().push_column_ref(0).build(),
            RpnExpressionBuilder::new()
                .push_column_ref(1)
                .push_constant(2.0)
                .push_fn_call(RpnFnArithmetic::<RealPlus>::new(), FieldTypeTp::Double)
                .build(),
        ];

        let aggr_definitions = vec![
            ExprDefBuilder::aggr_func(ExprType::Count, FieldTypeTp::LongLong)
                .push_child(ExprDefBuilder::constant_int(1))
                .build(),
            ExprDefBuilder::aggr_func(ExprType::Avg, FieldTypeTp::Double)
                .push_child(
                    ExprDefBuilder::scalar_func(ScalarFuncSig::PlusReal, FieldTypeTp::Double)
                        .push_child(ExprDefBuilder::column_ref(1, FieldTypeTp::Double))
                        .push_child(ExprDefBuilder::constant_real(1.0)),
                )
                .build(),
        ];

        let src_exec = make_src_executor();
        let mut exec = BatchStreamAggregationExecutor::new_for_test(
            src_exec,
            group_by_exps,
            aggr_definitions,
            AllAggrDefinitionParser,
        );

        let r = exec.next_batch(1);
        assert_eq!(r.data.rows_len(), 2);
        assert_eq!(r.data.columns_len(), 5);
        assert!(!r.is_drained.unwrap());
        // COUNT
        assert_eq!(r.data[0].decoded().as_int_slice(), &[Some(1), Some(2)]);
        // AVG_COUNT
        assert_eq!(r.data[1].decoded().as_int_slice(), &[Some(0), Some(2)]);
        // AVG_SUM
        assert_eq!(
            r.data[2].decoded().as_real_slice(),
            &[None, Real::new(5.0).ok()]
        );
        // col_0
        assert_eq!(r.data[3].decoded().as_bytes_slice(), &[None, None]);
        // col_1
        assert_eq!(
            r.data[4].decoded().as_real_slice(),
            &[None, Real::new(3.5).ok()]
        );

        let r = exec.next_batch(1);
        assert_eq!(r.data.rows_len(), 0);
        assert!(!r.is_drained.unwrap());

        let r = exec.next_batch(1);
        assert_eq!(r.data.rows_len(), 1);
        assert_eq!(r.data.columns_len(), 5);
        assert!(r.is_drained.unwrap());
        // COUNT
        assert_eq!(r.data[0].decoded().as_int_slice(), &[Some(5)]);
        // AVG_COUNT
        assert_eq!(r.data[1].decoded().as_int_slice(), &[Some(5)]);
        // AVG_SUM
        assert_eq!(
            r.data[2].decoded().as_real_slice(),
            &[Real::new(-20.0).ok()]
        );
        // col_0
        assert_eq!(
            r.data[3].decoded().as_bytes_slice(),
            &[Some(b"abc".to_vec())]
        );
        // col_1
        assert_eq!(r.data[4].decoded().as_real_slice(), &[Real::new(-3.0).ok()]);
    }

    /// Only have GROUP BY columns but no Aggregate Functions.
    ///
    /// E.g. SELECT 1 FROM t GROUP BY x
    #[test]
    fn test_no_fn() {
        let group_by_exps = vec![
            RpnExpressionBuilder::new().push_column_ref(0).build(),
            RpnExpressionBuilder::new().push_column_ref(1).build(),
        ];

        let src_exec = make_src_executor();
        let mut exec = BatchStreamAggregationExecutor::new_for_test(
            src_exec,
            group_by_exps,
            vec![],
            AllAggrDefinitionParser,
        );

        let r = exec.next_batch(1);
        assert_eq!(r.data.rows_len(), 2);
        assert_eq!(r.data.columns_len(), 2);
        assert!(!r.is_drained.unwrap());
        // col_0
        assert_eq!(r.data[0].decoded().as_bytes_slice(), &[None, None]);
        // col_1
        assert_eq!(
            r.data[1].decoded().as_real_slice(),
            &[None, Real::new(1.5).ok()]
        );

        let r = exec.next_batch(1);
        assert_eq!(r.data.rows_len(), 0);
        assert!(!r.is_drained.unwrap());

        let r = exec.next_batch(1);
        assert_eq!(r.data.rows_len(), 1);
        assert_eq!(r.data.columns_len(), 2);
        assert!(r.is_drained.unwrap());
        // col_0
        assert_eq!(
            r.data[0].decoded().as_bytes_slice(),
            &[Some(b"abc".to_vec())]
        );
        // col_1
        assert_eq!(r.data[1].decoded().as_real_slice(), &[Real::new(-5.0).ok()]);
    }

    /// Builds an executor that will return these data:
    ///
    /// == Schema ==
    /// Col0(Bytes)   Col1(Real)
    /// == Call #1 ==
    /// NULL          NULL
    /// NULL          1.5
    /// NULL          1.5
    /// abc           -5.0
    /// abc           -5.0
    /// == Call #2 ==
    /// abc           -5.0
    /// == Call #3 ==
    /// abc           -5.0
    /// abc           -5.0
    /// (drained)
    pub fn make_src_executor() -> MockExecutor {
        MockExecutor::new(
            vec![FieldTypeTp::VarString.into(), FieldTypeTp::Double.into()],
            vec![
                BatchExecuteResult {
                    data: LazyBatchColumnVec::from(vec![
                        VectorValue::Bytes(vec![
                            None,
                            None,
                            None,
                            Some(b"abc".to_vec()),
                            Some(b"abc".to_vec()),
                        ]),
                        VectorValue::Real(vec![
                            None,
                            Real::new(1.5).ok(),
                            Real::new(1.5).ok(),
                            Real::new(-5.0).ok(),
                            Real::new(-5.0).ok(),
                        ]),
                    ]),
                    warnings: EvalWarnings::default(),
                    is_drained: Ok(false),
                },
                BatchExecuteResult {
                    data: LazyBatchColumnVec::from(vec![
                        VectorValue::Bytes(vec![Some(b"abc".to_vec())]),
                        VectorValue::Real(vec![Real::new(-5.0).ok()]),
                    ]),
                    warnings: EvalWarnings::default(),
                    is_drained: Ok(false),
                },
                BatchExecuteResult {
                    data: LazyBatchColumnVec::from(vec![
                        VectorValue::Bytes(vec![Some(b"abc".to_vec()), Some(b"abc".to_vec())]),
                        VectorValue::Real(vec![Real::new(-5.0).ok(), Real::new(-5.0).ok()]),
                    ]),
                    warnings: EvalWarnings::default(),
                    is_drained: Ok(true),
                },
            ],
        )
    }
}
