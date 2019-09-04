// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::convert::TryFrom;
use std::sync::Arc;

use tidb_query_datatype::{EvalType, FieldTypeAccessor};
use tipb::Aggregation;
use tipb::{Expr, FieldType};

use crate::aggr_fn::*;
use crate::batch::executors::util::aggr_executor::*;
use crate::batch::executors::util::*;
use crate::batch::interface::*;
use crate::codec::batch::{LazyBatchColumn, LazyBatchColumnVec};
use crate::codec::data_type::*;
use crate::expr::{EvalConfig, EvalContext};
use crate::rpn_expr::RpnStackNode;
use crate::rpn_expr::{RpnExpression, RpnExpressionBuilder};
use crate::storage::IntervalRange;
use crate::Result;

pub struct BatchStreamAggregationExecutor<Src: BatchExecutor>(
    AggregationExecutor<Src, BatchStreamAggregationImpl>,
);

impl<Src: BatchExecutor> BatchExecutor for BatchStreamAggregationExecutor<Src> {
    type StorageStats = Src::StorageStats;

    #[inline]
    fn schema(&self) -> &[FieldType] {
        self.0.schema()
    }

    #[inline]
    fn next_batch(&mut self, scan_rows: usize) -> BatchExecuteResult {
        self.0.next_batch(scan_rows)
    }

    #[inline]
    fn collect_exec_stats(&mut self, dest: &mut ExecuteStats) {
        self.0.collect_exec_stats(dest);
    }

    #[inline]
    fn collect_storage_stats(&mut self, dest: &mut Self::StorageStats) {
        self.0.collect_storage_stats(dest);
    }

    #[inline]
    fn take_scanned_range(&mut self) -> IntervalRange {
        self.0.take_scanned_range()
    }
}

// We assign a dummy type `Box<dyn BatchExecutor<StorageStats = ()>>` so that we can omit the type
// when calling `check_supported`.
impl BatchStreamAggregationExecutor<Box<dyn BatchExecutor<StorageStats = ()>>> {
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

    /// Stores evaluation results of group by expressions.
    /// It is just used to reduce allocations. The lifetime is not really 'static. The elements
    /// are only valid in the same batch where they are added.
    group_by_results_unsafe: Vec<RpnStackNode<'static>>,

    /// Stores evaluation results of aggregate expressions.
    /// It is just used to reduce allocations. The lifetime is not really 'static. The elements
    /// are only valid in the same batch where they are added.
    aggr_expr_results_unsafe: Vec<RpnStackNode<'static>>,
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
                EvalType::try_from(exp.ret_field_type(src.schema()).as_accessor().tp()).unwrap()
            })
            .collect();

        let group_by_len = group_by_exps.len();
        let aggr_impl = BatchStreamAggregationImpl {
            group_by_exps,
            group_by_exps_types,
            keys: Vec::new(),
            states: Vec::new(),
            group_by_results_unsafe: Vec::with_capacity(group_by_len),
            aggr_expr_results_unsafe: Vec::with_capacity(aggr_defs.len()),
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
        mut input_physical_columns: LazyBatchColumnVec,
        input_logical_rows: &[usize],
    ) -> Result<()> {
        let context = &mut entities.context;
        let src_schema = entities.src.schema();

        let logical_rows_len = input_logical_rows.len();
        let group_by_len = self.group_by_exps.len();
        let aggr_fn_len = entities.each_aggr_fn.len();

        // Decode columns with mutable input first, so subsequent access to input can be immutable
        // (and the borrow checker will be happy)
        ensure_columns_decoded(
            &context.cfg.tz,
            &self.group_by_exps,
            src_schema,
            &mut input_physical_columns,
            input_logical_rows,
        )?;
        ensure_columns_decoded(
            &context.cfg.tz,
            &entities.each_aggr_exprs,
            src_schema,
            &mut input_physical_columns,
            input_logical_rows,
        )?;
        assert!(self.group_by_results_unsafe.is_empty());
        assert!(self.aggr_expr_results_unsafe.is_empty());
        unsafe {
            eval_exprs_decoded_no_lifetime(
                context,
                &self.group_by_exps,
                src_schema,
                &input_physical_columns,
                input_logical_rows,
                &mut self.group_by_results_unsafe,
            )?;
            eval_exprs_decoded_no_lifetime(
                context,
                &entities.each_aggr_exprs,
                src_schema,
                &input_physical_columns,
                input_logical_rows,
                &mut self.aggr_expr_results_unsafe,
            )?;
        }

        // Stores input references, clone them when needed
        let mut group_key_ref = Vec::with_capacity(group_by_len);
        let mut group_start_logical_row = 0;
        for logical_row_idx in 0..logical_rows_len {
            for group_by_result in &self.group_by_results_unsafe {
                group_key_ref.push(group_by_result.get_logical_scalar_ref(logical_row_idx));
            }
            match self.keys.rchunks_exact(group_by_len).next() {
                Some(current_key) if &group_key_ref[..] == current_key => {
                    group_key_ref.clear();
                }
                _ => {
                    // Update the complete group
                    if logical_row_idx > 0 {
                        update_current_states(
                            context,
                            &mut self.states,
                            aggr_fn_len,
                            &self.aggr_expr_results_unsafe,
                            group_start_logical_row,
                            logical_row_idx,
                        )?;
                    }

                    // create a new group
                    group_start_logical_row = logical_row_idx;
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
            &self.aggr_expr_results_unsafe,
            group_start_logical_row,
            logical_rows_len,
        )?;

        // Remember to remove expression results of the current batch. They are invalid
        // in the next batch.
        self.group_by_results_unsafe.clear();
        self.aggr_expr_results_unsafe.clear();

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

fn update_current_states(
    ctx: &mut EvalContext,
    states: &mut [Box<dyn AggrFunctionState>],
    aggr_fn_len: usize,
    aggr_expr_results: &[RpnStackNode<'_>],
    start_logical_row: usize,
    end_logical_row: usize,
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
                                state.update_repeat(
                                    ctx,
                                    scalar_value,
                                    end_logical_row - start_logical_row,
                                )?;
                            },
                        }
                    }
                }
                RpnStackNode::Vector { value, .. } => {
                    let physical_vec = value.as_ref();
                    let logical_rows = value.logical_rows();
                    match_template_evaluable! {
                        TT, match physical_vec {
                            VectorValue::TT(vec) => {
                                state.update_vector(
                                    ctx,
                                    vec,
                                    &logical_rows[start_logical_row..end_logical_row],
                                )?;
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

    use tidb_query_datatype::FieldTypeTp;
    use tipb::ScalarFuncSig;

    use crate::batch::executors::util::mock_executor::MockExecutor;
    use crate::expr::EvalWarnings;
    use crate::rpn_expr::impl_arithmetic::{arithmetic_fn_meta, RealPlus};
    use crate::rpn_expr::RpnExpressionBuilder;

    #[test]
    fn test_it_works_integration() {
        use tipb::ExprType;
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
                .push_fn_call(arithmetic_fn_meta::<RealPlus>(), 2, FieldTypeTp::Double)
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
        assert_eq!(&r.logical_rows, &[0, 1]);
        assert_eq!(r.physical_columns.rows_len(), 2);
        assert_eq!(r.physical_columns.columns_len(), 5);
        assert!(!r.is_drained.unwrap());
        // COUNT
        assert_eq!(
            r.physical_columns[0].decoded().as_int_slice(),
            &[Some(1), Some(2)]
        );
        // AVG_COUNT
        assert_eq!(
            r.physical_columns[1].decoded().as_int_slice(),
            &[Some(0), Some(2)]
        );
        // AVG_SUM
        assert_eq!(
            r.physical_columns[2].decoded().as_real_slice(),
            &[None, Real::new(5.0).ok()]
        );
        // col_0
        assert_eq!(
            r.physical_columns[3].decoded().as_bytes_slice(),
            &[None, None]
        );
        // col_1
        assert_eq!(
            r.physical_columns[4].decoded().as_real_slice(),
            &[None, Real::new(3.5).ok()]
        );

        let r = exec.next_batch(1);
        assert!(r.logical_rows.is_empty());
        assert_eq!(r.physical_columns.rows_len(), 0);
        assert!(!r.is_drained.unwrap());

        let r = exec.next_batch(1);
        assert_eq!(&r.logical_rows, &[0]);
        assert_eq!(r.physical_columns.rows_len(), 1);
        assert_eq!(r.physical_columns.columns_len(), 5);
        assert!(r.is_drained.unwrap());
        // COUNT
        assert_eq!(r.physical_columns[0].decoded().as_int_slice(), &[Some(5)]);
        // AVG_COUNT
        assert_eq!(r.physical_columns[1].decoded().as_int_slice(), &[Some(5)]);
        // AVG_SUM
        assert_eq!(
            r.physical_columns[2].decoded().as_real_slice(),
            &[Real::new(-20.0).ok()]
        );
        // col_0
        assert_eq!(
            r.physical_columns[3].decoded().as_bytes_slice(),
            &[Some(b"abc".to_vec())]
        );
        // col_1
        assert_eq!(
            r.physical_columns[4].decoded().as_real_slice(),
            &[Real::new(-3.0).ok()]
        );
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
        assert_eq!(&r.logical_rows, &[0, 1]);
        assert_eq!(r.physical_columns.rows_len(), 2);
        assert_eq!(r.physical_columns.columns_len(), 2);
        assert!(!r.is_drained.unwrap());
        // col_0
        assert_eq!(
            r.physical_columns[0].decoded().as_bytes_slice(),
            &[None, None]
        );
        // col_1
        assert_eq!(
            r.physical_columns[1].decoded().as_real_slice(),
            &[None, Real::new(1.5).ok()]
        );

        let r = exec.next_batch(1);
        assert!(r.logical_rows.is_empty());
        assert_eq!(r.physical_columns.rows_len(), 0);
        assert!(!r.is_drained.unwrap());

        let r = exec.next_batch(1);
        assert_eq!(&r.logical_rows, &[0]);
        assert_eq!(r.physical_columns.rows_len(), 1);
        assert_eq!(r.physical_columns.columns_len(), 2);
        assert!(r.is_drained.unwrap());
        // col_0
        assert_eq!(
            r.physical_columns[0].decoded().as_bytes_slice(),
            &[Some(b"abc".to_vec())]
        );
        // col_1
        assert_eq!(
            r.physical_columns[1].decoded().as_real_slice(),
            &[Real::new(-5.0).ok()]
        );
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
                    physical_columns: LazyBatchColumnVec::from(vec![
                        VectorValue::Bytes(vec![
                            Some(b"foo".to_vec()),
                            None,
                            Some(b"abc".to_vec()),
                            None,
                            None,
                            None,
                            Some(b"abc".to_vec()),
                        ]),
                        VectorValue::Real(vec![
                            Real::new(100.0).ok(),
                            Real::new(1.5).ok(),
                            Real::new(-5.0).ok(),
                            None,
                            Real::new(1.5).ok(),
                            None,
                            Real::new(-5.0).ok(),
                        ]),
                    ]),
                    logical_rows: vec![3, 1, 4, 2, 6],
                    warnings: EvalWarnings::default(),
                    is_drained: Ok(false),
                },
                BatchExecuteResult {
                    physical_columns: LazyBatchColumnVec::from(vec![
                        VectorValue::Bytes(vec![None, None, Some(b"abc".to_vec())]),
                        VectorValue::Real(vec![None, Real::new(100.0).ok(), Real::new(-5.0).ok()]),
                    ]),
                    logical_rows: vec![2],
                    warnings: EvalWarnings::default(),
                    is_drained: Ok(false),
                },
                BatchExecuteResult {
                    physical_columns: LazyBatchColumnVec::from(vec![
                        VectorValue::Bytes(vec![Some(b"abc".to_vec()), Some(b"abc".to_vec())]),
                        VectorValue::Real(vec![Real::new(-5.0).ok(), Real::new(-5.0).ok()]),
                    ]),
                    logical_rows: (0..2).collect(),
                    warnings: EvalWarnings::default(),
                    is_drained: Ok(true),
                },
            ],
        )
    }
}
