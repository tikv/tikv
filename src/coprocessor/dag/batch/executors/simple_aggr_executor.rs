// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

//! Simple aggregation is an aggregation that do not have `GROUP BY`s. It is more even more simpler
//! than stream aggregation.

use std::convert::TryFrom;
use std::sync::Arc;

use cop_datatype::{EvalType, FieldTypeAccessor};
use tipb::executor::Aggregation;
use tipb::expression::Expr;
use tipb::expression::FieldType;

use super::super::interface::*;
use crate::coprocessor::codec::batch::LazyBatchColumnVec;
use crate::coprocessor::codec::data_type::*;
use crate::coprocessor::codec::mysql::Tz;
use crate::coprocessor::dag::aggr_fn::*;
use crate::coprocessor::dag::exec_summary::ExecSummaryCollectorDisabled;
use crate::coprocessor::dag::expr::{EvalConfig, EvalContext};
use crate::coprocessor::dag::rpn_expr::types::RpnStackNode;
use crate::coprocessor::dag::rpn_expr::RpnExpression;
use crate::coprocessor::Result;

pub struct BatchSimpleAggregationExecutor<C: ExecSummaryCollector, Src: BatchExecutor> {
    summary_collector: C,
    context: EvalContext,
    src: Src,

    is_ended: bool,

    /// The states of each aggregate function.
    aggr_fn_states: Vec<Box<dyn AggrFunctionState>>,

    /// The output cardinality of each aggregate function.
    aggr_fn_output_cardinality: Vec<usize>,

    /// The schema of all aggregate function output columns.
    ///
    /// Elements are ordered in the same way as the passed in aggregate functions.
    ordered_schema: Vec<FieldType>,

    /// The type of all aggregate function output columns.
    ///
    /// Elements are ordered in the same way as the passed in aggregate functions.
    ordered_aggr_fn_output_types: Vec<EvalType>,

    /// All aggregate function expressions.
    ///
    /// Elements are ordered in the same way as the passed in aggregate functions.
    ordered_aggr_fn_input_exprs: Vec<RpnExpression>,
}

impl<Src: BatchExecutor> BatchSimpleAggregationExecutor<ExecSummaryCollectorDisabled, Src> {
    #[cfg(test)]
    pub fn new_for_test<F>(src: Src, aggr_definitions: Vec<Expr>, parse_aggr_definition: F) -> Self
    where
        F: Fn(
            Expr,
            &Tz,
            usize,
            &mut Vec<FieldType>,
            &mut Vec<RpnExpression>,
        ) -> Result<Box<dyn AggrFunction>>,
    {
        Self::new_impl(
            ExecSummaryCollectorDisabled,
            Arc::new(EvalConfig::default()),
            src,
            aggr_definitions,
            parse_aggr_definition,
        )
        .unwrap()
    }
}

impl BatchSimpleAggregationExecutor<ExecSummaryCollectorDisabled, Box<dyn BatchExecutor>> {
    /// Checks whether this executor can be used.
    #[inline]
    pub fn check_supported(descriptor: &Aggregation) -> Result<()> {
        assert_eq!(descriptor.get_group_by().len(), 0);
        let aggr_definitions = descriptor.get_agg_func();
        for def in aggr_definitions {
            AggrDefinitionParser::check_supported(def)?;
        }
        Ok(())
    }
}

impl<C: ExecSummaryCollector, Src: BatchExecutor> BatchSimpleAggregationExecutor<C, Src> {
    pub fn new(
        summary_collector: C,
        config: Arc<EvalConfig>,
        src: Src,
        aggr_definitions: Vec<Expr>,
    ) -> Result<Self> {
        Self::new_impl(
            summary_collector,
            config,
            src,
            aggr_definitions,
            AggrDefinitionParser::parse,
        )
    }

    /// Provides ability to customize `AggrDefinitionParser::parse`. Useful in tests.
    fn new_impl<F>(
        summary_collector: C,
        config: Arc<EvalConfig>,
        src: Src,
        aggr_definitions: Vec<Expr>,
        parse_aggr_definition: F,
    ) -> Result<Self>
    where
        F: Fn(
            Expr,
            &Tz,
            usize,
            &mut Vec<FieldType>,
            &mut Vec<RpnExpression>,
        ) -> Result<Box<dyn AggrFunction>>,
    {
        let aggr_len = aggr_definitions.len();
        if aggr_len == 0 {
            return Err(box_err!("There should be at least one aggregate function"));
        }

        let mut aggr_fn_states = Vec::with_capacity(aggr_len);
        let mut aggr_fn_output_cardinality = Vec::with_capacity(aggr_len);
        let mut ordered_schema = Vec::with_capacity(aggr_len * 2);
        let mut ordered_aggr_fn_input_exprs = Vec::with_capacity(aggr_len * 2);

        let schema = src.schema();
        let schema_len = schema.len();

        for def in aggr_definitions {
            let aggr_output_len = ordered_schema.len();
            let aggr_input_len = ordered_aggr_fn_input_exprs.len();

            let aggr_fn = parse_aggr_definition(
                def,
                &config.tz,
                schema_len,
                &mut ordered_schema,
                &mut ordered_aggr_fn_input_exprs,
            )?;

            assert!(ordered_schema.len() > aggr_output_len);
            let this_aggr_output_len = ordered_schema.len() - aggr_output_len;

            // We only support 1 parameter aggregate functions, so let's simply assert it.
            assert_eq!(ordered_aggr_fn_input_exprs.len(), aggr_input_len + 1);

            aggr_fn_states.push(aggr_fn.create_state());
            aggr_fn_output_cardinality.push(this_aggr_output_len);
        }

        let ordered_aggr_fn_output_types = ordered_schema
            .iter()
            .map(|ft| {
                // The unwrap is fine because aggregate function parser should never return an
                // eval type that we cannot process later. If we made a mistake there, then we
                // should panic.
                EvalType::try_from(ft.tp()).unwrap()
            })
            .collect();

        Ok(Self {
            summary_collector,
            context: EvalContext::new(config),
            src,
            is_ended: false,
            aggr_fn_states,
            aggr_fn_output_cardinality,
            ordered_schema,
            ordered_aggr_fn_output_types,
            ordered_aggr_fn_input_exprs,
        })
    }

    #[inline]
    fn process_src_data(&mut self, mut data: LazyBatchColumnVec) -> Result<()> {
        let rows_len = data.rows_len();
        if rows_len == 0 {
            return Ok(());
        }

        // There are multiple aggregate functions to calculate, so let's do it one by one.
        for (index, aggr_fn_state) in &mut self.aggr_fn_states.iter_mut().enumerate() {
            // First, evaluates the expression to get the data to aggregate.
            let eval_output = self.ordered_aggr_fn_input_exprs[index].eval(
                &mut self.context,
                rows_len,
                self.src.schema(),
                &mut data,
            )?;

            // Next, feed the evaluation result to the aggregate function.
            match eval_output {
                RpnStackNode::Scalar { value, .. } => {
                    match_template_evaluable! {
                        TT, match value {
                            ScalarValue::TT(scalar_value) => {
                                aggr_fn_state.update_repeat(&mut self.context, scalar_value, rows_len)?;
                            },
                        }
                    }
                }
                RpnStackNode::Vector { value, .. } => {
                    match_template_evaluable! {
                        TT, match &*value {
                            VectorValue::TT(vector_value) => {
                                aggr_fn_state.update_vector(&mut self.context, vector_value)?;
                            },
                        }
                    }
                }
            }
        }

        Ok(())
    }

    // Don't inline this function to reduce hot code size.
    #[inline(never)]
    fn aggregate_results(&mut self) -> Result<LazyBatchColumnVec> {
        // Construct empty columns. All columns are decoded.
        let mut output_columns: Vec<_> = self
            .ordered_aggr_fn_output_types
            .iter()
            .map(|eval_type| VectorValue::with_capacity(1, *eval_type))
            .collect();

        let mut output_offset = 0;
        for (index, aggr_fn_state) in &mut self.aggr_fn_states.iter_mut().enumerate() {
            let output_cardinality = self.aggr_fn_output_cardinality[index];
            assert!(output_cardinality > 0);

            if output_cardinality == 1 {
                // Single output column, we use `Vec<Option<T>>` as container.
                let output_type = self.ordered_aggr_fn_output_types[output_offset];
                match_template_evaluable! {
                    TT, match output_type {
                        EvalType::TT => {
                            let concrete_output_column: &mut Vec<Option<TT>> = output_columns[output_offset].as_mut();
                            aggr_fn_state.push_result(&mut self.context, concrete_output_column)?;
                        }
                    }
                }
            } else {
                // Multi output column, we use `[VectorValue]` as container.
                aggr_fn_state.push_result(
                    &mut self.context,
                    &mut output_columns[output_offset..output_offset + output_cardinality],
                )?;
            }

            output_offset += output_cardinality;
        }

        // Check whether data is actually outputted when there is no error. If not,
        // we should panic.
        let ret = LazyBatchColumnVec::from(output_columns);
        assert_eq!(ret.rows_len(), 1);
        ret.assert_columns_equal_length();

        Ok(ret)
    }

    #[inline]
    fn handle_next_batch(&mut self) -> Result<Option<LazyBatchColumnVec>> {
        // Use max batch size from the beginning because aggregation executor always scans all data
        let src_result = self
            .src
            .next_batch(crate::coprocessor::dag::batch_handler::BATCH_MAX_SIZE);

        self.context.warnings = src_result.warnings;

        // When there are errors in the underlying executor, there must be no aggregate output.
        // Thus we even don't need to update the aggregate function state and can return directly.
        let src_is_drained = src_result.is_drained?;

        // Consume all data from the underlying executor. We directly return when there are errors
        // for the same reason as above.
        self.process_src_data(src_result.data)?;

        // Aggregate a result if source executor is drained, otherwise just return nothing.
        if src_is_drained {
            Ok(Some(self.aggregate_results()?))
        } else {
            Ok(None)
        }
    }
}

impl<C: ExecSummaryCollector, Src: BatchExecutor> BatchExecutor
    for BatchSimpleAggregationExecutor<C, Src>
{
    #[inline]
    fn schema(&self) -> &[FieldType] {
        self.ordered_schema.as_slice()
    }

    #[inline]
    fn next_batch(&mut self, _scan_rows: usize) -> BatchExecuteResult {
        assert!(!self.is_ended);

        let timer = self.summary_collector.on_start_iterate();
        let result = self.handle_next_batch();

        let ret = match result {
            Err(e) => {
                // When there are error, we can just return empty data.
                self.is_ended = true;
                BatchExecuteResult {
                    data: LazyBatchColumnVec::empty(),
                    warnings: self.context.take_warnings(),
                    is_drained: Err(e),
                }
            }
            Ok(None) => {
                // When there is no error and is not drained, we also return empty data.
                BatchExecuteResult {
                    data: LazyBatchColumnVec::empty(),
                    warnings: self.context.take_warnings(),
                    is_drained: Ok(false),
                }
            }
            Ok(Some(data)) => {
                // When there is no error and aggregate finished, we return it as data.
                self.is_ended = true;
                BatchExecuteResult {
                    data,
                    warnings: self.context.take_warnings(),
                    is_drained: Ok(true),
                }
            }
        };

        self.summary_collector
            .on_finish_iterate(timer, ret.data.rows_len());
        ret
    }

    #[inline]
    fn collect_statistics(&mut self, destination: &mut BatchExecuteStatistics) {
        self.src.collect_statistics(destination);
        self.summary_collector
            .collect_into(&mut destination.summary_per_executor);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use cop_codegen::AggrFunction;
    use cop_datatype::FieldTypeTp;

    use crate::coprocessor::dag::batch::executors::util::mock_executor::MockExecutor;
    use crate::coprocessor::dag::expr::EvalWarnings;
    use crate::coprocessor::dag::rpn_expr::RpnExpressionBuilder;

    /// Builds an executor that will return these data:
    ///
    /// == Schema ==
    /// Col0(Real)   Col1(Real)  Col2(Bytes) Col3(Int)
    /// == Call #1 ==
    /// NULL         1.0         abc         1
    /// 7.0          2.0         NULL        NULL
    /// NULL         NULL        ""          NULL
    /// NULL         4.5         HelloWorld  NULL
    /// == Call #2 ==
    /// == Call #3 ==
    /// 1.5          4.5         aaaaa       5
    /// (drained)
    fn make_src_executor_using_fixture() -> MockExecutor {
        MockExecutor::new(
            vec![
                FieldTypeTp::Double.into(), // this column is not used
                FieldTypeTp::Double.into(),
                FieldTypeTp::VarString.into(),
                FieldTypeTp::LongLong.into(), // this column is not used
            ],
            vec![
                BatchExecuteResult {
                    data: LazyBatchColumnVec::from(vec![
                        VectorValue::Real(vec![None, Real::new(7.0).ok(), None, None]),
                        VectorValue::Real(vec![
                            Real::new(1.0).ok(),
                            Real::new(2.0).ok(),
                            None,
                            Real::new(4.5).ok(),
                        ]),
                        VectorValue::Bytes(vec![
                            Some(b"abc".to_vec()),
                            None,
                            Some(vec![]),
                            Some(b"HelloWorld".to_vec()),
                        ]),
                        VectorValue::Int(vec![Some(1), None, None, None]),
                    ]),
                    warnings: EvalWarnings::default(),
                    is_drained: Ok(false),
                },
                BatchExecuteResult {
                    data: LazyBatchColumnVec::empty(),
                    warnings: EvalWarnings::default(),
                    is_drained: Ok(false),
                },
                BatchExecuteResult {
                    data: LazyBatchColumnVec::from(vec![
                        VectorValue::Real(vec![Real::new(1.5).ok()]),
                        VectorValue::Real(vec![Real::new(4.5).ok()]),
                        VectorValue::Bytes(vec![Some(b"aaaaa".to_vec())]),
                        VectorValue::Int(vec![Some(5)]),
                    ]),
                    warnings: EvalWarnings::default(),
                    is_drained: Ok(true),
                },
            ],
        )
    }

    #[test]
    fn test_it_works_unit() {
        /// Aggregate function `Foo` accepts a Bytes column, returns a Int datum.
        ///
        /// The returned data is the sum of the length of all accepted bytes datums.
        #[derive(Debug, AggrFunction)]
        #[aggr_function(state = AggrFnFooState::new())]
        struct AggrFnFoo;

        #[derive(Debug)]
        struct AggrFnFooState {
            len: usize,
        }

        impl AggrFnFooState {
            pub fn new() -> Self {
                Self { len: 0 }
            }
        }

        impl ConcreteAggrFunctionState for AggrFnFooState {
            type ParameterType = Bytes;
            type ResultTargetType = Vec<Option<i64>>;

            fn update_concrete(
                &mut self,
                _ctx: &mut EvalContext,
                value: &Option<Self::ParameterType>,
            ) -> Result<()> {
                if let Some(value) = value {
                    self.len += value.len();
                }
                Ok(())
            }

            fn push_result_concrete(
                &self,
                _ctx: &mut EvalContext,
                target: &mut Self::ResultTargetType,
            ) -> Result<()> {
                target.push(Some(self.len as i64));
                Ok(())
            }
        }

        /// `Foo` returns a Int datum.
        fn push_foo_output_schema(output: &mut Vec<FieldType>) {
            output.push(FieldTypeTp::LongLong.into());
        }

        /// Aggregate function `Bar` accepts a Real column, returns `(a: Int, b: Int, c: Real)`,
        /// where `a` is the number of rows including nulls, `b` is the number of rows excluding
        /// nulls, `c` is the sum of all values.
        #[derive(Debug, AggrFunction)]
        #[aggr_function(state = AggrFnBarState::new())]
        struct AggrFnBar;

        #[derive(Debug)]
        struct AggrFnBarState {
            rows_with_null: usize,
            rows_without_null: usize,
            sum: Real,
        }

        impl AggrFnBarState {
            pub fn new() -> Self {
                Self {
                    rows_with_null: 0,
                    rows_without_null: 0,
                    sum: Real::from(0.0),
                }
            }
        }

        impl ConcreteAggrFunctionState for AggrFnBarState {
            type ParameterType = Real;
            type ResultTargetType = [VectorValue];

            fn update_concrete(
                &mut self,
                _ctx: &mut EvalContext,
                value: &Option<Self::ParameterType>,
            ) -> Result<()> {
                self.rows_with_null += 1;
                if let Some(value) = value {
                    self.rows_without_null += 1;
                    self.sum += *value;
                }
                Ok(())
            }

            fn push_result_concrete(
                &self,
                _ctx: &mut EvalContext,
                target: &mut Self::ResultTargetType,
            ) -> Result<()> {
                target[0].push_int(Some(self.rows_with_null as i64));
                target[1].push_int(Some(self.rows_without_null as i64));
                target[2].push_real(Some(self.sum));
                Ok(())
            }
        }

        /// `Bar` returns `(a: Int, b: Int, c: Real)`.
        fn push_bar_output_schema(output: &mut Vec<FieldType>) {
            output.push(FieldTypeTp::LongLong.into());
            output.push(FieldTypeTp::Long.into());
            output.push(FieldTypeTp::Double.into());
        }

        // This test creates a simple aggregation executor with the following aggregate functions:
        // - Foo("abc")
        // - Foo(NULL)
        // - Bar(42.5)
        // - Bar(NULL)
        // - Foo(col_2)
        // - Bar(col_1)
        // As a result, there should be 12 output columns.

        let src_exec = make_src_executor_using_fixture();

        // As a unit test, let's use the most simple way to build the executor. No complex parsers
        // involved.

        let aggr_definitions: Vec<_> = (0..6)
            .map(|index| {
                let mut exp = Expr::new();
                exp.mut_val().push(index as u8);
                exp
            })
            .collect();

        let mut exec = BatchSimpleAggregationExecutor::new_for_test(
            src_exec,
            aggr_definitions,
            |def, _, _, out_schema, out_exp| {
                match def.get_val()[0] {
                    0 => {
                        // Foo("abc") -> Int
                        push_foo_output_schema(out_schema);
                        out_exp.push(
                            RpnExpressionBuilder::new()
                                .push_constant(b"abc".to_vec())
                                .build(),
                        );
                        Ok(Box::new(AggrFnFoo))
                    }
                    1 => {
                        // Foo(NULL) -> Int
                        push_foo_output_schema(out_schema);
                        out_exp.push(
                            RpnExpressionBuilder::new()
                                .push_constant(ScalarValue::Bytes(None))
                                .build(),
                        );
                        Ok(Box::new(AggrFnFoo))
                    }
                    2 => {
                        // Bar(42.5) -> (Int, Int, Real)
                        push_bar_output_schema(out_schema);
                        out_exp.push(RpnExpressionBuilder::new().push_constant(42.5f64).build());
                        Ok(Box::new(AggrFnBar))
                    }
                    3 => {
                        // Bar(NULL) -> (Int, Int, Real)
                        push_bar_output_schema(out_schema);
                        out_exp.push(
                            RpnExpressionBuilder::new()
                                .push_constant(ScalarValue::Real(None))
                                .build(),
                        );
                        Ok(Box::new(AggrFnBar))
                    }
                    4 => {
                        // Foo(col_2) -> Int
                        push_foo_output_schema(out_schema);
                        out_exp.push(RpnExpressionBuilder::new().push_column_ref(2).build());
                        Ok(Box::new(AggrFnFoo))
                    }
                    5 => {
                        // Bar(col_1) -> (Int, Int, Real)
                        push_bar_output_schema(out_schema);
                        out_exp.push(RpnExpressionBuilder::new().push_column_ref(1).build());
                        Ok(Box::new(AggrFnBar))
                    }
                    _ => unreachable!(),
                }
            },
        );

        // The scan rows parameter has no effect for mock executor. We don't care.
        let r = exec.next_batch(1);
        assert_eq!(r.data.rows_len(), 0);
        assert!(!r.is_drained.unwrap());

        let r = exec.next_batch(1);
        assert_eq!(r.data.rows_len(), 0);
        assert!(!r.is_drained.unwrap());

        let r = exec.next_batch(1);
        assert_eq!(r.data.rows_len(), 1);
        assert_eq!(r.data.columns_len(), 12);
        // Foo("abc") for 5 rows, so it is 5*3.
        assert_eq!(r.data[0].decoded().as_int_slice(), &[Some(15)]);
        // Foo(NULL) for 5 rows, so it is 0.
        assert_eq!(r.data[1].decoded().as_int_slice(), &[Some(0)]);
        // Bar(42.5) for 5 rows, so it is (5, 5, 42.5*5).
        assert_eq!(r.data[2].decoded().as_int_slice(), &[Some(5)]);
        assert_eq!(r.data[3].decoded().as_int_slice(), &[Some(5)]);
        assert_eq!(
            r.data[4].decoded().as_real_slice(),
            &[Real::new(212.5).ok()]
        );
        // Bar(NULL) for 5 rows, so it is (5, 0, 0).
        assert_eq!(r.data[5].decoded().as_int_slice(), &[Some(5)]);
        assert_eq!(r.data[6].decoded().as_int_slice(), &[Some(0)]);
        assert_eq!(r.data[7].decoded().as_real_slice(), &[Real::new(0.0).ok()]);
        // Foo([abc, NULL, "", HelloWorld, aaaaa]) => 3+0+0+10+5
        assert_eq!(r.data[8].decoded().as_int_slice(), &[Some(18)]);
        // Bar([1.0, 2.0, NULL, 4.5, 4.5]) => (5, 4, 12.0)
        assert_eq!(r.data[9].decoded().as_int_slice(), &[Some(5)]);
        assert_eq!(r.data[10].decoded().as_int_slice(), &[Some(4)]);
        assert_eq!(
            r.data[11].decoded().as_real_slice(),
            &[Real::new(12.0).ok()]
        );
        assert!(r.is_drained.unwrap());
    }

    #[test]
    fn test_it_works_integration() {
        use tipb::expression::ExprType;
        use tipb_helper::ExprDefBuilder;

        // This test creates a simple aggregation executor with the following aggregate functions:
        // - COUNT(1)
        // - COUNT(4.5)
        // - COUNT(NULL)
        // - COUNT(col_1)
        // - AVG(42.5)
        // - AVG(NULL)
        // - AVG(col_0)
        // As a result, there should be 10 output columns.

        let src_exec = make_src_executor_using_fixture();
        let aggr_definitions = vec![
            ExprDefBuilder::aggr_func(ExprType::Count, FieldTypeTp::LongLong)
                .push_child(ExprDefBuilder::constant_int(1))
                .build(),
            ExprDefBuilder::aggr_func(ExprType::Count, FieldTypeTp::LongLong)
                .push_child(ExprDefBuilder::constant_real(4.5))
                .build(),
            ExprDefBuilder::aggr_func(ExprType::Count, FieldTypeTp::LongLong)
                .push_child(ExprDefBuilder::constant_null(FieldTypeTp::NewDecimal))
                .build(),
            ExprDefBuilder::aggr_func(ExprType::Count, FieldTypeTp::LongLong)
                .push_child(ExprDefBuilder::column_ref(1, FieldTypeTp::Double))
                .build(),
            ExprDefBuilder::aggr_func(ExprType::Avg, FieldTypeTp::Double)
                .push_child(ExprDefBuilder::constant_real(42.5))
                .build(),
            ExprDefBuilder::aggr_func(ExprType::Avg, FieldTypeTp::NewDecimal)
                .push_child(ExprDefBuilder::constant_null(FieldTypeTp::NewDecimal))
                .build(),
            ExprDefBuilder::aggr_func(ExprType::Avg, FieldTypeTp::Double)
                .push_child(ExprDefBuilder::column_ref(0, FieldTypeTp::Double))
                .build(),
        ];
        let mut exec = BatchSimpleAggregationExecutor::new_for_test(
            src_exec,
            aggr_definitions,
            AggrDefinitionParser::parse,
        );

        let r = exec.next_batch(1);
        assert_eq!(r.data.rows_len(), 0);
        assert!(!r.is_drained.unwrap());

        let r = exec.next_batch(1);
        assert_eq!(r.data.rows_len(), 0);
        assert!(!r.is_drained.unwrap());

        let r = exec.next_batch(1);
        assert_eq!(r.data.rows_len(), 1);
        assert_eq!(r.data.columns_len(), 10);
        // COUNT(1) for 5 rows, so it is 5.
        assert_eq!(r.data[0].decoded().as_int_slice(), &[Some(5)]);
        // COUNT(4.5) for 5 rows, so it is 5.
        assert_eq!(r.data[1].decoded().as_int_slice(), &[Some(5)]);
        // COUNT(NULL) for 5 rows, so it is 0.
        assert_eq!(r.data[2].decoded().as_int_slice(), &[Some(0)]);
        // COUNT([1.0, 2.0, NULL, 4.5, 4.5]) => 4
        assert_eq!(r.data[3].decoded().as_int_slice(), &[Some(4)]);
        // AVG(42.5) for 5 rows, so it is (5, 212.5). Notice that AVG returns sum.
        assert_eq!(r.data[4].decoded().as_int_slice(), &[Some(5)]);
        assert_eq!(
            r.data[5].decoded().as_real_slice(),
            &[Real::new(212.5).ok()]
        );
        // AVG(NULL) for 5 rows, so it is (0, NULL).
        assert_eq!(r.data[6].decoded().as_int_slice(), &[Some(0)]);
        assert_eq!(r.data[7].decoded().as_decimal_slice(), &[None]);
        // Foo([NULL, 7.0, NULL, NULL, 1.5]) => (2, 8.5)
        assert_eq!(r.data[8].decoded().as_int_slice(), &[Some(2)]);
        assert_eq!(r.data[9].decoded().as_real_slice(), &[Real::new(8.5).ok()]);
        assert!(r.is_drained.unwrap());
    }

    #[test]
    fn test_no_row() {
        #[derive(Debug, AggrFunction)]
        #[aggr_function(state = AggrFnFooState)]
        struct AggrFnFoo;

        #[derive(Debug)]
        struct AggrFnFooState;

        impl ConcreteAggrFunctionState for AggrFnFooState {
            type ParameterType = Real;
            type ResultTargetType = Vec<Option<i64>>;

            fn update_concrete(
                &mut self,
                _ctx: &mut EvalContext,
                _value: &Option<Self::ParameterType>,
            ) -> Result<()> {
                // Update should never be called since we are testing aggregate for no row.
                unreachable!()
            }

            fn push_result_concrete(
                &self,
                _ctx: &mut EvalContext,
                target: &mut Self::ResultTargetType,
            ) -> Result<()> {
                target.push(Some(42));
                Ok(())
            }
        }

        let src_exec = MockExecutor::new(
            vec![],
            vec![
                BatchExecuteResult {
                    data: LazyBatchColumnVec::empty(),
                    warnings: EvalWarnings::default(),
                    is_drained: Ok(false),
                },
                BatchExecuteResult {
                    data: LazyBatchColumnVec::empty(),
                    warnings: EvalWarnings::default(),
                    is_drained: Ok(true),
                },
            ],
        );

        let mut exec = BatchSimpleAggregationExecutor::new_for_test(
            src_exec,
            vec![Expr::new()],
            |_, _, _, out_schema, out_exp| {
                out_schema.push(FieldTypeTp::LongLong.into());
                out_exp.push(RpnExpressionBuilder::new().push_constant(5f64).build());
                Ok(Box::new(AggrFnFoo))
            },
        );

        let r = exec.next_batch(1);
        assert_eq!(r.data.rows_len(), 0);
        assert!(!r.is_drained.unwrap());

        let r = exec.next_batch(1);
        assert_eq!(r.data.rows_len(), 1);
        assert_eq!(r.data.columns_len(), 1);
        assert!(r.data[0].is_decoded());
        assert_eq!(r.data[0].decoded().as_int_slice(), &[Some(42)]);
        assert!(r.is_drained.unwrap());
    }
}
