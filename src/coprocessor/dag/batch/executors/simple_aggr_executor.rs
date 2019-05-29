// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

//! Simple aggregation is an aggregation that do not have `GROUP BY`s. It is more even more simpler
//! than stream aggregation.

use std::sync::Arc;

use tipb::executor::Aggregation;
use tipb::expression::{Expr, FieldType};

use crate::coprocessor::codec::batch::{LazyBatchColumn, LazyBatchColumnVec};
use crate::coprocessor::codec::data_type::*;
use crate::coprocessor::dag::aggr_fn::*;
use crate::coprocessor::dag::batch::executors::util::aggr_executor::*;
use crate::coprocessor::dag::batch::interface::*;
use crate::coprocessor::dag::expr::EvalConfig;
use crate::coprocessor::dag::rpn_expr::types::RpnStackNode;
use crate::coprocessor::Result;

pub struct BatchSimpleAggregationExecutor<Src: BatchExecutor>(
    AggregationExecutor<Src, SimpleAggregationImpl>,
);

impl<Src: BatchExecutor> BatchExecutor for BatchSimpleAggregationExecutor<Src> {
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

impl<Src: BatchExecutor> BatchSimpleAggregationExecutor<Src> {
    #[cfg(test)]
    pub fn new_for_test(
        src: Src,
        aggr_defs: Vec<Expr>,
        aggr_def_parser: impl AggrDefinitionParser,
    ) -> Self {
        Self::new_impl(
            Arc::new(EvalConfig::default()),
            src,
            aggr_defs,
            aggr_def_parser,
        )
        .unwrap()
    }
}

impl BatchSimpleAggregationExecutor<Box<dyn BatchExecutor>> {
    /// Checks whether this executor can be used.
    #[inline]
    pub fn check_supported(descriptor: &Aggregation) -> Result<()> {
        assert_eq!(descriptor.get_group_by().len(), 0);
        let aggr_definitions = descriptor.get_agg_func();
        if aggr_definitions.is_empty() {
            return Err(box_err!("Aggregation expression is empty"));
        }

        for def in aggr_definitions {
            AllAggrDefinitionParser.check_supported(def)?;
        }
        Ok(())
    }
}

impl<Src: BatchExecutor> BatchSimpleAggregationExecutor<Src> {
    pub fn new(config: Arc<EvalConfig>, src: Src, aggr_defs: Vec<Expr>) -> Result<Self> {
        Self::new_impl(config, src, aggr_defs, AllAggrDefinitionParser)
    }

    #[inline]
    fn new_impl(
        config: Arc<EvalConfig>,
        src: Src,
        aggr_defs: Vec<Expr>,
        aggr_def_parser: impl AggrDefinitionParser,
    ) -> Result<Self> {
        // Empty states is fine because it will be re-initialized later according to the content
        // in entities.
        let aggr_impl = SimpleAggregationImpl { states: Vec::new() };

        Ok(Self(AggregationExecutor::new(
            aggr_impl,
            src,
            config,
            aggr_defs,
            aggr_def_parser,
        )?))
    }
}

pub struct SimpleAggregationImpl {
    states: Vec<Box<dyn AggrFunctionState>>,
}

impl<Src: BatchExecutor> AggregationExecutorImpl<Src> for SimpleAggregationImpl {
    fn prepare_entities(&mut self, entities: &mut Entities<Src>) {
        let states = entities
            .each_aggr_fn
            .iter()
            .map(|f| f.create_state())
            .collect();
        self.states = states;
    }

    #[inline]
    fn process_batch_input(
        &mut self,
        entities: &mut Entities<Src>,
        mut input: LazyBatchColumnVec,
    ) -> Result<()> {
        let rows_len = input.rows_len();

        assert_eq!(self.states.len(), entities.each_aggr_exprs.len());

        for idx in 0..self.states.len() {
            let aggr_state = &mut self.states[idx];
            let aggr_expr = &entities.each_aggr_exprs[idx];
            let aggr_fn_input = aggr_expr.eval(
                &mut entities.context,
                rows_len,
                entities.src.schema(),
                &mut input,
            )?;

            match aggr_fn_input {
                RpnStackNode::Scalar { value, .. } => {
                    match_template_evaluable! {
                        TT, match value {
                            ScalarValue::TT(scalar_value) => {
                                aggr_state.update_repeat(&mut entities.context, scalar_value, rows_len)?;
                            },
                        }
                    }
                }
                RpnStackNode::Vector { value, .. } => {
                    match_template_evaluable! {
                        TT, match &*value {
                            VectorValue::TT(vector_value) => {
                                aggr_state.update_vector(&mut entities.context, vector_value)?;
                            },
                        }
                    }
                }
            }
        }

        Ok(())
    }

    #[inline]
    fn groups_len(&self) -> usize {
        1
    }

    #[inline]
    fn iterate_each_group_for_aggregation(
        &mut self,
        entities: &mut Entities<Src>,
        mut iteratee: impl FnMut(&mut Entities<Src>, &[Box<dyn AggrFunctionState>]) -> Result<()>,
    ) -> Result<Vec<LazyBatchColumn>> {
        iteratee(entities, &self.states)?;
        Ok(Vec::new())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use cop_codegen::AggrFunction;
    use cop_datatype::FieldTypeTp;

    use crate::coprocessor::codec::mysql::Tz;
    use crate::coprocessor::dag::batch::executors::util::aggr_executor::tests::*;
    use crate::coprocessor::dag::batch::executors::util::mock_executor::MockExecutor;
    use crate::coprocessor::dag::expr::{EvalContext, EvalWarnings};
    use crate::coprocessor::dag::rpn_expr::{RpnExpression, RpnExpressionBuilder};

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

            fn push_result(
                &self,
                _ctx: &mut EvalContext,
                target: &mut [VectorValue],
            ) -> Result<()> {
                target[0].push_int(Some(self.len as i64));
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

            fn push_result(
                &self,
                _ctx: &mut EvalContext,
                target: &mut [VectorValue],
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

        let src_exec = make_src_executor_1();

        // As a unit test, let's use the most simple way to build the executor. No complex parsers
        // involved.

        let aggr_definitions: Vec<_> = (0..6)
            .map(|index| {
                let mut exp = Expr::new();
                exp.mut_val().push(index as u8);
                exp
            })
            .collect();

        struct MyParser;

        impl AggrDefinitionParser for MyParser {
            fn check_supported(&self, _aggr_def: &Expr) -> Result<()> {
                unreachable!()
            }

            fn parse(
                &self,
                aggr_def: Expr,
                _time_zone: &Tz,
                _src_schema: &[FieldType],
                out_schema: &mut Vec<FieldType>,
                out_exp: &mut Vec<RpnExpression>,
            ) -> Result<Box<dyn AggrFunction>> {
                match aggr_def.get_val()[0] {
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
            }
        }

        let mut exec =
            BatchSimpleAggregationExecutor::new_for_test(src_exec, aggr_definitions, MyParser);

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

        let src_exec = make_src_executor_1();
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
            AllAggrDefinitionParser,
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

            fn update_concrete(
                &mut self,
                _ctx: &mut EvalContext,
                _value: &Option<Self::ParameterType>,
            ) -> Result<()> {
                // Update should never be called since we are testing aggregate for no row.
                unreachable!()
            }

            fn push_result(
                &self,
                _ctx: &mut EvalContext,
                target: &mut [VectorValue],
            ) -> Result<()> {
                target[0].push_int(Some(42));
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

        struct MyParser;

        impl AggrDefinitionParser for MyParser {
            fn check_supported(&self, _aggr_def: &Expr) -> Result<()> {
                unreachable!()
            }

            fn parse(
                &self,
                _aggr_def: Expr,
                _time_zone: &Tz,
                _src_schema: &[FieldType],
                out_schema: &mut Vec<FieldType>,
                out_exp: &mut Vec<RpnExpression>,
            ) -> Result<Box<dyn AggrFunction>> {
                out_schema.push(FieldTypeTp::LongLong.into());
                out_exp.push(RpnExpressionBuilder::new().push_constant(5f64).build());
                Ok(Box::new(AggrFnFoo))
            }
        }

        let mut exec =
            BatchSimpleAggregationExecutor::new_for_test(src_exec, vec![Expr::new()], MyParser);

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
