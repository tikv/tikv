// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

//! Simple aggregation is an aggregation that do not have `GROUP BY`s. It is more even more simpler
//! than stream aggregation.

use std::sync::Arc;

use tikv_util::trace::*;
use tipb::Aggregation;
use tipb::{Expr, FieldType};

use crate::interface::*;
use crate::util::aggr_executor::*;
use tidb_query_aggr::*;
use tidb_query_common::storage::IntervalRange;
use tidb_query_common::Result;
use tidb_query_datatype::codec::batch::{LazyBatchColumn, LazyBatchColumnVec};
use tidb_query_datatype::codec::data_type::*;
use tidb_query_datatype::expr::EvalConfig;
use tidb_query_expr::RpnStackNode;

pub struct BatchSimpleAggregationExecutor<Src: BatchExecutor>(
    AggregationExecutor<Src, SimpleAggregationImpl>,
);

impl<Src: BatchExecutor> BatchExecutor for BatchSimpleAggregationExecutor<Src> {
    type StorageStats = Src::StorageStats;

    #[inline]
    fn schema(&self) -> &[FieldType] {
        self.0.schema()
    }

    #[inline]
    #[trace("BatchSimpleAggregationExecutor::next_batch")]
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

    #[inline]
    fn can_be_cached(&self) -> bool {
        self.0.can_be_cached()
    }
}

// We assign a dummy type `Box<dyn BatchExecutor<StorageStats = ()>>` so that we can omit the type
// when calling `check_supported`.
impl BatchSimpleAggregationExecutor<Box<dyn BatchExecutor<StorageStats = ()>>> {
    /// Checks whether this executor can be used.
    #[inline]
    pub fn check_supported(descriptor: &Aggregation) -> Result<()> {
        assert_eq!(descriptor.get_group_by().len(), 0);
        let aggr_definitions = descriptor.get_agg_func();
        if aggr_definitions.is_empty() {
            return Err(other_err!("Aggregation expression is empty"));
        }

        for def in aggr_definitions {
            AllAggrDefinitionParser.check_supported(def)?;
        }
        Ok(())
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
        mut input_physical_columns: LazyBatchColumnVec,
        input_logical_rows: &[usize],
    ) -> Result<()> {
        let rows_len = input_logical_rows.len();

        assert_eq!(self.states.len(), entities.each_aggr_exprs.len());

        for idx in 0..self.states.len() {
            let aggr_state = &mut self.states[idx];
            let aggr_expr = &entities.each_aggr_exprs[idx];
            let aggr_fn_input = aggr_expr.eval(
                &mut entities.context,
                entities.src.schema(),
                &mut input_physical_columns,
                input_logical_rows,
                rows_len,
            )?;

            match aggr_fn_input {
                RpnStackNode::Scalar { value, .. } => {
                    match_template_evaluable! {
                        TT, match value.as_scalar_value_ref() {
                            ScalarValueRef::TT(scalar_value) => {
                                update_repeat!(
                                    aggr_state,
                                    &mut entities.context,
                                    scalar_value,
                                    rows_len
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
                                update_vector!(
                                    aggr_state,
                                    &mut entities.context,
                                    vec,
                                    logical_rows
                                )?;
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
    fn iterate_available_groups(
        &mut self,
        entities: &mut Entities<Src>,
        src_is_drained: bool,
        mut iteratee: impl FnMut(&mut Entities<Src>, &[Box<dyn AggrFunctionState>]) -> Result<()>,
    ) -> Result<Vec<LazyBatchColumn>> {
        assert!(src_is_drained);
        iteratee(entities, &self.states)?;
        Ok(Vec::new())
    }

    /// Simple aggregation can output aggregate results only if the source is drained.
    #[inline]
    fn is_partial_results_ready(&self) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use tidb_query_codegen::AggrFunction;
    use tidb_query_datatype::FieldTypeTp;

    use crate::util::aggr_executor::tests::*;
    use crate::util::mock_executor::MockExecutor;
    use tidb_query_datatype::expr::{EvalContext, EvalWarnings};
    use tidb_query_expr::{RpnExpression, RpnExpressionBuilder};

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
            type ParameterType = BytesRef<'static>;

            unsafe fn update_concrete_unsafe(
                &mut self,
                _ctx: &mut EvalContext,
                value: Option<Self::ParameterType>,
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
            type ParameterType = &'static Real;

            unsafe fn update_concrete_unsafe(
                &mut self,
                _ctx: &mut EvalContext,
                value: Option<Self::ParameterType>,
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
                let mut exp = Expr::default();
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
                _ctx: &mut EvalContext,
                _src_schema: &[FieldType],
                out_schema: &mut Vec<FieldType>,
                out_exp: &mut Vec<RpnExpression>,
            ) -> Result<Box<dyn AggrFunction>> {
                match aggr_def.get_val()[0] {
                    0 => {
                        // Foo("abc") -> Int
                        push_foo_output_schema(out_schema);
                        out_exp.push(
                            RpnExpressionBuilder::new_for_test()
                                .push_constant_for_test(b"abc".to_vec())
                                .build_for_test(),
                        );
                        Ok(Box::new(AggrFnFoo))
                    }
                    1 => {
                        // Foo(NULL) -> Int
                        push_foo_output_schema(out_schema);
                        out_exp.push(
                            RpnExpressionBuilder::new_for_test()
                                .push_constant_for_test(ScalarValue::Bytes(None))
                                .build_for_test(),
                        );
                        Ok(Box::new(AggrFnFoo))
                    }
                    2 => {
                        // Bar(42.5) -> (Int, Int, Real)
                        push_bar_output_schema(out_schema);
                        out_exp.push(
                            RpnExpressionBuilder::new_for_test()
                                .push_constant_for_test(42.5f64)
                                .build_for_test(),
                        );
                        Ok(Box::new(AggrFnBar))
                    }
                    3 => {
                        // Bar(NULL) -> (Int, Int, Real)
                        push_bar_output_schema(out_schema);
                        out_exp.push(
                            RpnExpressionBuilder::new_for_test()
                                .push_constant_for_test(ScalarValue::Real(None))
                                .build_for_test(),
                        );
                        Ok(Box::new(AggrFnBar))
                    }
                    4 => {
                        // Foo(col_2) -> Int
                        push_foo_output_schema(out_schema);
                        out_exp.push(
                            RpnExpressionBuilder::new_for_test()
                                .push_column_ref_for_test(2)
                                .build_for_test(),
                        );
                        Ok(Box::new(AggrFnFoo))
                    }
                    5 => {
                        // Bar(col_1) -> (Int, Int, Real)
                        push_bar_output_schema(out_schema);
                        out_exp.push(
                            RpnExpressionBuilder::new_for_test()
                                .push_column_ref_for_test(1)
                                .build_for_test(),
                        );
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
        assert!(r.logical_rows.is_empty());
        assert!(!r.is_drained.unwrap());

        let r = exec.next_batch(1);
        assert!(r.logical_rows.is_empty());
        assert!(!r.is_drained.unwrap());

        let r = exec.next_batch(1);
        assert_eq!(&r.logical_rows, &[0]);
        assert_eq!(r.physical_columns.rows_len(), 1);
        assert_eq!(r.physical_columns.columns_len(), 12);
        // Foo("abc") for 5 rows, so it is 5*3.
        assert_eq!(r.physical_columns[0].decoded().to_int_vec(), &[Some(15)]);
        // Foo(NULL) for 5 rows, so it is 0.
        assert_eq!(r.physical_columns[1].decoded().to_int_vec(), &[Some(0)]);
        // Bar(42.5) for 5 rows, so it is (5, 5, 42.5*5).
        assert_eq!(r.physical_columns[2].decoded().to_int_vec(), &[Some(5)]);
        assert_eq!(r.physical_columns[3].decoded().to_int_vec(), &[Some(5)]);
        assert_eq!(
            r.physical_columns[4].decoded().to_real_vec(),
            &[Real::new(212.5).ok()]
        );
        // Bar(NULL) for 5 rows, so it is (5, 0, 0).
        assert_eq!(r.physical_columns[5].decoded().to_int_vec(), &[Some(5)]);
        assert_eq!(r.physical_columns[6].decoded().to_int_vec(), &[Some(0)]);
        assert_eq!(
            r.physical_columns[7].decoded().to_real_vec(),
            &[Real::new(0.0).ok()]
        );
        // Foo([abc, NULL, "", HelloWorld, aaaaa]) => 3+0+0+10+5
        assert_eq!(r.physical_columns[8].decoded().to_int_vec(), &[Some(18)]);
        // Bar([1.0, 2.0, NULL, 4.5, 4.5]) => (5, 4, 12.0)
        assert_eq!(r.physical_columns[9].decoded().to_int_vec(), &[Some(5)]);
        assert_eq!(r.physical_columns[10].decoded().to_int_vec(), &[Some(4)]);
        assert_eq!(
            r.physical_columns[11].decoded().to_real_vec(),
            &[Real::new(12.0).ok()]
        );
        assert!(r.is_drained.unwrap());
    }

    #[test]
    fn test_it_works_integration() {
        use tipb::ExprType;
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
        assert!(r.logical_rows.is_empty());
        assert!(!r.is_drained.unwrap());

        let r = exec.next_batch(1);
        assert!(r.logical_rows.is_empty());
        assert!(!r.is_drained.unwrap());

        let r = exec.next_batch(1);
        assert_eq!(&r.logical_rows, &[0]);
        assert_eq!(r.physical_columns.rows_len(), 1);
        assert_eq!(r.physical_columns.columns_len(), 10);
        // COUNT(1) for 5 rows, so it is 5.
        assert_eq!(r.physical_columns[0].decoded().to_int_vec(), &[Some(5)]);
        // COUNT(4.5) for 5 rows, so it is 5.
        assert_eq!(r.physical_columns[1].decoded().to_int_vec(), &[Some(5)]);
        // COUNT(NULL) for 5 rows, so it is 0.
        assert_eq!(r.physical_columns[2].decoded().to_int_vec(), &[Some(0)]);
        // COUNT([1.0, 2.0, NULL, 4.5, 4.5]) => 4
        assert_eq!(r.physical_columns[3].decoded().to_int_vec(), &[Some(4)]);
        // AVG(42.5) for 5 rows, so it is (5, 212.5). Notice that AVG returns sum.
        assert_eq!(r.physical_columns[4].decoded().to_int_vec(), &[Some(5)]);
        assert_eq!(
            r.physical_columns[5].decoded().to_real_vec(),
            &[Real::new(212.5).ok()]
        );
        // AVG(NULL) for 5 rows, so it is (0, NULL).
        assert_eq!(r.physical_columns[6].decoded().to_int_vec(), &[Some(0)]);
        assert_eq!(r.physical_columns[7].decoded().to_decimal_vec(), &[None]);
        // Foo([NULL, 7.0, NULL, NULL, 1.5]) => (2, 8.5)
        assert_eq!(r.physical_columns[8].decoded().to_int_vec(), &[Some(2)]);
        assert_eq!(
            r.physical_columns[9].decoded().to_real_vec(),
            &[Real::new(8.5).ok()]
        );
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
            type ParameterType = &'static Real;

            unsafe fn update_concrete_unsafe(
                &mut self,
                _ctx: &mut EvalContext,
                _value: Option<Self::ParameterType>,
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
            vec![FieldTypeTp::LongLong.into()],
            vec![
                BatchExecuteResult {
                    physical_columns: LazyBatchColumnVec::from(vec![VectorValue::Int(
                        vec![Some(5)].into(),
                    )]),
                    logical_rows: Vec::new(),
                    warnings: EvalWarnings::default(),
                    is_drained: Ok(false),
                },
                BatchExecuteResult {
                    physical_columns: LazyBatchColumnVec::empty(),
                    logical_rows: Vec::new(),
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
                _ctx: &mut EvalContext,
                _src_schema: &[FieldType],
                out_schema: &mut Vec<FieldType>,
                out_exp: &mut Vec<RpnExpression>,
            ) -> Result<Box<dyn AggrFunction>> {
                out_schema.push(FieldTypeTp::LongLong.into());
                out_exp.push(
                    RpnExpressionBuilder::new_for_test()
                        .push_constant_for_test(5f64)
                        .build_for_test(),
                );
                Ok(Box::new(AggrFnFoo))
            }
        }

        let mut exec =
            BatchSimpleAggregationExecutor::new_for_test(src_exec, vec![Expr::default()], MyParser);

        let r = exec.next_batch(1);
        assert!(r.logical_rows.is_empty());
        assert!(!r.is_drained.unwrap());

        let r = exec.next_batch(1);
        assert_eq!(&r.logical_rows, &[0]);
        assert_eq!(r.physical_columns.rows_len(), 1);
        assert_eq!(r.physical_columns.columns_len(), 1);
        assert!(r.physical_columns[0].is_decoded());
        assert_eq!(r.physical_columns[0].decoded().to_int_vec(), &[Some(42)]);
        assert!(r.is_drained.unwrap());
    }
}
