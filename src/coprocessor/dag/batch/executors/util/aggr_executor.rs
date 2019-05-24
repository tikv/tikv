// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

//! Concept:
//!
//! ```ignore
//! SELECT COUNT(1), COUNT(COL) FROM TABLE GROUP BY COL+1, COL2
//!        ^^^^^     ^^^^^                                         : Aggregate Functions
//!              ^         ^^^                                     : Aggregate Function Expressions
//!                                                 ^^^^^  ^^^^    : Group By Expressions
//! ```
//!
//! The SQL above has 2 GROUP BY columns, so we say it's *group by cardinality* is 2.
//!
//! In the result:
//!
//! ```ignore
//!     COUNT(1)     COUNT(COL)         COL+1      COL2
//!     1            1                  1          1            <--- Each row is the result
//!     1            2                  1          1            <--- of a group
//!
//!     ^^^^^^^^^    ^^^^^^^^^^^                                : Aggregate Result Column
//!                                     ^^^^^^     ^^^^^        : Group By Column
//! ```
//!
//! Some aggregate function output multiple results, for example, `AVG(Int)` output two results:
//! count and sum. In this case we say that the result of `AVG(Int)` has a *cardinality* of 2.
//!

use std::convert::TryFrom;
use std::sync::Arc;

use cop_datatype::{EvalType, FieldTypeAccessor};
use tipb::expression::{Expr, FieldType};

use crate::coprocessor::codec::batch::{LazyBatchColumn, LazyBatchColumnVec};
use crate::coprocessor::codec::data_type::*;
use crate::coprocessor::dag::aggr_fn::*;
use crate::coprocessor::dag::batch::interface::*;
use crate::coprocessor::dag::expr::{EvalConfig, EvalContext};
use crate::coprocessor::dag::rpn_expr::RpnExpression;
use crate::coprocessor::Result;

pub trait AggregationExecutorImpl<Src: BatchExecutor>: Send {
    /// Accepts entities without any group by columns and modifies them optionally.
    ///
    /// Implementors should modify the `schema` entity when there are group by columns.
    ///
    /// This function will be called only once.
    fn prepare_entities(&mut self, entities: &mut Entities<Src>);

    /// Processes a set of columns which are emitted from the underlying executor.
    ///
    /// Implementors should update the aggregate function states according to the data of
    /// these columns.
    fn process_batch_input(
        &mut self,
        entities: &mut Entities<Src>,
        input: LazyBatchColumnVec,
    ) -> Result<()>;

    /// Returns the current number of groups.
    fn groups_len(&self) -> usize;

    /// Iterates aggregate function states for each group.
    ///
    /// Implementors should call `iteratee` for each group with the aggregate function states of
    /// that group as the argument.
    ///
    /// Implementors may return the content of each group as extra columns in the return value
    /// if there are group by columns.
    fn iterate_each_group_for_aggregation(
        &mut self,
        entities: &mut Entities<Src>,
        iteratee: impl FnMut(&mut Entities<Src>, &[Box<dyn AggrFunctionState>]) -> Result<()>,
    ) -> Result<Vec<LazyBatchColumn>>;
}

/// Some common data that need to be accessed by both `AggregationExecutor`
/// and `AggregationExecutorImpl`.
pub struct Entities<Src: BatchExecutor> {
    pub src: Src,
    pub context: EvalContext,

    /// The schema of the aggregation executor. It consists of aggregate result columns and
    /// group by columns.
    pub schema: Vec<FieldType>,

    /// The aggregate function.
    pub each_aggr_fn: Vec<Box<dyn AggrFunction>>,

    /// The (output result) cardinality of each aggregate function.
    pub each_aggr_cardinality: Vec<usize>,

    /// The (input) expression of each aggregate function.
    pub each_aggr_exprs: Vec<RpnExpression>,

    /// The eval type of the result columns of all aggregate functions. One aggregate function
    /// may have multiple result columns.
    pub all_result_column_types: Vec<EvalType>,
}

/// A shared executor implementation for simple aggregation, hash aggregation and
/// stream aggregation. Implementation differences are further given via `AggregationExecutorImpl`.
pub struct AggregationExecutor<Src: BatchExecutor, I: AggregationExecutorImpl<Src>> {
    imp: I,
    is_ended: bool,
    entities: Entities<Src>,
}

impl<Src: BatchExecutor, I: AggregationExecutorImpl<Src>> AggregationExecutor<Src, I> {
    pub fn new(
        mut imp: I,
        src: Src,
        config: Arc<EvalConfig>,
        aggr_defs: Vec<Expr>,
        aggr_def_parser: impl AggrDefinitionParser,
    ) -> Result<Self> {
        let aggr_fn_len = aggr_defs.len();
        assert!(aggr_fn_len > 0);

        let src_schema = src.schema();

        let mut schema = Vec::with_capacity(aggr_fn_len * 2);
        let mut each_aggr_fn = Vec::with_capacity(aggr_fn_len);
        let mut each_aggr_cardinality = Vec::with_capacity(aggr_fn_len);
        let mut each_aggr_exprs = Vec::with_capacity(aggr_fn_len);

        for aggr_def in aggr_defs {
            let schema_len = schema.len();
            let each_aggr_exprs_len = each_aggr_exprs.len();

            let aggr_fn = aggr_def_parser.parse(
                aggr_def,
                &config.tz,
                src_schema,
                &mut schema,
                &mut each_aggr_exprs,
            )?;

            assert!(schema.len() > schema_len);
            // Currently only support 1 parameter aggregate functions, so let's simply assert it.
            assert_eq!(each_aggr_exprs.len(), each_aggr_exprs_len + 1);

            each_aggr_fn.push(aggr_fn);
            each_aggr_cardinality.push(schema.len() - schema_len);
        }

        let all_result_column_types = schema
            .iter()
            .map(|ft| {
                // The unwrap is fine because aggregate function parser should never return an
                // eval type that we cannot process later. If we made a mistake there, then we
                // should panic.
                EvalType::try_from(ft.tp()).unwrap()
            })
            .collect();

        let mut entities = Entities {
            src,
            context: EvalContext::new(config),
            schema,
            each_aggr_fn,
            each_aggr_cardinality,
            each_aggr_exprs,
            all_result_column_types,
        };
        imp.prepare_entities(&mut entities);

        Ok(Self {
            imp,
            is_ended: false,
            entities,
        })
    }

    #[inline]
    fn handle_next_batch(&mut self) -> Result<Option<LazyBatchColumnVec>> {
        // Use max batch size from the beginning because aggregation
        // always needs to calculate over all data.
        let src_result = self
            .entities
            .src
            .next_batch(crate::coprocessor::dag::batch_handler::BATCH_MAX_SIZE);

        self.entities.context.warnings = src_result.warnings;

        // When there are errors in the underlying executor, there must be no aggregate output.
        // Thus we even don't need to update the aggregate function state and can return directly.
        let src_is_drained = src_result.is_drained?;

        // Consume all data from the underlying executor. We directly return when there are errors
        // for the same reason as above.
        if src_result.data.rows_len() > 0 {
            self.imp
                .process_batch_input(&mut self.entities, src_result.data)?;
        }

        // Aggregate results if source executor is drained, otherwise just return nothing.
        if src_is_drained {
            Ok(Some(self.aggregate()?))
        } else {
            Ok(None)
        }
    }

    /// Generates aggregation results.
    ///
    /// This function is ensured to be called at most once.
    fn aggregate(&mut self) -> Result<LazyBatchColumnVec> {
        let groups_len = self.imp.groups_len();
        let mut all_result_columns: Vec<_> = self
            .entities
            .all_result_column_types
            .iter()
            .map(|eval_type| VectorValue::with_capacity(groups_len, *eval_type))
            .collect();

        // Aggregate results for each group
        let group_by_columns = self.imp.iterate_each_group_for_aggregation(
            &mut self.entities,
            |entities, states| {
                assert_eq!(states.len(), entities.each_aggr_cardinality.len());

                let mut offset = 0;
                for (state, result_cardinality) in
                    states.iter().zip(&entities.each_aggr_cardinality)
                {
                    assert!(*result_cardinality > 0);

                    state.push_result(
                        &mut entities.context,
                        &mut all_result_columns[offset..offset + *result_cardinality],
                    )?;

                    offset += *result_cardinality;
                }

                Ok(())
            },
        )?;

        // The return columns consist of aggregate result columns and group by columns.
        let columns: Vec<_> = all_result_columns
            .into_iter()
            .map(|c| LazyBatchColumn::Decoded(c))
            .chain(group_by_columns)
            .collect();
        let ret = LazyBatchColumnVec::from(columns);
        ret.assert_columns_equal_length();
        Ok(ret)
    }
}

impl<Src: BatchExecutor, I: AggregationExecutorImpl<Src>> BatchExecutor
    for AggregationExecutor<Src, I>
{
    #[inline]
    fn schema(&self) -> &[FieldType] {
        self.entities.schema.as_slice()
    }

    #[inline]
    fn next_batch(&mut self, _scan_rows: usize) -> BatchExecuteResult {
        assert!(!self.is_ended);

        let result = self.handle_next_batch();

        match result {
            Err(e) => {
                // When there are error, we can just return empty data.
                self.is_ended = true;
                BatchExecuteResult {
                    data: LazyBatchColumnVec::empty(),
                    warnings: self.entities.context.take_warnings(),
                    is_drained: Err(e),
                }
            }
            Ok(None) => {
                // When there is no error and is not drained, we also return empty data.
                BatchExecuteResult {
                    data: LazyBatchColumnVec::empty(),
                    warnings: self.entities.context.take_warnings(),
                    is_drained: Ok(false),
                }
            }
            Ok(Some(data)) => {
                // When there is no error and aggregate finished, we return it as data.
                self.is_ended = true;
                BatchExecuteResult {
                    data,
                    warnings: self.entities.context.take_warnings(),
                    is_drained: Ok(true),
                }
            }
        }
    }

    #[inline]
    fn collect_statistics(&mut self, destination: &mut BatchExecuteStatistics) {
        self.entities.src.collect_statistics(destination);
    }
}

/// Shared test facilities for different aggregation executors.
#[cfg(test)]
pub mod tests {
    use cop_codegen::AggrFunction;
    use cop_datatype::FieldTypeTp;

    use crate::coprocessor::codec::batch::LazyBatchColumnVec;
    use crate::coprocessor::codec::data_type::*;
    use crate::coprocessor::dag::aggr_fn::*;
    use crate::coprocessor::dag::batch::executors::util::mock_executor::MockExecutor;
    use crate::coprocessor::dag::batch::interface::*;
    use crate::coprocessor::dag::expr::{EvalContext, EvalWarnings};
    use crate::coprocessor::Result;

    #[derive(Debug, AggrFunction)]
    #[aggr_function(state = AggrFnUnreachableState)]
    pub struct AggrFnUnreachable;

    #[derive(Debug)]
    pub struct AggrFnUnreachableState;

    impl ConcreteAggrFunctionState for AggrFnUnreachableState {
        type ParameterType = Real;

        fn update_concrete(
            &mut self,
            _ctx: &mut EvalContext,
            _value: &Option<Self::ParameterType>,
        ) -> Result<()> {
            unreachable!()
        }

        fn push_result(&self, _ctx: &mut EvalContext, _target: &mut [VectorValue]) -> Result<()> {
            unreachable!()
        }
    }

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
    pub fn make_src_executor_1() -> MockExecutor {
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
}
