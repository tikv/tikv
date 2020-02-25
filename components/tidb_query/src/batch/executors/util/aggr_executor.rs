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

use tidb_query_datatype::{EvalType, FieldTypeAccessor};
use tipb::{Expr, FieldType};

use crate::aggr_fn::*;
use crate::batch::interface::*;
use crate::codec::batch::{LazyBatchColumn, LazyBatchColumnVec};
use crate::codec::data_type::*;
use crate::expr::{EvalConfig, EvalContext};
use crate::rpn_expr::RpnExpression;
use crate::storage::IntervalRange;
use crate::Result;

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
        input_physical_columns: LazyBatchColumnVec,
        input_logical_rows: &[usize],
    ) -> Result<()>;

    /// Returns the current number of groups.
    ///
    /// Note that this number can be inaccurate because it is a hint for the capacity of the vector.
    fn groups_len(&self) -> usize;

    /// Iterates aggregate function states for each available group.
    ///
    /// Implementors should call `iteratee` for each group with the aggregate function states of
    /// that group as the argument.
    ///
    /// Implementors may return the content of each group as extra columns in the return value
    /// if there are group by columns.
    ///
    /// Implementors should not iterate the same group multiple times for the same partial
    /// input data.
    fn iterate_available_groups(
        &mut self,
        entities: &mut Entities<Src>,
        src_is_drained: bool,
        iteratee: impl FnMut(&mut Entities<Src>, &[Box<dyn AggrFunctionState>]) -> Result<()>,
    ) -> Result<Vec<LazyBatchColumn>>;

    /// Returns whether we can now output partial aggregate results when the source is not drained.
    ///
    /// This method is called only when the source is not drained because aggregate result is always
    /// ready if the source is drained and no error occurs.
    fn is_partial_results_ready(&self) -> bool;
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
        let src_schema = src.schema();

        let mut schema = Vec::with_capacity(aggr_fn_len * 2);
        let mut each_aggr_fn = Vec::with_capacity(aggr_fn_len);
        let mut each_aggr_cardinality = Vec::with_capacity(aggr_fn_len);
        let mut each_aggr_exprs = Vec::with_capacity(aggr_fn_len);
        let mut ctx = EvalContext::new(config.clone());

        for aggr_def in aggr_defs {
            let schema_len = schema.len();
            let each_aggr_exprs_len = each_aggr_exprs.len();

            let aggr_fn = aggr_def_parser.parse(
                aggr_def,
                &mut ctx,
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
                EvalType::try_from(ft.as_accessor().tp()).unwrap()
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

    /// Returns partial results of aggregation if available and whether the source is drained
    #[inline]
    fn handle_next_batch(&mut self) -> Result<(Option<LazyBatchColumnVec>, bool)> {
        // Use max batch size from the beginning because aggregation
        // always needs to calculate over all data.
        let src_result = self
            .entities
            .src
            .next_batch(crate::batch::runner::BATCH_MAX_SIZE);

        self.entities.context.warnings = src_result.warnings;

        // When there are errors in the underlying executor, there must be no aggregate output.
        // Thus we even don't need to update the aggregate function state and can return directly.
        let src_is_drained = src_result.is_drained?;

        // Consume all data from the underlying executor. We directly return when there are errors
        // for the same reason as above.
        if !src_result.logical_rows.is_empty() {
            self.imp.process_batch_input(
                &mut self.entities,
                src_result.physical_columns,
                &src_result.logical_rows,
            )?;
        }

        // aggregate result is always available when source is drained
        let result = if src_is_drained || self.imp.is_partial_results_ready() {
            Some(self.aggregate_partial_results(src_is_drained)?)
        } else {
            None
        };
        Ok((result, src_is_drained))
    }

    /// Generates aggregation results of available groups.
    fn aggregate_partial_results(&mut self, src_is_drained: bool) -> Result<LazyBatchColumnVec> {
        let groups_len = self.imp.groups_len();
        let mut all_result_columns: Vec<_> = self
            .entities
            .all_result_column_types
            .iter()
            .map(|eval_type| VectorValue::with_capacity(groups_len, *eval_type))
            .collect();

        // Pull aggregate results of each available group
        let group_by_columns = self.imp.iterate_available_groups(
            &mut self.entities,
            src_is_drained,
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
    type StorageStats = Src::StorageStats;

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
                    physical_columns: LazyBatchColumnVec::empty(),
                    logical_rows: Vec::new(),
                    warnings: self.entities.context.take_warnings(),
                    is_drained: Err(e),
                }
            }
            Ok((data, src_is_drained)) => {
                self.is_ended = src_is_drained;
                let logical_columns = data.unwrap_or_else(LazyBatchColumnVec::empty);
                let logical_rows = (0..logical_columns.rows_len()).collect();
                BatchExecuteResult {
                    physical_columns: logical_columns,
                    logical_rows,
                    warnings: self.entities.context.take_warnings(),
                    is_drained: Ok(src_is_drained),
                }
            }
        }
    }

    #[inline]
    fn collect_exec_stats(&mut self, dest: &mut ExecuteStats) {
        self.entities.src.collect_exec_stats(dest);
    }

    #[inline]
    fn collect_storage_stats(&mut self, dest: &mut Self::StorageStats) {
        self.entities.src.collect_storage_stats(dest);
    }

    #[inline]
    fn take_scanned_range(&mut self) -> IntervalRange {
        self.entities.src.take_scanned_range()
    }
}

/// Shared test facilities for different aggregation executors.
#[cfg(test)]
pub mod tests {
    use tidb_query_codegen::AggrFunction;
    use tidb_query_datatype::FieldTypeTp;

    use crate::aggr_fn::*;
    use crate::batch::executors::util::mock_executor::MockExecutor;
    use crate::batch::interface::*;
    use crate::codec::batch::LazyBatchColumnVec;
    use crate::codec::data_type::*;
    use crate::expr::{EvalContext, EvalWarnings};
    use crate::Result;

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

    /// Builds an executor that will return these logical data:
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
                    physical_columns: LazyBatchColumnVec::from(vec![
                        VectorValue::Real(vec![
                            None,
                            None,
                            None,
                            Real::new(-5.0).ok(),
                            Real::new(7.0).ok(),
                        ]),
                        VectorValue::Real(vec![
                            None,
                            Real::new(4.5).ok(),
                            Real::new(1.0).ok(),
                            None,
                            Real::new(2.0).ok(),
                        ]),
                        VectorValue::Bytes(vec![
                            Some(vec![]),
                            Some(b"HelloWorld".to_vec()),
                            Some(b"abc".to_vec()),
                            None,
                            None,
                        ]),
                        VectorValue::Int(vec![None, None, Some(1), Some(10), None]),
                    ]),
                    logical_rows: vec![2, 4, 0, 1],
                    warnings: EvalWarnings::default(),
                    is_drained: Ok(false),
                },
                BatchExecuteResult {
                    physical_columns: LazyBatchColumnVec::from(vec![
                        VectorValue::Real(vec![None]),
                        VectorValue::Real(vec![Real::new(-10.0).ok()]),
                        VectorValue::Bytes(vec![Some(b"foo".to_vec())]),
                        VectorValue::Int(vec![None]),
                    ]),
                    logical_rows: Vec::new(),
                    warnings: EvalWarnings::default(),
                    is_drained: Ok(false),
                },
                BatchExecuteResult {
                    physical_columns: LazyBatchColumnVec::from(vec![
                        VectorValue::Real(vec![Real::new(5.5).ok(), Real::new(1.5).ok()]),
                        VectorValue::Real(vec![None, Real::new(4.5).ok()]),
                        VectorValue::Bytes(vec![None, Some(b"aaaaa".to_vec())]),
                        VectorValue::Int(vec![None, Some(5)]),
                    ]),
                    logical_rows: vec![1],
                    warnings: EvalWarnings::default(),
                    is_drained: Ok(true),
                },
            ],
        )
    }
}
