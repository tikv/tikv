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
pub struct AggregationExecutor<
    C: ExecSummaryCollector,
    Src: BatchExecutor,
    I: AggregationExecutorImpl<Src>,
> {
    summary_collector: C,
    imp: I,
    is_ended: bool,
    entities: Entities<Src>,
}

impl<C: ExecSummaryCollector, Src: BatchExecutor, I: AggregationExecutorImpl<Src>>
    AggregationExecutor<C, Src, I>
{
    pub fn new(
        summary_collector: C,
        mut imp: I,
        src: Src,
        config: Arc<EvalConfig>,
        aggr_defs: Vec<Expr>,
        aggr_def_parser: impl AggrDefinitionParser,
    ) -> Result<Self> {
        let aggr_fn_len = aggr_defs.len();
        assert!(aggr_fn_len > 0);

        let src_schema = src.schema();
        let src_schema_len = src_schema.len();

        let mut schema = Vec::with_capacity(aggr_fn_len * 2);
        let mut each_aggr_fn = Vec::with_capacity(aggr_fn_len);
        let mut each_aggr_cardinality = Vec::with_capacity(aggr_fn_len);
        let mut each_aggr_exprs = Vec::with_capacity(aggr_fn_len * 2);

        for aggr_def in aggr_defs {
            let schema_len = schema.len();
            let each_aggr_exprs_len = each_aggr_exprs.len();

            let aggr_fn = aggr_def_parser.parse(
                aggr_def,
                &config.tz,
                src_schema_len,
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
            summary_collector,
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
        let mut all_result_columns: Vec<_> = self
            .entities
            .all_result_column_types
            .iter()
            // Assume that there can be 1024 groups.
            .map(|eval_type| VectorValue::with_capacity(1024, *eval_type))
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

impl<C: ExecSummaryCollector, Src: BatchExecutor, I: AggregationExecutorImpl<Src>> BatchExecutor
    for AggregationExecutor<C, Src, I>
{
    #[inline]
    fn schema(&self) -> &[FieldType] {
        self.entities.schema.as_slice()
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
        };

        self.summary_collector
            .on_finish_iterate(timer, ret.data.rows_len());
        ret
    }

    #[inline]
    fn collect_statistics(&mut self, destination: &mut BatchExecuteStatistics) {
        self.entities.src.collect_statistics(destination);
        self.summary_collector
            .collect_into(&mut destination.summary_per_executor);
    }
}
