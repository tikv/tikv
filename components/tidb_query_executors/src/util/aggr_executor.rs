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
//! The SQL above has 2 GROUP BY columns, so we say it's *group by cardinality*
//! is 2.
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
//! Some aggregate function output multiple results, for example, `AVG(Int)`
//! output two results: count and sum. In this case we say that the result of
//! `AVG(Int)` has a *cardinality* of 2.

use std::{convert::TryFrom, sync::Arc};

use async_trait::async_trait;
use tidb_query_aggr::*;
use tidb_query_common::{storage::IntervalRange, Result};
use tidb_query_datatype::{
    codec::{
        batch::{LazyBatchColumn, LazyBatchColumnVec},
        data_type::*,
    },
    expr::{EvalConfig, EvalContext},
    EvalType, FieldTypeAccessor,
};
use tidb_query_expr::RpnExpression;
use tipb::{Expr, FieldType};

use crate::interface::*;

pub trait AggregationExecutorImpl<Src: BatchExecutor>: Send {
    /// Accepts entities without any group by columns and modifies them
    /// optionally.
    ///
    /// Implementors should modify the `schema` entity when there are group by
    /// columns.
    ///
    /// This function will be called only once.
    fn prepare_entities(&mut self, entities: &mut Entities<Src>);

    /// Processes a set of columns which are emitted from the underlying
    /// executor.
    ///
    /// Implementors should update the aggregate function states according to
    /// the data of these columns.
    fn process_batch_input(
        &mut self,
        entities: &mut Entities<Src>,
        input_physical_columns: LazyBatchColumnVec,
        input_logical_rows: &[usize],
    ) -> Result<()>;

    /// Returns the current number of groups.
    ///
    /// Note that this number can be inaccurate because it is a hint for the
    /// capacity of the vector.
    fn groups_len(&self) -> usize;

    /// Iterates aggregate function states for each available group.
    ///
    /// Implementors should call `iteratee` for each group with the aggregate
    /// function states of that group as the argument.
    ///
    /// Implementors may return the content of each group as extra columns in
    /// the return value if there are group by columns.
    ///
    /// Implementors should not iterate the same group multiple times for the
    /// same partial input data.
    fn iterate_available_groups(
        &mut self,
        entities: &mut Entities<Src>,
        src_is_drained: BatchExecIsDrain,
        iteratee: impl FnMut(&mut Entities<Src>, &[Box<dyn AggrFunctionState>]) -> Result<()>,
    ) -> Result<Vec<LazyBatchColumn>>;

    /// Returns whether we can now output partial aggregate results when the
    /// source is not drained.
    ///
    /// This method is called only when the source is not drained because
    /// aggregate result is always ready if the source is drained and no
    /// error occurs.
    fn is_partial_results_ready(&self) -> bool;
}

/// Some common data that need to be accessed by both `AggregationExecutor`
/// and `AggregationExecutorImpl`.
pub struct Entities<Src: BatchExecutor> {
    pub src: Src,
    pub context: EvalContext,

    /// The schema of the aggregation executor. It consists of aggregate result
    /// columns and group by columns.
    pub schema: Vec<FieldType>,

    /// The aggregate function.
    pub each_aggr_fn: Vec<Box<dyn AggrFunction>>,

    /// The (output result) cardinality of each aggregate function.
    pub each_aggr_cardinality: Vec<usize>,

    /// The (input) expression of each aggregate function.
    pub each_aggr_exprs: Vec<RpnExpression>,

    /// The eval type of the result columns of all aggregate functions. One
    /// aggregate function may have multiple result columns.
    pub all_result_column_types: Vec<EvalType>,
}

/// A shared executor implementation for simple aggregation, hash aggregation
/// and stream aggregation. Implementation differences are further given via
/// `AggregationExecutorImpl`.
pub struct AggregationExecutor<Src: BatchExecutor, I: AggregationExecutorImpl<Src>> {
    imp: I,
    is_ended: bool,
    entities: Entities<Src>,
    required_row: Option<u64>,
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
            // Currently only support 1 parameter aggregate functions, so let's simply
            // assert it.
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
            required_row: ctx.cfg.paging_size,
        })
    }

    /// Returns partial results of aggregation if available and whether the
    /// source is drained
    #[inline]
    async fn handle_next_batch(
        &mut self,
    ) -> Result<(Option<LazyBatchColumnVec>, BatchExecIsDrain)> {
        // Use max batch size from the beginning because aggregation
        // always needs to calculate over all data.
        let src_result = self
            .entities
            .src
            .next_batch(crate::runner::BATCH_MAX_SIZE)
            .await;

        self.entities.context.warnings = src_result.warnings;

        // When there are errors in the underlying executor, there must be no aggregate
        // output. Thus we even don't need to update the aggregate function
        // state and can return directly.
        let mut src_is_drained = src_result.is_drained?;

        // Consume all data from the underlying executor. We directly return when there
        // are errors for the same reason as above.
        if !src_result.logical_rows.is_empty() {
            self.imp.process_batch_input(
                &mut self.entities,
                src_result.physical_columns,
                &src_result.logical_rows,
            )?;
        }

        if let Some(required_row) = self.required_row {
            if self.imp.groups_len() >= required_row as usize {
                src_is_drained = BatchExecIsDrain::PagingDrain;
            }
            // StreamAgg will return groups_len - 1 rows immediately
            if src_is_drained.is_remain() && self.imp.is_partial_results_ready() {
                self.required_row = Some(required_row + 1 - self.imp.groups_len() as u64)
            }
        }

        // aggregate result is always available when source is drained
        let result = if src_is_drained.stop() || self.imp.is_partial_results_ready() {
            Some(self.aggregate_partial_results(src_is_drained)?)
        } else {
            None
        };
        Ok((result, src_is_drained))
    }

    /// Generates aggregation results of available groups.
    fn aggregate_partial_results(
        &mut self,
        src_is_drained: BatchExecIsDrain,
    ) -> Result<LazyBatchColumnVec> {
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
            .map(LazyBatchColumn::Decoded)
            .chain(group_by_columns)
            .collect();
        let ret = LazyBatchColumnVec::from(columns);
        ret.assert_columns_equal_length();
        Ok(ret)
    }
}

#[async_trait]
impl<Src: BatchExecutor, I: AggregationExecutorImpl<Src>> BatchExecutor
    for AggregationExecutor<Src, I>
{
    type StorageStats = Src::StorageStats;

    #[inline]
    fn schema(&self) -> &[FieldType] {
        self.entities.schema.as_slice()
    }

    #[inline]
    async fn next_batch(&mut self, _scan_rows: usize) -> BatchExecuteResult {
        assert!(!self.is_ended);

        let result = self.handle_next_batch().await;

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
                self.is_ended = src_is_drained.stop();
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

    #[inline]
    fn can_be_cached(&self) -> bool {
        self.entities.src.can_be_cached()
    }
}

/// Shared test facilities for different aggregation executors.
#[cfg(test)]
pub mod tests {
    use tidb_query_aggr::*;
    use tidb_query_codegen::AggrFunction;
    use tidb_query_common::Result;
    use tidb_query_datatype::{
        builder::FieldTypeBuilder,
        codec::{batch::LazyBatchColumnVec, data_type::*},
        expr::{EvalContext, EvalWarnings},
        Collation, FieldTypeTp,
    };

    use crate::{interface::*, util::mock_executor::MockExecutor};

    #[derive(Debug, AggrFunction)]
    #[aggr_function(state = AggrFnUnreachableState)]
    pub struct AggrFnUnreachable;

    #[derive(Debug)]
    pub struct AggrFnUnreachableState;

    impl ConcreteAggrFunctionState for AggrFnUnreachableState {
        type ParameterType = &'static Real;

        unsafe fn update_concrete_unsafe(
            &mut self,
            _ctx: &mut EvalContext,
            _value: Option<Self::ParameterType>,
        ) -> Result<()> {
            unreachable!()
        }

        fn push_result(&self, _ctx: &mut EvalContext, _target: &mut [VectorValue]) -> Result<()> {
            unreachable!()
        }
    }

    /// Builds an executor that will return these logical data:
    ///
    /// ```text
    /// == Schema ==
    /// Col0(Real)   Col1(Real)  Col2(Bytes) Col3(Int)  Col4(Bytes-utf8_general_ci)
    /// == Call #1 ==
    /// NULL         1.0         abc         1          aa
    /// 7.0          2.0         NULL        NULL       aaa
    /// NULL         NULL        ""          NULL       áá
    /// NULL         4.5         HelloWorld  NULL       NULL
    /// == Call #2 ==
    /// == Call #3 ==
    /// 1.5          4.5         aaaaa       5          ááá
    /// (drained)
    /// ```
    pub fn make_src_executor_1() -> MockExecutor {
        MockExecutor::new(
            vec![
                FieldTypeTp::Double.into(),
                FieldTypeTp::Double.into(),
                FieldTypeTp::VarString.into(),
                FieldTypeTp::LongLong.into(),
                FieldTypeBuilder::new()
                    .tp(FieldTypeTp::VarString)
                    .collation(Collation::Utf8Mb4GeneralCi)
                    .into(),
            ],
            vec![
                BatchExecuteResult {
                    physical_columns: LazyBatchColumnVec::from(vec![
                        VectorValue::Real(
                            vec![None, None, None, Real::new(-5.0).ok(), Real::new(7.0).ok()]
                                .into(),
                        ),
                        VectorValue::Real(
                            vec![
                                None,
                                Real::new(4.5).ok(),
                                Real::new(1.0).ok(),
                                None,
                                Real::new(2.0).ok(),
                            ]
                            .into(),
                        ),
                        VectorValue::Bytes(
                            vec![
                                Some(vec![]),
                                Some(b"HelloWorld".to_vec()),
                                Some(b"abc".to_vec()),
                                None,
                                None,
                            ]
                            .into(),
                        ),
                        VectorValue::Int(vec![None, None, Some(1), Some(10), None].into()),
                        VectorValue::Bytes(
                            vec![
                                Some("áá".as_bytes().to_vec()),
                                None,
                                Some(b"aa".to_vec()),
                                Some("ááá".as_bytes().to_vec()),
                                Some(b"aaa".to_vec()),
                            ]
                            .into(),
                        ),
                    ]),
                    logical_rows: vec![2, 4, 0, 1],
                    warnings: EvalWarnings::default(),
                    is_drained: Ok(BatchExecIsDrain::Remain),
                },
                BatchExecuteResult {
                    physical_columns: LazyBatchColumnVec::from(vec![
                        VectorValue::Real(vec![None].into()),
                        VectorValue::Real(vec![Real::new(-10.0).ok()].into()),
                        VectorValue::Bytes(vec![Some(b"foo".to_vec())].into()),
                        VectorValue::Int(vec![None].into()),
                        VectorValue::Bytes(vec![None].into()),
                    ]),
                    logical_rows: Vec::new(),
                    warnings: EvalWarnings::default(),
                    is_drained: Ok(BatchExecIsDrain::Remain),
                },
                BatchExecuteResult {
                    physical_columns: LazyBatchColumnVec::from(vec![
                        VectorValue::Real(vec![Real::new(5.5).ok(), Real::new(1.5).ok()].into()),
                        VectorValue::Real(vec![None, Real::new(4.5).ok()].into()),
                        VectorValue::Bytes(vec![None, Some(b"aaaaa".to_vec())].into()),
                        VectorValue::Int(vec![None, Some(5)].into()),
                        VectorValue::Bytes(
                            vec![
                                Some("áá".as_bytes().to_vec()),
                                Some("ááá".as_bytes().to_vec()),
                            ]
                            .into(),
                        ),
                    ]),
                    logical_rows: vec![1],
                    warnings: EvalWarnings::default(),
                    is_drained: Ok(BatchExecIsDrain::Drain),
                },
            ],
        )
    }

    /// Builds an executor that will return these logical data:
    ///
    /// == Schema ==
    /// Col0(Real)   Col1(Real)
    /// == Call #1 ==
    /// NULL         1.0
    /// 7.0          2.0
    /// NULL         NULL
    /// NULL         4.5
    /// == Call #2 ==
    /// == Call #3 ==
    /// 1.5          4.5
    /// 6.0          6.0
    /// == Call #4 ==
    /// 6.0          6.0
    /// 7.0          7.0
    /// (drained)
    pub fn make_src_executor_2() -> MockExecutor {
        MockExecutor::new(
            vec![FieldTypeTp::Double.into(), FieldTypeTp::Double.into()],
            vec![
                BatchExecuteResult {
                    physical_columns: LazyBatchColumnVec::from(vec![
                        VectorValue::Real(
                            vec![None, None, None, Real::new(-5.0).ok(), Real::new(7.0).ok()]
                                .into(),
                        ),
                        VectorValue::Real(
                            vec![
                                None,
                                Real::new(4.5).ok(),
                                Real::new(1.0).ok(),
                                None,
                                Real::new(2.0).ok(),
                            ]
                            .into(),
                        ),
                    ]),
                    logical_rows: vec![2, 4, 0, 1],
                    warnings: EvalWarnings::default(),
                    is_drained: Ok(BatchExecIsDrain::Remain),
                },
                BatchExecuteResult {
                    physical_columns: LazyBatchColumnVec::from(vec![
                        VectorValue::Real(vec![None].into()),
                        VectorValue::Real(vec![Real::new(-10.0).ok()].into()),
                    ]),
                    logical_rows: Vec::new(),
                    warnings: EvalWarnings::default(),
                    is_drained: Ok(BatchExecIsDrain::Remain),
                },
                BatchExecuteResult {
                    physical_columns: LazyBatchColumnVec::from(vec![
                        VectorValue::Real(
                            vec![
                                Real::new(5.5).ok(),
                                Real::new(1.5).ok(),
                                Real::new(6.0).ok(),
                            ]
                            .into(),
                        ),
                        VectorValue::Real(
                            vec![None, Real::new(4.5).ok(), Real::new(6.0).ok()].into(),
                        ),
                    ]),
                    logical_rows: vec![1, 2],
                    warnings: EvalWarnings::default(),
                    is_drained: Ok(BatchExecIsDrain::Remain),
                },
                BatchExecuteResult {
                    physical_columns: LazyBatchColumnVec::from(vec![
                        VectorValue::Real(vec![Real::new(7.0).ok(), Real::new(6.0).ok()].into()),
                        VectorValue::Real(vec![Real::new(7.0).ok(), Real::new(6.0).ok()].into()),
                    ]),
                    logical_rows: vec![1, 0],
                    warnings: EvalWarnings::default(),
                    is_drained: Ok(BatchExecIsDrain::Drain),
                },
            ],
        )
    }

    #[test]
    #[allow(clippy::type_complexity)]
    fn test_agg_paging() {
        use std::sync::Arc;

        use futures::executor::block_on;
        use tidb_query_datatype::expr::EvalConfig;
        use tidb_query_expr::RpnExpressionBuilder;
        use tipb::ExprType;
        use tipb_helper::ExprDefBuilder;

        use crate::{
            BatchFastHashAggregationExecutor, BatchSlowHashAggregationExecutor,
            BatchStreamAggregationExecutor,
        };

        let group_by_exp = || {
            RpnExpressionBuilder::new_for_test()
                .push_column_ref_for_test(1)
                .build_for_test()
        };

        let aggr_definitions = vec![
            ExprDefBuilder::aggr_func(ExprType::Count, FieldTypeTp::LongLong)
                .push_child(ExprDefBuilder::constant_int(1))
                .build(),
        ];

        let exec_fast = |src_exec, paging_size| {
            let mut config = EvalConfig::default();
            config.paging_size = paging_size;
            let config = Arc::new(config);
            Box::new(BatchFastHashAggregationExecutor::new_for_test_with_config(
                config,
                src_exec,
                group_by_exp(),
                aggr_definitions.clone(),
                AllAggrDefinitionParser,
            )) as Box<dyn BatchExecutor<StorageStats = ()>>
        };

        let exec_slow = |src_exec, paging_size| {
            let mut config = EvalConfig::default();
            config.paging_size = paging_size;
            let config = Arc::new(config);
            Box::new(BatchSlowHashAggregationExecutor::new_for_test_with_config(
                config,
                src_exec,
                vec![group_by_exp()],
                aggr_definitions.clone(),
                AllAggrDefinitionParser,
            )) as Box<dyn BatchExecutor<StorageStats = ()>>
        };

        let test_paging_size = vec![2, 5, 7];
        let expect_call_num = vec![1, 3, 4];
        let expect_row_num = vec![vec![4], vec![0, 0, 5], vec![0, 0, 0, 6]];
        let executor_builders: Vec<Box<dyn Fn(MockExecutor, Option<u64>) -> _>> =
            vec![Box::new(exec_fast), Box::new(exec_slow)];
        for test_case in 0..test_paging_size.len() {
            let paging_size = test_paging_size[test_case];
            let call_num = expect_call_num[test_case];
            let row_num = &expect_row_num[test_case];
            for exec_builder in &executor_builders {
                let src_exec = make_src_executor_2();
                let mut exec = exec_builder(src_exec, Some(paging_size));
                for nth_call in 0..call_num {
                    let r = block_on(exec.next_batch(1));
                    if nth_call == call_num - 1 {
                        assert!(r.is_drained.unwrap().stop());
                    } else {
                        assert!(r.is_drained.unwrap().is_remain());
                    }
                    assert_eq!(r.physical_columns.rows_len(), row_num[nth_call]);
                }
            }
        }

        let expect_row_num2 = vec![vec![4], vec![3, 0, 2], vec![3, 0, 1, 2]];
        let exec_stream = |src_exec, paging_size| {
            let mut config = EvalConfig::default();
            config.paging_size = paging_size;
            let config = Arc::new(config);
            Box::new(BatchStreamAggregationExecutor::new_for_test_with_config(
                config,
                src_exec,
                vec![group_by_exp()],
                aggr_definitions.clone(),
                AllAggrDefinitionParser,
            )) as Box<dyn BatchExecutor<StorageStats = ()>>
        };
        for test_case in 0..test_paging_size.len() {
            let paging_size = test_paging_size[test_case];
            let call_num = expect_call_num[test_case];
            let row_num = &expect_row_num2[test_case];
            let mut exec = exec_stream(make_src_executor_2(), Some(paging_size));
            for nth_call in 0..call_num {
                let r = block_on(exec.next_batch(1));
                if nth_call == call_num - 1 {
                    assert!(r.is_drained.unwrap().stop());
                } else {
                    assert!(r.is_drained.unwrap().is_remain());
                }
                assert_eq!(r.physical_columns.rows_len(), row_num[nth_call]);
            }
        }
    }
}
