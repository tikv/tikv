// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::convert::TryFrom;
use std::hash::Hash;
use std::sync::Arc;

use tidb_query_datatype::Collation;
use tidb_query_datatype::{EvalType, FieldTypeAccessor};
use tikv_util::box_try;
use tikv_util::collections::HashMap;
use tipb::Aggregation;
use tipb::{Expr, FieldType};

use crate::aggr_fn::*;
use crate::batch::executors::util::aggr_executor::*;
use crate::batch::executors::util::hash_aggr_helper::HashAggregationHelper;
use crate::batch::interface::*;
use crate::codec::batch::{LazyBatchColumn, LazyBatchColumnVec};
use crate::codec::collation::{match_template_collator, SortKey};
use crate::codec::data_type::*;
use crate::expr::{EvalConfig, EvalContext};
use crate::rpn_expr::{RpnExpression, RpnExpressionBuilder};
use crate::storage::IntervalRange;
use crate::Result;

pub macro match_template_hashable($t:tt, $($tail:tt)*) {
    match_template::match_template! {
        $t = [Int, Real, Bytes, Duration, Decimal, DateTime],
        $($tail)*
    }
}

/// Fast Hash Aggregation Executor uses hash when comparing group key. It only supports one
/// group by column.
pub struct BatchFastHashAggregationExecutor<Src: BatchExecutor>(
    AggregationExecutor<Src, FastHashAggregationImpl>,
);

impl<Src: BatchExecutor> BatchExecutor for BatchFastHashAggregationExecutor<Src> {
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
impl BatchFastHashAggregationExecutor<Box<dyn BatchExecutor<StorageStats = ()>>> {
    /// Checks whether this executor can be used.
    #[inline]
    pub fn check_supported(descriptor: &Aggregation) -> Result<()> {
        let group_by_definitions = descriptor.get_group_by();
        assert!(!group_by_definitions.is_empty());
        if group_by_definitions.len() > 1 {
            return Err(other_err!("Multi group is not supported"));
        }

        let def = &group_by_definitions[0];

        // Only a subset of all eval types are supported.
        let eval_type = box_try!(EvalType::try_from(def.get_field_type().as_accessor().tp()));
        match eval_type {
            EvalType::Int
            | EvalType::Real
            | EvalType::Bytes
            | EvalType::Duration
            | EvalType::Decimal
            | EvalType::DateTime => {}
            _ => return Err(other_err!("Eval type {} is not supported", eval_type)),
        }

        RpnExpressionBuilder::check_expr_tree_supported(def)?;
        if RpnExpressionBuilder::is_expr_eval_to_scalar(def)? {
            return Err(other_err!("Group by expression is not a column"));
        }

        let aggr_definitions = descriptor.get_agg_func();
        for def in aggr_definitions {
            AllAggrDefinitionParser.check_supported(def)?;
        }
        Ok(())
    }
}

impl<Src: BatchExecutor> BatchFastHashAggregationExecutor<Src> {
    #[cfg(test)]
    pub fn new_for_test(
        src: Src,
        group_by_exp: RpnExpression,
        aggr_defs: Vec<Expr>,
        aggr_def_parser: impl AggrDefinitionParser,
    ) -> Self {
        Self::new_impl(
            Arc::new(EvalConfig::default()),
            src,
            group_by_exp,
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
        assert_eq!(group_by_exp_defs.len(), 1);
        let mut ctx = EvalContext::new(config.clone());
        let group_by_exp = RpnExpressionBuilder::build_from_expr_tree(
            group_by_exp_defs.into_iter().next().unwrap(),
            &mut ctx,
            src.schema().len(),
        )?;
        Self::new_impl(
            config,
            src,
            group_by_exp,
            aggr_defs,
            AllAggrDefinitionParser,
        )
    }

    #[inline]
    fn new_impl(
        config: Arc<EvalConfig>,
        src: Src,
        group_by_exp: RpnExpression,
        aggr_defs: Vec<Expr>,
        aggr_def_parser: impl AggrDefinitionParser,
    ) -> Result<Self> {
        let group_by_field_type = group_by_exp.ret_field_type(src.schema()).clone();
        let group_by_eval_type =
            EvalType::try_from(group_by_field_type.as_accessor().tp()).unwrap();
        let groups = match_template_hashable! {
            TT, match group_by_eval_type {
                EvalType::TT => Groups::TT(HashMap::default()),
                _ => unreachable!(),
            }
        };

        let aggr_impl = FastHashAggregationImpl {
            states: Vec::with_capacity(1024),
            groups,
            group_by_exp,
            group_by_field_type,
            states_offset_each_logical_row: Vec::with_capacity(
                crate::batch::runner::BATCH_MAX_SIZE,
            ),
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

/// All groups.
enum Groups {
    // The value of each hash table is the start index in `FastHashAggregationImpl::states`
    // field. When there are new groups (i.e. new entry in the hash table), the states of the groups
    // will be appended to `states`.
    Int(HashMap<Option<Int>, usize>),
    Real(HashMap<Option<Real>, usize>),
    Bytes(HashMap<Option<Bytes>, usize>),
    Duration(HashMap<Option<Duration>, usize>),
    Decimal(HashMap<Option<Decimal>, usize>),
    DateTime(HashMap<Option<DateTime>, usize>),
}

impl Groups {
    fn eval_type(&self) -> EvalType {
        match_template_hashable! {
            TT, match self {
                Groups::TT(_) => {
                    EvalType::TT
                },
            }
        }
    }

    fn len(&self) -> usize {
        match_template_hashable! {
            TT, match self {
                Groups::TT(groups) => {
                    groups.len()
                },
            }
        }
    }
}

pub struct FastHashAggregationImpl {
    states: Vec<Box<dyn AggrFunctionState>>,
    groups: Groups,
    group_by_exp: RpnExpression,
    group_by_field_type: FieldType,
    states_offset_each_logical_row: Vec<usize>,
}

impl<Src: BatchExecutor> AggregationExecutorImpl<Src> for FastHashAggregationImpl {
    #[inline]
    fn prepare_entities(&mut self, entities: &mut Entities<Src>) {
        entities.schema.push(self.group_by_field_type.clone());
    }

    #[inline]
    fn process_batch_input(
        &mut self,
        entities: &mut Entities<Src>,
        mut input_physical_columns: LazyBatchColumnVec,
        input_logical_rows: &[usize],
    ) -> Result<()> {
        // 1. Calculate which group each src row belongs to.
        self.states_offset_each_logical_row.clear();

        let group_by_result = self.group_by_exp.eval(
            &mut entities.context,
            entities.src.schema(),
            &mut input_physical_columns,
            input_logical_rows,
            input_logical_rows.len(),
        )?;
        // Unwrap is fine because we have verified the group by expression before.
        let group_by_value = group_by_result.vector_value().unwrap();
        let group_by_physical_vec = group_by_value.as_ref();
        let group_by_logical_rows = group_by_value.logical_rows();

        match_template::match_template! {
            TT = [Int, Real, Duration, Decimal, DateTime],
            match group_by_physical_vec {
                VectorValue::TT(v) => {
                    if let Groups::TT(group) = &mut self.groups {
                        calc_groups_each_row(
                            v,
                            group_by_logical_rows,
                            &entities.each_aggr_fn,
                            group,
                            &mut self.states,
                            &mut self.states_offset_each_logical_row,
                            |val| Ok(val)
                        )?;
                    } else {
                        panic!();
                    }
                },
                VectorValue::Bytes(v) => {
                    if let Groups::Bytes(group) = &mut self.groups {
                        match_template_collator!(
                            TT,
                            match self.group_by_field_type.collation().map_err(crate::codec::Error::from)? {
                                Collation::TT => {
                                    #[allow(clippy::transmute_ptr_to_ptr)]
                                    let group: &mut HashMap<Option<SortKey<Bytes, TT>>, usize> =
                                        unsafe { std::mem::transmute(group) };
                                    calc_groups_each_row(
                                        v,
                                        group_by_logical_rows,
                                        &entities.each_aggr_fn,
                                        group,
                                        &mut self.states,
                                        &mut self.states_offset_each_logical_row,
                                        |val| Ok(SortKey::map_option(val)?)
                                    )?;
                                }
                            }
                        )
                    } else {
                        panic!();
                    }
                },
                _ => unreachable!(),
            }
        }

        // 2. Update states according to the group.
        HashAggregationHelper::update_each_row_states_by_offset(
            entities,
            &mut input_physical_columns,
            input_logical_rows,
            &mut self.states,
            &self.states_offset_each_logical_row,
        )?;

        Ok(())
    }

    #[inline]
    fn groups_len(&self) -> usize {
        self.groups.len()
    }

    #[inline]
    fn iterate_available_groups(
        &mut self,
        entities: &mut Entities<Src>,
        src_is_drained: bool,
        mut iteratee: impl FnMut(&mut Entities<Src>, &[Box<dyn AggrFunctionState>]) -> Result<()>,
    ) -> Result<Vec<LazyBatchColumn>> {
        assert!(src_is_drained);

        let aggr_fns_len = entities.each_aggr_fn.len();
        let mut group_by_column = LazyBatchColumn::decoded_with_capacity_and_tp(
            self.groups.len(),
            self.groups.eval_type(),
        );

        match_template_hashable! {
            TT, match &mut self.groups {
                Groups::TT(groups) => {
                    // Take the map out, and then use `HashMap::into_iter()`. This should be faster
                    // then using `HashMap::drain()`.
                    // TODO: Verify performance difference.
                    let groups = std::mem::replace(groups, HashMap::default());
                    for (group_key, states_offset) in groups {
                        iteratee(entities, &self.states[states_offset..states_offset + aggr_fns_len])?;
                        group_by_column.mut_decoded().push(group_key);
                    }
                }
            }
        }

        Ok(vec![group_by_column])
    }

    /// Fast hash aggregation can output aggregate results only if the source is drained.
    #[inline]
    fn is_partial_results_ready(&self) -> bool {
        false
    }
}

fn calc_groups_each_row<T, S, F>(
    physical_column: &[Option<T>],
    logical_rows: &[usize],
    aggr_fns: &[Box<dyn AggrFunction>],
    group: &mut HashMap<Option<S>, usize>,
    states: &mut Vec<Box<dyn AggrFunctionState>>,
    states_offset_each_logical_row: &mut Vec<usize>,
    map_to_sort_key: F,
) -> Result<()>
where
    S: Hash + Eq + Clone,
    F: Fn(&Option<T>) -> Result<&Option<S>>,
{
    for physical_idx in logical_rows {
        let val = map_to_sort_key(&physical_column[*physical_idx])?;

        // Not using the entry API so that when entry exists there is no clone.
        match group.get(val) {
            Some(offset) => {
                // Group exists, use the offset of existing group.
                states_offset_each_logical_row.push(*offset);
            }
            None => {
                // Group does not exist, prepare groups.
                let offset = states.len();
                states_offset_each_logical_row.push(offset);
                group.insert(val.clone(), offset);
                for aggr_fn in aggr_fns {
                    states.push(aggr_fn.create_state());
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

    use crate::batch::executors::util::aggr_executor::tests::*;
    use crate::batch::executors::util::mock_executor::MockExecutor;
    use crate::batch::executors::BatchSlowHashAggregationExecutor;
    use crate::expr::EvalWarnings;
    use crate::rpn_expr::impl_arithmetic::{arithmetic_fn_meta, RealPlus};
    use crate::rpn_expr::{RpnExpression, RpnExpressionBuilder};

    // Test cases also cover BatchSlowHashAggregationExecutor.

    #[test]
    fn test_it_works_integration() {
        use tipb::ExprType;
        use tipb_helper::ExprDefBuilder;

        // This test creates a hash aggregation executor with the following aggregate functions:
        // - COUNT(1)
        // - COUNT(col_1 + 5.0)
        // - AVG(col_0)
        // And group by:
        // - col_0 + col_1

        let group_by_exp = || {
            RpnExpressionBuilder::new()
                .push_column_ref(0)
                .push_column_ref(1)
                .push_fn_call(arithmetic_fn_meta::<RealPlus>(), 2, FieldTypeTp::Double)
                .build()
        };

        let aggr_definitions = vec![
            ExprDefBuilder::aggr_func(ExprType::Count, FieldTypeTp::LongLong)
                .push_child(ExprDefBuilder::constant_int(1))
                .build(),
            ExprDefBuilder::aggr_func(ExprType::Count, FieldTypeTp::LongLong)
                .push_child(
                    ExprDefBuilder::scalar_func(ScalarFuncSig::PlusReal, FieldTypeTp::Double)
                        .push_child(ExprDefBuilder::column_ref(1, FieldTypeTp::Double))
                        .push_child(ExprDefBuilder::constant_real(5.0)),
                )
                .build(),
            ExprDefBuilder::aggr_func(ExprType::Avg, FieldTypeTp::Double)
                .push_child(ExprDefBuilder::column_ref(0, FieldTypeTp::Double))
                .build(),
        ];

        let exec_fast = |src_exec| {
            Box::new(BatchFastHashAggregationExecutor::new_for_test(
                src_exec,
                group_by_exp(),
                aggr_definitions.clone(),
                AllAggrDefinitionParser,
            )) as Box<dyn BatchExecutor<StorageStats = ()>>
        };

        let exec_slow = |src_exec| {
            Box::new(BatchSlowHashAggregationExecutor::new_for_test(
                src_exec,
                vec![group_by_exp()],
                aggr_definitions.clone(),
                AllAggrDefinitionParser,
            )) as Box<dyn BatchExecutor<StorageStats = ()>>
        };

        let executor_builders: Vec<Box<dyn FnOnce(MockExecutor) -> _>> =
            vec![Box::new(exec_fast), Box::new(exec_slow)];

        for exec_builder in executor_builders {
            let src_exec = make_src_executor_1();
            let mut exec = exec_builder(src_exec);

            let r = exec.next_batch(1);
            assert!(r.logical_rows.is_empty());
            assert_eq!(r.physical_columns.rows_len(), 0);
            assert!(!r.is_drained.unwrap());

            let r = exec.next_batch(1);
            assert!(r.logical_rows.is_empty());
            assert_eq!(r.physical_columns.rows_len(), 0);
            assert!(!r.is_drained.unwrap());

            let mut r = exec.next_batch(1);
            // col_0 + col_1 can result in [NULL, 9.0, 6.0], thus there will be three groups.
            assert_eq!(&r.logical_rows, &[0, 1, 2]);
            assert_eq!(r.physical_columns.rows_len(), 3);
            assert_eq!(r.physical_columns.columns_len(), 5); // 4 result column, 1 group by column

            // Let's check group by column first. Group by column is decoded in fast hash agg,
            // but not decoded in slow hash agg. So decode it anyway.
            r.physical_columns[4]
                .ensure_all_decoded(&mut EvalContext::default(), &exec.schema()[4])
                .unwrap();

            // The row order is not defined. Let's sort it by the group by column before asserting.
            let mut sort_column: Vec<(usize, _)> = r.physical_columns[4]
                .decoded()
                .as_real_slice()
                .iter()
                .map(|v| {
                    use std::hash::Hasher;
                    let mut s = std::collections::hash_map::DefaultHasher::new();
                    v.hash(&mut s);
                    s.finish()
                })
                .enumerate()
                .collect();
            sort_column.sort_by(|a, b| a.1.cmp(&b.1));

            // Use the order of the sorted column to sort other columns
            let ordered_column: Vec<_> = sort_column
                .iter()
                .map(|(idx, _)| r.physical_columns[4].decoded().as_real_slice()[*idx])
                .collect();
            assert_eq!(
                &ordered_column,
                &[Real::new(9.0).ok(), Real::new(6.0).ok(), None]
            );
            let ordered_column: Vec<_> = sort_column
                .iter()
                .map(|(idx, _)| r.physical_columns[0].decoded().as_int_slice()[*idx])
                .collect();
            assert_eq!(&ordered_column, &[Some(1), Some(1), Some(3)]);
            let ordered_column: Vec<_> = sort_column
                .iter()
                .map(|(idx, _)| r.physical_columns[1].decoded().as_int_slice()[*idx])
                .collect();
            assert_eq!(&ordered_column, &[Some(1), Some(1), Some(2)]);
            let ordered_column: Vec<_> = sort_column
                .iter()
                .map(|(idx, _)| r.physical_columns[2].decoded().as_int_slice()[*idx])
                .collect();
            assert_eq!(&ordered_column, &[Some(1), Some(1), Some(0)]);
            let ordered_column: Vec<_> = sort_column
                .iter()
                .map(|(idx, _)| r.physical_columns[3].decoded().as_real_slice()[*idx])
                .collect();
            assert_eq!(
                &ordered_column,
                &[Real::new(7.0).ok(), Real::new(1.5).ok(), None]
            );
        }
    }

    #[test]
    fn test_collation() {
        use tipb::ExprType;
        use tipb_helper::ExprDefBuilder;

        // This test creates a hash aggregation executor with the following aggregate functions:
        // - COUNT(col_0)
        // - AVG(col_1)
        // And group by:
        // - col_4

        let group_by_exp = || RpnExpressionBuilder::new().push_column_ref(4).build();

        let aggr_definitions = vec![
            ExprDefBuilder::aggr_func(ExprType::Count, FieldTypeTp::LongLong)
                .push_child(ExprDefBuilder::column_ref(0, FieldTypeTp::Double))
                .build(),
            ExprDefBuilder::aggr_func(ExprType::Avg, FieldTypeTp::Double)
                .push_child(ExprDefBuilder::column_ref(1, FieldTypeTp::Double))
                .build(),
        ];

        let exec_fast = |src_exec| {
            Box::new(BatchFastHashAggregationExecutor::new_for_test(
                src_exec,
                group_by_exp(),
                aggr_definitions.clone(),
                AllAggrDefinitionParser,
            )) as Box<dyn BatchExecutor<StorageStats = ()>>
        };

        let exec_slow = |src_exec| {
            Box::new(BatchSlowHashAggregationExecutor::new_for_test(
                src_exec,
                vec![group_by_exp()],
                aggr_definitions.clone(),
                AllAggrDefinitionParser,
            )) as Box<dyn BatchExecutor<StorageStats = ()>>
        };

        let executor_builders: Vec<Box<dyn FnOnce(MockExecutor) -> _>> =
            vec![Box::new(exec_fast), Box::new(exec_slow)];

        for exec_builder in executor_builders {
            let src_exec = make_src_executor_1();
            let mut exec = exec_builder(src_exec);

            let r = exec.next_batch(1);
            assert!(r.logical_rows.is_empty());
            assert_eq!(r.physical_columns.rows_len(), 0);
            assert!(!r.is_drained.unwrap());

            let r = exec.next_batch(1);
            assert!(r.logical_rows.is_empty());
            assert_eq!(r.physical_columns.rows_len(), 0);
            assert!(!r.is_drained.unwrap());

            let mut r = exec.next_batch(1);
            // col_4 can result in [NULL, "aa", "aaa"], thus there will be three groups.
            assert_eq!(&r.logical_rows, &[0, 1, 2]);
            assert_eq!(r.physical_columns.rows_len(), 3);
            assert_eq!(r.physical_columns.columns_len(), 4); // 3 result column, 1 group by column

            // Let's check group by column first. Group by column is decoded in fast hash agg,
            // but not decoded in slow hash agg. So decode it anyway.
            r.physical_columns[3]
                .ensure_all_decoded(&mut EvalContext::default(), &exec.schema()[3])
                .unwrap();

            // The row order is not defined. Let's sort it by the group by column before asserting.
            let mut sort_column: Vec<(usize, _)> = r.physical_columns[3]
                .decoded()
                .as_bytes_slice()
                .iter()
                .enumerate()
                .collect();
            sort_column.sort_by(|a, b| a.1.cmp(&b.1));

            // Use the order of the sorted column to sort other columns
            let ordered_column: Vec<_> = sort_column
                .iter()
                .map(|(idx, _)| r.physical_columns[3].decoded().as_bytes_slice()[*idx].clone())
                .collect();
            assert_eq!(
                &ordered_column,
                &[None, Some(b"aa".to_vec()), Some(b"aaa".to_vec())]
            );
            let ordered_column: Vec<_> = sort_column
                .iter()
                .map(|(idx, _)| r.physical_columns[0].decoded().as_int_slice()[*idx])
                .collect();
            assert_eq!(&ordered_column, &[Some(0), Some(0), Some(2)]);
            let ordered_column: Vec<_> = sort_column
                .iter()
                .map(|(idx, _)| r.physical_columns[1].decoded().as_int_slice()[*idx])
                .collect();
            assert_eq!(&ordered_column, &[Some(1), Some(1), Some(2)]);
            let ordered_column: Vec<_> = sort_column
                .iter()
                .map(|(idx, _)| r.physical_columns[2].decoded().as_real_slice()[*idx])
                .collect();
            assert_eq!(
                &ordered_column,
                &[
                    Real::new(4.5).ok(),
                    Real::new(1.0).ok(),
                    Real::new(6.5).ok()
                ]
            );
        }
    }

    #[test]
    fn test_no_row() {
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
                out_exp.push(RpnExpressionBuilder::new().push_constant(1).build());
                Ok(Box::new(AggrFnUnreachable))
            }
        }

        let exec_fast = |src_exec| {
            Box::new(BatchFastHashAggregationExecutor::new_for_test(
                src_exec,
                RpnExpressionBuilder::new().push_column_ref(0).build(),
                vec![Expr::default()],
                MyParser,
            )) as Box<dyn BatchExecutor<StorageStats = ()>>
        };

        let exec_slow = |src_exec| {
            Box::new(BatchSlowHashAggregationExecutor::new_for_test(
                src_exec,
                vec![RpnExpressionBuilder::new().push_column_ref(0).build()],
                vec![Expr::default()],
                MyParser,
            )) as Box<dyn BatchExecutor<StorageStats = ()>>
        };

        let executor_builders: Vec<Box<dyn FnOnce(MockExecutor) -> _>> =
            vec![Box::new(exec_fast), Box::new(exec_slow)];

        for exec_builder in executor_builders {
            let src_exec = MockExecutor::new(
                vec![FieldTypeTp::LongLong.into()],
                vec![
                    BatchExecuteResult {
                        physical_columns: LazyBatchColumnVec::empty(),
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
            let mut exec = exec_builder(src_exec);

            let r = exec.next_batch(1);
            assert!(r.logical_rows.is_empty());
            assert_eq!(r.physical_columns.rows_len(), 0);
            assert!(!r.is_drained.unwrap());

            let r = exec.next_batch(1);
            assert!(r.logical_rows.is_empty());
            assert_eq!(r.physical_columns.rows_len(), 0);
            assert!(r.is_drained.unwrap());
        }
    }

    /// Only have GROUP BY columns but no Aggregate Functions.
    ///
    /// E.g. SELECT 1 FROM t GROUP BY x
    #[test]
    fn test_no_aggr_fn() {
        let group_by_exp = || RpnExpressionBuilder::new().push_column_ref(0).build();

        let exec_fast = |src_exec| {
            Box::new(BatchFastHashAggregationExecutor::new_for_test(
                src_exec,
                group_by_exp(),
                vec![],
                AllAggrDefinitionParser,
            )) as Box<dyn BatchExecutor<StorageStats = ()>>
        };

        let exec_slow = |src_exec| {
            Box::new(BatchSlowHashAggregationExecutor::new_for_test(
                src_exec,
                vec![group_by_exp()],
                vec![],
                AllAggrDefinitionParser,
            )) as Box<dyn BatchExecutor<StorageStats = ()>>
        };

        let executor_builders: Vec<Box<dyn FnOnce(MockExecutor) -> _>> =
            vec![Box::new(exec_fast), Box::new(exec_slow)];

        for exec_builder in executor_builders {
            let src_exec = make_src_executor_1();
            let mut exec = exec_builder(src_exec);

            let r = exec.next_batch(1);
            assert!(r.logical_rows.is_empty());
            assert_eq!(r.physical_columns.rows_len(), 0);
            assert!(!r.is_drained.unwrap());

            let r = exec.next_batch(1);
            assert!(r.logical_rows.is_empty());
            assert_eq!(r.physical_columns.rows_len(), 0);
            assert!(!r.is_drained.unwrap());

            let mut r = exec.next_batch(1);
            assert_eq!(&r.logical_rows, &[0, 1, 2]);
            assert_eq!(r.physical_columns.rows_len(), 3);
            assert_eq!(r.physical_columns.columns_len(), 1); // 0 result column, 1 group by column
            r.physical_columns[0]
                .ensure_all_decoded(&mut EvalContext::default(), &exec.schema()[0])
                .unwrap();
            let mut sort_column: Vec<(usize, _)> = r.physical_columns[0]
                .decoded()
                .as_real_slice()
                .iter()
                .map(|v| {
                    use std::hash::{Hash, Hasher};
                    let mut s = std::collections::hash_map::DefaultHasher::new();
                    v.hash(&mut s);
                    s.finish()
                })
                .enumerate()
                .collect();
            sort_column.sort_by(|a, b| a.1.cmp(&b.1));
            let ordered_column: Vec<_> = sort_column
                .iter()
                .map(|(idx, _)| r.physical_columns[0].decoded().as_real_slice()[*idx])
                .collect();
            assert_eq!(
                &ordered_column,
                &[Real::new(1.5).ok(), None, Real::new(7.0).ok()]
            );
        }
    }
}
