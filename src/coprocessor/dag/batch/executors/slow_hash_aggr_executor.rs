// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use hashbrown::hash_map::Entry;
use smallvec::SmallVec;

use tikv_util::collections::HashMap;
use tipb::executor::Aggregation;
use tipb::expression::{Expr, FieldType};

use crate::coprocessor::codec::batch::{LazyBatchColumn, LazyBatchColumnVec};
use crate::coprocessor::dag::aggr_fn::*;
use crate::coprocessor::dag::batch::executors::util::aggr_executor::*;
use crate::coprocessor::dag::batch::executors::util::hash_aggr_helper::HashAggregationHelper;
use crate::coprocessor::dag::batch::interface::*;
use crate::coprocessor::dag::expr::EvalConfig;
use crate::coprocessor::dag::rpn_expr::{RpnExpression, RpnExpressionBuilder};
use crate::coprocessor::Result;

/// Slow Hash Aggregation Executor supports multiple groups but uses less efficient ways to
/// store group keys in hash tables.
///
/// FIXME: It is not correct to just store the serialized data as the group key.
/// See pingcap/tidb#10467.
pub struct BatchSlowHashAggregationExecutor<Src: BatchExecutor>(
    AggregationExecutor<Src, SlowHashAggregationImpl>,
);

impl<Src: BatchExecutor> BatchExecutor for BatchSlowHashAggregationExecutor<Src> {
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

impl<Src: BatchExecutor> BatchSlowHashAggregationExecutor<Src> {
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

impl BatchSlowHashAggregationExecutor<Box<dyn BatchExecutor>> {
    /// Checks whether this executor can be used.
    #[inline]
    pub fn check_supported(descriptor: &Aggregation) -> Result<()> {
        let group_by_definitions = descriptor.get_group_by();
        assert!(!group_by_definitions.is_empty());
        for def in group_by_definitions {
            RpnExpressionBuilder::check_expr_tree_supported(def)?;
            if RpnExpressionBuilder::is_expr_eval_to_scalar(def)? {
                return Err(box_err!("Group by expression is not a column"));
            }
        }

        let aggr_definitions = descriptor.get_agg_func();
        for def in aggr_definitions {
            AllAggrDefinitionParser.check_supported(def)?;
        }
        Ok(())
    }
}

impl<Src: BatchExecutor> BatchSlowHashAggregationExecutor<Src> {
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
        let aggr_impl = SlowHashAggregationImpl {
            states: Vec::with_capacity(1024),
            groups: HashMap::default(),
            group_by_exps,
            states_offset_each_row: Vec::with_capacity(
                crate::coprocessor::dag::batch_handler::BATCH_MAX_SIZE,
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

pub struct SlowHashAggregationImpl {
    states: Vec<Box<dyn AggrFunctionState>>,
    groups: HashMap<Vec<u8>, GroupInfo>,
    group_by_exps: Vec<RpnExpression>,
    states_offset_each_row: Vec<usize>,
}

#[derive(Debug)]
struct GroupInfo {
    /// The start offset of the states of this group stored in
    /// `SlowHashAggregationImpl::states`.
    states_start_offset: usize,

    /// The offset of each GROUP BY column in the group key. There will be an additional offset
    /// at the end, which equals to the group key length.
    ///
    /// For example, if the group key `AaaFfffff` is composed by two group column `[Aaa, Ffffff]`,
    /// then the value of this field is `[0, 3, 9]`.
    ///
    /// Note 1:
    /// We want aggregation executor to provide each GROUP BY column independently, which will be
    /// useful in future executors (like Projection executor). However, currently this segmentation
    /// is not useful because we return data by row and columns will be always pasted together.
    /// Thus segmentation or not doesn't make any differences.
    ///
    /// Note 2:
    /// We use `SmallVec<[..; 6]>` so that when group by columns <= 5 SmallVec avoids allocation.
    /// We think the case that there are more than 5 group by columns are rare.
    ///
    /// Note 3:
    /// We use `SmallVec<[u32; ..]>` instead of `SmallVec<[usize; ..]>` because the length of the
    /// group key will not exceed `u32`. Using `u32` instead of `usize` makes this field small
    /// and can be fit into the cache line better.
    group_key_offsets: SmallVec<[u32; 6]>,
}

impl<Src: BatchExecutor> AggregationExecutorImpl<Src> for SlowHashAggregationImpl {
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
        let rows_len = input.rows_len();

        // 1. Calculate which group each src row belongs to.
        self.states_offset_each_row.clear();

        let src_schema = entities.src.schema();

        // TODO: Eliminate these allocations using an allocator.
        let mut group_by_keys = Vec::with_capacity(rows_len);
        let mut group_by_keys_offsets = Vec::with_capacity(rows_len);
        for _ in 0..rows_len {
            group_by_keys.push(Vec::with_capacity(32));
            group_by_keys_offsets.push(SmallVec::new());
        }
        for group_by_exp in &self.group_by_exps {
            let group_by_result =
                group_by_exp.eval(&mut entities.context, rows_len, src_schema, &mut input)?;
            // Unwrap is fine because we have verified the group by expression before.
            let group_column = group_by_result.vector_value().unwrap();
            let field_type = group_by_result.field_type();
            for row_index in 0..rows_len {
                group_by_keys_offsets[row_index].push(group_by_keys[row_index].len() as u32);
                group_column.encode(row_index, field_type, &mut group_by_keys[row_index])?;
            }
        }
        // One extra offset, to be used as the end offset.
        for row_index in 0..rows_len {
            group_by_keys_offsets[row_index].push(group_by_keys[row_index].len() as u32);
        }

        for (group_key, group_key_offsets) in group_by_keys.into_iter().zip(group_by_keys_offsets) {
            match self.groups.entry(group_key) {
                Entry::Vacant(entry) => {
                    let offset = self.states.len();
                    self.states_offset_each_row.push(offset);
                    entry.insert(GroupInfo {
                        states_start_offset: offset,
                        group_key_offsets,
                    });
                    for aggr_fn in &entities.each_aggr_fn {
                        self.states.push(aggr_fn.create_state());
                    }
                }
                Entry::Occupied(entry) => {
                    self.states_offset_each_row
                        .push(entry.get().states_start_offset);
                }
            }
        }

        // 2. Update states according to the group.
        HashAggregationHelper::update_each_row_states_by_offset(
            entities,
            &mut input,
            &mut self.states,
            &self.states_offset_each_row,
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

        let number_of_groups = self.groups.len();
        let group_by_exps_len = self.group_by_exps.len();
        let mut group_by_columns: Vec<_> = self
            .group_by_exps
            .iter()
            .map(|_| LazyBatchColumn::raw_with_capacity(number_of_groups))
            .collect();
        let aggr_fns_len = entities.each_aggr_fn.len();

        let groups = std::mem::replace(&mut self.groups, HashMap::default());
        for (group_key, group_info) in groups {
            iteratee(
                entities,
                &self.states
                    [group_info.states_start_offset..group_info.states_start_offset + aggr_fns_len],
            )?;

            // Extract group column from group key for each group
            for group_index in 0..group_by_exps_len {
                let offset_begin = group_info.group_key_offsets[group_index] as usize;
                let offset_end = group_info.group_key_offsets[group_index + 1] as usize;
                group_by_columns[group_index]
                    .mut_raw()
                    .push(&group_key[offset_begin..offset_end]);
            }
        }

        Ok(group_by_columns)
    }

    /// Slow hash aggregation can output aggregate results only if the source is drained.
    #[inline]
    fn is_partial_results_ready(&self) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use cop_datatype::FieldTypeTp;
    use tipb::expression::ScalarFuncSig;

    use crate::coprocessor::codec::data_type::*;
    use crate::coprocessor::codec::mysql::Tz;
    use crate::coprocessor::dag::batch::executors::util::aggr_executor::tests::*;
    use crate::coprocessor::dag::rpn_expr::impl_arithmetic::{RealPlus, RpnFnArithmetic};
    use crate::coprocessor::dag::rpn_expr::RpnExpressionBuilder;

    #[test]
    fn test_it_works_integration() {
        use tipb::expression::ExprType;
        use tipb_helper::ExprDefBuilder;

        // This test creates a hash aggregation executor with the following aggregate functions:
        // - COUNT(1)
        // - AVG(col_0 + 5.0)
        // And group by:
        // - col_3
        // - col_0 + 1

        let group_by_exps = vec![
            RpnExpressionBuilder::new().push_column_ref(3).build(),
            RpnExpressionBuilder::new()
                .push_column_ref(0)
                .push_constant(1.0)
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
                        .push_child(ExprDefBuilder::column_ref(0, FieldTypeTp::Double))
                        .push_child(ExprDefBuilder::constant_real(5.0)),
                )
                .build(),
        ];

        let src_exec = make_src_executor_1();
        let mut exec = BatchSlowHashAggregationExecutor::new_for_test(
            src_exec,
            group_by_exps,
            aggr_definitions,
            AllAggrDefinitionParser,
        );

        let r = exec.next_batch(1);
        assert_eq!(r.data.rows_len(), 0);
        assert!(!r.is_drained.unwrap());

        let r = exec.next_batch(1);
        assert_eq!(r.data.rows_len(), 0);
        assert!(!r.is_drained.unwrap());

        let mut r = exec.next_batch(1);
        // col_3, col_0 + 1 can result in:
        // 1,     NULL
        // NULL,  8
        // NULL,  NULL
        // 5,     2.5
        // Thus there are 4 groups.
        assert_eq!(r.data.rows_len(), 4);
        assert_eq!(r.data.columns_len(), 5); // 3 result column, 2 group by column

        // Let's check the two group by column first.
        r.data[3]
            .ensure_decoded(&Tz::utc(), &exec.schema()[3])
            .unwrap();
        assert_eq!(
            r.data[3].decoded().as_int_slice(),
            &[Some(5), None, None, Some(1)]
        );
        r.data[4]
            .ensure_decoded(&Tz::utc(), &exec.schema()[4])
            .unwrap();
        assert_eq!(
            r.data[4].decoded().as_real_slice(),
            &[Real::new(2.5).ok(), Real::new(8.0).ok(), None, None]
        );

        assert_eq!(
            r.data[0].decoded().as_int_slice(),
            &[Some(1), Some(1), Some(2), Some(1)]
        );
        assert_eq!(
            r.data[1].decoded().as_int_slice(),
            &[Some(1), Some(1), Some(0), Some(0)]
        );
        assert_eq!(
            r.data[2].decoded().as_real_slice(),
            &[Real::new(6.5).ok(), Real::new(12.0).ok(), None, None]
        );
    }
}
