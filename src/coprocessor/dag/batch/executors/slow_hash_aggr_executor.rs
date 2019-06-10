// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::hash::{Hash, Hasher};
use std::ptr::NonNull;
use std::sync::Arc;

use hashbrown::hash_map::Entry;

use tikv_util::collections::HashMap;
use tipb::executor::Aggregation;
use tipb::expression::{Expr, FieldType};

use crate::coprocessor::codec::batch::{LazyBatchColumn, LazyBatchColumnVec};
use crate::coprocessor::codec::data_type::{ScalarValue, VectorValue};
use crate::coprocessor::dag::aggr_fn::*;
use crate::coprocessor::dag::batch::executors::util::aggr_executor::*;
use crate::coprocessor::dag::batch::interface::*;
use crate::coprocessor::dag::expr::EvalConfig;
use crate::coprocessor::dag::rpn_expr::types::RpnStackNode;
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
        let group_by_len = group_by_exps.len();
        let aggr_impl = SlowHashAggregationImpl {
            states: Vec::with_capacity(1024),
            groups: HashMap::default(),
            group_by_exps,
            group_key_buffer: Box::new(Vec::new()),
            group_key_offsets: vec![0],
            group_by_results: Vec::with_capacity(group_by_len),
            aggr_expr_results: Vec::with_capacity(aggr_defs.len()),
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

    /// The value is the group index. `states` and `group_key_offsets` are stored in
    /// the order of group index.
    groups: HashMap<GroupKeyRefUnsafe, usize>,
    group_by_exps: Vec<RpnExpression>,

    #[allow(clippy::box_vec)]
    group_key_buffer: Box<Vec<u8>>,
    group_key_offsets: Vec<usize>,

    /// Stores evaluation results of group by expressions.
    /// It is just used to reduce allocations. The lifetime is not really 'static. The elements
    /// are only valid in the same batch where they are added.
    group_by_results: Vec<RpnStackNode<'static>>,

    /// Stores evaluation results of aggregate expressions.
    /// It is just used to reduce allocations. The lifetime is not really 'static. The elements
    /// are only valid in the same batch where they are added.
    aggr_expr_results: Vec<RpnStackNode<'static>>,
}

unsafe impl Send for SlowHashAggregationImpl {}

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
        let context = &mut entities.context;
        let src_schema = entities.src.schema();
        let rows_len = input.rows_len();
        let group_by_len = self.group_by_exps.len();
        let aggr_fn_len = entities.each_aggr_fn.len();

        // Decode columns with mutable input first, so subsequent access to input can be immutable
        // (and the borrow checker will be happy)
        ensure_columns_decoded(&context.cfg.tz, &self.group_by_exps, src_schema, &mut input)?;
        ensure_columns_decoded(
            &context.cfg.tz,
            &entities.each_aggr_exprs,
            src_schema,
            &mut input,
        )?;
        assert!(self.group_by_results.is_empty());
        assert!(self.aggr_expr_results.is_empty());
        unsafe {
            eval_exprs_no_lifetime(
                context,
                &self.group_by_exps,
                src_schema,
                &input,
                &mut self.group_by_results,
            )?;
            eval_exprs_no_lifetime(
                context,
                &entities.each_aggr_exprs,
                src_schema,
                &input,
                &mut self.aggr_expr_results,
            )?;
        }

        for row_index in 0..rows_len {
            let offset_begin = self.group_key_buffer.len();
            for group_by_result in &self.group_by_results {
                match group_by_result {
                    RpnStackNode::Vector { value, field_type } => {
                        value.encode(row_index, field_type, &mut self.group_key_buffer)?;
                        self.group_key_offsets.push(self.group_key_buffer.len());
                    }
                    // we have checked that group by cannot be a scalar
                    _ => unreachable!(),
                }
            }
            let group_key_ref_unsafe = GroupKeyRefUnsafe {
                buffer: (&*self.group_key_buffer).into(),
                begin: offset_begin,
                end: self.group_key_buffer.len(),
            };
            let group_len = self.groups.len();
            let group_index = match self.groups.entry(group_key_ref_unsafe) {
                Entry::Vacant(entry) => {
                    // if it's a new group, the group index is the current group count
                    entry.insert(group_len);
                    for aggr_fn in &entities.each_aggr_fn {
                        self.states.push(aggr_fn.create_state());
                    }
                    group_len
                }
                Entry::Occupied(entry) => {
                    // remove the duplicated group key
                    self.group_key_buffer.truncate(offset_begin);
                    self.group_key_offsets
                        .truncate(self.group_key_offsets.len() - group_by_len);
                    *entry.get()
                }
            };
            let state_begin = group_index * aggr_fn_len;
            for (aggr_expr_result, state) in self
                .aggr_expr_results
                .iter()
                .zip(&mut self.states[state_begin..state_begin + aggr_fn_len])
            {
                match aggr_expr_result {
                    RpnStackNode::Scalar { value, .. } => {
                        match_template_evaluable! {
                            TT, match value {
                                ScalarValue::TT(scalar_value) => {
                                    state.update(context, scalar_value)?;
                                },
                            }
                        }
                    }
                    RpnStackNode::Vector { value, .. } => {
                        match_template_evaluable! {
                            TT, match &**value {
                                VectorValue::TT(vector_value) => {
                                    state.update(context, &vector_value[row_index])?;
                                },
                            }
                        }
                    }
                }
            }
        }

        // Remember to remove expression results of the current batch. They are invalid
        // in the next batch.
        self.group_by_results.clear();
        self.aggr_expr_results.clear();

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
        for (_, group_index) in groups {
            let states_start_offset = group_index * aggr_fns_len;
            iteratee(
                entities,
                &self.states[states_start_offset..states_start_offset + aggr_fns_len],
            )?;

            // Extract group column from group key for each group
            let group_key_offsets = &self.group_key_offsets[group_index * group_by_exps_len..];
            for group_index in 0..group_by_exps_len {
                let offset_begin = group_key_offsets[group_index];
                let offset_end = group_key_offsets[group_index + 1];
                group_by_columns[group_index]
                    .mut_raw()
                    .push(&self.group_key_buffer[offset_begin..offset_end]);
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

/// A reference to a group key slice in the `group_key_buffer` of `SlowHashAggregationImpl`.
///
/// It is safe as soon as it doesn't outlive the `SlowHashAggregationImpl` that creates this
/// reference.
struct GroupKeyRefUnsafe {
    /// Points to the `group_key_buffer` of `SlowHashAggregationImpl`
    buffer: NonNull<Vec<u8>>,
    begin: usize,
    end: usize,
}

impl Hash for GroupKeyRefUnsafe {
    fn hash<H: Hasher>(&self, state: &mut H) {
        unsafe { self.buffer.as_ref()[self.begin..self.end].hash(state) }
    }
}

impl PartialEq for GroupKeyRefUnsafe {
    fn eq(&self, other: &GroupKeyRefUnsafe) -> bool {
        unsafe {
            self.buffer.as_ref()[self.begin..self.end]
                == other.buffer.as_ref()[other.begin..other.end]
        }
    }
}

impl Eq for GroupKeyRefUnsafe {}

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
