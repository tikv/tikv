// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::convert::TryFrom;
use std::sync::Arc;

use cop_datatype::{EvalType, FieldTypeAccessor};
use tikv_util::collections::HashMap;
use tipb::executor::Aggregation;
use tipb::expression::{Expr, FieldType};

use crate::coprocessor::codec::batch::{LazyBatchColumn, LazyBatchColumnVec};
use crate::coprocessor::codec::data_type::*;
use crate::coprocessor::dag::aggr_fn::*;
use crate::coprocessor::dag::batch::executors::util::aggr_executor::*;
use crate::coprocessor::dag::batch::executors::util::hash_aggr_helper::HashAggregationHelper;
use crate::coprocessor::dag::batch::interface::*;
use crate::coprocessor::dag::exec_summary::ExecSummaryCollectorDisabled;
use crate::coprocessor::dag::expr::EvalConfig;
use crate::coprocessor::dag::rpn_expr::{RpnExpression, RpnExpressionBuilder};
use crate::coprocessor::Result;

/// Single Group Hash Aggregation Executor uses hash when comparing group key, which is faster
/// than multi-group's implementation.
pub struct BatchSingleGroupHashAggregationExecutor<C: ExecSummaryCollector, Src: BatchExecutor>(
    AggregationExecutor<C, Src, SingleGroupHashAggregationImpl>,
);

impl<C: ExecSummaryCollector, Src: BatchExecutor> BatchExecutor
    for BatchSingleGroupHashAggregationExecutor<C, Src>
{
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

impl<Src: BatchExecutor>
    BatchSingleGroupHashAggregationExecutor<ExecSummaryCollectorDisabled, Src>
{
    #[cfg(test)]
    pub fn new_for_test(
        src: Src,
        group_by_exp: RpnExpression,
        aggr_defs: Vec<Expr>,
        aggr_def_parser: impl AggrDefinitionParser,
    ) -> Self {
        Self::new_impl(
            ExecSummaryCollectorDisabled,
            Arc::new(EvalConfig::default()),
            src,
            group_by_exp,
            aggr_defs,
            aggr_def_parser,
        )
        .unwrap()
    }
}

impl BatchSingleGroupHashAggregationExecutor<ExecSummaryCollectorDisabled, Box<dyn BatchExecutor>> {
    /// Checks whether this executor can be used.
    #[inline]
    pub fn check_supported(descriptor: &Aggregation) -> Result<()> {
        let group_by_definitions = descriptor.get_group_by();
        assert_eq!(group_by_definitions.len(), 1);
        let def = &group_by_definitions[0];
        RpnExpressionBuilder::check_expr_tree_supported(def)?;
        if RpnExpressionBuilder::is_expr_eval_to_scalar(def)? {
            return Err(box_err!("Group by expression is not a column"));
        }

        let aggr_definitions = descriptor.get_agg_func();
        for def in aggr_definitions {
            AllAggrDefinitionParser.check_supported(def)?;
        }
        Ok(())
    }
}

impl<C: ExecSummaryCollector, Src: BatchExecutor> BatchSingleGroupHashAggregationExecutor<C, Src> {
    pub fn new(
        summary_collector: C,
        config: Arc<EvalConfig>,
        src: Src,
        group_by_exp_def: Expr,
        aggr_defs: Vec<Expr>,
    ) -> Result<Self> {
        let group_by_exp = RpnExpressionBuilder::build_from_expr_tree(
            group_by_exp_def,
            &config.tz,
            src.schema().len(),
        )?;
        Self::new_impl(
            summary_collector,
            config,
            src,
            group_by_exp,
            aggr_defs,
            AllAggrDefinitionParser,
        )
    }

    #[inline]
    fn new_impl(
        summary_collector: C,
        config: Arc<EvalConfig>,
        src: Src,
        group_by_exp: RpnExpression,
        aggr_defs: Vec<Expr>,
        aggr_def_parser: impl AggrDefinitionParser,
    ) -> Result<Self> {
        let group_by_field_type = group_by_exp.ret_field_type(src.schema()).clone();
        let group_by_eval_type = EvalType::try_from(group_by_field_type.tp()).unwrap();
        let groups = match_template_evaluable! {
            TT, match group_by_eval_type {
                EvalType::TT => Groups::TT(HashMap::default()),
            }
        };

        let aggr_impl = SingleGroupHashAggregationImpl {
            states: Vec::with_capacity(1024),
            groups,
            group_by_exp,
            group_by_field_type: Some(group_by_field_type),
        };

        Ok(Self(AggregationExecutor::new(
            summary_collector,
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
    // The value of each hash table is the start index in `SingleGroupHashAggregationImpl::states`
    // field. When there are new groups (i.e. new entry in the hash table), the states of the groups
    // will be appended to `states`.
    Int(HashMap<Option<Int>, usize>),
    Real(HashMap<Option<Real>, usize>),
    Decimal(HashMap<Option<Decimal>, usize>),
    Bytes(HashMap<Option<Bytes>, usize>),
    DateTime(HashMap<Option<DateTime>, usize>),
    Duration(HashMap<Option<Duration>, usize>),
    Json(HashMap<Option<Json>, usize>),
}

impl Groups {
    fn eval_type(&self) -> EvalType {
        match_template_evaluable! {
            TT, match self {
                Groups::TT(_) => {
                    EvalType::TT
                },
            }
        }
    }

    fn len(&self) -> usize {
        match_template_evaluable! {
            TT, match self {
                Groups::TT(groups) => {
                    groups.len()
                },
            }
        }
    }
}

pub struct SingleGroupHashAggregationImpl {
    states: Vec<Box<dyn AggrFunctionState>>,
    groups: Groups,
    group_by_exp: RpnExpression,
    group_by_field_type: Option<FieldType>,
}

impl<Src: BatchExecutor> AggregationExecutorImpl<Src> for SingleGroupHashAggregationImpl {
    #[inline]
    fn prepare_entities(&mut self, entities: &mut Entities<Src>) {
        entities
            .schema
            .push(self.group_by_field_type.take().unwrap());
    }

    #[inline]
    fn process_batch_input(
        &mut self,
        entities: &mut Entities<Src>,
        mut input: LazyBatchColumnVec,
    ) -> Result<()> {
        let rows_len = input.rows_len();

        // 1. Calculate which group each src row belongs to.
        // TODO: Reuse this vector each iterate.
        let mut states_offset_each_row = Vec::with_capacity(rows_len);
        let group_by_result = self.group_by_exp.eval(
            &mut entities.context,
            input.rows_len(),
            entities.src.schema(),
            &mut input,
        )?;
        // Unwrap is fine because we have verified the group by expression before.
        let group_by_vector = group_by_result.vector_value().unwrap();
        match_template_evaluable! {
            TT, match group_by_vector {
                VectorValue::TT(v) => {
                    if let Groups::TT(group) = &mut self.groups {
                        calc_groups_each_row(
                            v,
                            &entities.each_aggr_fn,
                            group,
                            &mut self.states,
                            &mut states_offset_each_row
                        );
                    } else {
                        panic!();
                    }
                },
            }
        }

        // 2. Update states according to the group.
        HashAggregationHelper::update_each_row_states_by_offset(
            entities,
            &mut input,
            &mut self.states,
            &states_offset_each_row,
        )?;

        Ok(())
    }

    #[inline]
    fn iterate_each_group_for_aggregation(
        &mut self,
        entities: &mut Entities<Src>,
        mut iteratee: impl FnMut(&mut Entities<Src>, &[Box<dyn AggrFunctionState>]) -> Result<()>,
    ) -> Result<Vec<LazyBatchColumn>> {
        let aggr_fns_len = entities.each_aggr_fn.len();
        let mut group_by_column = LazyBatchColumn::decoded_with_capacity_and_tp(
            self.groups.len(),
            self.groups.eval_type(),
        );

        match_template_evaluable! {
            TT, match &mut self.groups {
                Groups::TT(groups) => {
                    // Single group column
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
}

fn calc_groups_each_row<T: Evaluable>(
    rows: &[Option<T>],
    aggr_fns: &[Box<dyn AggrFunction>],
    group: &mut HashMap<Option<T>, usize>,
    states: &mut Vec<Box<dyn AggrFunctionState>>,
    output_states_offset_each_row: &mut Vec<usize>,
) {
    for val in rows {
        // Not using the entry API so that when entry exists there is no clone.
        match group.get(val) {
            Some(offset) => {
                // Group exists, use the offset of existing group.
                output_states_offset_each_row.push(*offset);
            }
            None => {
                // Group does not exist, prepare groups.
                let offset = states.len();
                output_states_offset_each_row.push(offset);
                group.insert(val.clone(), offset);
                for aggr_fn in aggr_fns {
                    states.push(aggr_fn.create_state());
                }
            }
        }
    }
}
