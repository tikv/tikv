// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use super::aggr_executor::*;
use crate::coprocessor::codec::batch::LazyBatchColumnVec;
use crate::coprocessor::codec::data_type::*;
use crate::coprocessor::dag::aggr_fn::AggrFunctionState;
use crate::coprocessor::dag::batch::interface::*;
use crate::coprocessor::dag::rpn_expr::types::RpnStackNode;
use crate::coprocessor::Result;

pub struct HashAggregationHelper;

impl HashAggregationHelper {
    /// Updates states for each row.
    ///
    /// Each row may belong to a different group. States of all groups should be passed in altogether
    /// in a single vector and the states of each row should be specified by an offset vector.
    pub fn update_each_row_states_by_offset<Src: BatchExecutor>(
        entities: &mut Entities<Src>,
        input: &mut LazyBatchColumnVec,
        states: &mut [Box<dyn AggrFunctionState>],
        states_offset_each_row: &[usize],
    ) -> Result<()> {
        let rows_len = input.rows_len();
        let src_schema = entities.src.schema();

        for idx in 0..entities.each_aggr_fn.len() {
            let aggr_expr = &entities.each_aggr_exprs[idx];
            let aggr_expr_result =
                aggr_expr.eval(&mut entities.context, rows_len, src_schema, input)?;
            match aggr_expr_result {
                RpnStackNode::Scalar { value, .. } => {
                    match_template_evaluable! {
                        TT, match value {
                            ScalarValue::TT(scalar_value) => {
                                for offset in states_offset_each_row {
                                    let aggr_fn_state = &mut states[*offset + idx];
                                    aggr_fn_state.update(&mut entities.context, scalar_value)?;
                                }
                            },
                        }
                    }
                }
                RpnStackNode::Vector { value, .. } => {
                    match_template_evaluable! {
                        TT, match &*value {
                            VectorValue::TT(vector_value) => {
                                for (row_index, offset) in states_offset_each_row.iter().enumerate() {
                                    let aggr_fn_state = &mut states[*offset + idx];
                                    aggr_fn_state.update(&mut entities.context, &vector_value[row_index])?;
                                }
                            }
                        }
                    }
                }
            }
        }

        Ok(())
    }
}
