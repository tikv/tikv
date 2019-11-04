// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use bumpalo::Bump;

use super::aggr_executor::*;
use crate::aggr_fn::AggrFunctionState;
use crate::batch::interface::*;
use crate::codec::batch::LazyBatchColumnVec;
use crate::codec::data_type::*;
use crate::rpn_expr::RpnStackNode;
use crate::Result;

pub struct HashAggregationHelper;

impl HashAggregationHelper {
    /// Updates states for each row.
    ///
    /// Each row may belong to a different group. States of all groups should be passed in altogether
    /// in a single vector and the states of each row should be specified by an offset vector.
    pub fn update_each_row_states_by_offset<Src: BatchExecutor>(
        entities: &mut Entities<Src>,
        input_physical_columns: &mut LazyBatchColumnVec,
        input_logical_rows: &[usize],
        states: &mut [Box<dyn AggrFunctionState>],
        states_offset_each_logical_row: &[usize],
    ) -> Result<()> {
        let logical_rows_len = input_logical_rows.len();
        let src_schema = entities.src.schema();
        let arena = Bump::with_capacity(1 << 12);

        for idx in 0..entities.each_aggr_fn.len() {
            let aggr_expr = &entities.each_aggr_exprs[idx];
            aggr_expr.eval(
                &mut entities.context,
                src_schema,
                input_physical_columns,
                input_logical_rows,
                logical_rows_len,
                &arena,
                |aggr_expr_result, ctx| {
                    match aggr_expr_result {
                        RpnStackNode::Scalar { value, .. } => {
                            match_template_evaluable! {
                                TT, match value {
                                    ScalarValue::TT(scalar_value) => {
                                        for offset in states_offset_each_logical_row {
                                            let aggr_fn_state = &mut states[*offset + idx];
                                            aggr_fn_state.update(ctx, scalar_value)?;
                                        }
                                    },
                                }
                            }
                        }
                        RpnStackNode::Vector { value, .. } => {
                            match_template_evaluable! {
                                TT, match value.physical_value {
                                    VectorValueRef::TT(vec) => {
                                        for (states_offset, physical_idx) in states_offset_each_logical_row
                                            .iter()
                                            .zip(value.logical_rows)
                                        {
                                            let aggr_fn_state = &mut states[*states_offset + idx];
                                            aggr_fn_state.update(ctx, &vec[*physical_idx])?;
                                        }
                                    }
                                }
                            }
                        }
                    }
                    Ok(())
                }
            )?;
        }

        Ok(())
    }
}
