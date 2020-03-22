// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use super::aggr_executor::*;
use crate::interface::*;
use tidb_query_common::Result;
use tidb_query_datatype::codec::batch::LazyBatchColumnVec;
use tidb_query_datatype::codec::data_type::*;
use tidb_query_vec_aggr::AggrFunctionState;
use tidb_query_vec_expr::RpnStackNode;

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

        for idx in 0..entities.each_aggr_fn.len() {
            let aggr_expr = &entities.each_aggr_exprs[idx];
            let aggr_expr_result = aggr_expr.eval(
                &mut entities.context,
                src_schema,
                input_physical_columns,
                input_logical_rows,
                logical_rows_len,
            )?;
            match aggr_expr_result {
                RpnStackNode::Scalar { value, .. } => {
                    match_template_evaluable! {
                        TT, match value {
                            ScalarValue::TT(scalar_value) => {
                                for offset in states_offset_each_logical_row {
                                    let aggr_fn_state = &mut states[*offset + idx];
                                    aggr_fn_state.update(&mut entities.context, scalar_value)?;
                                }
                            },
                        }
                    }
                }
                RpnStackNode::Vector { value, .. } => {
                    let physical_vec = value.as_ref();
                    let logical_rows = value.logical_rows();
                    match_template_evaluable! {
                        TT, match physical_vec {
                            VectorValue::TT(vec) => {
                                for (states_offset, physical_idx) in states_offset_each_logical_row
                                    .iter()
                                    .zip(logical_rows)
                                {
                                    let aggr_fn_state = &mut states[*states_offset + idx];
                                    aggr_fn_state.update(&mut entities.context, &vec[*physical_idx])?;
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
