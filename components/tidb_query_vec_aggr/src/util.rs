// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::convert::TryFrom;

use tidb_query_datatype::builder::FieldTypeBuilder;
use tidb_query_datatype::{EvalType, FieldTypeAccessor, FieldTypeTp};
use tipb::{Expr, FieldType};

use tidb_query_common::Result;
use tidb_query_vec_expr::impl_cast::get_cast_fn_rpn_node;
use tidb_query_vec_expr::{RpnExpression, RpnExpressionBuilder};

/// Checks whether or not there is only one child and the child expression is supported.
pub fn check_aggr_exp_supported_one_child(aggr_def: &Expr) -> Result<()> {
    if aggr_def.get_children().len() != 1 {
        return Err(other_err!(
            "Expect 1 parameter, but got {}",
            aggr_def.get_children().len()
        ));
    }

    // Check whether parameter expression is supported.
    let child = &aggr_def.get_children()[0];
    RpnExpressionBuilder::check_expr_tree_supported(child)?;

    Ok(())
}

/// Rewrites the expression to insert necessary cast functions for SUM and AVG aggregate functions.
///
/// See `typeInfer4Sum` and `typeInfer4Avg` in TiDB.
///
/// TODO: This logic should be performed by TiDB.
pub fn rewrite_exp_for_sum_avg(schema: &[FieldType], exp: &mut RpnExpression) -> Result<()> {
    let ret_field_type = exp.ret_field_type(schema);
    let ret_eval_type = box_try!(EvalType::try_from(ret_field_type.as_accessor().tp()));
    let new_ret_field_type = match ret_eval_type {
        EvalType::Decimal | EvalType::Real => {
            // No need to cast. Return directly without changing anything.
            return Ok(());
        }
        EvalType::Int => FieldTypeBuilder::new()
            .tp(FieldTypeTp::NewDecimal)
            .flen(tidb_query_datatype::MAX_DECIMAL_WIDTH)
            .build(),
        _ => FieldTypeBuilder::new()
            .tp(FieldTypeTp::Double)
            .flen(tidb_query_datatype::MAX_REAL_WIDTH)
            .decimal(tidb_query_datatype::UNSPECIFIED_LENGTH)
            .build(),
    };
    let node = get_cast_fn_rpn_node(exp.is_last_constant(), ret_field_type, new_ret_field_type)?;
    exp.push(node);
    Ok(())
}

/// Rewrites the expression to insert necessary cast functions for Bit operation family functions.
pub fn rewrite_exp_for_bit_op(schema: &[FieldType], exp: &mut RpnExpression) -> Result<()> {
    let ret_field_type = exp.ret_field_type(schema);
    let ret_eval_type = box_try!(EvalType::try_from(ret_field_type.as_accessor().tp()));
    let new_ret_field_type = match ret_eval_type {
        EvalType::Int => {
            return Ok(());
        }
        _ => FieldTypeBuilder::new().tp(FieldTypeTp::LongLong).build(),
    };
    let node = get_cast_fn_rpn_node(exp.is_last_constant(), ret_field_type, new_ret_field_type)?;
    exp.push(node);
    Ok(())
}
