// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

pub mod aggr_executor;
pub mod hash_aggr_helper;
#[cfg(test)]
pub mod mock_executor;
pub mod scan_executor;

use tidb_query_common::Result;
use tidb_query_datatype::{codec::batch::LazyBatchColumnVec, expr::EvalContext};
use tidb_query_expr::{RpnExpression, RpnStackNode};
use tipb::FieldType;

/// Decodes all columns that are not decoded.
pub fn ensure_columns_decoded(
    ctx: &mut EvalContext,
    exprs: &[RpnExpression],
    schema: &[FieldType],
    input_physical_columns: &mut LazyBatchColumnVec,
    input_logical_rows: &[usize],
) -> Result<()> {
    for expr in exprs {
        expr.ensure_columns_decoded(ctx, schema, input_physical_columns, input_logical_rows)?;
    }
    Ok(())
}

/// Evaluates expressions and outputs the result into the given Vec. Lifetime of the expressions
/// are erased.
pub unsafe fn eval_exprs_decoded_no_lifetime<'a>(
    ctx: &mut EvalContext,
    exprs: &[RpnExpression],
    schema: &[FieldType],
    input_physical_columns: &LazyBatchColumnVec,
    input_logical_rows: &[usize],
    output: &mut Vec<RpnStackNode<'a>>,
) -> Result<()> {
    unsafe fn erase_lifetime<'a, T: ?Sized>(v: &T) -> &'a T {
        &*(v as *const T)
    }

    for expr in exprs {
        output.push(erase_lifetime(expr).eval_decoded(
            ctx,
            erase_lifetime(schema),
            erase_lifetime(input_physical_columns),
            erase_lifetime(input_logical_rows),
            input_logical_rows.len(),
        )?)
    }
    Ok(())
}
