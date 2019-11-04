// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

pub mod aggr_executor;
pub mod hash_aggr_helper;
#[cfg(test)]
pub mod mock_executor;
pub mod scan_executor;

use std::mem;

use bumpalo::Bump;

use tipb::FieldType;

use crate::codec::batch::LazyBatchColumnVec;
use crate::codec::mysql::Tz;
use crate::expr::EvalContext;
use crate::rpn_expr::RpnExpression;
use crate::rpn_expr::RpnStackNode;
use crate::Result;

/// Decodes all columns that are not decoded.
pub fn ensure_columns_decoded(
    tz: &Tz,
    exprs: &[RpnExpression],
    schema: &[FieldType],
    input_physical_columns: &mut LazyBatchColumnVec,
    input_logical_rows: &[usize],
) -> Result<()> {
    for expr in exprs {
        expr.ensure_columns_decoded(tz, schema, input_physical_columns, input_logical_rows)?;
    }
    Ok(())
}

/// Evaluates expressions and outputs the result into the given Vec. Lifetime of the expressions
/// are erased.
pub fn eval_exprs_decoded<F>(
    ctx: &mut EvalContext,
    exprs: &[RpnExpression],
    schema: &[FieldType],
    input_physical_columns: &LazyBatchColumnVec,
    input_logical_rows: &[usize],
    empty_buffer: &mut Vec<RpnStackNode<'static>>,
    arena: &Bump,
    f: F,
) -> Result<()>
where
    F: for<'arena> FnOnce(&mut Vec<RpnStackNode<'arena>>, &mut EvalContext) -> Result<()>,
{
    empty_buffer.clear();

    for expr in exprs {
        expr.eval_decoded(
            ctx,
            schema,
            input_physical_columns,
            input_logical_rows,
            input_logical_rows.len(),
            arena,
            |result, _ctx| {
                let lifetime_erased_result =
                    unsafe { mem::transmute::<RpnStackNode<'_>, RpnStackNode<'static>>(result) };
                empty_buffer.push(lifetime_erased_result);

                Ok(())
            },
        )?;
    }

    f(empty_buffer, ctx)?;
    empty_buffer.clear();

    Ok(())
}
