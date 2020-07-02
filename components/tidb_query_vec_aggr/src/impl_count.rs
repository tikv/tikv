// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use tidb_query_codegen::AggrFunction;
use tidb_query_datatype::builder::FieldTypeBuilder;
use tidb_query_datatype::{FieldTypeFlag, FieldTypeTp};
use tipb::{Expr, ExprType, FieldType};

use super::*;
use tidb_query_common::Result;
use tidb_query_datatype::codec::data_type::*;
use tidb_query_datatype::expr::EvalContext;
use tidb_query_vec_expr::{RpnExpression, RpnExpressionBuilder};

/// The parser for COUNT aggregate function.
pub struct AggrFnDefinitionParserCount;

impl super::AggrDefinitionParser for AggrFnDefinitionParserCount {
    fn check_supported(&self, aggr_def: &Expr) -> Result<()> {
        assert_eq!(aggr_def.get_tp(), ExprType::Count);
        super::util::check_aggr_exp_supported_one_child(aggr_def)
    }

    fn parse(
        &self,
        mut aggr_def: Expr,
        ctx: &mut EvalContext,
        // We use the same structure for all data types, so this parameter is not needed.
        src_schema: &[FieldType],
        out_schema: &mut Vec<FieldType>,
        out_exp: &mut Vec<RpnExpression>,
    ) -> Result<Box<dyn super::AggrFunction>> {
        assert_eq!(aggr_def.get_tp(), ExprType::Count);
        let child = aggr_def.take_children().into_iter().next().unwrap();

        // COUNT outputs one column.
        out_schema.push(
            FieldTypeBuilder::new()
                .tp(FieldTypeTp::LongLong)
                .flag(FieldTypeFlag::UNSIGNED)
                .build(),
        );

        // COUNT doesn't need to cast, so using the expression directly.
        out_exp.push(RpnExpressionBuilder::build_from_expr_tree(
            child,
            ctx,
            src_schema.len(),
        )?);

        Ok(Box::new(AggrFnCount))
    }
}

/// The COUNT aggregate function.
#[derive(Debug, AggrFunction)]
#[aggr_function(state = AggrFnStateCount::new())]
pub struct AggrFnCount;

/// The state of the COUNT aggregate function.
#[derive(Debug)]
pub struct AggrFnStateCount {
    count: usize,
}

impl AggrFnStateCount {
    pub fn new() -> Self {
        Self { count: 0 }
    }
}

// Here we manually implement `AggrFunctionStateUpdatePartial` so that `update_repeat` and
// `update_vector` can be faster. Also note that we support all kind of
// `AggrFunctionStateUpdatePartial` for the COUNT aggregate function.

impl<T: EvaluableRef<'static>> super::AggrFunctionStateUpdatePartial<T> for AggrFnStateCount {
    #[inline]
    unsafe fn update_unsafe(&mut self, _ctx: &mut EvalContext, value: Option<T>) -> Result<()> {
        if value.is_some() {
            self.count += 1;
        }
        Ok(())
    }

    #[inline]
    unsafe fn update_repeat_unsafe(
        &mut self,
        _ctx: &mut EvalContext,
        value: Option<T>,
        repeat_times: usize,
    ) -> Result<()> {
        // Will be used for expressions like `COUNT(1)`.
        if value.is_some() {
            self.count += repeat_times;
        }
        Ok(())
    }

    #[inline]
    unsafe fn update_vector_unsafe(
        &mut self,
        _ctx: &mut EvalContext,
        _phantom_data: Option<T>,
        physical_values: T::ChunkedType,
        logical_rows: &[usize],
    ) -> Result<()> {
        // Will be used for expressions like `COUNT(col)`.
        for physical_index in logical_rows {
            if physical_values.get_option_ref(*physical_index).is_some() {
                self.count += 1;
            }
        }
        Ok(())
    }
}

impl super::AggrFunctionState for AggrFnStateCount {
    #[inline]
    fn push_result(&self, _ctx: &mut EvalContext, target: &mut [VectorValue]) -> Result<()> {
        assert_eq!(target.len(), 1);
        target[0].push(Some(self.count as Int));
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use tidb_query_datatype::EvalType;

    use super::super::AggrFunction;
    use super::*;

    #[test]
    fn test_update() {
        let mut ctx = EvalContext::default();
        let function = AggrFnCount;
        let mut state = function.create_state();

        let mut result = [VectorValue::with_capacity(0, EvalType::Int)];

        state.push_result(&mut ctx, &mut result).unwrap();
        assert_eq!(result[0].as_int_vec(), &[Some(0)]);

        update!(state, &mut ctx, Option::<&Real>::None).unwrap();

        result[0].clear();
        state.push_result(&mut ctx, &mut result).unwrap();
        assert_eq!(result[0].as_int_vec(), &[Some(0)]);

        update!(state, &mut ctx, Real::new(5.0).ok().as_ref()).unwrap();
        update!(state, &mut ctx, Option::<&Real>::None).unwrap();
        update!(state, &mut ctx, Some(&7i64)).unwrap();

        result[0].clear();
        state.push_result(&mut ctx, &mut result).unwrap();
        assert_eq!(result[0].as_int_vec(), &[Some(2)]);

        update_repeat!(state, &mut ctx, Some(&3i64), 4).unwrap();
        update_repeat!(state, &mut ctx, Option::<&Int>::None, 7).unwrap();

        result[0].clear();
        state.push_result(&mut ctx, &mut result).unwrap();
        assert_eq!(result[0].as_int_vec(), &[Some(6)]);

        let chunked_vec: NotChunkedVec<Int> = vec![Some(1i64), None, Some(-1i64)].into();
        update_vector!(state, &mut ctx, &chunked_vec, &[1, 2]).unwrap();

        result[0].clear();
        state.push_result(&mut ctx, &mut result).unwrap();
        assert_eq!(result[0].as_int_vec(), &[Some(7)]);
    }
}
