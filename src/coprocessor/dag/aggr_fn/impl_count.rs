// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use cop_codegen::AggrFunction;
use cop_datatype::{FieldTypeFlag, FieldTypeTp};
use tipb::expression::{Expr, ExprType, FieldType};

use crate::coprocessor::codec::data_type::*;
use crate::coprocessor::codec::mysql::Tz;
use crate::coprocessor::dag::expr::EvalContext;
use crate::coprocessor::dag::rpn_expr::{RpnExpression, RpnExpressionBuilder};
use crate::coprocessor::Result;

/// The parser for COUNT aggregate function.
pub struct AggrFnDefinitionParserCount;

impl super::parser::Parser for AggrFnDefinitionParserCount {
    fn check_supported(&self, aggr_def: &Expr) -> Result<()> {
        assert_eq!(aggr_def.get_tp(), ExprType::Count);
        if aggr_def.get_children().len() != 1 {
            return Err(box_err!(
                "Expect 1 parameter, but got {}",
                aggr_def.get_children().len()
            ));
        }

        // Only check whether or not the children expr is supported.
        let child = &aggr_def.get_children()[0];
        RpnExpressionBuilder::check_expr_tree_supported(child)?;

        Ok(())
    }

    fn parse(
        &self,
        mut aggr_def: Expr,
        time_zone: &Tz,
        max_columns: usize,
        // We use the same structure for all data types, so this parameter is not needed.
        _schema: &[FieldType],
        out_schema: &mut Vec<FieldType>,
        out_exp: &mut Vec<RpnExpression>,
    ) -> Result<Box<dyn super::AggrFunction>> {
        use cop_datatype::FieldTypeAccessor;

        assert_eq!(aggr_def.get_tp(), ExprType::Count);
        let child = aggr_def.take_children().into_iter().next().unwrap();

        // COUNT outputs one column.
        out_schema.push({
            let mut ft = FieldType::new();
            ft.as_mut_accessor()
                .set_tp(FieldTypeTp::LongLong)
                .set_flag(FieldTypeFlag::UNSIGNED);
            ft
        });

        // COUNT doesn't need to cast, so using the expression directly.
        out_exp.push(RpnExpressionBuilder::build_from_expr_tree(
            child,
            time_zone,
            max_columns,
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

impl<T: Evaluable> super::AggrFunctionStateUpdatePartial<T> for AggrFnStateCount {
    #[inline]
    fn update(&mut self, _ctx: &mut EvalContext, value: &Option<T>) -> Result<()> {
        if value.is_some() {
            self.count += 1;
        }
        Ok(())
    }

    #[inline]
    fn update_repeat(
        &mut self,
        _ctx: &mut EvalContext,
        value: &Option<T>,
        repeat_times: usize,
    ) -> Result<()> {
        // Will be used for expressions like `COUNT(1)`.
        if value.is_some() {
            self.count += repeat_times;
        }
        Ok(())
    }

    #[inline]
    fn update_vector(&mut self, _ctx: &mut EvalContext, values: &[Option<T>]) -> Result<()> {
        // Will be used for expressions like `COUNT(col)`.
        for value in values {
            if value.is_some() {
                self.count += 1;
            }
        }
        Ok(())
    }
}

impl<T> super::AggrFunctionStateResultPartial<T> for AggrFnStateCount
where
    T: super::AggrResultAppendable + ?Sized,
{
    #[inline]
    default fn push_result(&self, _ctx: &mut EvalContext, _target: &mut T) -> Result<()> {
        panic!("Unmatched result append target type")
    }
}

impl super::AggrFunctionStateResultPartial<Vec<Option<Int>>> for AggrFnStateCount {
    #[inline]
    fn push_result(&self, _ctx: &mut EvalContext, target: &mut Vec<Option<Int>>) -> Result<()> {
        target.push(Some(self.count as Int));
        Ok(())
    }
}

impl super::AggrFunctionState for AggrFnStateCount {}

#[cfg(test)]
mod tests {
    use cop_datatype::EvalType;

    use super::super::AggrFunction;
    use super::*;

    #[test]
    fn test_update() {
        let mut ctx = EvalContext::default();
        let function = AggrFnCount;
        let mut state = function.create_state();

        let mut result = VectorValue::with_capacity(0, EvalType::Int);
        let mut_inner = AsMut::<Vec<Option<Int>>>::as_mut(&mut result);

        state.push_result(&mut ctx, mut_inner).unwrap();
        assert_eq!(mut_inner.as_slice(), &[Some(0)]);

        state.update(&mut ctx, &Option::<Real>::None).unwrap();

        mut_inner.clear();
        state.push_result(&mut ctx, mut_inner).unwrap();
        assert_eq!(mut_inner.as_slice(), &[Some(0)]);

        state.update(&mut ctx, &Some(5.0f64)).unwrap();
        state.update(&mut ctx, &Option::<Real>::None).unwrap();
        state.update(&mut ctx, &Some(7i64)).unwrap();

        mut_inner.clear();
        state.push_result(&mut ctx, mut_inner).unwrap();
        assert_eq!(mut_inner.as_slice(), &[Some(2)]);

        state.update_repeat(&mut ctx, &Some(3i64), 4).unwrap();
        state
            .update_repeat(&mut ctx, &Option::<Int>::None, 7)
            .unwrap();

        mut_inner.clear();
        state.push_result(&mut ctx, mut_inner).unwrap();
        assert_eq!(mut_inner.as_slice(), &[Some(6)]);

        state
            .update_vector(&mut ctx, &[Some(1i64), None, Some(-1i64)])
            .unwrap();

        mut_inner.clear();
        state.push_result(&mut ctx, mut_inner).unwrap();
        assert_eq!(mut_inner.as_slice(), &[Some(8)]);
    }
}
