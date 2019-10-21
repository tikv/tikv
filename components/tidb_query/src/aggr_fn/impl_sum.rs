// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use tidb_query_codegen::AggrFunction;
use tidb_query_datatype::EvalType;
use tipb::{Expr, ExprType, FieldType};

use super::summable::Summable;
use crate::codec::data_type::*;
use crate::expr::EvalContext;
use crate::rpn_expr::{RpnExpression, RpnExpressionBuilder};
use crate::Result;

/// The parser for SUM aggregate function.
pub struct AggrFnDefinitionParserSum;

impl super::parser::AggrDefinitionParser for AggrFnDefinitionParserSum {
    fn check_supported(&self, aggr_def: &Expr) -> Result<()> {
        assert_eq!(aggr_def.get_tp(), ExprType::Sum);
        super::util::check_aggr_exp_supported_one_child(aggr_def)
    }

    fn parse(
        &self,
        mut aggr_def: Expr,
        ctx: &mut EvalContext,
        src_schema: &[FieldType],
        out_schema: &mut Vec<FieldType>,
        out_exp: &mut Vec<RpnExpression>,
    ) -> Result<Box<dyn super::AggrFunction>> {
        use std::convert::TryFrom;
        use tidb_query_datatype::FieldTypeAccessor;

        assert_eq!(aggr_def.get_tp(), ExprType::Sum);

        let out_ft = aggr_def.take_field_type();
        let out_et = box_try!(EvalType::try_from(out_ft.as_accessor().tp()));

        // Rewrite expression, inserting CAST if necessary. See `typeInfer4Sum` in TiDB.
        let child = aggr_def.take_children().into_iter().next().unwrap();
        let mut exp = RpnExpressionBuilder::build_from_expr_tree(child, ctx, src_schema.len())?;
        // The rewrite should always success.
        super::util::rewrite_exp_for_sum_avg(src_schema, &mut exp).unwrap();

        let rewritten_eval_type =
            EvalType::try_from(exp.ret_field_type(src_schema).as_accessor().tp()).unwrap();
        if out_et != rewritten_eval_type {
            return Err(other_err!(
                "Unexpected return field type {}",
                out_ft.as_accessor().tp()
            ));
        }

        // SUM outputs one column.
        out_schema.push(out_ft);
        out_exp.push(exp);

        // Choose a type-aware SUM implementation based on the eval type after rewriting exp.
        Ok(match rewritten_eval_type {
            EvalType::Decimal => Box::new(AggrFnSum::<Decimal>::new()),
            EvalType::Real => Box::new(AggrFnSum::<Real>::new()),
            // If we meet unexpected types after rewriting, it is an implementation fault.
            _ => unreachable!(),
        })
    }
}

/// The SUM aggregate function.
///
/// Note that there are `SUM(Decimal) -> Decimal` and `SUM(Double) -> Double`.
#[derive(Debug, AggrFunction)]
#[aggr_function(state = AggrFnStateSum::<T>::new())]
pub struct AggrFnSum<T>
where
    T: Summable,
    VectorValue: VectorValueExt<T>,
{
    _phantom: std::marker::PhantomData<T>,
}

impl<T> AggrFnSum<T>
where
    T: Summable,
    VectorValue: VectorValueExt<T>,
{
    pub fn new() -> Self {
        Self {
            _phantom: std::marker::PhantomData,
        }
    }
}

/// The state of the SUM aggregate function.
#[derive(Debug)]
pub struct AggrFnStateSum<T>
where
    T: Summable,
    VectorValue: VectorValueExt<T>,
{
    sum: T,
    has_value: bool,
}

impl<T> AggrFnStateSum<T>
where
    T: Summable,
    VectorValue: VectorValueExt<T>,
{
    pub fn new() -> Self {
        Self {
            sum: T::zero(),
            has_value: false,
        }
    }
}

impl<T> super::ConcreteAggrFunctionState for AggrFnStateSum<T>
where
    T: Summable,
    VectorValue: VectorValueExt<T>,
{
    type ParameterType = T;

    #[inline]
    fn update_concrete(&mut self, ctx: &mut EvalContext, value: &Option<T>) -> Result<()> {
        match value {
            None => Ok(()),
            Some(value) => {
                self.sum.add_assign(ctx, value)?;
                self.has_value = true;
                Ok(())
            }
        }
    }

    #[inline]
    fn push_result(&self, _ctx: &mut EvalContext, target: &mut [VectorValue]) -> Result<()> {
        if !self.has_value {
            target[0].push(None);
        } else {
            target[0].push(Some(self.sum.clone()));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use tidb_query_datatype::{FieldTypeAccessor, FieldTypeTp};
    use tipb_helper::ExprDefBuilder;

    use crate::aggr_fn::parser::AggrDefinitionParser;
    use crate::codec::batch::{LazyBatchColumn, LazyBatchColumnVec};

    /// SUM(Bytes) should produce (Real).
    #[test]
    fn test_integration() {
        let expr = ExprDefBuilder::aggr_func(ExprType::Sum, FieldTypeTp::Double)
            .push_child(ExprDefBuilder::column_ref(0, FieldTypeTp::VarString))
            .build();
        AggrFnDefinitionParserSum.check_supported(&expr).unwrap();

        let src_schema = [FieldTypeTp::VarString.into()];
        let mut columns = LazyBatchColumnVec::from(vec![{
            let mut col = LazyBatchColumn::decoded_with_capacity_and_tp(0, EvalType::Bytes);
            col.mut_decoded().push_bytes(Some(b"12.5".to_vec()));
            col.mut_decoded().push_bytes(None);
            col.mut_decoded().push_bytes(Some(b"10000.0".to_vec()));
            col.mut_decoded().push_bytes(Some(b"42.0".to_vec()));
            col.mut_decoded().push_bytes(None);
            col
        }]);
        let logical_rows = vec![0, 1, 3, 4];

        let mut schema = vec![];
        let mut exp = vec![];
        let mut ctx = EvalContext::default();

        let aggr_fn = AggrFnDefinitionParserSum
            .parse(expr, &mut ctx, &src_schema, &mut schema, &mut exp)
            .unwrap();
        assert_eq!(schema.len(), 1);
        assert_eq!(schema[0].as_accessor().tp(), FieldTypeTp::Double);

        assert_eq!(exp.len(), 1);

        let mut state = aggr_fn.create_state();

        let exp_result = exp[0]
            .eval(&mut ctx, &src_schema, &mut columns, &logical_rows, 4)
            .unwrap();
        let exp_result = exp_result.vector_value().unwrap();
        let slice: &[Option<Real>] = exp_result.as_ref().as_ref();
        state
            .update_vector(&mut ctx, slice, exp_result.logical_rows())
            .unwrap();

        let mut aggr_result = [VectorValue::with_capacity(0, EvalType::Real)];
        state.push_result(&mut ctx, &mut aggr_result).unwrap();

        assert_eq!(aggr_result[0].as_real_slice(), &[Real::new(54.5).ok()]);
    }

    #[test]
    fn test_illegal_request() {
        let expr = ExprDefBuilder::aggr_func(ExprType::Sum, FieldTypeTp::Double) // Expect NewDecimal but give Double
            .push_child(ExprDefBuilder::column_ref(0, FieldTypeTp::LongLong)) // FIXME: This type can be incorrect as well
            .build();
        AggrFnDefinitionParserSum.check_supported(&expr).unwrap();

        let src_schema = [FieldTypeTp::LongLong.into()];
        let mut schema = vec![];
        let mut exp = vec![];
        let mut ctx = EvalContext::default();
        AggrFnDefinitionParserSum
            .parse(expr, &mut ctx, &src_schema, &mut schema, &mut exp)
            .unwrap_err();
    }
}
