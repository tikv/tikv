// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use tidb_query_codegen::AggrFunction;
use tidb_query_datatype::builder::FieldTypeBuilder;
use tidb_query_datatype::{EvalType, FieldTypeFlag, FieldTypeTp};
use tipb::{Expr, ExprType, FieldType};

use super::summable::Summable;
use crate::codec::data_type::*;
use crate::expr::EvalContext;
use crate::rpn_expr::{RpnExpression, RpnExpressionBuilder};
use crate::Result;

/// The parser for AVG aggregate function.
pub struct AggrFnDefinitionParserAvg;

impl super::AggrDefinitionParser for AggrFnDefinitionParserAvg {
    fn check_supported(&self, aggr_def: &Expr) -> Result<()> {
        assert_eq!(aggr_def.get_tp(), ExprType::Avg);
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

        assert_eq!(aggr_def.get_tp(), ExprType::Avg);

        let col_sum_ft = aggr_def.take_field_type();
        let col_sum_et = box_try!(EvalType::try_from(col_sum_ft.as_accessor().tp()));

        // Rewrite expression to insert CAST() if needed.
        let child = aggr_def.take_children().into_iter().next().unwrap();
        let mut exp = RpnExpressionBuilder::build_from_expr_tree(child, ctx, src_schema.len())?;
        super::util::rewrite_exp_for_sum_avg(src_schema, &mut exp).unwrap();

        let rewritten_eval_type =
            EvalType::try_from(exp.ret_field_type(src_schema).as_accessor().tp()).unwrap();
        if col_sum_et != rewritten_eval_type {
            return Err(other_err!(
                "Unexpected return field type {}",
                col_sum_ft.as_accessor().tp()
            ));
        }

        // AVG outputs two columns.
        out_schema.push(
            FieldTypeBuilder::new()
                .tp(FieldTypeTp::LongLong)
                .flag(FieldTypeFlag::UNSIGNED)
                .build(),
        );
        out_schema.push(col_sum_ft);
        out_exp.push(exp);

        Ok(match rewritten_eval_type {
            EvalType::Decimal => Box::new(AggrFnAvg::<Decimal>::new()),
            EvalType::Real => Box::new(AggrFnAvg::<Real>::new()),
            _ => unreachable!(),
        })
    }
}

/// The AVG aggregate function.
///
/// Note that there are `AVG(Decimal) -> (Int, Decimal)` and `AVG(Double) -> (Int, Double)`.
#[derive(Debug, AggrFunction)]
#[aggr_function(state = AggrFnStateAvg::<T>::new())]
pub struct AggrFnAvg<T>
where
    T: Summable,
    VectorValue: VectorValueExt<T>,
{
    _phantom: std::marker::PhantomData<T>,
}

impl<T> AggrFnAvg<T>
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

/// The state of the AVG aggregate function.
#[derive(Debug)]
pub struct AggrFnStateAvg<T>
where
    T: Summable,
    VectorValue: VectorValueExt<T>,
{
    sum: T,
    count: usize,
}

impl<T> AggrFnStateAvg<T>
where
    T: Summable,
    VectorValue: VectorValueExt<T>,
{
    pub fn new() -> Self {
        Self {
            sum: T::zero(),
            count: 0,
        }
    }
}

impl<T> super::ConcreteAggrFunctionState for AggrFnStateAvg<T>
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
                self.count += 1;
                Ok(())
            }
        }
    }

    #[inline]
    fn push_result(&self, _ctx: &mut EvalContext, target: &mut [VectorValue]) -> Result<()> {
        // Note: The result of `AVG()` is returned as `(count, sum)`.
        assert_eq!(target.len(), 2);
        target[0].push_int(Some(self.count as Int));
        if self.count == 0 {
            target[1].push(None);
        } else {
            target[1].push(Some(self.sum.clone()));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::super::AggrFunction;
    use super::*;

    use tidb_query_datatype::FieldTypeAccessor;
    use tipb_helper::ExprDefBuilder;

    use crate::aggr_fn::parser::AggrDefinitionParser;
    use crate::codec::batch::{LazyBatchColumn, LazyBatchColumnVec};

    #[test]
    fn test_update() {
        let mut ctx = EvalContext::default();
        let function = AggrFnAvg::<Real>::new();
        let mut state = function.create_state();

        let mut result = [
            VectorValue::with_capacity(0, EvalType::Int),
            VectorValue::with_capacity(0, EvalType::Real),
        ];
        state.push_result(&mut ctx, &mut result[..]).unwrap();
        assert_eq!(result[0].as_int_slice(), &[Some(0)]);
        assert_eq!(result[1].as_real_slice(), &[None]);

        state.update(&mut ctx, &Option::<Real>::None).unwrap();

        state.push_result(&mut ctx, &mut result[..]).unwrap();
        assert_eq!(result[0].as_int_slice(), &[Some(0), Some(0)]);
        assert_eq!(result[1].as_real_slice(), &[None, None]);

        state.update(&mut ctx, &Real::new(5.0).ok()).unwrap();
        state.update(&mut ctx, &Option::<Real>::None).unwrap();
        state.update(&mut ctx, &Real::new(10.0).ok()).unwrap();

        state.push_result(&mut ctx, &mut result[..]).unwrap();
        assert_eq!(result[0].as_int_slice(), &[Some(0), Some(0), Some(2)]);
        assert_eq!(
            result[1].as_real_slice(),
            &[None, None, Real::new(15.0).ok()]
        );

        state
            .update_vector(
                &mut ctx,
                &[Real::new(0.0).ok(), Real::new(-4.5).ok(), None],
                &[0, 1, 2],
            )
            .unwrap();

        state.push_result(&mut ctx, &mut result[..]).unwrap();
        assert_eq!(
            result[0].as_int_slice(),
            &[Some(0), Some(0), Some(2), Some(4)]
        );
        assert_eq!(
            result[1].as_real_slice(),
            &[None, None, Real::new(15.0).ok(), Real::new(10.5).ok()]
        );
    }

    /// AVG(IntColumn) should produce (Int, Decimal).
    #[test]
    fn test_integration() {
        let expr = ExprDefBuilder::aggr_func(ExprType::Avg, FieldTypeTp::NewDecimal)
            .push_child(ExprDefBuilder::column_ref(0, FieldTypeTp::LongLong))
            .build();
        AggrFnDefinitionParserAvg.check_supported(&expr).unwrap();

        let src_schema = [FieldTypeTp::LongLong.into()];
        let mut columns = LazyBatchColumnVec::from(vec![{
            let mut col = LazyBatchColumn::decoded_with_capacity_and_tp(0, EvalType::Int);
            col.mut_decoded().push_int(Some(100));
            col.mut_decoded().push_int(Some(1));
            col.mut_decoded().push_int(None);
            col.mut_decoded().push_int(Some(42));
            col.mut_decoded().push_int(None);
            col
        }]);

        let mut schema = vec![];
        let mut exp = vec![];

        let mut ctx = EvalContext::default();
        let aggr_fn = AggrFnDefinitionParserAvg
            .parse(expr, &mut ctx, &src_schema, &mut schema, &mut exp)
            .unwrap();
        assert_eq!(schema.len(), 2);
        assert_eq!(schema[0].as_accessor().tp(), FieldTypeTp::LongLong);
        assert_eq!(schema[1].as_accessor().tp(), FieldTypeTp::NewDecimal);

        assert_eq!(exp.len(), 1);

        let mut state = aggr_fn.create_state();
        let mut ctx = EvalContext::default();

        let exp_result = exp[0]
            .eval(&mut ctx, &src_schema, &mut columns, &[4, 1, 2, 3], 4)
            .unwrap();
        let exp_result = exp_result.vector_value().unwrap();
        let slice: &[Option<Decimal>] = exp_result.as_ref().as_ref();
        state
            .update_vector(&mut ctx, slice, exp_result.logical_rows())
            .unwrap();

        let mut aggr_result = [
            VectorValue::with_capacity(0, EvalType::Int),
            VectorValue::with_capacity(0, EvalType::Decimal),
        ];
        state.push_result(&mut ctx, &mut aggr_result).unwrap();

        assert_eq!(aggr_result[0].as_int_slice(), &[Some(2)]);
        assert_eq!(
            aggr_result[1].as_decimal_slice(),
            &[Some(Decimal::from(43u64))]
        );
    }

    #[test]
    fn test_illegal_request() {
        let expr = ExprDefBuilder::aggr_func(ExprType::Avg, FieldTypeTp::Double) // Expect NewDecimal but give Real
            .push_child(ExprDefBuilder::column_ref(0, FieldTypeTp::LongLong)) // FIXME: This type can be incorrect as well
            .build();
        AggrFnDefinitionParserAvg.check_supported(&expr).unwrap();

        let src_schema = [FieldTypeTp::LongLong.into()];
        let mut schema = vec![];
        let mut exp = vec![];
        let mut ctx = EvalContext::default();
        AggrFnDefinitionParserAvg
            .parse(expr, &mut ctx, &src_schema, &mut schema, &mut exp)
            .unwrap_err();
    }
}
