// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use cop_codegen::AggrFunction;
use cop_datatype::{EvalType, FieldTypeFlag, FieldTypeTp};
use tipb::expression::{Expr, ExprType, FieldType};

use super::summable::Summable;
use crate::coprocessor::codec::data_type::*;
use crate::coprocessor::codec::mysql::Tz;
use crate::coprocessor::dag::expr::EvalContext;
use crate::coprocessor::dag::rpn_expr::{RpnExpression, RpnExpressionBuilder};
use crate::coprocessor::{Error, Result};

/// The parser for AVG aggregate function.
pub struct AggrFnDefinitionParserAvg;

impl super::parser::Parser for AggrFnDefinitionParserAvg {
    fn check_supported(&self, aggr_def: &Expr) -> Result<()> {
        use cop_datatype::FieldTypeAccessor;
        use std::convert::TryFrom;

        assert_eq!(aggr_def.get_tp(), ExprType::Avg);
        if aggr_def.get_children().len() != 1 {
            return Err(box_err!(
                "Expect 1 parameter, but got {}",
                aggr_def.get_children().len()
            ));
        }

        // Check whether or not the children's field type is supported. Currently we only support
        // Double and Decimal and does not support other types (which need casting).
        let child = &aggr_def.get_children()[0];
        let eval_type = EvalType::try_from(child.get_field_type().tp())
            .map_err(|e| Error::Other(box_err!(e)))?;
        match eval_type {
            EvalType::Real | EvalType::Decimal => {}
            _ => return Err(box_err!("Cast from {:?} is not supported", eval_type)),
        }

        // Check whether parameter expression is supported.
        RpnExpressionBuilder::check_expr_tree_supported(child)?;

        Ok(())
    }

    fn parse(
        &self,
        mut aggr_def: Expr,
        time_zone: &Tz,
        max_columns: usize,
        out_schema: &mut Vec<FieldType>,
        out_exp: &mut Vec<RpnExpression>,
    ) -> Result<Box<dyn super::AggrFunction>> {
        use cop_datatype::FieldTypeAccessor;
        use std::convert::TryFrom;

        assert_eq!(aggr_def.get_tp(), ExprType::Avg);
        let child = aggr_def.take_children().into_iter().next().unwrap();
        let eval_type = EvalType::try_from(child.get_field_type().tp()).unwrap();

        // AVG outputs two columns.
        out_schema.push({
            let mut ft = FieldType::new();
            ft.as_mut_accessor()
                .set_tp(FieldTypeTp::LongLong)
                .set_flag(FieldTypeFlag::UNSIGNED);
            ft
        });
        out_schema.push(aggr_def.take_field_type());

        // Currently we don't support casting in `check_supported`, so we can directly use the
        // built expression.
        out_exp.push(RpnExpressionBuilder::build_from_expr_tree(
            child,
            time_zone,
            max_columns,
        )?);

        // Choose a type-aware AVG implementation based on eval type.
        match eval_type {
            EvalType::Real => Ok(Box::new(AggrFnAvg::<Real>::new())),
            EvalType::Decimal => Ok(Box::new(AggrFnAvg::<Decimal>::new())),
            _ => unreachable!(),
        }
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
            .update_vector(&mut ctx, &[Real::new(0.0).ok(), Real::new(-4.5).ok(), None])
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
}
