// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

use tidb_query_codegen::AggrFunction;
use tidb_query_common::Result;
use tidb_query_datatype::{
    codec::{data_type::*, Error},
    expr::EvalContext,
    EvalType, FieldTypeAccessor, FieldTypeTp,
};
use tidb_query_expr::RpnExpression;
use tipb::{Expr, ExprType, FieldType};

use super::*;

/// The parser for SUM_INT aggregate function.
pub struct AggrFnDefinitionParserSumInt;

impl super::parser::AggrDefinitionParser for AggrFnDefinitionParserSumInt {
    fn check_supported(&self, aggr_def: &Expr) -> Result<()> {
        assert_eq!(aggr_def.get_tp(), ExprType::SumInt);
        super::util::check_aggr_exp_supported_one_child(aggr_def)
    }

    #[inline]
    fn parse_rpn(
        &self,
        mut root_expr: Expr,
        exp: RpnExpression,
        _ctx: &mut EvalContext,
        src_schema: &[FieldType],
        out_schema: &mut Vec<FieldType>,
        out_exp: &mut Vec<RpnExpression>,
    ) -> Result<Box<dyn AggrFunction>> {
        assert_eq!(root_expr.get_tp(), ExprType::SumInt);

        let out_ft = root_expr.take_field_type();
        let out_tp = out_ft.as_accessor().tp();
        let out_et = box_try!(EvalType::try_from(out_tp));
        if out_et != EvalType::Int || out_tp != FieldTypeTp::LongLong {
            return Err(other_err!(
                "Unexpected return field type {} for SUM_INT",
                out_tp
            ));
        }

        let arg_ft = exp.ret_field_type(src_schema);
        let arg_tp = arg_ft.as_accessor().tp();
        match arg_tp {
            FieldTypeTp::Tiny
            | FieldTypeTp::Short
            | FieldTypeTp::Int24
            | FieldTypeTp::Long
            | FieldTypeTp::LongLong => {}
            _ => {
                return Err(other_err!(
                    "SUM_INT only accepts integer arguments, but got {}",
                    arg_tp
                ));
            }
        }

        let out_is_unsigned = out_ft.as_accessor().is_unsigned();
        let arg_is_unsigned = arg_ft.as_accessor().is_unsigned();
        if out_is_unsigned != arg_is_unsigned {
            return Err(other_err!(
                "Unexpected unsigned flag for SUM_INT: return_is_unsigned={}, arg_is_unsigned={}",
                out_is_unsigned,
                arg_is_unsigned
            ));
        }

        // SUM_INT outputs one column.
        out_schema.push(out_ft);
        out_exp.push(exp);

        Ok(if out_is_unsigned {
            Box::new(AggrFnSumIntUnsigned)
        } else {
            Box::new(AggrFnSumIntSigned)
        })
    }
}

/// The SUM_INT aggregate function for signed integer arguments.
#[derive(Debug, AggrFunction)]
#[aggr_function(state = AggrFnStateSumIntSigned::new())]
pub struct AggrFnSumIntSigned;

#[derive(Debug)]
pub struct AggrFnStateSumIntSigned {
    sum: i64,
    has_value: bool,
}

impl AggrFnStateSumIntSigned {
    pub fn new() -> Self {
        Self {
            sum: 0,
            has_value: false,
        }
    }

    #[inline]
    fn update_concrete<'a, TT>(&mut self, _ctx: &mut EvalContext, value: Option<TT>) -> Result<()>
    where
        TT: EvaluableRef<'a, EvaluableType = Int>,
    {
        match value {
            None => Ok(()),
            Some(value) => {
                let value = value.into_owned_value();
                if !self.has_value {
                    self.sum = value;
                    self.has_value = true;
                    return Ok(());
                }
                self.sum = self.sum.checked_add(value).ok_or_else(|| {
                    Error::overflow("BIGINT", format!("({}, {})", self.sum, value))
                })?;
                Ok(())
            }
        }
    }
}

impl super::ConcreteAggrFunctionState for AggrFnStateSumIntSigned {
    type ParameterType = &'static Int;

    impl_concrete_state! { Self::ParameterType }

    #[inline]
    fn push_result(&self, _ctx: &mut EvalContext, target: &mut [VectorValue]) -> Result<()> {
        assert_eq!(target.len(), 1);
        if !self.has_value {
            target[0].push_int(None);
        } else {
            target[0].push_int(Some(self.sum));
        }
        Ok(())
    }
}

/// The SUM_INT aggregate function for unsigned integer arguments.
#[derive(Debug, AggrFunction)]
#[aggr_function(state = AggrFnStateSumIntUnsigned::new())]
pub struct AggrFnSumIntUnsigned;

#[derive(Debug)]
pub struct AggrFnStateSumIntUnsigned {
    sum: u64,
    has_value: bool,
}

impl AggrFnStateSumIntUnsigned {
    pub fn new() -> Self {
        Self {
            sum: 0,
            has_value: false,
        }
    }

    #[inline]
    fn update_concrete<'a, TT>(&mut self, _ctx: &mut EvalContext, value: Option<TT>) -> Result<()>
    where
        TT: EvaluableRef<'a, EvaluableType = Int>,
    {
        match value {
            None => Ok(()),
            Some(value) => {
                let value = value.into_owned_value() as u64;
                if !self.has_value {
                    self.sum = value;
                    self.has_value = true;
                    return Ok(());
                }
                self.sum = self.sum.checked_add(value).ok_or_else(|| {
                    Error::overflow("BIGINT UNSIGNED", format!("({}, {})", self.sum, value))
                })?;
                Ok(())
            }
        }
    }
}

impl super::ConcreteAggrFunctionState for AggrFnStateSumIntUnsigned {
    type ParameterType = &'static Int;

    impl_concrete_state! { Self::ParameterType }

    #[inline]
    fn push_result(&self, _ctx: &mut EvalContext, target: &mut [VectorValue]) -> Result<()> {
        assert_eq!(target.len(), 1);
        if !self.has_value {
            target[0].push_int(None);
        } else {
            // Note: Integer values are carried by `i64` in TiKV/TiDB DAG, and will be
            // interpreted by the output field type's UNSIGNED flag in upper
            // layer.
            target[0].push_int(Some(self.sum as Int));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use tidb_query_datatype::{
        builder::FieldTypeBuilder,
        codec::batch::{LazyBatchColumn, LazyBatchColumnVec},
        EvalType, FieldTypeFlag,
    };
    use tipb_helper::ExprDefBuilder;

    use super::*;
    use crate::parser::AggrDefinitionParser;

    #[test]
    fn test_sum_int_signed_integration() {
        let expr = ExprDefBuilder::aggr_func(ExprType::SumInt, FieldTypeTp::LongLong)
            .push_child(ExprDefBuilder::column_ref(0, FieldTypeTp::Long))
            .build();
        AggrFnDefinitionParserSumInt.check_supported(&expr).unwrap();

        let src_schema = [FieldTypeTp::Long.into()];
        let mut columns = LazyBatchColumnVec::from(vec![{
            let mut col = LazyBatchColumn::decoded_with_capacity_and_tp(0, EvalType::Int);
            col.mut_decoded().push_int(Some(1));
            col.mut_decoded().push_int(None);
            col.mut_decoded().push_int(Some(5));
            col.mut_decoded().push_int(Some(-2));
            col
        }]);
        let logical_rows = vec![0, 1, 2, 3];

        let mut schema = vec![];
        let mut exp = vec![];
        let mut ctx = EvalContext::default();

        let aggr_fn = AggrFnDefinitionParserSumInt
            .parse(expr, &mut ctx, &src_schema, &mut schema, &mut exp)
            .unwrap();
        assert_eq!(schema.len(), 1);
        assert_eq!(schema[0].as_accessor().tp(), FieldTypeTp::LongLong);
        assert!(!schema[0].as_accessor().is_unsigned());

        let mut state = aggr_fn.create_state();

        let exp_result = exp[0]
            .eval(&mut ctx, &src_schema, &mut columns, &logical_rows, 4)
            .unwrap();
        let exp_result = exp_result.vector_value().unwrap();
        let vec = exp_result.as_ref().to_int_vec();
        let chunked_vec: ChunkedVecSized<Int> = vec.into();
        update_vector!(state, &mut ctx, chunked_vec, exp_result.logical_rows()).unwrap();

        let mut aggr_result = [VectorValue::with_capacity(0, EvalType::Int)];
        state.push_result(&mut ctx, &mut aggr_result).unwrap();
        assert_eq!(aggr_result[0].to_int_vec(), &[Some(4)]);
    }

    #[test]
    fn test_sum_int_unsigned_integration() {
        let out_ft = FieldTypeBuilder::new()
            .tp(FieldTypeTp::LongLong)
            .flag(FieldTypeFlag::UNSIGNED)
            .build();
        let in_ft = FieldTypeBuilder::new()
            .tp(FieldTypeTp::LongLong)
            .flag(FieldTypeFlag::UNSIGNED)
            .build();

        let expr = ExprDefBuilder::aggr_func(ExprType::SumInt, out_ft)
            .push_child(ExprDefBuilder::column_ref(0, in_ft))
            .build();
        AggrFnDefinitionParserSumInt.check_supported(&expr).unwrap();

        let src_schema = [FieldTypeBuilder::new()
            .tp(FieldTypeTp::LongLong)
            .flag(FieldTypeFlag::UNSIGNED)
            .build()];

        let mut columns = LazyBatchColumnVec::from(vec![{
            let mut col = LazyBatchColumn::decoded_with_capacity_and_tp(0, EvalType::Int);
            col.mut_decoded().push_int(Some(1));
            col.mut_decoded().push_int(Some(2));
            col.mut_decoded().push_int(None);
            col.mut_decoded().push_int(Some(3));
            col
        }]);
        let logical_rows = vec![0, 1, 2, 3];

        let mut schema = vec![];
        let mut exp = vec![];
        let mut ctx = EvalContext::default();

        let aggr_fn = AggrFnDefinitionParserSumInt
            .parse(expr, &mut ctx, &src_schema, &mut schema, &mut exp)
            .unwrap();
        assert_eq!(schema.len(), 1);
        assert!(schema[0].as_accessor().is_unsigned());

        let mut state = aggr_fn.create_state();

        let exp_result = exp[0]
            .eval(&mut ctx, &src_schema, &mut columns, &logical_rows, 4)
            .unwrap();
        let exp_result = exp_result.vector_value().unwrap();
        let vec = exp_result.as_ref().to_int_vec();
        let chunked_vec: ChunkedVecSized<Int> = vec.into();
        update_vector!(state, &mut ctx, chunked_vec, exp_result.logical_rows()).unwrap();

        let mut aggr_result = [VectorValue::with_capacity(0, EvalType::Int)];
        state.push_result(&mut ctx, &mut aggr_result).unwrap();
        assert_eq!(aggr_result[0].to_int_vec(), &[Some(6)]);
    }

    #[test]
    fn test_sum_int_illegal_request() {
        let expr = ExprDefBuilder::aggr_func(ExprType::SumInt, FieldTypeTp::LongLong)
            .push_child(ExprDefBuilder::column_ref(0, FieldTypeTp::VarString))
            .build();
        AggrFnDefinitionParserSumInt.check_supported(&expr).unwrap();

        let src_schema = [FieldTypeTp::VarString.into()];
        let mut schema = vec![];
        let mut exp = vec![];
        let mut ctx = EvalContext::default();
        AggrFnDefinitionParserSumInt
            .parse(expr, &mut ctx, &src_schema, &mut schema, &mut exp)
            .unwrap_err();
    }
}
