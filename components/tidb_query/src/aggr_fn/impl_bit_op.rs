// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::convert::TryFrom;

use tidb_query_codegen::AggrFunction;
use tidb_query_datatype::{EvalType, FieldTypeAccessor};
use tipb::{Expr, ExprType, FieldType};

use crate::codec::data_type::*;
use crate::expr::EvalContext;
use crate::rpn_expr::{RpnExpression, RpnExpressionBuilder};
use crate::Result;

/// A trait for all bit operations
pub trait BitOp: Clone + std::fmt::Debug + Send + Sync + 'static {
    /// Returns the bit operation type
    fn tp() -> ExprType;

    /// Returns the bit operation initial state
    fn init_state() -> u64;

    /// Executes the special bit operation
    fn op(lhs: &mut u64, rhs: u64);
}

macro_rules! bit_op {
    ($name:ident, $tp:path, $init:tt, $op:tt) => {
        #[derive(Debug, Clone, Copy)]
        pub struct $name;
        impl BitOp for $name {
            fn tp() -> ExprType {
                $tp
            }

            fn init_state() -> u64 {
                $init
            }

            fn op(lhs: &mut u64, rhs: u64) {
                *lhs $op rhs
            }
        }
    };
}

bit_op!(BitAnd, ExprType::AggBitAnd, 0xffff_ffff_ffff_ffff, &=);
bit_op!(BitOr, ExprType::AggBitOr, 0, |=);
bit_op!(BitXor, ExprType::AggBitXor, 0, ^=);

/// The parser for bit operation aggregate functions.
pub struct AggrFnDefinitionParserBitOp<T: BitOp>(std::marker::PhantomData<T>);

impl<T: BitOp> AggrFnDefinitionParserBitOp<T> {
    pub fn new() -> Self {
        AggrFnDefinitionParserBitOp(std::marker::PhantomData)
    }
}

impl<T: BitOp> super::AggrDefinitionParser for AggrFnDefinitionParserBitOp<T> {
    fn check_supported(&self, aggr_def: &Expr) -> Result<()> {
        assert_eq!(aggr_def.get_tp(), T::tp());

        super::util::check_aggr_exp_supported_one_child(aggr_def)?;

        // Check whether or not the children's field type is supported. Currently we only support
        // Int and does not support other types (which need casting).
        // TODO: remove this check after implementing `CAST as Int`
        let child = &aggr_def.get_children()[0];
        let eval_type = box_try!(EvalType::try_from(
            child.get_field_type().as_accessor().tp()
        ));
        match eval_type {
            EvalType::Int => {}
            _ => return Err(other_err!("Cast from {:?} is not supported", eval_type)),
        }
        Ok(())
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
        assert_eq!(aggr_def.get_tp(), T::tp());

        // bit operation outputs one column.
        out_schema.push(aggr_def.take_field_type());

        // Rewrite expression to insert CAST() if needed.
        let child = aggr_def.take_children().into_iter().next().unwrap();
        let mut exp = RpnExpressionBuilder::build_from_expr_tree(child, ctx, src_schema.len())?;
        super::util::rewrite_exp_for_bit_op(src_schema, &mut exp).unwrap();
        out_exp.push(exp);

        Ok(Box::new(AggrFnBitOp::<T>(std::marker::PhantomData)))
    }
}

/// The bit operation aggregate functions.
#[derive(Debug, AggrFunction)]
#[aggr_function(state = AggrFnStateBitOp::<T>::new())]
pub struct AggrFnBitOp<T: BitOp>(std::marker::PhantomData<T>);

/// The state of the BitAnd aggregate function.
#[derive(Debug)]
pub struct AggrFnStateBitOp<T: BitOp> {
    c: u64,
    _phantom: std::marker::PhantomData<T>,
}

impl<T: BitOp> AggrFnStateBitOp<T> {
    pub fn new() -> Self {
        Self {
            c: T::init_state(),
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<T: BitOp> super::ConcreteAggrFunctionState for AggrFnStateBitOp<T> {
    type ParameterType = Int;

    #[inline]
    fn update_concrete(
        &mut self,
        _ctx: &mut EvalContext,
        value: &Option<Self::ParameterType>,
    ) -> Result<()> {
        match value {
            None => Ok(()),
            Some(value) => {
                T::op(&mut self.c, *value as u64);
                Ok(())
            }
        }
    }

    #[inline]
    fn push_result(&self, _ctx: &mut EvalContext, target: &mut [VectorValue]) -> Result<()> {
        target[0].push_int(Some(self.c as Int));
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::super::AggrFunction;
    use super::*;

    use tidb_query_datatype::FieldTypeTp;
    use tipb_helper::ExprDefBuilder;

    use crate::aggr_fn::parser::AggrDefinitionParser;
    use crate::codec::batch::{LazyBatchColumn, LazyBatchColumnVec};

    #[test]
    fn test_bit_and() {
        let mut ctx = EvalContext::default();
        let function = AggrFnBitOp::<BitAnd>(std::marker::PhantomData);
        let mut state = function.create_state();

        let mut result = [VectorValue::with_capacity(0, EvalType::Int)];

        state.push_result(&mut ctx, &mut result).unwrap();
        assert_eq!(
            result[0].as_int_slice(),
            &[Some(0xffff_ffff_ffff_ffff_u64 as i64)]
        );

        state.update(&mut ctx, &Option::<Int>::None).unwrap();
        result[0].clear();
        state.push_result(&mut ctx, &mut result).unwrap();
        assert_eq!(
            result[0].as_int_slice(),
            &[Some(0xffff_ffff_ffff_ffff_u64 as i64)]
        );

        // 7 & 4 == 4
        state.update(&mut ctx, &Some(7i64)).unwrap();
        result[0].clear();
        state.push_result(&mut ctx, &mut result).unwrap();
        assert_eq!(result[0].as_int_slice(), &[Some(7)]);

        state.update(&mut ctx, &Some(4i64)).unwrap();
        result[0].clear();
        state.push_result(&mut ctx, &mut result).unwrap();
        assert_eq!(result[0].as_int_slice(), &[Some(4)]);

        state.update_repeat(&mut ctx, &Some(4), 10).unwrap();
        state
            .update_repeat(&mut ctx, &Option::<Int>::None, 7)
            .unwrap();

        result[0].clear();
        state.push_result(&mut ctx, &mut result).unwrap();
        assert_eq!(result[0].as_int_slice(), &[Some(4)]);

        // Reset the state
        let mut state = function.create_state();
        // 7 & 1 == 1
        state.update(&mut ctx, &Some(7i64)).unwrap();
        state
            .update_vector(&mut ctx, &[Some(1i64), None, Some(1i64)], &[0, 1, 2])
            .unwrap();
        result[0].clear();
        state.push_result(&mut ctx, &mut result).unwrap();
        assert_eq!(result[0].as_int_slice(), &[Some(1)]);

        // 7 & 1 & 2 == 0
        state.update(&mut ctx, &Some(2i64)).unwrap();
        result[0].clear();
        state.push_result(&mut ctx, &mut result).unwrap();
        assert_eq!(result[0].as_int_slice(), &[Some(0)]);
    }

    #[test]
    fn test_bit_or() {
        let mut ctx = EvalContext::default();
        let function = AggrFnBitOp::<BitOr>(std::marker::PhantomData);
        let mut state = function.create_state();

        let mut result = [VectorValue::with_capacity(0, EvalType::Int)];

        state.push_result(&mut ctx, &mut result).unwrap();
        assert_eq!(result[0].as_int_slice(), &[Some(0)]);

        state.update(&mut ctx, &Option::<Int>::None).unwrap();
        result[0].clear();
        state.push_result(&mut ctx, &mut result).unwrap();
        assert_eq!(result[0].as_int_slice(), &[Some(0)]);

        // 1 | 4 == 5
        state.update(&mut ctx, &Some(1i64)).unwrap();
        result[0].clear();
        state.push_result(&mut ctx, &mut result).unwrap();
        assert_eq!(result[0].as_int_slice(), &[Some(1)]);

        state.update(&mut ctx, &Some(4i64)).unwrap();
        result[0].clear();
        state.push_result(&mut ctx, &mut result).unwrap();
        assert_eq!(result[0].as_int_slice(), &[Some(5)]);

        state.update_repeat(&mut ctx, &Some(8), 10).unwrap();
        state
            .update_repeat(&mut ctx, &Option::<Int>::None, 7)
            .unwrap();

        result[0].clear();
        state.push_result(&mut ctx, &mut result).unwrap();
        assert_eq!(result[0].as_int_slice(), &[Some(13)]);

        // 13 | 2 == 15
        state.update(&mut ctx, &Some(2i64)).unwrap();
        state
            .update_vector(&mut ctx, &[Some(2i64), None, Some(1i64)], &[0, 1, 2])
            .unwrap();
        result[0].clear();
        state.push_result(&mut ctx, &mut result).unwrap();
        assert_eq!(result[0].as_int_slice(), &[Some(15)]);

        // 15 | 2 == 15
        state.update(&mut ctx, &Some(2i64)).unwrap();
        result[0].clear();
        state.push_result(&mut ctx, &mut result).unwrap();
        assert_eq!(result[0].as_int_slice(), &[Some(15)]);

        // 15 | 2 | -1 == 18446744073709551615
        state.update(&mut ctx, &Some(-1i64)).unwrap();
        result[0].clear();
        state.push_result(&mut ctx, &mut result).unwrap();
        assert_eq!(
            result[0].as_int_slice(),
            &[Some(18446744073709551615u64 as i64)]
        );
    }

    #[test]
    fn test_bit_xor() {
        let mut ctx = EvalContext::default();
        let function = AggrFnBitOp::<BitXor>(std::marker::PhantomData);
        let mut state = function.create_state();

        let mut result = [VectorValue::with_capacity(0, EvalType::Int)];

        state.push_result(&mut ctx, &mut result).unwrap();
        assert_eq!(result[0].as_int_slice(), &[Some(0)]);

        state.update(&mut ctx, &Option::<Int>::None).unwrap();
        result[0].clear();
        state.push_result(&mut ctx, &mut result).unwrap();
        assert_eq!(result[0].as_int_slice(), &[Some(0)]);

        // 1 ^ 5 == 4
        state.update(&mut ctx, &Some(1i64)).unwrap();
        result[0].clear();
        state.push_result(&mut ctx, &mut result).unwrap();
        assert_eq!(result[0].as_int_slice(), &[Some(1)]);

        state.update(&mut ctx, &Some(5i64)).unwrap();
        result[0].clear();
        state.push_result(&mut ctx, &mut result).unwrap();
        assert_eq!(result[0].as_int_slice(), &[Some(4)]);

        // 1 ^ 5 ^ 8 == 12
        state.update_repeat(&mut ctx, &Some(8), 9).unwrap();
        state
            .update_repeat(&mut ctx, &Option::<Int>::None, 7)
            .unwrap();

        result[0].clear();
        state.push_result(&mut ctx, &mut result).unwrap();
        assert_eq!(result[0].as_int_slice(), &[Some(12)]);

        // Will not change due to xor even times
        state.update_repeat(&mut ctx, &Some(9), 10).unwrap();
        result[0].clear();
        state.push_result(&mut ctx, &mut result).unwrap();
        assert_eq!(result[0].as_int_slice(), &[Some(12)]);

        // 1 ^ 5 ^ 8 ^ ^ 2 ^ 2 ^ 1 == 13
        state.update(&mut ctx, &Some(2i64)).unwrap();
        state
            .update_vector(&mut ctx, &[Some(2i64), None, Some(1i64)], &[0, 1, 2])
            .unwrap();
        result[0].clear();
        state.push_result(&mut ctx, &mut result).unwrap();
        assert_eq!(result[0].as_int_slice(), &[Some(13)]);

        // 13 ^ 2 == 15
        state.update(&mut ctx, &Some(2i64)).unwrap();
        result[0].clear();
        state.push_result(&mut ctx, &mut result).unwrap();
        assert_eq!(result[0].as_int_slice(), &[Some(15)]);

        // 15 ^ 2 ^ -1 == 18446744073709551602
        state.update(&mut ctx, &Some(2i64)).unwrap();
        state.update(&mut ctx, &Some(-1i64)).unwrap();
        result[0].clear();
        state.push_result(&mut ctx, &mut result).unwrap();
        assert_eq!(
            result[0].as_int_slice(),
            &[Some(18446744073709551602u64 as i64)]
        );
    }

    #[test]
    fn test_integration() {
        let bit_and_parser = AggrFnDefinitionParserBitOp::<BitAnd>::new();
        let bit_or_parser = AggrFnDefinitionParserBitOp::<BitOr>::new();
        let bit_xor_parser = AggrFnDefinitionParserBitOp::<BitXor>::new();

        let bit_and = ExprDefBuilder::aggr_func(ExprType::AggBitAnd, FieldTypeTp::LongLong)
            .push_child(ExprDefBuilder::column_ref(0, FieldTypeTp::LongLong))
            .build();
        bit_and_parser.check_supported(&bit_and).unwrap();

        let bit_or = ExprDefBuilder::aggr_func(ExprType::AggBitOr, FieldTypeTp::LongLong)
            .push_child(ExprDefBuilder::column_ref(0, FieldTypeTp::LongLong))
            .build();
        bit_or_parser.check_supported(&bit_or).unwrap();

        let bit_xor = ExprDefBuilder::aggr_func(ExprType::AggBitXor, FieldTypeTp::LongLong)
            .push_child(ExprDefBuilder::column_ref(0, FieldTypeTp::LongLong))
            .build();
        bit_xor_parser.check_supported(&bit_xor).unwrap();

        let src_schema = [FieldTypeTp::LongLong.into()];
        let mut columns = LazyBatchColumnVec::from(vec![{
            let mut col = LazyBatchColumn::decoded_with_capacity_and_tp(0, EvalType::Int);
            col.mut_decoded().push_int(Some(1000));
            col.mut_decoded().push_int(Some(1));
            col.mut_decoded().push_int(Some(23));
            col.mut_decoded().push_int(Some(42));
            col.mut_decoded().push_int(None);
            col.mut_decoded().push_int(Some(99));
            col.mut_decoded().push_int(Some(-1));
            col.mut_decoded().push_int(Some(1000));
            col
        }]);
        let logical_rows = vec![6, 3, 4, 5, 1, 2];

        let mut schema = vec![];
        let mut exp = vec![];

        let mut ctx = EvalContext::default();
        let bit_and_fn = bit_and_parser
            .parse(bit_and, &mut ctx, &src_schema, &mut schema, &mut exp)
            .unwrap();
        assert_eq!(schema.len(), 1);
        assert_eq!(schema[0].as_accessor().tp(), FieldTypeTp::LongLong);
        assert_eq!(exp.len(), 1);

        let bit_or_fn = bit_or_parser
            .parse(bit_or, &mut ctx, &src_schema, &mut schema, &mut exp)
            .unwrap();
        assert_eq!(schema.len(), 2);
        assert_eq!(schema[1].as_accessor().tp(), FieldTypeTp::LongLong);
        assert_eq!(exp.len(), 2);

        let bit_xor_fn = bit_xor_parser
            .parse(bit_xor, &mut ctx, &src_schema, &mut schema, &mut exp)
            .unwrap();
        assert_eq!(schema.len(), 3);
        assert_eq!(schema[2].as_accessor().tp(), FieldTypeTp::LongLong);
        assert_eq!(exp.len(), 3);

        let mut bit_and_state = bit_and_fn.create_state();
        let mut bit_or_state = bit_or_fn.create_state();
        let mut bit_xor_state = bit_xor_fn.create_state();

        let mut aggr_result = [VectorValue::with_capacity(0, EvalType::Int)];

        // bit and
        {
            let bit_and_result = exp[0]
                .eval(&mut ctx, &src_schema, &mut columns, &logical_rows, 6)
                .unwrap();
            let bit_and_result = bit_and_result.vector_value().unwrap();
            let bit_and_slice: &[Option<Int>] = bit_and_result.as_ref().as_ref();
            bit_and_state
                .update_vector(&mut ctx, bit_and_slice, bit_and_result.logical_rows())
                .unwrap();
            bit_and_state
                .push_result(&mut ctx, &mut aggr_result)
                .unwrap();
        }

        // bit or
        {
            let bit_or_result = exp[1]
                .eval(&mut ctx, &src_schema, &mut columns, &logical_rows, 6)
                .unwrap();
            let bit_or_result = bit_or_result.vector_value().unwrap();
            let bit_or_slice: &[Option<Int>] = bit_or_result.as_ref().as_ref();
            bit_or_state
                .update_vector(&mut ctx, bit_or_slice, bit_or_result.logical_rows())
                .unwrap();
            bit_or_state
                .push_result(&mut ctx, &mut aggr_result)
                .unwrap();
        }

        // bit xor
        {
            let bit_xor_result = exp[2]
                .eval(&mut ctx, &src_schema, &mut columns, &logical_rows, 6)
                .unwrap();
            let bit_xor_result = bit_xor_result.vector_value().unwrap();
            let bit_xor_slice: &[Option<Int>] = bit_xor_result.as_ref().as_ref();
            bit_xor_state
                .update_vector(&mut ctx, bit_xor_slice, bit_xor_result.logical_rows())
                .unwrap();
            bit_xor_state
                .push_result(&mut ctx, &mut aggr_result)
                .unwrap();
        }

        assert_eq!(
            aggr_result[0].as_int_slice(),
            &[
                Some(0),
                Some(18446744073709551615u64 as i64),
                Some(18446744073709551520u64 as i64)
            ]
        );
    }
}
