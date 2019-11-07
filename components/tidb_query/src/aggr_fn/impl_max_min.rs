// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::cmp::Ordering;
use std::convert::TryFrom;

use tidb_query_codegen::AggrFunction;
use tidb_query_datatype::{EvalType, FieldTypeAccessor};
use tipb::{Expr, ExprType, FieldType};

use crate::codec::data_type::*;
use crate::expr::EvalContext;
use crate::rpn_expr::{RpnExpression, RpnExpressionBuilder};
use crate::Result;

/// A trait for MAX/MIN aggregation functions
pub trait Extremum: Clone + std::fmt::Debug + Send + Sync + 'static {
    const TP: ExprType;
    const ORD: Ordering;
}

#[derive(Debug, Clone, Copy)]
pub struct Max;

#[derive(Debug, Clone, Copy)]
pub struct Min;

impl Extremum for Max {
    const TP: ExprType = ExprType::Max;
    const ORD: Ordering = Ordering::Less;
}

impl Extremum for Min {
    const TP: ExprType = ExprType::Min;
    const ORD: Ordering = Ordering::Greater;
}

/// The parser for `MAX/MIN` aggregate functions.
pub struct AggrFnDefinitionParserExtremum<T: Extremum>(std::marker::PhantomData<T>);

impl<T: Extremum> AggrFnDefinitionParserExtremum<T> {
    pub fn new() -> Self {
        Self(std::marker::PhantomData)
    }
}

impl<T: Extremum> super::AggrDefinitionParser for AggrFnDefinitionParserExtremum<T> {
    fn check_supported(&self, aggr_def: &Expr) -> Result<()> {
        assert_eq!(aggr_def.get_tp(), T::TP);
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
        assert_eq!(aggr_def.get_tp(), T::TP);
        let child = aggr_def.take_children().into_iter().next().unwrap();
        let eval_type = EvalType::try_from(child.get_field_type().as_accessor().tp()).unwrap();

        let out_ft = aggr_def.take_field_type();
        let out_et = box_try!(EvalType::try_from(out_ft.as_accessor().tp()));

        if out_et != eval_type {
            return Err(other_err!(
                "Unexpected return field type {}",
                out_ft.as_accessor().tp()
            ));
        }

        // `MAX/MIN` outputs one column which has the same type with its child
        out_schema.push(out_ft);
        out_exp.push(RpnExpressionBuilder::build_from_expr_tree(
            child,
            ctx,
            src_schema.len(),
        )?);

        match_template_evaluable! {
            TT, match eval_type {
                EvalType::TT => Ok(Box::new(AggFnExtremum::<TT, T>::new()))
            }
        }
    }
}

/// The MAX/MIN aggregate functions.
#[derive(Debug, AggrFunction)]
#[aggr_function(state = AggFnStateExtremum::<T>::new(self.ord))]
pub struct AggFnExtremum<T, E>
where
    T: Evaluable + Ord,
    E: Extremum,
    VectorValue: VectorValueExt<T>,
{
    _phantom: std::marker::PhantomData<(T, E)>,
    ord: Ordering,
}

impl<T, E> AggFnExtremum<T, E>
where
    T: Evaluable + Ord,
    E: Extremum,
    VectorValue: VectorValueExt<T>,
{
    fn new() -> Self {
        Self {
            _phantom: std::marker::PhantomData,
            ord: E::ORD,
        }
    }
}

/// The state of the MAX/MIN aggregate function.
#[derive(Debug)]
pub struct AggFnStateExtremum<T>
where
    T: Evaluable + Ord,
    VectorValue: VectorValueExt<T>,
{
    ord: Ordering,
    extremum: Option<T>,
}

impl<T> AggFnStateExtremum<T>
where
    T: Evaluable + Ord,
    VectorValue: VectorValueExt<T>,
{
    pub fn new(ord: Ordering) -> Self {
        Self {
            ord,
            extremum: None,
        }
    }
}

impl<T> super::ConcreteAggrFunctionState for AggFnStateExtremum<T>
where
    T: Evaluable + Ord,
    VectorValue: VectorValueExt<T>,
{
    type ParameterType = T;

    #[inline]
    fn update_concrete(
        &mut self,
        _ctx: &mut EvalContext,
        value: &Option<Self::ParameterType>,
    ) -> Result<()> {
        if value.is_some() && (self.extremum.is_none() || self.extremum.cmp(value) == self.ord) {
            self.extremum = value.clone();
        }
        Ok(())
    }

    #[inline]
    fn push_result(&self, _ctx: &mut EvalContext, target: &mut [VectorValue]) -> Result<()> {
        target[0].push(self.extremum.clone());
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use tidb_query_datatype::EvalType;
    use tipb_helper::ExprDefBuilder;

    use super::*;
    use crate::aggr_fn::parser::AggrDefinitionParser;
    use crate::aggr_fn::AggrFunction;
    use crate::codec::batch::{LazyBatchColumn, LazyBatchColumnVec};
    use tidb_query_datatype::{FieldTypeAccessor, FieldTypeTp};

    #[test]
    fn test_max() {
        let mut ctx = EvalContext::default();
        let function = AggFnExtremum::<Int, Max>::new();
        let mut state = function.create_state();

        let mut result = [VectorValue::with_capacity(0, EvalType::Int)];

        state.push_result(&mut ctx, &mut result).unwrap();
        assert_eq!(result[0].as_int_slice(), &[None]);

        state.update(&mut ctx, &Option::<Int>::None).unwrap();
        result[0].clear();
        state.push_result(&mut ctx, &mut result).unwrap();
        assert_eq!(result[0].as_int_slice(), &[None]);

        state.update(&mut ctx, &Some(7i64)).unwrap();
        result[0].clear();
        state.push_result(&mut ctx, &mut result).unwrap();
        assert_eq!(result[0].as_int_slice(), &[Some(7)]);

        state.update(&mut ctx, &Some(4i64)).unwrap();
        result[0].clear();
        state.push_result(&mut ctx, &mut result).unwrap();
        assert_eq!(result[0].as_int_slice(), &[Some(7)]);

        state.update_repeat(&mut ctx, &Some(20), 10).unwrap();
        state
            .update_repeat(&mut ctx, &Option::<Int>::None, 7)
            .unwrap();

        result[0].clear();
        state.push_result(&mut ctx, &mut result).unwrap();
        assert_eq!(result[0].as_int_slice(), &[Some(20)]);

        // update vector
        state.update(&mut ctx, &Some(7i64)).unwrap();
        state
            .update_vector(&mut ctx, &[Some(21i64), None, Some(22i64)], &[0, 1, 2])
            .unwrap();
        result[0].clear();
        state.push_result(&mut ctx, &mut result).unwrap();
        assert_eq!(result[0].as_int_slice(), &[Some(22)]);

        state.update(&mut ctx, &Some(40i64)).unwrap();
        result[0].clear();
        state.push_result(&mut ctx, &mut result).unwrap();
        assert_eq!(result[0].as_int_slice(), &[Some(40)]);
    }

    #[test]
    fn test_min() {
        let mut ctx = EvalContext::default();
        let function = AggFnExtremum::<Int, Min>::new();
        let mut state = function.create_state();

        let mut result = [VectorValue::with_capacity(0, EvalType::Int)];

        state.push_result(&mut ctx, &mut result).unwrap();
        assert_eq!(result[0].as_int_slice(), &[None]);

        state.update(&mut ctx, &Option::<Int>::None).unwrap();
        result[0].clear();
        state.push_result(&mut ctx, &mut result).unwrap();
        assert_eq!(result[0].as_int_slice(), &[None]);

        state.update(&mut ctx, &Some(100i64)).unwrap();
        result[0].clear();
        state.push_result(&mut ctx, &mut result).unwrap();
        assert_eq!(result[0].as_int_slice(), &[Some(100)]);

        state.update(&mut ctx, &Some(90i64)).unwrap();
        result[0].clear();
        state.push_result(&mut ctx, &mut result).unwrap();
        assert_eq!(result[0].as_int_slice(), &[Some(90)]);

        state.update_repeat(&mut ctx, &Some(80), 10).unwrap();
        state
            .update_repeat(&mut ctx, &Option::<Int>::None, 10)
            .unwrap();

        result[0].clear();
        state.push_result(&mut ctx, &mut result).unwrap();
        assert_eq!(result[0].as_int_slice(), &[Some(80)]);

        // update vector
        state.update(&mut ctx, &Some(70i64)).unwrap();
        state
            .update_vector(&mut ctx, &[Some(69i64), None, Some(68i64)], &[0, 1, 2])
            .unwrap();
        result[0].clear();
        state.push_result(&mut ctx, &mut result).unwrap();
        assert_eq!(result[0].as_int_slice(), &[Some(68)]);

        state.update(&mut ctx, &Some(2i64)).unwrap();
        result[0].clear();
        state.push_result(&mut ctx, &mut result).unwrap();
        assert_eq!(result[0].as_int_slice(), &[Some(2)]);

        state.update(&mut ctx, &Some(-1i64)).unwrap();
        result[0].clear();
        state.push_result(&mut ctx, &mut result).unwrap();
        assert_eq!(result[0].as_int_slice(), &[Some(-1i64)]);
    }

    #[test]
    fn test_integration() {
        let max_parser = AggrFnDefinitionParserExtremum::<Max>::new();
        let min_parser = AggrFnDefinitionParserExtremum::<Min>::new();

        let max = ExprDefBuilder::aggr_func(ExprType::Max, FieldTypeTp::LongLong)
            .push_child(ExprDefBuilder::column_ref(0, FieldTypeTp::LongLong))
            .build();
        max_parser.check_supported(&max).unwrap();

        let min = ExprDefBuilder::aggr_func(ExprType::Min, FieldTypeTp::LongLong)
            .push_child(ExprDefBuilder::column_ref(0, FieldTypeTp::LongLong))
            .build();
        min_parser.check_supported(&min).unwrap();

        let src_schema = [FieldTypeTp::LongLong.into()];
        let mut columns = LazyBatchColumnVec::from(vec![{
            let mut col = LazyBatchColumn::decoded_with_capacity_and_tp(0, EvalType::Int);
            col.mut_decoded().push_int(Some(10000));
            col.mut_decoded().push_int(Some(1));
            col.mut_decoded().push_int(Some(23));
            col.mut_decoded().push_int(Some(42));
            col.mut_decoded().push_int(Some(-10000));
            col.mut_decoded().push_int(None);
            col.mut_decoded().push_int(Some(99));
            col.mut_decoded().push_int(Some(-1));
            col
        }]);
        let logical_rows = vec![3, 2, 6, 5, 1, 7];

        let mut schema = vec![];
        let mut exp = vec![];

        let mut ctx = EvalContext::default();
        let max_fn = max_parser
            .parse(max, &mut ctx, &src_schema, &mut schema, &mut exp)
            .unwrap();
        assert_eq!(schema.len(), 1);
        assert_eq!(schema[0].as_accessor().tp(), FieldTypeTp::LongLong);
        assert_eq!(exp.len(), 1);

        let min_fn = min_parser
            .parse(min, &mut ctx, &src_schema, &mut schema, &mut exp)
            .unwrap();
        assert_eq!(schema.len(), 2);
        assert_eq!(schema[1].as_accessor().tp(), FieldTypeTp::LongLong);
        assert_eq!(exp.len(), 2);

        let mut ctx = EvalContext::default();
        let mut max_state = max_fn.create_state();
        let mut min_state = min_fn.create_state();

        let mut aggr_result = [VectorValue::with_capacity(0, EvalType::Int)];

        // max
        {
            let max_result = exp[0]
                .eval(&mut ctx, &src_schema, &mut columns, &logical_rows, 6)
                .unwrap();
            let max_result = max_result.vector_value().unwrap();
            let max_slice: &[Option<Int>] = max_result.as_ref().as_ref();
            max_state
                .update_vector(&mut ctx, max_slice, max_result.logical_rows())
                .unwrap();
            max_state.push_result(&mut ctx, &mut aggr_result).unwrap();
        }

        // min
        {
            let min_result = exp[0]
                .eval(&mut ctx, &src_schema, &mut columns, &logical_rows, 6)
                .unwrap();
            let min_result = min_result.vector_value().unwrap();
            let min_slice: &[Option<Int>] = min_result.as_ref().as_ref();
            min_state
                .update_vector(&mut ctx, min_slice, min_result.logical_rows())
                .unwrap();
            min_state.push_result(&mut ctx, &mut aggr_result).unwrap();
        }

        assert_eq!(aggr_result[0].as_int_slice(), &[Some(99), Some(-1i64),]);
    }

    #[test]
    fn test_illegal_request() {
        let expr = ExprDefBuilder::aggr_func(ExprType::Max, FieldTypeTp::Double) // Expect LongLong but give Real
            .push_child(ExprDefBuilder::column_ref(0, FieldTypeTp::LongLong))
            .build();
        AggrFnDefinitionParserExtremum::<Max>::new()
            .check_supported(&expr)
            .unwrap();

        let src_schema = [FieldTypeTp::LongLong.into()];
        let mut schema = vec![];
        let mut exp = vec![];
        let mut ctx = EvalContext::default();
        AggrFnDefinitionParserExtremum::<Max>::new()
            .parse(expr, &mut ctx, &src_schema, &mut schema, &mut exp)
            .unwrap_err();
    }
}
