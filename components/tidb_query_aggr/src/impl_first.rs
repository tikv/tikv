// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::marker::PhantomData;

use tidb_query_codegen::AggrFunction;
use tidb_query_common::Result;
use tidb_query_datatype::{codec::data_type::*, expr::EvalContext, EvalType};
use tidb_query_expr::RpnExpression;
use tipb::{Expr, ExprType, FieldType};

use super::*;

/// The parser for FIRST aggregate function.
pub struct AggrFnDefinitionParserFirst;

impl super::AggrDefinitionParser for AggrFnDefinitionParserFirst {
    fn check_supported(&self, aggr_def: &Expr) -> Result<()> {
        assert_eq!(aggr_def.get_tp(), ExprType::First);
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
        use std::convert::TryFrom;

        use tidb_query_datatype::FieldTypeAccessor;

        assert_eq!(root_expr.get_tp(), ExprType::First);

        let eval_type =
            EvalType::try_from(exp.ret_field_type(src_schema).as_accessor().tp()).unwrap();

        let out_ft = root_expr.take_field_type();

        let out_et = box_try!(EvalType::try_from(out_ft.as_accessor().tp()));

        if out_et != eval_type {
            return Err(other_err!(
                "Unexpected return field type {}",
                out_ft.as_accessor().tp()
            ));
        }

        // FIRST outputs one column with the same type as its child
        out_schema.push(out_ft);
        out_exp.push(exp);

        match_template::match_template! {
            TT = [
                Int => &'static Int,
                Real => &'static Real,
                Duration => &'static Duration,
                Decimal => &'static Decimal,
                DateTime => &'static DateTime,
                Json => JsonRef<'static>,
                Bytes => BytesRef<'static>,
                Enum => EnumRef<'static>,
                Set => SetRef<'static>,
            ],
            match eval_type {
                EvalType::TT => Ok(Box::new(AggrFnFirst::<TT>::new())),
            }
        }
    }
}

/// The FIRST aggregate function.
#[derive(Debug, AggrFunction)]
#[aggr_function(state = AggrFnStateFirst::<T>::new())]
pub struct AggrFnFirst<T>(PhantomData<T>)
where
    T: EvaluableRef<'static> + 'static,
    VectorValue: VectorValueExt<T::EvaluableType>;

impl<T> AggrFnFirst<T>
where
    T: EvaluableRef<'static> + 'static,
    VectorValue: VectorValueExt<T::EvaluableType>,
{
    fn new() -> Self {
        AggrFnFirst(PhantomData)
    }
}

/// The state of the FIRST aggregate function.
#[derive(Debug)]
pub enum AggrFnStateFirst<T>
where
    T: EvaluableRef<'static> + 'static,
    VectorValue: VectorValueExt<T::EvaluableType>,
{
    Empty,
    Valued(Option<T::EvaluableType>),
}

impl<T> AggrFnStateFirst<T>
where
    T: EvaluableRef<'static> + 'static,
    VectorValue: VectorValueExt<T::EvaluableType>,
{
    pub fn new() -> Self {
        AggrFnStateFirst::Empty
    }

    #[inline]
    fn update<'a, TT>(&mut self, _ctx: &mut EvalContext, value: Option<TT>) -> Result<()>
    where
        TT: EvaluableRef<'a, EvaluableType = T::EvaluableType>,
    {
        if let AggrFnStateFirst::Empty = self {
            // TODO: avoid this clone
            *self = AggrFnStateFirst::Valued(value.map(|x| x.into_owned_value()));
        }
        Ok(())
    }

    #[inline]
    fn update_repeat<'a, TT>(
        &mut self,
        ctx: &mut EvalContext,
        value: Option<TT>,
        repeat_times: usize,
    ) -> Result<()>
    where
        TT: EvaluableRef<'a, EvaluableType = T::EvaluableType>,
    {
        assert!(repeat_times > 0);
        self.update(ctx, value)
    }

    #[inline]
    fn update_vector<'a, TT, CC>(
        &mut self,
        ctx: &mut EvalContext,
        _phantom_data: Option<TT>,
        physical_values: CC,
        logical_rows: &[usize],
    ) -> Result<()>
    where
        TT: EvaluableRef<'a, EvaluableType = T::EvaluableType>,
        CC: ChunkRef<'a, TT>,
    {
        if let Some(physical_index) = logical_rows.first() {
            self.update(ctx, physical_values.get_option_ref(*physical_index))?;
        }
        Ok(())
    }
}

// Here we manually implement `AggrFunctionStateUpdatePartial` instead of implementing
// `ConcreteAggrFunctionState` so that `update_repeat` and `update_vector` can be faster.
impl<T> super::AggrFunctionStateUpdatePartial<T> for AggrFnStateFirst<T>
where
    T: EvaluableRef<'static> + 'static,
    VectorValue: VectorValueExt<T::EvaluableType>,
{
    // ChunkedType has been implemented in AggrFunctionStateUpdatePartial<T1> for AggrFnStateFirst<T2>
    impl_state_update_partial! { T }
}

// In order to make `AggrFnStateFirst` satisfy the `AggrFunctionState` trait, we default impl all
// `AggrFunctionStateUpdatePartial` of `Evaluable` for all `AggrFnStateFirst`.
impl_unmatched_function_state! { AggrFnStateFirst<T> }

impl<T> super::AggrFunctionState for AggrFnStateFirst<T>
where
    T: EvaluableRef<'static> + 'static,
    VectorValue: VectorValueExt<T::EvaluableType>,
{
    fn push_result(&self, _ctx: &mut EvalContext, target: &mut [VectorValue]) -> Result<()> {
        assert_eq!(target.len(), 1);
        let res = if let AggrFnStateFirst::Valued(v) = self {
            v.clone()
        } else {
            None
        };
        target[0].push(res);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tidb_query_datatype::FieldTypeTp;
    use tikv_util::buffer_vec::BufferVec;
    use tipb_helper::ExprDefBuilder;

    use super::{super::AggrFunction, *};
    use crate::AggrDefinitionParser;

    #[test]
    fn test_update() {
        let mut ctx = EvalContext::default();
        let function = AggrFnFirst::<&'static Int>::new();
        let mut state = function.create_state();

        let mut result = [VectorValue::with_capacity(0, EvalType::Int)];
        state.push_result(&mut ctx, &mut result[..]).unwrap();
        assert_eq!(result[0].to_int_vec(), &[None]);

        update!(state, &mut ctx, Some(&1)).unwrap();
        state.push_result(&mut ctx, &mut result[..]).unwrap();
        assert_eq!(result[0].to_int_vec(), &[None, Some(1)]);

        update!(state, &mut ctx, Some(&2)).unwrap();
        state.push_result(&mut ctx, &mut result[..]).unwrap();
        assert_eq!(result[0].to_int_vec(), &[None, Some(1), Some(1)]);
    }

    #[test]
    fn test_update_enum() {
        let mut ctx = EvalContext::default();
        let function = AggrFnFirst::<EnumRef<'static>>::new();
        let mut state = function.create_state();

        let mut result = [VectorValue::with_capacity(0, EvalType::Enum)];

        update!(state, &mut ctx, Some(EnumRef::new("bbb".as_bytes(), &1))).unwrap();
        state.push_result(&mut ctx, &mut result[..]).unwrap();
        assert_eq!(
            result[0].to_enum_vec(),
            vec![Some(Enum::new("bbb".as_bytes().to_vec(), 1))]
        );

        update!(state, &mut ctx, Some(EnumRef::new("aaa".as_bytes(), &2))).unwrap();
        state.push_result(&mut ctx, &mut result[..]).unwrap();
        assert_eq!(
            result[0].to_enum_vec(),
            vec![
                Some(Enum::new("bbb".as_bytes().to_vec(), 1)),
                Some(Enum::new("bbb".as_bytes().to_vec(), 1))
            ]
        );
    }

    #[test]
    fn test_update_set() {
        let mut ctx = EvalContext::default();
        let function = AggrFnFirst::<SetRef<'static>>::new();
        let mut state = function.create_state();

        let mut result = [VectorValue::with_capacity(0, EvalType::Set)];

        let mut buf = BufferVec::new();
        buf.push("我好强啊");
        buf.push("我太强啦");
        let buf = Arc::new(buf);

        update!(state, &mut ctx, Some(SetRef::new(&buf, 0b11))).unwrap();
        state.push_result(&mut ctx, &mut result[..]).unwrap();
        assert_eq!(
            result[0].to_set_vec(),
            vec![Some(Set::new(buf.clone(), 0b11))]
        );

        update!(state, &mut ctx, Some(SetRef::new(&buf, 0b10))).unwrap();
        state.push_result(&mut ctx, &mut result[..]).unwrap();
        assert_eq!(
            result[0].to_set_vec(),
            vec![Some(Set::new(buf.clone(), 0b11)), Some(Set::new(buf, 0b11))]
        );
    }

    #[test]
    fn test_update_repeat() {
        let mut ctx = EvalContext::default();
        let function = AggrFnFirst::<BytesRef<'static>>::new();
        let mut state = function.create_state();

        let mut result = [VectorValue::with_capacity(0, EvalType::Bytes)];

        update_repeat!(state, &mut ctx, Some(&[1u8] as BytesRef<'_>), 2).unwrap();
        state.push_result(&mut ctx, &mut result[..]).unwrap();
        assert_eq!(result[0].to_bytes_vec(), &[Some(vec![1])]);

        update_repeat!(state, &mut ctx, Some(&[2u8] as BytesRef<'_>), 3).unwrap();
        state.push_result(&mut ctx, &mut result[..]).unwrap();
        assert_eq!(result[0].to_bytes_vec(), &[Some(vec![1]), Some(vec![1])]);
    }

    #[test]
    fn test_update_vector() {
        let mut ctx = EvalContext::default();
        let function = AggrFnFirst::<&'static Int>::new();
        let mut state = function.create_state();
        let mut result = [VectorValue::with_capacity(0, EvalType::Int)];

        update_vector!(
            state,
            &mut ctx,
            ChunkedVecSized::from_slice(&[Some(0); 0]),
            &[]
        )
        .unwrap();
        state.push_result(&mut ctx, &mut result[..]).unwrap();
        assert_eq!(result[0].to_int_vec(), &[None]);

        result[0].clear();
        update_vector!(
            state,
            &mut ctx,
            ChunkedVecSized::from_slice(&[Some(1)]),
            &[]
        )
        .unwrap();
        state.push_result(&mut ctx, &mut result[..]).unwrap();
        assert_eq!(result[0].to_int_vec(), &[None]);

        result[0].clear();
        update_vector!(
            state,
            &mut ctx,
            ChunkedVecSized::from_slice(&[None, Some(2)]),
            &[0, 1]
        )
        .unwrap();
        state.push_result(&mut ctx, &mut result[..]).unwrap();
        assert_eq!(result[0].to_int_vec(), &[None]);

        result[0].clear();
        update_vector!(
            state,
            &mut ctx,
            ChunkedVecSized::from_slice(&[Some(1)]),
            &[0]
        )
        .unwrap();
        state.push_result(&mut ctx, &mut result[..]).unwrap();
        assert_eq!(result[0].to_int_vec(), &[None]);

        // Reset state
        let mut state = function.create_state();

        result[0].clear();
        update_vector!(
            state,
            &mut ctx,
            ChunkedVecSized::from_slice(&[None, Some(2)]),
            &[1, 0]
        )
        .unwrap();
        state.push_result(&mut ctx, &mut result[..]).unwrap();
        assert_eq!(result[0].to_int_vec(), &[Some(2)]);
    }

    #[test]
    fn test_illegal_request() {
        let expr = ExprDefBuilder::aggr_func(ExprType::First, FieldTypeTp::Double) // Expect LongLong but give Double
            .push_child(ExprDefBuilder::column_ref(0, FieldTypeTp::LongLong))
            .build();
        AggrFnDefinitionParserFirst.check_supported(&expr).unwrap();

        let src_schema = [FieldTypeTp::LongLong.into()];
        let mut schema = vec![];
        let mut exp = vec![];
        let mut ctx = EvalContext::default();
        AggrFnDefinitionParserFirst
            .parse(expr, &mut ctx, &src_schema, &mut schema, &mut exp)
            .unwrap_err();
    }
}
