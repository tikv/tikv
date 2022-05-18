// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::{cmp::Ordering, convert::TryFrom};

use tidb_query_codegen::AggrFunction;
use tidb_query_common::Result;
use tidb_query_datatype::{
    codec::{collation::Collator, data_type::*},
    expr::EvalContext,
    match_template_collator, Collation, EvalType, FieldTypeAccessor, FieldTypeFlag,
};
use tidb_query_expr::RpnExpression;
use tipb::{Expr, ExprType, FieldType};

use super::*;

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
pub struct AggrFnDefinitionParserExtremum<E: Extremum>(std::marker::PhantomData<E>);

impl<E: Extremum> AggrFnDefinitionParserExtremum<E> {
    pub fn new() -> Self {
        Self(std::marker::PhantomData)
    }
}

impl<E: Extremum> super::AggrDefinitionParser for AggrFnDefinitionParserExtremum<E> {
    fn check_supported(&self, aggr_def: &Expr) -> Result<()> {
        assert_eq!(aggr_def.get_tp(), E::TP);
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
        assert_eq!(root_expr.get_tp(), E::TP);
        let eval_type =
            EvalType::try_from(exp.ret_field_type(src_schema).as_accessor().tp()).unwrap();
        let is_unsigned = exp
            .ret_field_type(src_schema)
            .as_accessor()
            .flag()
            .contains(FieldTypeFlag::UNSIGNED);

        let out_ft = root_expr.take_field_type();
        let out_et = box_try!(EvalType::try_from(out_ft.as_accessor().tp()));
        let out_coll = box_try!(out_ft.as_accessor().collation());

        if out_et != eval_type {
            return Err(other_err!(
                "Unexpected return field type {}",
                out_ft.as_accessor().tp()
            ));
        }

        // `MAX/MIN` outputs one column which has the same type with its child
        out_schema.push(out_ft);
        out_exp.push(exp);

        match_template::match_template! {
            T = [
                Real => &'static Real,
                Duration => &'static Duration,
                Decimal => &'static Decimal,
                DateTime => &'static DateTime,
                Json => JsonRef<'static>,
            ],
            match eval_type {
                EvalType::T => Ok(Box::new(AggFnExtremum::<T, E>::new())),
                EvalType::Int => match is_unsigned {
                    false => Ok(Box::new(AggFnExtremumForInt::<E, false>::new())),
                    true => Ok(Box::new(AggFnExtremumForInt::<E, true>::new())),
                },
                EvalType::Enum => Ok(Box::new(AggFnExtremumForEnum::<E>::new())),
                EvalType::Set => Ok(Box::new(AggFnExtremumForSet::<E>::new())),
                EvalType::Bytes => match_template_collator! {
                    C, match out_coll {
                        Collation::C => Ok(Box::new(AggFnExtremumForBytes::<C, E>::new()))
                    }
                }
            }
        }
    }
}

#[derive(Debug, AggrFunction)]
#[aggr_function(state = AggFnStateExtremum4Bytes::<C, E>::new())]
pub struct AggFnExtremumForBytes<C, E>
where
    C: Collator,
    E: Extremum,
    VectorValue: VectorValueExt<Bytes>,
{
    _phantom: std::marker::PhantomData<(C, E)>,
}

impl<C, E> AggFnExtremumForBytes<C, E>
where
    C: Collator,
    E: Extremum,
    VectorValue: VectorValueExt<Bytes>,
{
    fn new() -> Self {
        Self {
            _phantom: std::marker::PhantomData,
        }
    }
}

#[derive(Debug)]
pub struct AggFnStateExtremum4Bytes<C, E>
where
    VectorValue: VectorValueExt<Bytes>,
    C: Collator,
    E: Extremum,
{
    extremum: Option<Bytes>,
    _phantom: std::marker::PhantomData<(C, E)>,
}

impl<C, E> AggFnStateExtremum4Bytes<C, E>
where
    VectorValue: VectorValueExt<Bytes>,
    C: Collator,
    E: Extremum,
{
    pub fn new() -> Self {
        Self {
            extremum: None,
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<C, E> super::ConcreteAggrFunctionState for AggFnStateExtremum4Bytes<C, E>
where
    VectorValue: VectorValueExt<Bytes>,
    C: Collator,
    E: Extremum,
{
    type ParameterType = BytesRef<'static>;

    #[inline]
    unsafe fn update_concrete_unsafe(
        &mut self,
        _ctx: &mut EvalContext,
        value: Option<Self::ParameterType>,
    ) -> Result<()> {
        if value.is_none() {
            return Ok(());
        }

        if self.extremum.is_none() {
            self.extremum = value.map(|x| x.into_owned_value());
            return Ok(());
        }

        if C::sort_compare(self.extremum.as_ref().unwrap(), value.as_ref().unwrap())? == E::ORD {
            self.extremum = value.map(|x| x.into_owned_value());
        }
        Ok(())
    }

    #[inline]
    fn push_result(&self, _ctx: &mut EvalContext, target: &mut [VectorValue]) -> Result<()> {
        target[0].push(self.extremum.clone());
        Ok(())
    }
}

#[derive(Debug, AggrFunction)]
#[aggr_function(state = AggFnStateExtremumForEnum::<E>::new())]
pub struct AggFnExtremumForEnum<E>
where
    E: Extremum,
    VectorValue: VectorValueExt<Enum>,
{
    _phantom: std::marker::PhantomData<E>,
}

impl<E> AggFnExtremumForEnum<E>
where
    E: Extremum,
    VectorValue: VectorValueExt<Enum>,
{
    fn new() -> Self {
        Self {
            _phantom: std::marker::PhantomData,
        }
    }
}

#[derive(Debug)]
pub struct AggFnStateExtremumForEnum<E>
where
    E: Extremum,
    VectorValue: VectorValueExt<Enum>,
{
    extremum: Option<Enum>,
    _phantom: std::marker::PhantomData<E>,
}

impl<E> AggFnStateExtremumForEnum<E>
where
    E: Extremum,
    VectorValue: VectorValueExt<Enum>,
{
    pub fn new() -> Self {
        Self {
            extremum: None,
            _phantom: std::marker::PhantomData,
        }
    }

    /// # Notes
    ///
    /// For MAX(), MySQL currently compares ENUM and SET columns by their string value rather
    /// than by the string's relative position in the set. This differs from how ORDER BY
    /// compares them.
    ///
    /// ref: https://dev.mysql.com/doc/refman/5.7/en/aggregate-functions.html#function_max
    #[inline]
    fn update_concrete(
        &mut self,
        _ctx: &mut EvalContext,
        value: Option<EnumRef<'_>>,
    ) -> Result<()> {
        let extreme_ref = self
            .extremum
            .as_ref()
            .map(|x| EnumRef::from_owned_value(unsafe { std::mem::transmute(x) }));

        if value.is_some()
            && (self.extremum.is_none()
                || extreme_ref.unwrap().as_str()?.cmp(value.unwrap().as_str()?) == E::ORD)
        {
            self.extremum = value.map(|x| x.into_owned_value());
        }
        Ok(())
    }
}

impl<E> super::ConcreteAggrFunctionState for AggFnStateExtremumForEnum<E>
where
    E: Extremum,
    VectorValue: VectorValueExt<Enum>,
{
    type ParameterType = EnumRef<'static>;

    impl_concrete_state! { Self::ParameterType }

    #[inline]
    fn push_result(&self, _ctx: &mut EvalContext, target: &mut [VectorValue]) -> Result<()> {
        target[0].push(self.extremum.clone());
        Ok(())
    }
}

/// TODO: we need to reduce the code duplicate for Enum/Set/Bytes/Sized.
#[derive(Debug, AggrFunction)]
#[aggr_function(state = AggFnStateExtremumForSet::<E>::new())]
pub struct AggFnExtremumForSet<E>
where
    E: Extremum,
    VectorValue: VectorValueExt<Set>,
{
    _phantom: std::marker::PhantomData<E>,
}

impl<E> AggFnExtremumForSet<E>
where
    E: Extremum,
    VectorValue: VectorValueExt<Set>,
{
    fn new() -> Self {
        Self {
            _phantom: std::marker::PhantomData,
        }
    }
}

#[derive(Debug)]
pub struct AggFnStateExtremumForSet<E>
where
    E: Extremum,
    VectorValue: VectorValueExt<Set>,
{
    extremum: Option<Set>,
    _phantom: std::marker::PhantomData<E>,
}

impl<E> AggFnStateExtremumForSet<E>
where
    E: Extremum,
    VectorValue: VectorValueExt<Set>,
{
    pub fn new() -> Self {
        Self {
            extremum: None,
            _phantom: std::marker::PhantomData,
        }
    }

    /// # Notes
    ///
    /// For MAX(), MySQL currently compares ENUM and SET columns by their string value rather
    /// than by the string's relative position in the set. This differs from how ORDER BY
    /// compares them.
    ///
    /// ref: https://dev.mysql.com/doc/refman/5.7/en/aggregate-functions.html#function_max
    #[inline]
    fn update_concrete(&mut self, _ctx: &mut EvalContext, value: Option<SetRef<'_>>) -> Result<()> {
        let extreme_ref = self
            .extremum
            .as_ref()
            .map(|x| SetRef::from_owned_value(unsafe { std::mem::transmute(x) }));

        if value.is_some()
            && (self.extremum.is_none()
                || extreme_ref
                    .unwrap()
                    .to_string()
                    .cmp(&value.unwrap().to_string())
                    == E::ORD)
        {
            self.extremum = value.map(|x| x.into_owned_value());
        }
        Ok(())
    }
}

impl<E> super::ConcreteAggrFunctionState for AggFnStateExtremumForSet<E>
where
    E: Extremum,
    VectorValue: VectorValueExt<Set>,
{
    type ParameterType = SetRef<'static>;

    impl_concrete_state! { Self::ParameterType }

    #[inline]
    fn push_result(&self, _ctx: &mut EvalContext, target: &mut [VectorValue]) -> Result<()> {
        target[0].push(self.extremum.clone());
        Ok(())
    }
}

/// The MAX/MIN aggregate functions.
#[derive(Debug, AggrFunction)]
#[aggr_function(state = AggFnStateExtremum::<T, E>::new())]
pub struct AggFnExtremum<T, E>
where
    T: EvaluableRef<'static> + 'static + Ord,
    E: Extremum,
    VectorValue: VectorValueExt<T::EvaluableType>,
{
    _phantom: std::marker::PhantomData<(T, E)>,
}

impl<T, E> AggFnExtremum<T, E>
where
    T: EvaluableRef<'static> + 'static + Ord,
    E: Extremum,
    VectorValue: VectorValueExt<T::EvaluableType>,
{
    fn new() -> Self {
        Self {
            _phantom: std::marker::PhantomData,
        }
    }
}

/// The state of the MAX/MIN aggregate function.
#[derive(Debug)]
pub struct AggFnStateExtremum<T, E>
where
    T: EvaluableRef<'static> + 'static + Ord,
    E: Extremum,
    VectorValue: VectorValueExt<T::EvaluableType>,
{
    extremum_value: Option<T::EvaluableType>,
    _phantom: std::marker::PhantomData<E>,
}

impl<T, E> AggFnStateExtremum<T, E>
where
    T: EvaluableRef<'static> + 'static + Ord,
    E: Extremum,
    VectorValue: VectorValueExt<T::EvaluableType>,
{
    pub fn new() -> Self {
        Self {
            extremum_value: None,
            _phantom: std::marker::PhantomData,
        }
    }

    #[inline]
    fn update_concrete<'a, TT>(&mut self, _ctx: &mut EvalContext, value: Option<TT>) -> Result<()>
    where
        TT: EvaluableRef<'a, EvaluableType = T::EvaluableType> + Ord,
    {
        let extreme_ref = self
            .extremum_value
            .as_ref()
            .map(|x| TT::from_owned_value(unsafe { std::mem::transmute(x) }));
        if value.is_some() && (self.extremum_value.is_none() || extreme_ref.cmp(&value) == E::ORD) {
            self.extremum_value = value.map(|x| x.into_owned_value());
        }
        Ok(())
    }
}

impl<T, E> super::ConcreteAggrFunctionState for AggFnStateExtremum<T, E>
where
    T: EvaluableRef<'static> + 'static + Ord,
    E: Extremum,
    VectorValue: VectorValueExt<T::EvaluableType>,
{
    type ParameterType = T;

    impl_concrete_state! { Self::ParameterType }

    #[inline]
    fn push_result(&self, _ctx: &mut EvalContext, target: &mut [VectorValue]) -> Result<()> {
        target[0].push(self.extremum_value.clone());
        Ok(())
    }
}

#[derive(Debug, AggrFunction)]
#[aggr_function(state = AggFnStateExtremumForInt::<E, IS_UNSIGNED>::new())]
pub struct AggFnExtremumForInt<E, const IS_UNSIGNED: bool>
where
    E: Extremum,
    VectorValue: VectorValueExt<Int>,
{
    _phantom: std::marker::PhantomData<E>,
}

impl<E, const IS_UNSIGNED: bool> AggFnExtremumForInt<E, IS_UNSIGNED>
where
    E: Extremum,
    VectorValue: VectorValueExt<Int>,
{
    pub fn new() -> Self {
        Self {
            _phantom: std::marker::PhantomData,
        }
    }
}

#[derive(Debug)]
pub struct AggFnStateExtremumForInt<E, const IS_UNSIGNED: bool>
where
    E: Extremum,
    VectorValue: VectorValueExt<Int>,
{
    extremum: Option<Int>,
    _phantom: std::marker::PhantomData<E>,
}

impl<E, const IS_UNSIGNED: bool> AggFnStateExtremumForInt<E, IS_UNSIGNED>
where
    E: Extremum,
    VectorValue: VectorValueExt<Int>,
{
    pub fn new() -> Self {
        Self {
            extremum: None,
            _phantom: std::marker::PhantomData,
        }
    }

    #[inline]
    pub fn update_concrete(&mut self, _ctx: &mut EvalContext, value: Option<&Int>) -> Result<()> {
        if value.is_some() {
            if self.extremum.is_none() {
                self.extremum = value.copied();
                return Ok(());
            }
            if IS_UNSIGNED {
                let v1 = self.extremum.map(|x| x as u64);
                let v2 = value.map(|x| *x as u64);
                if v1.cmp(&v2) == E::ORD {
                    self.extremum = value.copied()
                }
            } else {
                let v1 = self.extremum.map(|x| x as i64);
                let v2 = value.map(|x| *x as i64);
                if v1.cmp(&v2) == E::ORD {
                    self.extremum = value.copied()
                }
            }
        }
        Ok(())
    }
}

impl<E, const IS_UNSIGNED: bool> super::ConcreteAggrFunctionState
    for AggFnStateExtremumForInt<E, IS_UNSIGNED>
where
    E: Extremum,
    VectorValue: VectorValueExt<Int>,
{
    type ParameterType = &'static Int;

    impl_concrete_state! { Self::ParameterType }

    #[inline]
    fn push_result(&self, _ctx: &mut EvalContext, target: &mut [VectorValue]) -> Result<()> {
        target[0].push(self.extremum);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tidb_query_datatype::{
        codec::batch::{LazyBatchColumn, LazyBatchColumnVec},
        EvalType, FieldTypeAccessor, FieldTypeTp,
    };
    use tikv_util::buffer_vec::BufferVec;
    use tipb_helper::ExprDefBuilder;

    use super::*;
    use crate::{parser::AggrDefinitionParser, AggrFunction};

    #[test]
    fn test_max() {
        let mut ctx = EvalContext::default();
        let function = AggFnExtremum::<&'static Int, Max>::new();
        let mut state = function.create_state();

        let mut result = [VectorValue::with_capacity(0, EvalType::Int)];

        state.push_result(&mut ctx, &mut result).unwrap();
        assert_eq!(result[0].to_int_vec(), &[None]);

        update!(state, &mut ctx, Option::<&Int>::None).unwrap();
        result[0].clear();
        state.push_result(&mut ctx, &mut result).unwrap();
        assert_eq!(result[0].to_int_vec(), &[None]);

        update!(state, &mut ctx, Some(&7i64)).unwrap();
        result[0].clear();
        state.push_result(&mut ctx, &mut result).unwrap();
        assert_eq!(result[0].to_int_vec(), &[Some(7)]);

        update!(state, &mut ctx, Some(&4i64)).unwrap();
        result[0].clear();
        state.push_result(&mut ctx, &mut result).unwrap();
        assert_eq!(result[0].to_int_vec(), &[Some(7)]);

        update_repeat!(state, &mut ctx, Some(&20), 10).unwrap();
        update_repeat!(state, &mut ctx, Option::<&Int>::None, 7).unwrap();

        result[0].clear();
        state.push_result(&mut ctx, &mut result).unwrap();
        assert_eq!(result[0].to_int_vec(), &[Some(20)]);

        // update vector
        update!(state, &mut ctx, Some(&7i64)).unwrap();
        update_vector!(
            state,
            &mut ctx,
            ChunkedVecSized::from_slice(&[Some(21i64), None, Some(22i64)]),
            &[0, 1, 2]
        )
        .unwrap();
        result[0].clear();
        state.push_result(&mut ctx, &mut result).unwrap();
        assert_eq!(result[0].to_int_vec(), &[Some(22)]);

        update!(state, &mut ctx, Some(&40i64)).unwrap();
        result[0].clear();
        state.push_result(&mut ctx, &mut result).unwrap();
        assert_eq!(result[0].to_int_vec(), &[Some(40)]);
    }

    #[test]
    fn test_max_enum() {
        let mut ctx = EvalContext::default();
        let function = AggFnExtremumForEnum::<Max>::new();
        let mut state = function.create_state();

        let mut result = [VectorValue::with_capacity(0, EvalType::Enum)];

        state.push_result(&mut ctx, &mut result).unwrap();
        assert_eq!(result[0].to_enum_vec(), &[None]);

        update!(state, &mut ctx, Some(EnumRef::new("bbb".as_bytes(), &1))).unwrap();
        result[0].clear();
        state.push_result(&mut ctx, &mut result).unwrap();
        assert_eq!(
            result[0].to_enum_vec(),
            vec![Some(Enum::new("bbb".as_bytes().to_vec(), 1))]
        );

        update!(state, &mut ctx, Some(EnumRef::new("bbb".as_bytes(), &1))).unwrap();
        update!(state, &mut ctx, Some(EnumRef::new("aaa".as_bytes(), &2))).unwrap();
        result[0].clear();
        state.push_result(&mut ctx, &mut result).unwrap();
        assert_eq!(
            result[0].to_enum_vec(),
            vec![Some(Enum::new("bbb".as_bytes().to_vec(), 1))]
        );
    }

    #[test]
    fn test_max_set() {
        let mut ctx = EvalContext::default();
        let function = AggFnExtremumForSet::<Max>::new();
        let mut state = function.create_state();

        let mut result = [VectorValue::with_capacity(0, EvalType::Set)];

        let mut buf = BufferVec::new();
        buf.push("B - 我好强啊");
        buf.push("A - 我太强啦");
        let buf = Arc::new(buf);

        state.push_result(&mut ctx, &mut result).unwrap();
        assert_eq!(result[0].to_set_vec(), &[None]);

        update!(state, &mut ctx, Some(SetRef::new(&buf, 0b01))).unwrap();
        result[0].clear();
        state.push_result(&mut ctx, &mut result).unwrap();
        assert_eq!(
            result[0].to_set_vec(),
            vec![Some(Set::new(buf.clone(), 0b01))]
        );

        update!(state, &mut ctx, Some(SetRef::new(&buf, 0b01))).unwrap();
        update!(state, &mut ctx, Some(SetRef::new(&buf, 0b10))).unwrap();
        update!(state, &mut ctx, Some(SetRef::new(&buf, 0b11))).unwrap();
        result[0].clear();
        state.push_result(&mut ctx, &mut result).unwrap();
        assert_eq!(result[0].to_set_vec(), vec![Some(Set::new(buf, 0b11))]);
    }

    #[test]
    fn test_min() {
        let mut ctx = EvalContext::default();
        let function = AggFnExtremum::<&'static Int, Min>::new();
        let mut state = function.create_state();

        let mut result = [VectorValue::with_capacity(0, EvalType::Int)];

        state.push_result(&mut ctx, &mut result).unwrap();
        assert_eq!(result[0].to_int_vec(), &[None]);

        update!(state, &mut ctx, Option::<&Int>::None).unwrap();
        result[0].clear();
        state.push_result(&mut ctx, &mut result).unwrap();
        assert_eq!(result[0].to_int_vec(), &[None]);

        update!(state, &mut ctx, Some(&100i64)).unwrap();
        result[0].clear();
        state.push_result(&mut ctx, &mut result).unwrap();
        assert_eq!(result[0].to_int_vec(), &[Some(100)]);

        update!(state, &mut ctx, Some(&90i64)).unwrap();
        result[0].clear();
        state.push_result(&mut ctx, &mut result).unwrap();
        assert_eq!(result[0].to_int_vec(), &[Some(90)]);

        update_repeat!(state, &mut ctx, Some(&80), 10).unwrap();
        update_repeat!(state, &mut ctx, Option::<&Int>::None, 10).unwrap();

        result[0].clear();
        state.push_result(&mut ctx, &mut result).unwrap();
        assert_eq!(result[0].to_int_vec(), &[Some(80)]);

        // update vector
        update!(state, &mut ctx, Some(&70i64)).unwrap();
        update_vector!(
            state,
            &mut ctx,
            ChunkedVecSized::from_slice(&[Some(69i64), None, Some(68i64)]),
            &[0, 1, 2]
        )
        .unwrap();
        result[0].clear();
        state.push_result(&mut ctx, &mut result).unwrap();
        assert_eq!(result[0].to_int_vec(), &[Some(68)]);

        update!(state, &mut ctx, Some(&2i64)).unwrap();
        result[0].clear();
        state.push_result(&mut ctx, &mut result).unwrap();
        assert_eq!(result[0].to_int_vec(), &[Some(2)]);

        update!(state, &mut ctx, Some(&-1i64)).unwrap();
        result[0].clear();
        state.push_result(&mut ctx, &mut result).unwrap();
        assert_eq!(result[0].to_int_vec(), &[Some(-1i64)]);
    }

    #[test]
    fn test_collation() {
        let mut ctx = EvalContext::default();
        let cases = vec![
            (Collation::Binary, true, vec!["B", "a"], "a"),
            (Collation::Utf8Mb4Bin, true, vec!["B", "a"], "a"),
            (Collation::Utf8Mb4GeneralCi, true, vec!["B", "a"], "B"),
            (Collation::Utf8Mb4UnicodeCi, true, vec!["ß", "sr"], "ß"),
            (Collation::Utf8Mb4BinNoPadding, true, vec!["B", "a"], "a"),
            (Collation::Binary, false, vec!["B", "a"], "B"),
            (Collation::Utf8Mb4Bin, false, vec!["B", "a"], "B"),
            (Collation::Utf8Mb4GeneralCi, false, vec!["B", "a"], "a"),
            (Collation::Utf8Mb4UnicodeCi, false, vec!["ß", "st"], "ß"),
            (Collation::Utf8Mb4BinNoPadding, false, vec!["B", "a"], "B"),
        ];
        for (coll, is_max, args, expected) in cases {
            let function = match_template_collator! {
                TT, match coll {
                    Collation::TT => {
                        if is_max {
                            Box::new(AggFnExtremumForBytes::<TT, Max>::new()) as Box<dyn AggrFunction>
                        } else {
                            Box::new(AggFnExtremumForBytes::<TT, Min>::new()) as Box<dyn AggrFunction>
                        }
                    }
                }
            };
            let mut state = function.create_state();
            let mut result = [VectorValue::with_capacity(0, EvalType::Bytes)];
            state.push_result(&mut ctx, &mut result).unwrap();
            assert_eq!(result[0].to_bytes_vec(), &[None]);

            for arg in args {
                update!(
                    state,
                    &mut ctx,
                    Some(&String::from(arg).into_bytes() as BytesRef<'_>)
                )
                .unwrap();
            }
            result[0].clear();
            state.push_result(&mut ctx, &mut result).unwrap();
            assert_eq!(
                result[0].to_bytes_vec(),
                [Some(String::from(expected).into_bytes())]
            );
        }
    }

    #[test]
    fn test_integration() {
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
        let expected_res = vec![Some(99), Some(-1i64)];
        let mut field_type = FieldType::default();
        let fta = field_type.as_mut_accessor();
        fta.set_tp(FieldTypeTp::LongLong);

        test_integration_util(
            field_type.tp(),
            field_type.flag(),
            &mut columns,
            &logical_rows,
            &expected_res,
        );
    }

    #[test]
    fn test_aggr_unsigned_signed_int() {
        // test unsigned bigint
        let mut columns = LazyBatchColumnVec::from(vec![{
            let mut col = LazyBatchColumn::decoded_with_capacity_and_tp(0, EvalType::Int);
            col.mut_decoded().push_int(None);
            col.mut_decoded().push_int(Some(-1));
            col.mut_decoded().push_int(Some(2420174916247255494));
            col.mut_decoded().push_int(Some(3899490809029152765));
            col
        }]);
        let logical_rows = vec![0, 1, 2, 3];
        let expected_res = vec![Some(-1), Some(2420174916247255494)];
        let mut field_type = FieldType::default();
        let fta = field_type.as_mut_accessor();
        fta.set_tp(FieldTypeTp::LongLong);
        fta.set_flag(FieldTypeFlag::UNSIGNED);

        test_integration_util(
            field_type.tp(),
            field_type.flag(),
            &mut columns,
            &logical_rows,
            &expected_res,
        );

        // test signed bigint
        let mut columns = LazyBatchColumnVec::from(vec![{
            let mut col = LazyBatchColumn::decoded_with_capacity_and_tp(0, EvalType::Int);
            col.mut_decoded().push_int(None);
            col.mut_decoded().push_int(Some(-1));
            col.mut_decoded().push_int(Some(2420174916247255494));
            col.mut_decoded().push_int(Some(3899490809029152765));
            col
        }]);
        let logical_rows = vec![0, 1, 2, 3];
        let expected_res = vec![Some(3899490809029152765), Some(-1)];
        let mut field_type = FieldType::default();
        let fta = field_type.as_mut_accessor();
        fta.set_tp(FieldTypeTp::LongLong);

        test_integration_util(
            field_type.tp(),
            field_type.flag(),
            &mut columns,
            &logical_rows,
            &expected_res,
        );
    }

    fn test_integration_util(
        field_type: FieldTypeTp,
        child_flag: FieldTypeFlag,
        columns: &mut LazyBatchColumnVec,
        logical_rows: &[usize],
        expected_res: &[Option<Int>],
    ) {
        let max_parser = AggrFnDefinitionParserExtremum::<Max>::new();
        let min_parser = AggrFnDefinitionParserExtremum::<Min>::new();

        let mut child_field_type: FieldType = field_type.into();
        child_field_type.as_mut_accessor().set_flag(child_flag);

        let max = ExprDefBuilder::aggr_func(ExprType::Max, field_type)
            .push_child(ExprDefBuilder::column_ref(0, child_field_type.clone()))
            .build();
        max_parser.check_supported(&max).unwrap();

        let min = ExprDefBuilder::aggr_func(ExprType::Min, field_type)
            .push_child(ExprDefBuilder::column_ref(0, child_field_type.clone()))
            .build();
        min_parser.check_supported(&min).unwrap();

        let src_schema = [child_field_type];

        let mut schema = vec![];
        let mut exp = vec![];

        let mut ctx = EvalContext::default();
        let max_fn = max_parser
            .parse(max, &mut ctx, &src_schema, &mut schema, &mut exp)
            .unwrap();
        assert_eq!(schema.len(), 1);
        assert_eq!(schema[0].as_accessor().tp(), field_type);
        assert_eq!(exp.len(), 1);

        let min_fn = min_parser
            .parse(min, &mut ctx, &src_schema, &mut schema, &mut exp)
            .unwrap();
        assert_eq!(schema.len(), 2);
        assert_eq!(schema[1].as_accessor().tp(), field_type);
        assert_eq!(exp.len(), 2);

        let mut ctx = EvalContext::default();
        let mut max_state = max_fn.create_state();
        let mut min_state = min_fn.create_state();

        let mut aggr_result = [VectorValue::with_capacity(0, EvalType::Int)];

        // max
        {
            let max_result = exp[0]
                .eval(
                    &mut ctx,
                    &src_schema,
                    columns,
                    logical_rows,
                    logical_rows.len(),
                )
                .unwrap();
            let max_result = max_result.vector_value().unwrap();
            let max_slice: ChunkedVecSized<Int> = max_result.as_ref().to_int_vec().into();
            update_vector!(max_state, &mut ctx, max_slice, max_result.logical_rows()).unwrap();
            max_state.push_result(&mut ctx, &mut aggr_result).unwrap();
        }

        // min
        {
            let min_result = exp[0]
                .eval(
                    &mut ctx,
                    &src_schema,
                    columns,
                    logical_rows,
                    logical_rows.len(),
                )
                .unwrap();
            let min_result = min_result.vector_value().unwrap();
            let min_slice: ChunkedVecSized<Int> = min_result.as_ref().to_int_vec().into();
            update_vector!(min_state, &mut ctx, min_slice, min_result.logical_rows()).unwrap();
            min_state.push_result(&mut ctx, &mut aggr_result).unwrap();
        }

        assert_eq!(aggr_result[0].to_int_vec(), &(*expected_res));
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
