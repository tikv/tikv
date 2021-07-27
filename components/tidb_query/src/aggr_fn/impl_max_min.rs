// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::cmp::Ordering;
use std::convert::TryFrom;

use tidb_query_codegen::AggrFunction;
use tidb_query_datatype::{Collation, EvalType, FieldTypeAccessor, FieldTypeFlag};
use tipb::{Expr, ExprType, FieldType};

use crate::codec::collation::*;
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
        let is_unsigned = child
            .get_field_type()
            .as_accessor()
            .flag()
            .contains(FieldTypeFlag::UNSIGNED);
        let out_coll = box_try!(out_ft.as_accessor().collation());

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

        if out_et == EvalType::Bytes {
            return match_template_collator! {
                C, match out_coll {
                    Collation::C => Ok(Box::new(AggFnExtremumForBytes::<C, T>::new()))
                }
            };
        }

        if eval_type == EvalType::Int {
            if is_unsigned {
                return Ok(Box::new(
                    AggFnExtremumForInt::<T, AggUnsignedIntCompator>::new(),
                ));
            } else {
                return Ok(Box::new(
                    AggFnExtremumForInt::<T, AggSignedIntCompator>::new(),
                ));
            }
        }

        match_template_evaluable! {
            TT, match eval_type {
                EvalType::TT => Ok(Box::new(AggFnExtremum::<TT, T>::new()))
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
    type ParameterType = Bytes;

    #[inline]
    fn update_concrete(
        &mut self,
        _ctx: &mut EvalContext,
        value: &Option<Self::ParameterType>,
    ) -> Result<()> {
        if value.is_none() {
            return Ok(());
        }

        if self.extremum.is_none() {
            self.extremum = value.clone();
            return Ok(());
        }

        if C::sort_compare(&self.extremum.as_ref().unwrap(), &value.as_ref().unwrap())? == E::ORD {
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

/// The MAX/MIN aggregate functions.
#[derive(Debug, AggrFunction)]
#[aggr_function(state = AggFnStateExtremum::<T, E>::new())]
pub struct AggFnExtremum<T, E>
where
    T: Evaluable + Ord,
    E: Extremum,
    VectorValue: VectorValueExt<T>,
{
    _phantom: std::marker::PhantomData<(T, E)>,
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
        }
    }
}

/// The state of the MAX/MIN aggregate function.
#[derive(Debug)]
pub struct AggFnStateExtremum<T, E>
where
    T: Evaluable + Ord,
    E: Extremum,
    VectorValue: VectorValueExt<T>,
{
    extremum_value: Option<T>,
    _phantom: std::marker::PhantomData<E>,
}

impl<T, E> AggFnStateExtremum<T, E>
where
    T: Evaluable + Ord,
    E: Extremum,
    VectorValue: VectorValueExt<T>,
{
    pub fn new() -> Self {
        Self {
            extremum_value: None,
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<T, E> super::ConcreteAggrFunctionState for AggFnStateExtremum<T, E>
where
    T: Evaluable + Ord,
    E: Extremum,
    VectorValue: VectorValueExt<T>,
{
    type ParameterType = T;

    #[inline]
    fn update_concrete(
        &mut self,
        _ctx: &mut EvalContext,
        value: &Option<Self::ParameterType>,
    ) -> Result<()> {
        if value.is_some()
            && (self.extremum_value.is_none() || self.extremum_value.cmp(value) == E::ORD)
        {
            self.extremum_value = value.clone();
        }
        Ok(())
    }

    #[inline]
    fn push_result(&self, _ctx: &mut EvalContext, target: &mut [VectorValue]) -> Result<()> {
        target[0].push(self.extremum_value.clone());
        Ok(())
    }
}

#[derive(Debug, AggrFunction)]
#[aggr_function(state = AggFnStateExtremumForInt::<E, COMPATOR>::new())]
pub struct AggFnExtremumForInt<E, COMPATOR>
where
    E: Extremum,
    VectorValue: VectorValueExt<Int>,
    COMPATOR: AggIntCompator,
{
    _phantom: std::marker::PhantomData<E>,
    _compator_marker: std::marker::PhantomData<COMPATOR>,
}

impl<E, COMPATOR> AggFnExtremumForInt<E, COMPATOR>
where
    E: Extremum,
    VectorValue: VectorValueExt<Int>,
    COMPATOR: AggIntCompator,
{
    pub fn new() -> Self {
        Self {
            _phantom: std::marker::PhantomData,
            _compator_marker: std::marker::PhantomData,
        }
    }
}

#[derive(Debug)]
pub struct AggFnStateExtremumForInt<E, COMPATOR>
where
    E: Extremum,
    VectorValue: VectorValueExt<Int>,
    COMPATOR: AggIntCompator,
{
    extremum: Option<Int>,
    _phantom: std::marker::PhantomData<E>,
    _compator_marker: std::marker::PhantomData<COMPATOR>,
}

impl<E, COMPATOR> AggFnStateExtremumForInt<E, COMPATOR>
where
    E: Extremum,
    VectorValue: VectorValueExt<Int>,
    COMPATOR: AggIntCompator,
{
    pub fn new() -> Self {
        Self {
            extremum: None,
            _phantom: std::marker::PhantomData,
            _compator_marker: std::marker::PhantomData,
        }
    }
}

impl<E, COMPATOR> super::ConcreteAggrFunctionState for AggFnStateExtremumForInt<E, COMPATOR>
where
    E: Extremum,
    VectorValue: VectorValueExt<Int>,
    COMPATOR: AggIntCompator,
{
    type ParameterType = Int;

    #[inline]
    fn update_concrete(&mut self, _ctx: &mut EvalContext, value: &Option<Int>) -> Result<()> {
        if value.is_some() {
            if self.extremum.is_none() {
                self.extremum = *value;
                return Ok(());
            }
            if COMPATOR::compare(&self.extremum, &value, E::ORD) {
                self.extremum = *value;
            }
        }
        Ok(())
    }

    #[inline]
    fn push_result(&self, _ctx: &mut EvalContext, target: &mut [VectorValue]) -> Result<()> {
        target[0].push(self.extremum);
        Ok(())
    }
}

pub trait AggIntCompator: std::fmt::Debug + Send + 'static {
    fn compare(extremum: &Option<Int>, new_val: &Option<Int>, ordering: Ordering) -> bool;
}

#[derive(Debug)]
struct AggSignedIntCompator {}

impl AggIntCompator for AggSignedIntCompator {
    fn compare(extremum: &Option<Int>, new_val: &Option<Int>, ordering: Ordering) -> bool {
        let v1 = extremum.map(|x| x as i64);
        let v2 = new_val.map(|x| x as i64);
        v1.cmp(&v2) == ordering
    }
}

#[derive(Debug)]
struct AggUnsignedIntCompator {}

impl AggIntCompator for AggUnsignedIntCompator {
    fn compare(extremum: &Option<Int>, new_val: &Option<Int>, ordering: Ordering) -> bool {
        let v1 = extremum.map(|x| x as u64);
        let v2 = new_val.map(|x| x as u64);
        v1.cmp(&v2) == ordering
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
    use tidb_query_datatype::{FieldTypeAccessor, FieldTypeFlag, FieldTypeTp};

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
            assert_eq!(result[0].as_bytes_slice(), &[None]);

            for arg in args {
                state
                    .update(&mut ctx, &Some(String::from(arg).into_bytes()))
                    .unwrap();
            }
            result[0].clear();
            state.push_result(&mut ctx, &mut result).unwrap();
            assert_eq!(
                result[0].as_bytes_slice(),
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
                    &logical_rows,
                    logical_rows.len(),
                )
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
                .eval(
                    &mut ctx,
                    &src_schema,
                    columns,
                    &logical_rows,
                    logical_rows.len(),
                )
                .unwrap();
            let min_result = min_result.vector_value().unwrap();
            let min_slice: &[Option<Int>] = min_result.as_ref().as_ref();
            min_state
                .update_vector(&mut ctx, min_slice, min_result.logical_rows())
                .unwrap();
            min_state.push_result(&mut ctx, &mut aggr_result).unwrap();
        }

        assert_eq!(aggr_result[0].as_int_slice(), &(*expected_res));
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
