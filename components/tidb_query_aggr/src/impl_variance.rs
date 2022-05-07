// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use tidb_query_codegen::AggrFunction;
use tidb_query_common::Result;
use tidb_query_datatype::{
    builder::FieldTypeBuilder, codec::data_type::*, expr::EvalContext, EvalType, FieldTypeFlag,
    FieldTypeTp,
};
use tidb_query_expr::RpnExpression;
use tipb::{Expr, ExprType, FieldType};

use super::{summable::Summable, *};

/// A trait for VARIANCE aggregation functions
pub trait VarianceType: Clone + std::fmt::Debug + Send + Sync + 'static {
    /// Checks whether the given expression type refers to the type of variance.
    fn check_expr_type(tt: ExprType) -> bool;

    /// Computes the final variance of values.
    fn compute_final_variance<T: Summable>(variance: &T, count: usize) -> Result<T>;
}

#[derive(Debug, Clone, Copy)]
pub struct Sample;

#[derive(Debug, Clone, Copy)]
pub struct Population;

impl VarianceType for Sample {
    fn check_expr_type(tt: ExprType) -> bool {
        tt == ExprType::VarSamp
    }

    fn compute_final_variance<T: Summable>(variance: &T, count: usize) -> Result<T> {
        variance.div(&T::from_usize(count - 1)?)
    }
}

impl VarianceType for Population {
    fn check_expr_type(tt: ExprType) -> bool {
        tt == ExprType::Variance || tt == ExprType::VarPop
    }

    fn compute_final_variance<T: Summable>(variance: &T, count: usize) -> Result<T> {
        variance.div(&T::from_usize(count)?)
    }
}

/// The parser for VARIANCE aggregate function.
pub struct AggrFnDefinitionParserVariance<V: VarianceType>(std::marker::PhantomData<V>);

impl<V: VarianceType> AggrFnDefinitionParserVariance<V> {
    pub fn new() -> Self {
        Self(std::marker::PhantomData)
    }
}

impl<V: VarianceType> super::AggrDefinitionParser for AggrFnDefinitionParserVariance<V> {
    fn check_supported(&self, aggr_def: &Expr) -> Result<()> {
        assert!(V::check_expr_type(aggr_def.get_tp()));
        super::util::check_aggr_exp_supported_one_child(aggr_def)
    }

    #[inline]
    fn parse_rpn(
        &self,
        mut root_expr: Expr,
        mut exp: RpnExpression,
        _ctx: &mut EvalContext,
        src_schema: &[FieldType],
        out_schema: &mut Vec<FieldType>,
        out_exp: &mut Vec<RpnExpression>,
    ) -> Result<Box<dyn AggrFunction>> {
        use std::convert::TryFrom;

        use tidb_query_datatype::FieldTypeAccessor;

        assert!(V::check_expr_type(root_expr.get_tp()));

        let out_ft = root_expr.take_field_type();
        let out_et = box_try!(EvalType::try_from(out_ft.as_accessor().tp()));

        // Rewrite expression to insert CAST() if needed. The rewrite should always succeed.
        super::util::rewrite_exp_for_sum_avg(src_schema, &mut exp).unwrap();

        let rewritten_eval_type =
            EvalType::try_from(exp.ret_field_type(src_schema).as_accessor().tp()).unwrap();
        if out_et != rewritten_eval_type {
            return Err(other_err!(
                "Unexpected return field type {}",
                out_ft.as_accessor().tp()
            ));
        }

        // VARIANCE outputs three columns (count, sum, variance).
        out_schema.push(
            FieldTypeBuilder::new()
                .tp(FieldTypeTp::LongLong)
                .flag(FieldTypeFlag::UNSIGNED)
                .build(),
        );
        out_schema.push(out_ft.clone());
        out_schema.push(out_ft);
        out_exp.push(exp);

        // Choose a type-aware VARIANCE implementation based on the eval type after rewriting exp.
        Ok(match rewritten_eval_type {
            EvalType::Decimal => Box::new(AggrFnVariance::<Decimal, V>::new()),
            EvalType::Real => Box::new(AggrFnVariance::<Real, V>::new()),
            EvalType::Enum => Box::new(AggrFnVarianceForEnum::<V>::new()),
            EvalType::Set => Box::new(AggrFnVarianceForSet::<V>::new()),
            // If we meet unexpected types after rewriting, it is an implementation fault.
            _ => unreachable!(),
        })
    }
}

/// The VARIANCE aggregate function.
///
/// Note that there are `VARIANCE(Decimal) -> Decimal` and `VARIANCE(Double) -> Double`.
#[derive(Debug, AggrFunction)]
#[aggr_function(state = AggrFnStateVariance::<T, V>::new())]
pub struct AggrFnVariance<T, V>
where
    T: Summable,
    V: VarianceType,
    VectorValue: VectorValueExt<T>,
{
    _phantom: std::marker::PhantomData<(T, V)>,
}

impl<T, V> AggrFnVariance<T, V>
where
    T: Summable,
    V: VarianceType,
    VectorValue: VectorValueExt<T>,
{
    pub fn new() -> Self {
        Self {
            _phantom: std::marker::PhantomData,
        }
    }
}

/// The state of the VARIANCE aggregate function.
#[derive(Debug)]
pub struct AggrFnStateVariance<T, V>
where
    T: Summable,
    V: VarianceType,
    VectorValue: VectorValueExt<T>,
{
    count: usize,
    sum: T,
    variance: T,
    _phantom: std::marker::PhantomData<V>,
}

impl<T, V> AggrFnStateVariance<T, V>
where
    T: Summable,
    V: VarianceType,
    VectorValue: VectorValueExt<T>,
{
    pub fn new() -> Self {
        Self {
            count: 0,
            sum: T::zero(),
            variance: T::zero(),
            _phantom: std::marker::PhantomData,
        }
    }

    #[inline]
    fn update_concrete<'a, TT>(&mut self, ctx: &mut EvalContext, value: Option<TT>) -> Result<()>
    where
        TT: EvaluableRef<'a, EvaluableType = T>,
    {
        match value {
            None => Ok(()),
            Some(value) => {
                let value = value.into_owned_value();

                self.count += 1;
                self.sum.add_assign(ctx, &value)?;
                if self.count > 1 {
                    // t := (count * input) - sum
                    let t = value.mul(&T::from_usize(self.count)?)?.sub(&self.sum)?;
                    // variance += (t * t) / (count * (count - 1))
                    self.variance.add_assign(
                        ctx,
                        &t.mul(&t)?
                            .div(&T::from_usize(self.count * (self.count - 1))?)?,
                    )?;
                }
                Ok(())
            }
        }
    }
}

impl<T, V> super::ConcreteAggrFunctionState for AggrFnStateVariance<T, V>
where
    T: Summable,
    V: VarianceType,
    VectorValue: VectorValueExt<T>,
{
    type ParameterType = &'static T;

    impl_concrete_state! { Self::ParameterType }

    #[inline]
    fn push_result(&self, _ctx: &mut EvalContext, target: &mut [VectorValue]) -> Result<()> {
        // Note: The result of `VARIANCE()` is returned as `(count, sum, variance)`.
        assert_eq!(target.len(), 3);
        target[0].push_int(Some(self.count as Int));
        if self.count > 0 {
            target[1].push(Some(self.sum.clone()));
            target[2].push(Some(V::compute_final_variance(&self.variance, self.count)?));
        } else {
            target[1].push(None as Option<T>);
            target[2].push(None as Option<T>);
        }
        Ok(())
    }
}

#[derive(Debug, AggrFunction)]
#[aggr_function(state = AggrFnStateVarianceForEnum::<V>::new())]
pub struct AggrFnVarianceForEnum<V>
where
    V: VarianceType,
    VectorValue: VectorValueExt<Decimal>,
{
    _phantom: std::marker::PhantomData<V>,
}

impl<V> AggrFnVarianceForEnum<V>
where
    V: VarianceType,
    VectorValue: VectorValueExt<Decimal>,
{
    pub fn new() -> Self {
        Self {
            _phantom: std::marker::PhantomData,
        }
    }
}

/// The state of the VARIANCE aggregate function.
#[derive(Debug)]
pub struct AggrFnStateVarianceForEnum<V>
where
    V: VarianceType,
    VectorValue: VectorValueExt<Decimal>,
{
    count: usize,
    sum: Decimal,
    variance: Decimal,
    _phantom: std::marker::PhantomData<V>,
}

impl<V> AggrFnStateVarianceForEnum<V>
where
    V: VarianceType,
    VectorValue: VectorValueExt<Decimal>,
{
    pub fn new() -> Self {
        Self {
            count: 0,
            sum: Decimal::zero(),
            variance: Decimal::zero(),
            _phantom: std::marker::PhantomData,
        }
    }

    /// # Notes
    ///
    /// Functions such as SUM() or AVG() or VARIANCE() that expect a numeric argument cast the
    /// argument to a number if necessary. For ENUM values, the index number is used in the
    /// calculation.
    ///
    /// ref: https://dev.mysql.com/doc/refman/8.0/en/enum.html
    #[inline]
    fn update_concrete(&mut self, ctx: &mut EvalContext, value: Option<EnumRef<'_>>) -> Result<()> {
        match value {
            None => Ok(()),
            Some(value) => {
                let value = Decimal::from(value.value());

                self.count += 1;
                self.sum.add_assign(ctx, &value)?;
                if self.count > 1 {
                    // t := (count * input) - sum
                    let t = Summable::sub(
                        &Summable::mul(&value, &Summable::from_usize(self.count)?)?,
                        &self.sum,
                    )?;
                    // variance += (t * t) / (count * (count - 1))
                    self.variance.add_assign(
                        ctx,
                        &Summable::div(
                            &Summable::mul(&t, &t)?,
                            &Summable::from_usize(self.count * (self.count - 1))?,
                        )?,
                    )?;
                }
                Ok(())
            }
        }
    }
}

impl<V> super::ConcreteAggrFunctionState for AggrFnStateVarianceForEnum<V>
where
    V: VarianceType,
    VectorValue: VectorValueExt<Decimal>,
{
    type ParameterType = EnumRef<'static>;

    impl_concrete_state! { Self::ParameterType }

    #[inline]
    fn push_result(&self, _ctx: &mut EvalContext, target: &mut [VectorValue]) -> Result<()> {
        // Note: The result of `VARIANCE()` is returned as `(count, sum, variance)`.
        assert_eq!(target.len(), 3);
        target[0].push_int(Some(self.count as Int));
        if self.count > 0 {
            target[1].push(Some(self.sum));
            target[2].push(Some(V::compute_final_variance(&self.variance, self.count)?));
        } else {
            target[1].push(None as Option<Decimal>);
            target[2].push(None as Option<Decimal>);
        }
        Ok(())
    }
}

#[derive(Debug, AggrFunction)]
#[aggr_function(state = AggrFnStateVarianceForSet::<V>::new())]
pub struct AggrFnVarianceForSet<V>
where
    V: VarianceType,
    VectorValue: VectorValueExt<Decimal>,
{
    _phantom: std::marker::PhantomData<V>,
}

impl<V> AggrFnVarianceForSet<V>
where
    V: VarianceType,
    VectorValue: VectorValueExt<Decimal>,
{
    pub fn new() -> Self {
        Self {
            _phantom: std::marker::PhantomData,
        }
    }
}

/// The state of the VARIANCE aggregate function.
#[derive(Debug)]
pub struct AggrFnStateVarianceForSet<V>
where
    V: VarianceType,
    VectorValue: VectorValueExt<Decimal>,
{
    count: usize,
    sum: Decimal,
    variance: Decimal,
    _phantom: std::marker::PhantomData<V>,
}

impl<V> AggrFnStateVarianceForSet<V>
where
    V: VarianceType,
    VectorValue: VectorValueExt<Decimal>,
{
    pub fn new() -> Self {
        Self {
            count: 0,
            sum: Decimal::zero(),
            variance: Decimal::zero(),
            _phantom: std::marker::PhantomData,
        }
    }

    /// # Notes
    ///
    /// Functions such as SUM() or AVG() or VARIANCE() that expect a numeric argument cast the
    /// argument to a number if necessary. For ENUM values, the index number is used in the
    /// calculation.
    ///
    /// ref: https://dev.mysql.com/doc/refman/8.0/en/enum.html
    #[inline]
    fn update_concrete(&mut self, ctx: &mut EvalContext, value: Option<SetRef<'_>>) -> Result<()> {
        match value {
            None => Ok(()),
            Some(value) => {
                let value = Decimal::from(value.value());

                self.count += 1;
                self.sum.add_assign(ctx, &value)?;
                if self.count > 1 {
                    // t := (count * input) - sum
                    let t = Summable::sub(
                        &Summable::mul(&value, &Summable::from_usize(self.count)?)?,
                        &self.sum,
                    )?;
                    // variance += (t * t) / (count * (count - 1))
                    self.variance.add_assign(
                        ctx,
                        &Summable::div(
                            &Summable::mul(&t, &t)?,
                            &Summable::from_usize(self.count * (self.count - 1))?,
                        )?,
                    )?;
                }
                Ok(())
            }
        }
    }
}

impl<V> super::ConcreteAggrFunctionState for AggrFnStateVarianceForSet<V>
where
    V: VarianceType,
    VectorValue: VectorValueExt<Decimal>,
{
    type ParameterType = SetRef<'static>;

    impl_concrete_state! { Self::ParameterType }

    #[inline]
    fn push_result(&self, _ctx: &mut EvalContext, target: &mut [VectorValue]) -> Result<()> {
        // Note: The result of `VARIANCE()` is returned as `(count, sum, variance)`.
        assert_eq!(target.len(), 3);
        target[0].push_int(Some(self.count as Int));
        if self.count > 0 {
            target[1].push(Some(self.sum));
            target[2].push(Some(V::compute_final_variance(&self.variance, self.count)?));
        } else {
            target[1].push(None as Option<Decimal>);
            target[2].push(None as Option<Decimal>);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tidb_query_datatype::{
        codec::batch::{LazyBatchColumn, LazyBatchColumnVec},
        FieldTypeAccessor, FieldTypeTp,
    };
    use tikv_util::buffer_vec::BufferVec;
    use tipb_helper::ExprDefBuilder;

    use super::*;
    use crate::parser::AggrDefinitionParser;

    #[test]
    fn test_variance_enum() {
        let mut ctx = EvalContext::default();
        let function = AggrFnVarianceForEnum::<Population>::new();
        let mut state = function.create_state();

        let mut result = [
            VectorValue::with_capacity(0, EvalType::Int),
            VectorValue::with_capacity(0, EvalType::Decimal),
            VectorValue::with_capacity(0, EvalType::Decimal),
        ];

        state.push_result(&mut ctx, &mut result[..]).unwrap();
        assert_eq!(result[0].to_int_vec(), &[Some(0)]);
        assert_eq!(result[1].to_decimal_vec(), &[None]);
        assert_eq!(result[2].to_decimal_vec(), &[None]);

        update!(state, &mut ctx, Some(EnumRef::new("bbb".as_bytes(), &1))).unwrap();
        update!(state, &mut ctx, Some(EnumRef::new("aaa".as_bytes(), &2))).unwrap();
        result[0].clear();
        result[1].clear();
        result[2].clear();
        state.push_result(&mut ctx, &mut result[..]).unwrap();
        assert_eq!(result[0].to_int_vec(), &[Some(2)]);
        assert_eq!(result[1].to_decimal_vec(), &[Decimal::from_f64(3.0).ok()]);
        assert_eq!(result[2].to_decimal_vec(), &[Decimal::from_f64(0.25).ok()]);

        update!(state, &mut ctx, Option::<EnumRef<'_>>::None).unwrap();
        result[0].clear();
        result[1].clear();
        result[2].clear();
        state.push_result(&mut ctx, &mut result[..]).unwrap();
        assert_eq!(result[0].to_int_vec(), &[Some(2)]);
        assert_eq!(result[1].to_decimal_vec(), &[Decimal::from_f64(3.0).ok()]);
        assert_eq!(result[2].to_decimal_vec(), &[Decimal::from_f64(0.25).ok()]);
    }

    #[test]
    fn test_variance_set() {
        let mut ctx = EvalContext::default();
        let function = AggrFnVarianceForSet::<Population>::new();
        let mut state = function.create_state();

        let mut result = [
            VectorValue::with_capacity(0, EvalType::Int),
            VectorValue::with_capacity(0, EvalType::Decimal),
            VectorValue::with_capacity(0, EvalType::Decimal),
        ];

        let mut buf = BufferVec::new();
        buf.push("我好强啊");
        buf.push("我太强啦");
        let buf = Arc::new(buf);

        state.push_result(&mut ctx, &mut result[..]).unwrap();
        assert_eq!(result[0].to_int_vec(), &[Some(0)]);
        assert_eq!(result[1].to_decimal_vec(), &[None]);
        assert_eq!(result[2].to_decimal_vec(), &[None]);

        update!(state, &mut ctx, Some(SetRef::new(&buf, 0b01))).unwrap();
        update!(state, &mut ctx, Some(SetRef::new(&buf, 0b10))).unwrap();
        result[0].clear();
        result[1].clear();
        result[2].clear();
        state.push_result(&mut ctx, &mut result[..]).unwrap();
        assert_eq!(result[0].to_int_vec(), &[Some(2)]);
        assert_eq!(result[1].to_decimal_vec(), &[Decimal::from_f64(3.0).ok()]);
        assert_eq!(result[2].to_decimal_vec(), &[Decimal::from_f64(0.25).ok()]);

        update!(state, &mut ctx, Option::<SetRef<'_>>::None).unwrap();
        result[0].clear();
        result[1].clear();
        result[2].clear();
        state.push_result(&mut ctx, &mut result[..]).unwrap();
        assert_eq!(result[0].to_int_vec(), &[Some(2)]);
        assert_eq!(result[1].to_decimal_vec(), &[Decimal::from_f64(3.0).ok()]);
        assert_eq!(result[2].to_decimal_vec(), &[Decimal::from_f64(0.25).ok()]);
    }

    #[test]
    fn test_integration() {
        let pop_var_parser = AggrFnDefinitionParserVariance::<Population>::new();
        let samp_var_parser = AggrFnDefinitionParserVariance::<Sample>::new();

        let pop_var_expr = ExprDefBuilder::aggr_func(ExprType::Variance, FieldTypeTp::Double)
            .push_child(ExprDefBuilder::column_ref(0, FieldTypeTp::VarString))
            .build();
        pop_var_parser.check_supported(&pop_var_expr).unwrap();

        let samp_var_expr = ExprDefBuilder::aggr_func(ExprType::VarSamp, FieldTypeTp::Double)
            .push_child(ExprDefBuilder::column_ref(0, FieldTypeTp::VarString))
            .build();
        samp_var_parser.check_supported(&samp_var_expr).unwrap();

        let src_schema = [FieldTypeTp::VarString.into()];
        let mut columns = LazyBatchColumnVec::from(vec![{
            let mut col = LazyBatchColumn::decoded_with_capacity_and_tp(0, EvalType::Bytes);
            col.mut_decoded().push_bytes(Some(b"12.5".to_vec()));
            col.mut_decoded().push_bytes(None);
            col.mut_decoded().push_bytes(Some(b"10.0".to_vec()));
            col.mut_decoded().push_bytes(Some(b"42.0".to_vec()));
            col.mut_decoded().push_bytes(None);
            col
        }]);
        let logical_rows = vec![0, 1, 3, 4];

        let mut schema = vec![];
        let mut exp = vec![];
        let mut ctx = EvalContext::default();

        let pop_var_aggr_fn = pop_var_parser
            .parse(pop_var_expr, &mut ctx, &src_schema, &mut schema, &mut exp)
            .unwrap();
        assert_eq!(schema.len(), 3);
        assert_eq!(schema[0].as_accessor().tp(), FieldTypeTp::LongLong);
        assert_eq!(schema[1].as_accessor().tp(), FieldTypeTp::Double);
        assert_eq!(schema[2].as_accessor().tp(), FieldTypeTp::Double);
        assert_eq!(exp.len(), 1);

        let samp_var_aggr_fn = samp_var_parser
            .parse(samp_var_expr, &mut ctx, &src_schema, &mut schema, &mut exp)
            .unwrap();
        assert_eq!(schema.len(), 6);
        assert_eq!(schema[3].as_accessor().tp(), FieldTypeTp::LongLong);
        assert_eq!(schema[4].as_accessor().tp(), FieldTypeTp::Double);
        assert_eq!(schema[5].as_accessor().tp(), FieldTypeTp::Double);
        assert_eq!(exp.len(), 2);

        let mut pop_var_state = pop_var_aggr_fn.create_state();
        let mut samp_var_state = samp_var_aggr_fn.create_state();

        // VARIANCE will return <Int, Decimal, Decimal>
        let mut aggr_result = [
            VectorValue::with_capacity(0, EvalType::Int),
            VectorValue::with_capacity(0, EvalType::Real),
            VectorValue::with_capacity(0, EvalType::Real),
        ];

        // population variance
        {
            let pop_var_result = exp[0]
                .eval(&mut ctx, &src_schema, &mut columns, &logical_rows, 4)
                .unwrap();
            let pop_var_result = pop_var_result.vector_value().unwrap();
            let pop_var_slice: ChunkedVecSized<Real> = pop_var_result.as_ref().to_real_vec().into();
            update_vector!(
                pop_var_state,
                &mut ctx,
                pop_var_slice,
                pop_var_result.logical_rows()
            )
            .unwrap();
            pop_var_state
                .push_result(&mut ctx, &mut aggr_result)
                .unwrap();
        }

        // sample variance
        {
            let samp_var_result = exp[0]
                .eval(&mut ctx, &src_schema, &mut columns, &logical_rows, 4)
                .unwrap();
            let samp_var_result = samp_var_result.vector_value().unwrap();
            let samp_var_slice: ChunkedVecSized<Real> =
                samp_var_result.as_ref().to_real_vec().into();
            update_vector!(
                samp_var_state,
                &mut ctx,
                samp_var_slice,
                samp_var_result.logical_rows()
            )
            .unwrap();
            samp_var_state
                .push_result(&mut ctx, &mut aggr_result)
                .unwrap();
        }

        // count:
        assert_eq!(aggr_result[0].to_int_vec(), &[Some(2), Some(2)]);
        // sum:
        assert_eq!(
            aggr_result[1].to_real_vec(),
            &[Real::new(54.5).ok(), Real::new(54.5).ok()]
        );
        // variance (population, sample):
        assert_eq!(
            aggr_result[2].to_real_vec(),
            &[Real::new(217.5625).ok(), Real::new(435.125).ok()]
        );
    }

    #[test]
    fn test_illegal_request() {
        let pop_var_parser = AggrFnDefinitionParserVariance::<Population>::new();

        let expr = ExprDefBuilder::aggr_func(ExprType::Variance, FieldTypeTp::Double) // Expect NewDecimal but give Double
            .push_child(ExprDefBuilder::column_ref(0, FieldTypeTp::LongLong)) // FIXME: This type can be incorrect as well
            .build();
        pop_var_parser.check_supported(&expr).unwrap();

        let src_schema = [FieldTypeTp::LongLong.into()];
        let mut schema = vec![];
        let mut exp = vec![];
        let mut ctx = EvalContext::default();
        pop_var_parser
            .parse(expr, &mut ctx, &src_schema, &mut schema, &mut exp)
            .unwrap_err();
    }
}
