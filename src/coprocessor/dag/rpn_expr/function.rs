// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use super::types::{ConcreteLogicalVectorView, LogicalVectorView, RpnFnCallPayload, RpnStackNode};
use crate::coprocessor::codec::data_type::{Evaluable, ScalarValue, VectorValue};
use crate::coprocessor::dag::expr::EvalContext;
use crate::coprocessor::Result;

/// A trait for all RPN functions.
///
/// This trait can be auto derived by using `cop_codegen::RpnFunction`.
pub trait RpnFunction: std::fmt::Debug + Send + Sync + 'static {
    /// The display name of the function.
    fn name(&self) -> &'static str;

    /// The accepted argument length of this RPN function.
    ///
    /// Currently we do not support variable arguments.
    fn args_len(&self) -> usize;

    /// Evaluates the function according to given raw arguments. A raw argument contains the
    /// argument value and the argument field type.
    fn eval(&self, context: &mut EvalContext, payload: RpnFnCallPayload<'_>)
        -> Result<VectorValue>;

    /// Clones current instance into a trait object.
    fn box_clone(&self) -> Box<dyn RpnFunction>;
}

impl Clone for Box<dyn RpnFunction> {
    #[inline]
    fn clone(&self) -> Self {
        self.box_clone()
    }
}

impl<T: RpnFunction + ?Sized> RpnFunction for Box<T> {
    #[inline]
    fn name(&self) -> &'static str {
        (**self).name()
    }

    #[inline]
    fn args_len(&self) -> usize {
        (**self).args_len()
    }

    #[inline]
    fn eval(
        &self,
        context: &mut EvalContext,
        payload: RpnFnCallPayload<'_>,
    ) -> Result<VectorValue> {
        (**self).eval(context, payload)
    }

    #[inline]
    fn box_clone(&self) -> Box<dyn RpnFunction> {
        (**self).box_clone()
    }
}

pub struct Helper;

impl Helper {
    /// Evaluates a function without argument to produce a vector value.
    ///
    /// The function will be called multiple times to fill the vector.
    #[inline]
    pub fn eval_0_arg<Ret, F>(
        mut f: F,
        context: &mut EvalContext,
        payload: RpnFnCallPayload<'_>,
    ) -> Result<VectorValue>
    where
        Ret: Evaluable,
        F: FnMut(&mut EvalContext, RpnFnCallPayload<'_>) -> Result<Option<Ret>>,
    {
        assert_eq!(payload.args_len(), 0);

        let rows = payload.output_rows();
        let mut result = Vec::with_capacity(rows);
        for _ in 0..rows {
            result.push(f(context, payload)?);
        }
        Ok(Ret::into_vector_value(result))
    }

    /// Evaluates a function with 1 scalar or vector argument to produce a vector value.
    ///
    /// The function will be called multiple times to fill the vector.
    #[inline]
    pub fn eval_1_arg<Arg0, Ret, F>(
        mut f: F,
        context: &mut EvalContext,
        payload: RpnFnCallPayload<'_>,
    ) -> Result<VectorValue>
    where
        Arg0: Evaluable,
        Ret: Evaluable,
        F: FnMut(&mut EvalContext, RpnFnCallPayload<'_>, &Option<Arg0>) -> Result<Option<Ret>>,
    {
        assert_eq!(payload.args_len(), 1);

        let rows = payload.output_rows();
        let mut result = Vec::with_capacity(rows);
        match payload.raw_arg_at(0) {
            RpnStackNode::Scalar { value: v, .. } => {
                let value = Arg0::borrow_scalar_value(v);
                for _ in 0..rows {
                    result.push(f(context, payload, value)?);
                }
            }
            RpnStackNode::Vector { value: v, .. } => {
                let physical_values = Arg0::borrow_vector_value(v.as_ref());
                let logical_rows = v.logical_rows();
                assert_eq!(rows, logical_rows.len());
                for physical_idx in logical_rows {
                    result.push(f(context, payload, &physical_values[*physical_idx])?);
                }
            }
        }
        Ok(Ret::into_vector_value(result))
    }

    /// Evaluates a function with 2 scalar or vector arguments to produce a vector value.
    ///
    /// The function will be called multiple times to fill the vector.
    #[inline]
    pub fn eval_2_args<Arg0, Arg1, Ret, F>(
        f: F,
        context: &mut EvalContext,
        payload: RpnFnCallPayload<'_>,
    ) -> Result<VectorValue>
    where
        Arg0: Evaluable,
        Arg1: Evaluable,
        Ret: Evaluable,
        F: FnMut(
            &mut EvalContext,
            RpnFnCallPayload<'_>,
            &Option<Arg0>,
            &Option<Arg1>,
        ) -> Result<Option<Ret>>,
    {
        assert_eq!(payload.args_len(), 2);

        match (payload.raw_arg_at(0), payload.raw_arg_at(1)) {
            (RpnStackNode::Scalar { value: lhs, .. }, RpnStackNode::Scalar { value: rhs, .. }) => {
                Self::eval_2_args_scalar_scalar(f, context, payload, lhs, rhs)
            }
            (RpnStackNode::Scalar { value: lhs, .. }, RpnStackNode::Vector { value: rhs, .. }) => {
                Self::eval_2_args_scalar_vector(
                    f,
                    context,
                    payload,
                    lhs,
                    rhs.as_ref(),
                    rhs.logical_rows(),
                )
            }
            (RpnStackNode::Vector { value: lhs, .. }, RpnStackNode::Scalar { value: rhs, .. }) => {
                Self::eval_2_args_vector_scalar(
                    f,
                    context,
                    payload,
                    lhs.as_ref(),
                    lhs.logical_rows(),
                    rhs,
                )
            }
            (RpnStackNode::Vector { value: lhs, .. }, RpnStackNode::Vector { value: rhs, .. }) => {
                Self::eval_2_args_vector_vector(
                    f,
                    context,
                    payload,
                    lhs.as_ref(),
                    lhs.logical_rows(),
                    rhs.as_ref(),
                    rhs.logical_rows(),
                )
            }
        }
    }

    #[inline]
    fn eval_2_args_scalar_scalar<Arg0, Arg1, Ret, F>(
        mut f: F,
        context: &mut EvalContext,
        payload: RpnFnCallPayload<'_>,
        lhs: &ScalarValue,
        rhs: &ScalarValue,
    ) -> Result<VectorValue>
    where
        Arg0: Evaluable,
        Arg1: Evaluable,
        Ret: Evaluable,
        F: FnMut(
            &mut EvalContext,
            RpnFnCallPayload<'_>,
            &Option<Arg0>,
            &Option<Arg1>,
        ) -> Result<Option<Ret>>,
    {
        let rows = payload.output_rows();
        let mut result = Vec::with_capacity(rows);
        let lhs = Arg0::borrow_scalar_value(lhs);
        let rhs = Arg1::borrow_scalar_value(rhs);
        for _ in 0..rows {
            result.push(f(context, payload, lhs, rhs)?);
        }
        Ok(Ret::into_vector_value(result))
    }

    #[inline]
    fn eval_2_args_scalar_vector<Arg0, Arg1, Ret, F>(
        mut f: F,
        context: &mut EvalContext,
        payload: RpnFnCallPayload<'_>,
        lhs: &ScalarValue,
        rhs: &VectorValue,
        rhs_logical_rows: &[usize],
    ) -> Result<VectorValue>
    where
        Arg0: Evaluable,
        Arg1: Evaluable,
        Ret: Evaluable,
        F: FnMut(
            &mut EvalContext,
            RpnFnCallPayload<'_>,
            &Option<Arg0>,
            &Option<Arg1>,
        ) -> Result<Option<Ret>>,
    {
        let rows = payload.output_rows();
        assert_eq!(rows, rhs_logical_rows.len());
        let mut result = Vec::with_capacity(rows);
        let lhs = Arg0::borrow_scalar_value(lhs);
        let rhs = Arg1::borrow_vector_value(rhs);
        for physical_idx in rhs_logical_rows {
            result.push(f(context, payload, lhs, &rhs[*physical_idx])?);
        }
        Ok(Ret::into_vector_value(result))
    }

    #[inline]
    fn eval_2_args_vector_scalar<Arg0, Arg1, Ret, F>(
        mut f: F,
        context: &mut EvalContext,
        payload: RpnFnCallPayload<'_>,
        lhs: &VectorValue,
        lhs_logical_rows: &[usize],
        rhs: &ScalarValue,
    ) -> Result<VectorValue>
    where
        Arg0: Evaluable,
        Arg1: Evaluable,
        Ret: Evaluable,
        F: FnMut(
            &mut EvalContext,
            RpnFnCallPayload<'_>,
            &Option<Arg0>,
            &Option<Arg1>,
        ) -> Result<Option<Ret>>,
    {
        let rows = payload.output_rows();
        assert_eq!(rows, lhs_logical_rows.len());
        let mut result = Vec::with_capacity(rows);
        let lhs = Arg0::borrow_vector_value(lhs);
        let rhs = Arg1::borrow_scalar_value(rhs);
        for physical_idx in lhs_logical_rows {
            result.push(f(context, payload, &lhs[*physical_idx], rhs)?);
        }
        Ok(Ret::into_vector_value(result))
    }

    #[inline]
    fn eval_2_args_vector_vector<Arg0, Arg1, Ret, F>(
        mut f: F,
        context: &mut EvalContext,
        payload: RpnFnCallPayload<'_>,
        lhs: &VectorValue,
        lhs_logical_rows: &[usize],
        rhs: &VectorValue,
        rhs_logical_rows: &[usize],
    ) -> Result<VectorValue>
    where
        Arg0: Evaluable,
        Arg1: Evaluable,
        Ret: Evaluable,
        F: FnMut(
            &mut EvalContext,
            RpnFnCallPayload<'_>,
            &Option<Arg0>,
            &Option<Arg1>,
        ) -> Result<Option<Ret>>,
    {
        let rows = payload.output_rows();
        assert_eq!(rows, lhs_logical_rows.len());
        assert_eq!(rows, rhs_logical_rows.len());
        let mut result = Vec::with_capacity(rows);
        let lhs = Arg0::borrow_vector_value(lhs);
        let rhs = Arg1::borrow_vector_value(rhs);
        for i in 0..rows {
            result.push(f(
                context,
                payload,
                &lhs[lhs_logical_rows[i]],
                &rhs[rhs_logical_rows[i]],
            )?);
        }
        Ok(Ret::into_vector_value(result))
    }

    /// Evaluates a function with 3 scalar or vector arguments to produce a vector value.
    ///
    /// The function will be called multiple times to fill the vector. For each function call,
    /// there will be one indirection to support both scalar and vector arguments.
    #[inline]
    pub fn eval_3_args<Arg0, Arg1, Arg2, Ret, F>(
        mut f: F,
        context: &mut EvalContext,
        payload: RpnFnCallPayload<'_>,
    ) -> Result<VectorValue>
    where
        Arg0: Evaluable,
        Arg1: Evaluable,
        Arg2: Evaluable,
        for<'a> ConcreteLogicalVectorView<'a, Arg0>: From<LogicalVectorView<'a>>,
        for<'a> ConcreteLogicalVectorView<'a, Arg1>: From<LogicalVectorView<'a>>,
        for<'a> ConcreteLogicalVectorView<'a, Arg2>: From<LogicalVectorView<'a>>,
        Ret: Evaluable,
        F: FnMut(
            &mut EvalContext,
            RpnFnCallPayload<'_>,
            &Option<Arg0>,
            &Option<Arg1>,
            &Option<Arg2>,
        ) -> Result<Option<Ret>>,
    {
        assert_eq!(payload.args_len(), 3);

        let rows = payload.output_rows();
        let mut result = Vec::with_capacity(rows);
        let arg0: ConcreteLogicalVectorView<'_, Arg0> =
            payload.raw_arg_at(0).as_vector_view().into();
        let arg1: ConcreteLogicalVectorView<'_, Arg1> =
            payload.raw_arg_at(1).as_vector_view().into();
        let arg2: ConcreteLogicalVectorView<'_, Arg2> =
            payload.raw_arg_at(2).as_vector_view().into();
        for i in 0..rows {
            result.push(f(context, payload, &arg0[i], &arg1[i], &arg2[i])?);
        }
        Ok(Ret::into_vector_value(result))
    }
}
