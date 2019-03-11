// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

use super::types::RpnFnCallPayload;
use crate::coprocessor::codec::data_type::{Evaluable, ScalarValue, VectorValue};
use crate::coprocessor::dag::expr::EvalContext;
use crate::coprocessor::Result;

/// A trait for all RPN functions.
pub trait RpnFunction: std::fmt::Debug + Send + Sync + 'static {
    /// The display name of the function.
    fn name(&self) -> &'static str;

    /// The accepted argument length of this RPN function.
    ///
    /// Currently we do not support variable arguments.
    fn args_len(&self) -> usize;

    /// Evaluates the function according to given raw arguments. A raw argument contains the
    /// argument value and the argument field type.
    fn eval(
        &self,
        rows: usize,
        context: &mut EvalContext,
        payload: RpnFnCallPayload,
    ) -> Result<VectorValue>;
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
        rows: usize,
        context: &mut EvalContext,
        payload: RpnFnCallPayload,
    ) -> Result<VectorValue> {
        (**self).eval(rows, context, payload)
    }
}

pub struct Helper;

impl Helper {
    /// Evaluates a function without argument to produce a vector value.
    ///
    /// The function will be called multiple times to fill the vector.
    #[inline(always)]
    pub fn eval_0_arg<Ret, F>(
        rows: usize,
        mut f: F,
        context: &mut EvalContext,
        payload: RpnFnCallPayload,
    ) -> Result<VectorValue>
    where
        Ret: Evaluable,
        F: FnMut(&mut EvalContext, RpnFnCallPayload) -> Result<Ret>,
    {
        assert_eq!(payload.args_len(), 0);

        let mut result = Vec::with_capacity(rows);
        for _ in 0..rows {
            result.push(f(context, payload)?);
        }
        Ok(Ret::into_vector_value(result))
    }

    /// Evaluates a function with 1 scalar or vector argument to produce a vector value.
    ///
    /// The function will be called multiple times to fill the vector.
    #[inline(always)]
    pub fn eval_1_arg<Arg0, Ret, F>(
        rows: usize,
        mut f: F,
        context: &mut EvalContext,
        payload: RpnFnCallPayload,
    ) -> Result<VectorValue>
    where
        Arg0: Evaluable,
        Ret: Evaluable,
        F: FnMut(&mut EvalContext, RpnFnCallPayload, &Arg0) -> Result<Ret>,
    {
        assert_eq!(payload.args_len(), 1);

        let mut result = Vec::with_capacity(rows);
        if payload.raw_arg_at(0).is_scalar() {
            let v = Arg0::borrow_scalar_value(payload.raw_arg_at(0).scalar_value().unwrap());
            for _ in 0..rows {
                result.push(f(context, payload, v)?);
            }
        } else {
            let v = Arg0::borrow_vector_value(payload.raw_arg_at(0).vector_value().unwrap());
            assert_eq!(rows, v.len());
            for i in 0..rows {
                result.push(f(context, payload, &v[i])?);
            }
        }
        Ok(Ret::into_vector_value(result))
    }

    /// Evaluates a function with 2 scalar or vector arguments to produce a vector value.
    ///
    /// The function will be called multiple times to fill the vector.
    #[inline(always)]
    pub fn eval_2_args<Arg0, Arg1, Ret, F>(
        rows: usize,
        f: F,
        context: &mut EvalContext,
        payload: RpnFnCallPayload,
    ) -> Result<VectorValue>
    where
        Arg0: Evaluable,
        Arg1: Evaluable,
        Ret: Evaluable,
        F: FnMut(&mut EvalContext, RpnFnCallPayload, &Arg0, &Arg1) -> Result<Ret>,
    {
        assert_eq!(payload.args_len(), 2);

        if payload.raw_arg_at(0).is_scalar() {
            if payload.raw_arg_at(1).is_scalar() {
                Self::eval_2_args_scalar_scalar(
                    rows,
                    f,
                    context,
                    payload,
                    payload.raw_arg_at(0).scalar_value().unwrap(),
                    payload.raw_arg_at(1).scalar_value().unwrap(),
                )
            } else {
                Self::eval_2_args_scalar_vector(
                    rows,
                    f,
                    context,
                    payload,
                    payload.raw_arg_at(0).scalar_value().unwrap(),
                    payload.raw_arg_at(1).vector_value().unwrap(),
                )
            }
        } else {
            if payload.raw_arg_at(1).is_scalar() {
                Self::eval_2_args_vector_scalar(
                    rows,
                    f,
                    context,
                    payload,
                    payload.raw_arg_at(0).vector_value().unwrap(),
                    payload.raw_arg_at(1).scalar_value().unwrap(),
                )
            } else {
                Self::eval_2_args_vector_vector(
                    rows,
                    f,
                    context,
                    payload,
                    payload.raw_arg_at(0).vector_value().unwrap(),
                    payload.raw_arg_at(1).vector_value().unwrap(),
                )
            }
        }
    }

    #[inline(always)]
    fn eval_2_args_scalar_scalar<Arg0, Arg1, Ret, F>(
        rows: usize,
        mut f: F,
        context: &mut EvalContext,
        payload: RpnFnCallPayload,
        lhs: &ScalarValue,
        rhs: &ScalarValue,
    ) -> Result<VectorValue>
    where
        Arg0: Evaluable,
        Arg1: Evaluable,
        Ret: Evaluable,
        F: FnMut(&mut EvalContext, RpnFnCallPayload, &Arg0, &Arg1) -> Result<Ret>,
    {
        let mut result = Vec::with_capacity(rows);
        let lhs = Arg0::borrow_scalar_value(lhs);
        let rhs = Arg1::borrow_scalar_value(rhs);
        for _ in 0..rows {
            result.push(f(context, payload, lhs, rhs)?);
        }
        Ok(Ret::into_vector_value(result))
    }

    #[inline(always)]
    fn eval_2_args_scalar_vector<Arg0, Arg1, Ret, F>(
        rows: usize,
        mut f: F,
        context: &mut EvalContext,
        payload: RpnFnCallPayload,
        lhs: &ScalarValue,
        rhs: &VectorValue,
    ) -> Result<VectorValue>
    where
        Arg0: Evaluable,
        Arg1: Evaluable,
        Ret: Evaluable,
        F: FnMut(&mut EvalContext, RpnFnCallPayload, &Arg0, &Arg1) -> Result<Ret>,
    {
        assert_eq!(rows, rhs.len());
        let mut result = Vec::with_capacity(rows);
        let lhs = Arg0::borrow_scalar_value(lhs);
        let rhs = Arg1::borrow_vector_value(rhs);
        for i in 0..rows {
            result.push(f(context, payload, lhs, &rhs[i])?);
        }
        Ok(Ret::into_vector_value(result))
    }

    #[inline(always)]
    fn eval_2_args_vector_scalar<Arg0, Arg1, Ret, F>(
        rows: usize,
        mut f: F,
        context: &mut EvalContext,
        payload: RpnFnCallPayload,
        lhs: &VectorValue,
        rhs: &ScalarValue,
    ) -> Result<VectorValue>
    where
        Arg0: Evaluable,
        Arg1: Evaluable,
        Ret: Evaluable,
        F: FnMut(&mut EvalContext, RpnFnCallPayload, &Arg0, &Arg1) -> Result<Ret>,
    {
        assert_eq!(rows, lhs.len());
        let mut result = Vec::with_capacity(rows);
        let lhs = Arg0::borrow_vector_value(lhs);
        let rhs = Arg1::borrow_scalar_value(rhs);
        for i in 0..rows {
            result.push(f(context, payload, &lhs[i], rhs)?);
        }
        Ok(Ret::into_vector_value(result))
    }

    #[inline(always)]
    fn eval_2_args_vector_vector<Arg0, Arg1, Ret, F>(
        rows: usize,
        mut f: F,
        context: &mut EvalContext,
        payload: RpnFnCallPayload,
        lhs: &VectorValue,
        rhs: &VectorValue,
    ) -> Result<VectorValue>
    where
        Arg0: Evaluable,
        Arg1: Evaluable,
        Ret: Evaluable,
        F: FnMut(&mut EvalContext, RpnFnCallPayload, &Arg0, &Arg1) -> Result<Ret>,
    {
        assert_eq!(rows, lhs.len());
        assert_eq!(rows, rhs.len());
        let mut result = Vec::with_capacity(rows);
        let lhs = Arg0::borrow_vector_value(lhs);
        let rhs = Arg1::borrow_vector_value(rhs);
        for i in 0..rows {
            result.push(f(context, payload, &lhs[i], &rhs[i])?);
        }
        Ok(Ret::into_vector_value(result))
    }

    /// Evaluates a function with 3 scalar or vector arguments to produce a vector value.
    ///
    /// The function will be called multiple times to fill the vector. For each function call,
    /// there will be one indirection to support both scalar and vector arguments.
    #[inline(always)]
    pub fn eval_3_args<Arg0, Arg1, Arg2, Ret, F>(
        rows: usize,
        mut f: F,
        context: &mut EvalContext,
        payload: RpnFnCallPayload,
    ) -> Result<VectorValue>
    where
        Arg0: Evaluable,
        Arg1: Evaluable,
        Arg2: Evaluable,
        Ret: Evaluable,
        F: FnMut(&mut EvalContext, RpnFnCallPayload, &Arg0, &Arg1, &Arg2) -> Result<Ret>,
    {
        assert_eq!(payload.args_len(), 3);

        let mut result = Vec::with_capacity(rows);
        let arg0 = Arg0::borrow_vector_like_specialized(payload.raw_arg_at(0).as_vector_like());
        let arg1 = Arg1::borrow_vector_like_specialized(payload.raw_arg_at(1).as_vector_like());
        let arg2 = Arg2::borrow_vector_like_specialized(payload.raw_arg_at(2).as_vector_like());
        for i in 0..rows {
            result.push(f(context, payload, &arg0[i], &arg1[i], &arg2[i])?);
        }
        Ok(Ret::into_vector_value(result))
    }
}

/// Implements `RpnFunction` automatically for structure that accepts 0, 1, 2 or 3 arguments.
///
/// The structure must have a `call` member function accepting corresponding number of scalar
/// arguments.
#[macro_export]
macro_rules! impl_template_fn {
    (0 arg @ $name:ident) => {
        impl_template_fn! { @inner $name, 0, eval_0_arg }
    };
    (1 arg @ $name:ident) => {
        impl_template_fn! { @inner $name, 1, eval_1_arg }
    };
    (2 arg @ $name:ident) => {
        impl_template_fn! { @inner $name, 2, eval_2_args }
    };
    (3 arg @ $name:ident) => {
        impl_template_fn! { @inner $name, 3, eval_3_args }
    };
    (@inner $name:ident, $args:expr, $eval_fn:ident) => {
        impl $crate::coprocessor::dag::rpn_expr::RpnFunction for $name {
            #[inline]
            fn name(&self) -> &'static str {
                stringify!($name)
            }

            #[inline]
            fn args_len(&self) -> usize {
                $args
            }

            #[inline]
            fn eval(
                &self,
                rows: usize,
                context: &mut $crate::coprocessor::dag::expr::EvalContext,
                payload: $crate::coprocessor::dag::rpn_expr::types::RpnFnCallPayload,
            ) -> $crate::coprocessor::Result<$crate::coprocessor::codec::data_type::VectorValue>
            {
                $crate::coprocessor::dag::rpn_expr::function::Helper::$eval_fn(
                    rows,
                    Self::call,
                    context,
                    payload,
                )
            }
        }
    };
}
