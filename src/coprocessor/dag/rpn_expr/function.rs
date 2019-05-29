// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use super::types::{RpnFnCallPayload, RpnStackNode};
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
    fn eval(
        &self,
        rows: usize,
        context: &mut EvalContext,
        payload: RpnFnCallPayload<'_>,
    ) -> Result<VectorValue>;

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
        rows: usize,
        context: &mut EvalContext,
        payload: RpnFnCallPayload<'_>,
    ) -> Result<VectorValue> {
        (**self).eval(rows, context, payload)
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
        rows: usize,
        mut f: F,
        context: &mut EvalContext,
        payload: RpnFnCallPayload<'_>,
    ) -> Result<VectorValue>
    where
        Ret: Evaluable,
        F: FnMut(&mut EvalContext, RpnFnCallPayload<'_>) -> Result<Option<Ret>>,
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
    #[inline]
    pub fn eval_1_arg<Arg0, Ret, F>(
        rows: usize,
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
    #[inline]
    pub fn eval_2_args<Arg0, Arg1, Ret, F>(
        rows: usize,
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

    #[inline]
    fn eval_2_args_scalar_scalar<Arg0, Arg1, Ret, F>(
        rows: usize,
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
        rows: usize,
        mut f: F,
        context: &mut EvalContext,
        payload: RpnFnCallPayload<'_>,
        lhs: &ScalarValue,
        rhs: &VectorValue,
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
        assert_eq!(rows, rhs.len());
        let mut result = Vec::with_capacity(rows);
        let lhs = Arg0::borrow_scalar_value(lhs);
        let rhs = Arg1::borrow_vector_value(rhs);
        for i in 0..rows {
            result.push(f(context, payload, lhs, &rhs[i])?);
        }
        Ok(Ret::into_vector_value(result))
    }

    #[inline]
    fn eval_2_args_vector_scalar<Arg0, Arg1, Ret, F>(
        rows: usize,
        mut f: F,
        context: &mut EvalContext,
        payload: RpnFnCallPayload<'_>,
        lhs: &VectorValue,
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
        assert_eq!(rows, lhs.len());
        let mut result = Vec::with_capacity(rows);
        let lhs = Arg0::borrow_vector_value(lhs);
        let rhs = Arg1::borrow_scalar_value(rhs);
        for i in 0..rows {
            result.push(f(context, payload, &lhs[i], rhs)?);
        }
        Ok(Ret::into_vector_value(result))
    }

    #[inline]
    fn eval_2_args_vector_vector<Arg0, Arg1, Ret, F>(
        rows: usize,
        mut f: F,
        context: &mut EvalContext,
        payload: RpnFnCallPayload<'_>,
        lhs: &VectorValue,
        rhs: &VectorValue,
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
    #[inline]
    pub fn eval_3_args<Arg0, Arg1, Arg2, Ret, F>(
        rows: usize,
        mut f: F,
        context: &mut EvalContext,
        payload: RpnFnCallPayload<'_>,
    ) -> Result<VectorValue>
    where
        Arg0: Evaluable,
        Arg1: Evaluable,
        Arg2: Evaluable,
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

trait ArgMeta {
    fn build_and_run<A: Args>(
        self,
        rows: usize,
        context: &mut EvalContext,
        payload: RpnFnCallPayload<'_>,
        position: usize,
        args: A,
    );
}

struct Arg<T, Rem: ArgMeta> {
    remains: Rem,
    _phantom: std::marker::PhantomData<T>,
}

impl<T: Evaluable, Rem: ArgMeta> ArgMeta for Arg<T, Rem> {
    fn build_and_run<A: Args>(
        self,
        rows: usize,
        context: &mut EvalContext,
        payload: RpnFnCallPayload<'_>,
        position: usize,
        args: A,
    ) {
        match payload.raw_arg_at(position) {
            RpnStackNode::Scalar { value, .. } => self.remains.build_and_run(
                rows,
                context,
                payload,
                position + 1,
                args.add_scalar(T::borrow_scalar_value(value)),
            ),
            RpnStackNode::Vector { value, .. } => self.remains.build_and_run(
                rows,
                context,
                payload,
                position + 1,
                args.add_vector(T::borrow_vector_value(value)),
            ),
        }
    }
}

struct TestFunctionRunner;

impl ArgMeta for TestFunctionRunner {
    fn build_and_run<A: Args>(
        self,
        rows: usize,
        context: &mut EvalContext,
        payload: RpnFnCallPayload<'_>,
        position: usize,
        args: A,
    ) {
        let (arg2, args) = args.extract(0);
        let (arg1, args) = args.extract(0);
        let (arg0, args) = args.extract(0);
    }
}

trait Args {
    type Type;
    type Rem: Args;

    fn extract(&self, index: usize) -> (&Option<Self::Type>, &Self::Rem);

    fn add_scalar<T>(self, value: &Option<T>) -> ScalarArg<'_, T, Self>
    where
        Self: Sized,
    {
        ScalarArg {
            value,
            remains: self,
        }
    }

    fn add_vector<T>(self, values: &[Option<T>]) -> VectorArg<'_, T, Self>
    where
        Self: Sized,
    {
        VectorArg {
            values,
            remains: self,
        }
    }
}

struct VectorArg<'a, T, A: Args> {
    values: &'a [Option<T>],
    remains: A,
}

impl<'a, T, A: Args> Args for VectorArg<'a, T, A> {
    type Type = T;
    type Rem = A;

    fn extract(&self, index: usize) -> (&Option<T>, &A) {
        (&self.values[index], &self.remains)
    }
}

struct ScalarArg<'a, T, A: Args> {
    value: &'a Option<T>,
    remains: A,
}

impl<'a, T, A: Args> Args for ScalarArg<'a, T, A> {
    type Type = T;
    type Rem = A;

    fn extract(&self, _index: usize) -> (&Option<T>, &A) {
        (&self.value, &self.remains)
    }
}

struct Null;

impl Args for Null {
    type Type = ();
    type Rem = Null;

    fn extract(&self, index: usize) -> (&Option<Self::Type>, &Self::Rem) {
        unreachable!()
    }
}
