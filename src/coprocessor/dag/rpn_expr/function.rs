// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

//! People implementing RPN functions with fixed argument type and count don't necessarily
//! understand how `Evaluator` and `RpnDef` work. There's a procedure macro called `rpn_fn`
//! helping you create RPN functions. For example:
//!
//! ```
//! use cop_codegen::rpn_fn;
//!
//! #[rpn_fn]
//! fn foo(lhs: &Option<Int>, rhs: &Option<Int>) -> Result<Option<Int>> {
//!     // Your RPN function logic
//! }
//! ```
//!
//! You can still call the `foo` function as what it looks. The macro doesn't change the function
//! itself. Instead, it creates a `foo_fn()` function (simply add `_fn` to the original function
//! name) that generates an `RpnFn` struct.
//!
//! If you needs `EvalContext` or the raw `RpnFnCallPayload`, just put it ahead of the function
//! parameters, and add `ctx` or `payload` argument to the attribute. For example:
//!
//! ```
//! // This generates `with_context_fn() -> RpnFn`
//! #[rpn_fn(ctx)]
//! fn with_context(ctx: &mut EvalContext, param: &Option<Decimal>) -> Result<Option<Int>> {
//!     // Your RPN function logic
//! }
//!
//! // This generates `with_ctx_and_payload_fn() -> RpnFn`
//! #[rpn_fn(ctx, payload)]
//! fn with_ctx_and_payload(
//!     ctx: &mut EvalContext,
//!     payload: RpnFnCallPayload<'_>
//! ) -> Result<Option<Int>> {
//!     // Your RPN function logic
//! }
//! ```
//!
//! A trait whose name looks like `CamelCasedFnName_Fn` is created by the macro. If you need to
//! customize the execution logic for specific argument type, you may implement it on your own.
//! For example, you are going to implement an RPN function called `regex_match` taking two
//! arguments, the regex and the string to match. You want to build the regex only once if the
//! first argument is a scalar. The code may look like:
//!
//! ```
//! fn regex_match_impl(regex: &Regex, text: &Option<Bytes>) -> Result<Option<i32>> {
//!     // match text
//! }
//!
//! #[rpn_fn]
//! fn regex_match(regex: &Option<Bytes>, text: &Option<Bytes>) -> Result<Option<i32>> {
//!     let regex = build_regex(regex);
//!     regex_match_impl(&regex, text)
//! }
//!
//! // Pay attention that the first argument is specialized to `ScalarArg`
//! impl<'a, Arg1> RegexMatch_Fn for Arg<ScalarArg<'a, Bytes>, Arg<Arg1, Null>>
//! where Arg1: RpnFnArg<Type = &'a Option<Bytes>> {
//!     fn eval(
//!         self,
//!         rows: usize,
//!         ctx: &mut EvalContext,
//!         payload: RpnFnCallPayload<'_>,
//!     ) -> Result<VectorValue> {
//!         let (regex, arg) = self.extract(0);
//!         let regex = build_regex(regex);
//!         let mut result = Vec::with_capacity(rows);
//!         for row in 0..rows {
//!             let (text, _) = arg.extract(row);
//!             result.push(regex_match_impl(&regex, text)?);
//!         }
//!         Ok(Evaluable::into_vector_value(result))
//!     }
//! }
//! ```
//!
//! If you are curious about what code the macro will generate, check the test code
//! in `components/cop_codegen/rpn_functions.rs`.

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

#[derive(Clone, Copy)]
/// An RPN function
pub struct RpnFn {
    /// The display name of the function.
    pub name: &'static str,

    /// The accepted argument length of this RPN function.
    ///
    /// Currently we do not support variable arguments.
    pub args_len: usize,

    /// The function receiving raw argument.
    ///
    /// The first parameter is the number of rows.
    /// The second and the third are the evaluation context and the payload containing the
    /// argument value and the argument field type.
    pub fn_ptr: fn(usize, &mut EvalContext, RpnFnCallPayload<'_>) -> Result<VectorValue>,
}

impl std::fmt::Debug for RpnFn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}({} args)", self.name, self.args_len)
    }
}

/// A single argument of an RPN function.
pub trait RpnFnArg: std::fmt::Debug {
    type Type;

    /// Gets the value in the given row.
    fn get(&self, row: usize) -> Self::Type;
}

/// Represents an RPN function argument of a `ScalarValue`.
#[derive(Clone, Copy, Debug)]
pub struct ScalarArg<'a, T: Evaluable>(&'a Option<T>);

impl<'a, T: Evaluable> RpnFnArg for ScalarArg<'a, T> {
    type Type = &'a Option<T>;

    /// Gets the value in the given row. All rows of a `ScalarArg` share the same value.
    #[inline]
    fn get(&self, _row: usize) -> &'a Option<T> {
        self.0
    }
}

/// Represents an RPN function argument of a `VectorValue`.
#[derive(Clone, Copy, Debug)]
pub struct VectorArg<'a, T: Evaluable>(&'a [Option<T>]);

impl<'a, T: Evaluable> RpnFnArg for VectorArg<'a, T> {
    type Type = &'a Option<T>;

    #[inline]
    fn get(&self, row: usize) -> &'a Option<T> {
        &self.0[row]
    }
}

/// Partial or complete argument definition of an RPN function.
///
/// `ArgDef` is constructed at the beginning of evaluating an RPN function. The types of
/// `RpnFnArg`s are determined at this stage. So there won't be dynamic dispatch or enum matches
/// when the function is applied to each row of the input.
pub trait ArgDef: std::fmt::Debug {}

/// RPN function argument definitions in the form of a linked list.
///
/// For example, if an RPN function foo(Int, Real, Decimal) is applied to input of a scalar of
/// integer, a vector of reals and a vector of decimals, the constructed `ArgDef` will be
/// `Arg<ScalarArg<Int>, Arg<VectorValue<Real>, Arg<VectorValue<Decimal>, Null>>>`. `Null`
/// indicates the end of the argument list.
#[derive(Debug)]
pub struct Arg<A: RpnFnArg, Rem: ArgDef> {
    arg: A,
    rem: Rem,
}

impl<A: RpnFnArg, Rem: ArgDef> ArgDef for Arg<A, Rem> {}

impl<A: RpnFnArg, Rem: ArgDef> Arg<A, Rem> {
    /// Gets the value of the head argument in the given row and returns the remaining argument
    /// list.
    #[inline]
    pub fn extract(&self, row: usize) -> (A::Type, &Rem) {
        (self.arg.get(row), &self.rem)
    }
}

/// Represents the end of the argument list.
#[derive(Debug)]
pub struct Null;

impl ArgDef for Null {}

/// A generic evaluator of an RPN function.
///
/// For every RPN function, the evaluator should be created first. Then, call its `eval` method
/// with the input to get the result vector.
///
/// There are two kinds of evaluators in general:
/// - `ArgConstructor`: It's a provided `Evaluator`. It is used in the `rpn_fn` attribute macro
///   to generate the `ArgDef`. The `def` parameter of its eval method is the already constructed
///   `ArgDef`. If it is the outmost evaluator, `def` should be `Null`.
/// - Custom evaluators which do the actual execution of the RPN function. The `def` parameter of
///   its eval method is the constructed `ArgDef`. Implementors can then extract values from the
///   arguments, execute the RPN function and fill the result vector.
pub trait Evaluator {
    fn eval(
        self,
        def: impl ArgDef,
        rows: usize,
        context: &mut EvalContext,
        payload: RpnFnCallPayload<'_>,
    ) -> Result<VectorValue>;
}

pub struct ArgConstructor<E: Evaluator> {
    arg_index: usize,
    inner: E,
}

impl<E: Evaluator> ArgConstructor<E> {
    #[inline]
    pub fn new(arg_index: usize, inner: E) -> Self {
        ArgConstructor { arg_index, inner }
    }
}

impl<E: Evaluator> Evaluator for ArgConstructor<E> {
    fn eval(
        self,
        def: impl ArgDef,
        rows: usize,
        context: &mut EvalContext,
        payload: RpnFnCallPayload<'_>,
    ) -> Result<VectorValue> {
        match payload.raw_arg_at(self.arg_index) {
            RpnStackNode::Scalar { value, .. } => {
                match_template_evaluable! {
                    TT, match value {
                        ScalarValue::TT(v) => {
                            let new_def = Arg {
                                arg: ScalarArg(v),
                                rem: def,
                            };
                            self.inner.eval(new_def, rows, context, payload)
                        }
                    }
                }
            }
            RpnStackNode::Vector { value, .. } => {
                match_template_evaluable! {
                    TT, match **value {
                        VectorValue::TT(ref v) => {
                            let new_def = Arg {
                                arg: VectorArg(v),
                                rem: def,
                            };
                            self.inner.eval(new_def, rows, context, payload)
                        }
                    }
                }
            }
        }
    }
}
