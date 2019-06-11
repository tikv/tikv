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
//! If you are curious about what code the macro will generate, check the test code
//! in `components/cop_codegen/rpn_functions.rs`.

use super::types::{RpnFnCallPayload, RpnStackNode};
use crate::coprocessor::codec::data_type::{Evaluable, ScalarValue, VectorValue};
use crate::coprocessor::dag::expr::EvalContext;
use crate::coprocessor::Result;

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
    fn eval<D: ArgDef>(
        self,
        def: D,
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
    pub fn new(arg_index: usize, inner: E) -> Self {
        ArgConstructor { arg_index, inner }
    }
}

impl<E: Evaluator> Evaluator for ArgConstructor<E> {
    fn eval<D: ArgDef>(
        self,
        def: D,
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
