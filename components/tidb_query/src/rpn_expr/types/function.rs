// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

//! People implementing RPN functions with fixed argument type and count don't necessarily
//! need to understand how `Evaluator` and `RpnDef` work. There's a procedural macro called
//! `rpn_fn` defined in `tidb_query_codegen` to help you create RPN functions. For example:
//!
//! ```ignore
//! use tidb_query_codegen::rpn_fn;
//!
//! #[rpn_fn]
//! fn foo(lhs: &Option<Int>, rhs: &Option<Int>) -> Result<Option<Int>> {
//!     // Your RPN function logic
//! }
//! ```
//!
//! You can still call the `foo` function directly; the macro preserves the original function
//! It creates a `foo_fn_meta()` function (simply add `_fn_meta` to the original
//! function name) which generates an `RpnFnMeta` struct.
//!
//! For more information on the procedural macro, see the documentation in
//! `components/tidb_query_codegen/src/rpn_function`.

use std::any::Any;
use std::convert::TryFrom;
use std::marker::PhantomData;

use tidb_query_datatype::{EvalType, FieldTypeAccessor};
use tipb::{Expr, FieldType};

use super::RpnStackNode;
use crate::codec::data_type::{Evaluable, ScalarValueRef, VectorValue};
use crate::expr::EvalContext;
use crate::Result;

/// Metadata of an RPN function.
#[derive(Clone, Copy)]
pub struct RpnFnMeta {
    /// The display name of the RPN function. Mainly used in tests.
    pub name: &'static str,

    /// Validator against input expression tree.
    pub validator_ptr: fn(expr: &Expr) -> Result<()>,

    /// The metadata constructor of the RPN function.
    pub metadata_expr_ptr: fn(expr: &mut Expr) -> Result<Box<dyn Any + Send>>,

    #[allow(clippy::type_complexity)]
    /// The RPN function.
    pub fn_ptr: fn(
        // Common arguments
        ctx: &mut EvalContext,
        output_rows: usize,
        args: &[RpnStackNode<'_>],
        // Uncommon arguments are grouped together
        extra: &mut RpnFnCallExtra<'_>,
        metadata: &(dyn Any + Send),
    ) -> Result<VectorValue>,
}

impl std::fmt::Debug for RpnFnMeta {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name)
    }
}

/// Extra information about an RPN function call.
pub struct RpnFnCallExtra<'a> {
    /// The field type of the return value.
    pub ret_field_type: &'a FieldType,
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
pub struct VectorArg<'a, T: Evaluable> {
    physical_col: &'a [Option<T>],
    logical_rows: &'a [usize],
}

impl<'a, T: Evaluable> RpnFnArg for VectorArg<'a, T> {
    type Type = &'a Option<T>;

    #[inline]
    fn get(&self, row: usize) -> &'a Option<T> {
        &self.physical_col[self.logical_rows[row]]
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
        ctx: &mut EvalContext,
        output_rows: usize,
        args: &[RpnStackNode<'_>],
        extra: &mut RpnFnCallExtra<'_>,
        metadata: &(dyn Any + Send),
    ) -> Result<VectorValue>;
}

pub struct ArgConstructor<A: Evaluable, E: Evaluator> {
    arg_index: usize,
    inner: E,
    _phantom: PhantomData<A>,
}

impl<A: Evaluable, E: Evaluator> ArgConstructor<A, E> {
    #[inline]
    pub fn new(arg_index: usize, inner: E) -> Self {
        ArgConstructor {
            arg_index,
            inner,
            _phantom: PhantomData,
        }
    }
}

impl<A: Evaluable, E: Evaluator> Evaluator for ArgConstructor<A, E> {
    fn eval(
        self,
        def: impl ArgDef,
        ctx: &mut EvalContext,
        output_rows: usize,
        args: &[RpnStackNode<'_>],
        extra: &mut RpnFnCallExtra<'_>,
        metadata: &(dyn Any + Send),
    ) -> Result<VectorValue> {
        match &args[self.arg_index] {
            RpnStackNode::Scalar { value, .. } => {
                let v = A::borrow_scalar_value(value);
                let new_def = Arg {
                    arg: ScalarArg(v),
                    rem: def,
                };
                self.inner
                    .eval(new_def, ctx, output_rows, args, extra, metadata)
            }
            RpnStackNode::Vector { value, .. } => {
                let logical_rows = value.logical_rows();
                let v = A::borrow_vector_value(value.as_ref());
                let new_def = Arg {
                    arg: VectorArg {
                        physical_col: v,
                        logical_rows,
                    },
                    rem: def,
                };
                self.inner
                    .eval(new_def, ctx, output_rows, args, extra, metadata)
            }
        }
    }
}

/// Validates whether the return type of an expression node meets expectation.
pub fn validate_expr_return_type(expr: &Expr, et: EvalType) -> Result<()> {
    let received_et = box_try!(EvalType::try_from(expr.get_field_type().as_accessor().tp()));
    if et == received_et {
        Ok(())
    } else {
        Err(other_err!("Expect `{}`, received `{}`", et, received_et))
    }
}

/// Validates whether the number of arguments of an expression node meets expectation.
pub fn validate_expr_arguments_eq(expr: &Expr, args: usize) -> Result<()> {
    let received_args = expr.get_children().len();
    if received_args == args {
        Ok(())
    } else {
        Err(other_err!(
            "Expect {} arguments, received {}",
            args,
            received_args
        ))
    }
}

/// Validates whether the number of arguments of an expression node >= expectation.
pub fn validate_expr_arguments_gte(expr: &Expr, args: usize) -> Result<()> {
    let received_args = expr.get_children().len();
    if received_args >= args {
        Ok(())
    } else {
        Err(other_err!(
            "Expect at least {} arguments, received {}",
            args,
            received_args
        ))
    }
}

/// Validates whether the number of arguments of an expression node <= expectation.
pub fn validate_expr_arguments_lte(expr: &Expr, args: usize) -> Result<()> {
    let received_args = expr.get_children().len();
    if received_args <= args {
        Ok(())
    } else {
        Err(other_err!(
            "Expect at most {} arguments, received {}",
            args,
            received_args
        ))
    }
}

thread_local! {
    pub static VARG_PARAM_BUF: std::cell::RefCell<Vec<usize>> =
        std::cell::RefCell::new(Vec::with_capacity(20));

    pub static RAW_VARG_PARAM_BUF: std::cell::RefCell<Vec<ScalarValueRef<'static>>> =
        std::cell::RefCell::new(Vec::with_capacity(20));
}

pub fn extract_metadata_from_val<T: protobuf::Message + Default>(val: &[u8]) -> Result<T> {
    if val.is_empty() {
        Ok(T::default())
    } else {
        protobuf::parse_from_bytes::<T>(val)
            .map_err(|e| other_err!("Decode metadata failed: {}", e))
    }
}
