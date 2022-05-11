// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

//! People implementing RPN functions with fixed argument type and count don't necessarily
//! need to understand how `Evaluator` and `RpnDef` work. There's a procedural macro called
//! `rpn_fn` defined in `tidb_query_codegen` to help you create RPN functions. For example:
//!
//! ```ignore
//! use tidb_query_codegen::rpn_fn;
//!
//! #[rpn_fn(nullable)]
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

use std::{any::Any, convert::TryFrom, marker::PhantomData};

use static_assertions::assert_eq_size;
use tidb_query_common::Result;
use tidb_query_datatype::{codec::data_type::*, expr::EvalContext, EvalType, FieldTypeAccessor};
use tipb::{Expr, FieldType};

use super::{expr_eval::LogicalRows, RpnStackNode};

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

    /// Gets the bit vector of the arg.
    /// Returns `None` if scalar value, and bool indicates whether
    /// all is null or isn't null, otherwise a BitVec.
    /// Returns `Some` if vector value, and bool indicates whether
    /// stored bitmap vector has the same layout as elements,
    /// aka. logical_rows is identical or not. If logical_rows is
    /// identical, the second tuple element yields true.
    fn get_bit_vec(&self) -> (Option<&BitVec>, bool);
}

/// Represents an RPN function argument of a `ScalarValue`.
#[derive(Clone, Copy, Debug)]
pub struct ScalarArg<'a, T: EvaluableRef<'a>>(Option<T>, PhantomData<&'a T>);

impl<'a, T: EvaluableRef<'a>> ScalarArg<'a, T> {
    pub fn new(data: Option<T>) -> Self {
        Self(data, PhantomData)
    }
}

impl<'a, T: EvaluableRef<'a>> RpnFnArg for ScalarArg<'a, T> {
    type Type = Option<T>;

    /// Gets the value in the given row. All rows of a `ScalarArg` share the same value.
    #[inline]
    fn get(&self, _row: usize) -> Option<T> {
        self.0.clone()
    }

    // All items of scalar arg is either not null or null
    #[inline]
    fn get_bit_vec(&self) -> (Option<&BitVec>, bool) {
        (None, self.0.is_some())
    }
}

/// Represents an RPN function argument of a `VectorValue`.
#[derive(Clone, Copy, Debug)]
pub struct VectorArg<'a, T: 'a + EvaluableRef<'a>, C: 'a + ChunkRef<'a, T>> {
    physical_col: C,
    logical_rows: LogicalRows<'a>,
    _phantom: PhantomData<T>,
}

impl<'a, T: EvaluableRef<'a>, C: 'a + ChunkRef<'a, T>> RpnFnArg for VectorArg<'a, T, C> {
    type Type = Option<T>;

    #[inline]
    fn get(&self, row: usize) -> Option<T> {
        let logical_index = self.logical_rows.get_idx(row);
        self.physical_col.get_option_ref(logical_index)
    }

    #[inline]
    fn get_bit_vec(&self) -> (Option<&BitVec>, bool) {
        (
            Some(self.physical_col.get_bit_vec()),
            self.logical_rows.is_ident(),
        )
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

    /// Gets the bit vector of each arg
    #[inline]
    pub fn get_bit_vec(&self) -> ((Option<&BitVec>, bool), &Rem) {
        (self.arg.get_bit_vec(), &self.rem)
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
pub trait Evaluator<'a> {
    fn eval(
        self,
        def: impl ArgDef,
        ctx: &mut EvalContext,
        output_rows: usize,
        args: &'a [RpnStackNode<'a>],
        extra: &mut RpnFnCallExtra<'_>,
        metadata: &(dyn Any + Send),
    ) -> Result<VectorValue>;
}

pub struct ArgConstructor<'a, A: EvaluableRef<'a>, E: Evaluator<'a>> {
    arg_index: usize,
    inner: E,
    _phantom: PhantomData<&'a A>,
}

impl<'a, A: EvaluableRef<'a>, E: Evaluator<'a>> ArgConstructor<'a, A, E> {
    #[inline]
    pub fn new(arg_index: usize, inner: E) -> Self {
        ArgConstructor {
            arg_index,
            inner,
            _phantom: PhantomData,
        }
    }
}

impl<'a, A: EvaluableRef<'a>, E: Evaluator<'a>> Evaluator<'a> for ArgConstructor<'a, A, E> {
    fn eval(
        self,
        def: impl ArgDef,
        ctx: &mut EvalContext,
        output_rows: usize,
        args: &'a [RpnStackNode<'a>],
        extra: &mut RpnFnCallExtra<'_>,
        metadata: &(dyn Any + Send),
    ) -> Result<VectorValue> {
        match &args[self.arg_index] {
            RpnStackNode::Scalar { value, .. } => {
                let v = A::borrow_scalar_value_ref(value.as_scalar_value_ref());
                let new_def = Arg {
                    arg: ScalarArg::new(v),
                    rem: def,
                };
                self.inner
                    .eval(new_def, ctx, output_rows, args, extra, metadata)
            }
            RpnStackNode::Vector { value, .. } => {
                let logical_rows = value.logical_rows_struct();

                let v = A::borrow_vector_value(value.as_ref());

                let new_def = Arg {
                    arg: VectorArg {
                        physical_col: v,
                        logical_rows,
                        _phantom: PhantomData,
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
        match (et, received_et) {
            (EvalType::Int, EvalType::Enum) | (EvalType::Bytes, EvalType::Enum) => Ok(()),
            _ => Err(other_err!("Expect `{}`, received `{}`", et, received_et)),
        }
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

// `VARG_PARAM_BUF` is a thread-local cache for evaluating vargs
// `rpn_fn`. In this way, we can reduce overhead of allocating new Vec.
// According to https://doc.rust-lang.org/std/mem/fn.size_of.html ,
// &T and Option<&T> has the same size.
assert_eq_size!(usize, Option<&Int>);
assert_eq_size!(usize, Option<&Real>);
assert_eq_size!(usize, Option<&Decimal>);
assert_eq_size!(usize, Option<&Bytes>);
assert_eq_size!(usize, Option<&DateTime>);
assert_eq_size!(usize, Option<&Duration>);
assert_eq_size!(usize, Option<&Json>);

thread_local! {
    pub static VARG_PARAM_BUF: std::cell::RefCell<Vec<usize>> =
        std::cell::RefCell::new(Vec::with_capacity(20));

    pub static VARG_PARAM_BUF_BYTES_REF: std::cell::RefCell<Vec<Option<BytesRef<'static>>>> =
        std::cell::RefCell::new(Vec::with_capacity(20));

    pub static VARG_PARAM_BUF_JSON_REF: std::cell::RefCell<Vec<Option<JsonRef<'static>>>> =
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
