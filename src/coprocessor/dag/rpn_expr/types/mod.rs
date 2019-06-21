// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

mod expr;
mod expr_builder;
mod expr_eval;
#[cfg(test)]
pub mod test_util;

pub use self::expr::{RpnExpression, RpnExpressionNode};
pub use self::expr_builder::RpnExpressionBuilder;
pub use self::expr_eval::RpnStackNode;

use tipb::expression::FieldType;

use crate::coprocessor::codec::data_type::ScalarArg;

/// A structure for holding argument values and type information of arguments and return values.
///
/// It can simplify function signatures without losing performance where only argument values are
/// needed in most cases.
///
/// NOTE: This structure must be very fast to copy because it will be passed by value directly
/// (i.e. Copy), instead of by reference, for **EACH** function invocation.
#[derive(Clone, Copy)]
pub struct RpnFnCallPayload<'a> {
    output_rows: usize,
    raw_args: &'a [RpnStackNode<'a>],
    implicit_args: &'a [ScalarArg],
    ret_field_type: &'a FieldType,
}

impl<'a> RpnFnCallPayload<'a> {
    /// The number of arguments.
    #[inline]
    pub fn args_len(&'a self) -> usize {
        self.raw_args.len()
    }

    /// Gets the raw argument at specific position.
    #[inline]
    pub fn raw_arg_at(&'a self, position: usize) -> &'a RpnStackNode<'a> {
        &self.raw_args[position]
    }

    /// Gets the field type of the argument at specific position.
    #[inline]
    pub fn field_type_at(&'a self, position: usize) -> &'a FieldType {
        self.raw_args[position].field_type()
    }

    /// Gets the field type of the return value.
    #[inline]
    pub fn return_field_type(&'a self) -> &'a FieldType {
        self.ret_field_type
    }

    /// Gets expected rows of output.
    #[inline]
    pub fn output_rows(&'a self) -> usize {
        self.output_rows
    }

    /// Gets the length of implicit arguments
    #[inline]
    pub fn implicit_args_len(&'a self) -> usize {
        self.implicit_args.len()
    }

    /// Get implicit argument at the special position
    #[inline]
    pub fn imp_param_at(&'a self, position: usize) -> &'a ScalarArg {
        &self.implicit_args[position]
    }
}
