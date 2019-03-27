// Copyright 2019 TiKV Project Authors.
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

mod expr;
mod expr_builder;
mod expr_eval;

pub use self::expr::RpnExpression;
pub use self::expr_builder::RpnExpressionBuilder;
pub use self::expr_eval::RpnStackNode;

use tipb::expression::FieldType;

/// A structure for holding argument values and type information of arguments and return values.
///
/// It can simplify function signatures without losing performance where only argument values are
/// needed in most cases.
///
/// NOTE: This structure must be very fast to copy because it will be passed by value directly
/// (i.e. Copy), instead of by reference, for **EACH** function invocation.
#[derive(Clone, Copy)]
pub struct RpnFnCallPayload<'a> {
    raw_args: &'a [RpnStackNode<'a>],
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
}
