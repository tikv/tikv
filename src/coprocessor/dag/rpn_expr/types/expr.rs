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

use tipb::expression::FieldType;

use super::super::function::RpnFunction;
use crate::coprocessor::codec::data_type::ScalarValue;

/// A type for each node in the RPN expression list.
#[derive(Debug)]
pub enum RpnExpressionNode {
    /// Represents a function call.
    FnCall {
        func: Box<dyn RpnFunction>,
        field_type: FieldType,
    },

    /// Represents a scalar constant value.
    Constant {
        value: ScalarValue,
        field_type: FieldType,
    },

    /// Represents a reference to a column in the columns specified in evaluation.
    ColumnRef { offset: usize },
}

impl RpnExpressionNode {
    /// Gets the field type.
    #[inline]
    pub fn field_type(&self) -> Option<&FieldType> {
        match self {
            RpnExpressionNode::FnCall { ref field_type, .. } => Some(field_type),
            RpnExpressionNode::Constant { ref field_type, .. } => Some(field_type),
            RpnExpressionNode::ColumnRef { .. } => None,
        }
    }

    /// Borrows the function instance for `FnCall` variant.
    #[inline]
    pub fn fn_call_func(&self) -> Option<&dyn RpnFunction> {
        match self {
            RpnExpressionNode::FnCall { ref func, .. } => Some(&*func),
            _ => None,
        }
    }

    /// Borrows the constant value for `Constant` variant.
    #[inline]
    pub fn constant_value(&self) -> Option<&ScalarValue> {
        match self {
            RpnExpressionNode::Constant { ref value, .. } => Some(value),
            _ => None,
        }
    }

    /// Gets the column offset for `ColumnRef` variant.
    #[inline]
    pub fn column_ref_offset(&self) -> Option<usize> {
        match self {
            RpnExpressionNode::ColumnRef { ref offset, .. } => Some(*offset),
            _ => None,
        }
    }
}

/// An expression in Reverse Polish notation, which is simply a list of RPN expression nodes.
///
/// You may want to build it using `RpnExpressionBuilder`.
#[derive(Debug)]
pub struct RpnExpression(Vec<RpnExpressionNode>);

impl std::ops::Deref for RpnExpression {
    type Target = Vec<RpnExpressionNode>;

    fn deref(&self) -> &Vec<RpnExpressionNode> {
        &self.0
    }
}

impl std::ops::DerefMut for RpnExpression {
    fn deref_mut(&mut self) -> &mut Vec<RpnExpressionNode> {
        &mut self.0
    }
}

impl From<Vec<RpnExpressionNode>> for RpnExpression {
    fn from(v: Vec<RpnExpressionNode>) -> Self {
        Self(v)
    }
}

impl AsRef<[RpnExpressionNode]> for RpnExpression {
    fn as_ref(&self) -> &[RpnExpressionNode] {
        self.0.as_ref()
    }
}

// See `RpnExpression::eval` in `expr_eval`.

#[cfg(test)]
pub mod tests {
    use super::super::RpnFnCallPayload;

    use crate::coprocessor::dag::expr::EvalContext;
    use crate::coprocessor::Result;

    /// An RPN function for test. It accepts 1 int argument, returns the value in float.
    #[derive(Debug, Clone, Copy)]
    pub struct FnA;

    impl_template_fn! { 1 arg @ FnA }

    impl FnA {
        #[inline(always)]
        fn call(
            _ctx: &mut EvalContext,
            _payload: RpnFnCallPayload<'_>,
            v: &Option<i64>,
        ) -> Result<Option<f64>> {
            Ok(v.map(|v| v as f64))
        }
    }

    /// An RPN function for test. It accepts 2 float arguments, returns their sum in int.
    #[derive(Debug, Clone, Copy)]
    pub struct FnB;

    impl_template_fn! { 2 arg @ FnB }

    impl FnB {
        #[inline(always)]
        fn call(
            _ctx: &mut EvalContext,
            _payload: RpnFnCallPayload<'_>,
            v1: &Option<f64>,
            v2: &Option<f64>,
        ) -> Result<Option<i64>> {
            if v1.is_none() || v2.is_none() {
                return Ok(None);
            }
            Ok(Some((v1.as_ref().unwrap() + v2.as_ref().unwrap()) as i64))
        }
    }

    /// An RPN function for test. It accepts 3 int arguments, returns their sum in int.
    #[derive(Debug, Clone, Copy)]
    pub struct FnC;

    impl_template_fn! { 3 arg @ FnC }

    impl FnC {
        #[inline(always)]
        fn call(
            _ctx: &mut EvalContext,
            _payload: RpnFnCallPayload<'_>,
            v1: &Option<i64>,
            v2: &Option<i64>,
            v3: &Option<i64>,
        ) -> Result<Option<i64>> {
            if v1.is_none() || v2.is_none() || v3.is_none() {
                return Ok(None);
            }
            Ok(Some(
                v1.as_ref().unwrap() + v2.as_ref().unwrap() + v3.as_ref().unwrap(),
            ))
        }
    }

    /// An RPN function for test. It accepts 3 float arguments, returns their sum in float.
    #[derive(Debug, Clone, Copy)]
    pub struct FnD;

    impl_template_fn! { 3 arg @ FnD }

    impl FnD {
        #[inline(always)]
        fn call(
            _ctx: &mut EvalContext,
            _payload: RpnFnCallPayload<'_>,
            v1: &Option<f64>,
            v2: &Option<f64>,
            v3: &Option<f64>,
        ) -> Result<Option<f64>> {
            if v1.is_none() || v2.is_none() || v3.is_none() {
                return Ok(None);
            }
            Ok(Some(
                v1.as_ref().unwrap() + v2.as_ref().unwrap() + v3.as_ref().unwrap(),
            ))
        }
    }
}
