// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

#![allow(unused)]

use tipb::expression::FieldType;

use crate::coprocessor::codec::batch::LazyBatchColumnVec;
use crate::coprocessor::codec::data_type::{Evaluable, ScalarValue};
use crate::coprocessor::dag::expr::EvalContext;
use crate::coprocessor::dag::rpn_expr::types::RpnStackNode;
use crate::coprocessor::dag::rpn_expr::{RpnExpressionBuilder, RpnFunction};
use crate::coprocessor::Result;

/// Helper utility to evaluate RPN function over scalar inputs.
///
/// This structure should be only useful in tests because it is not very efficient.
pub struct RpnFnScalarEvaluator {
    rpn_expr_builder: RpnExpressionBuilder,
    return_field_type: Option<FieldType>,
    context: Option<EvalContext>,
}

impl RpnFnScalarEvaluator {
    /// Creates a new `RpnFnScalarEvaluator`.
    pub fn new() -> Self {
        Self {
            rpn_expr_builder: RpnExpressionBuilder::new(),
            return_field_type: None,
            context: None,
        }
    }

    /// Pushes a parameter as the value of an argument for evaluation. The field type will be auto
    /// inferred by choosing an arbitrary field type that matches the field type of the given
    /// value.
    pub fn push_param(mut self, value: impl Into<ScalarValue>) -> Self {
        self.rpn_expr_builder = self.rpn_expr_builder.push_constant(value);
        self
    }

    /// Pushes a parameter as the value of an argument for evaluation using a specified field type.
    pub fn push_param_with_field_type(
        mut self,
        value: impl Into<ScalarValue>,
        field_type: impl Into<FieldType>,
    ) -> Self {
        self.rpn_expr_builder = self
            .rpn_expr_builder
            .push_constant_with_field_type(value, field_type);
        self
    }

    /// Sets the return field type.
    ///
    /// If not set, the evaluation will use an inferred return field type by choosing an arbitrary
    /// field type that matches the field type of the generic type `T` when calling `evaluate()`.
    pub fn return_field_type(mut self, field_type: impl Into<FieldType>) -> Self {
        self.return_field_type = Some(field_type.into());
        self
    }

    /// Sets the context to use during evaluation.
    ///
    /// If not set, a default `EvalContext` will be used.
    pub fn context(mut self, context: EvalContext) -> Self {
        self.context = Some(context);
        self
    }

    /// Evaluates the given function by using collected parameters.
    pub fn evaluate<T: Evaluable>(self, func: impl RpnFunction) -> Result<Option<T>> {
        let return_field_type = match self.return_field_type {
            Some(ft) => ft,
            None => T::EVAL_TYPE.into_certain_field_type_tp_for_test().into(),
        };
        let mut context = match self.context {
            Some(ctx) => ctx,
            None => EvalContext::default(),
        };
        let expr = self
            .rpn_expr_builder
            .push_fn_call(func, return_field_type)
            .build();
        let mut columns = LazyBatchColumnVec::empty();
        let ret = expr.eval(&mut context, 1, &[], &mut columns)?;
        match ret {
            // Only used in tests, so clone is fine.
            RpnStackNode::Scalar { value, .. } => Ok(T::borrow_scalar_value(value).clone()),
            RpnStackNode::Vector { value, .. } => {
                let value = value.as_ref();
                assert_eq!(value.len(), 1);
                Ok(T::borrow_vector_value(value)[0].clone())
            }
        }
    }
}
