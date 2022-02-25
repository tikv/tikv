// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::any::Any;

use tipb::{Expr, ExprType, FieldType, ScalarFuncSig};

use crate::types::function::RpnFnMeta;
use crate::RpnExpressionBuilder;
use tidb_query_common::Result;
use tidb_query_datatype::codec::batch::LazyBatchColumnVec;
use tidb_query_datatype::codec::data_type::{EvaluableRet, ScalarValue};
use tidb_query_datatype::expr::EvalContext;

/// Helper utility to evaluate RPN function over scalar inputs.
///
/// This structure should be only useful in tests because it is not very efficient.
pub struct RpnFnScalarEvaluator {
    rpn_expr_builder: RpnExpressionBuilder,
    return_field_type: Option<FieldType>,
    context: Option<EvalContext>,
    metadata: Option<Box<dyn Any + Send>>,
}

impl RpnFnScalarEvaluator {
    /// Creates a new `RpnFnScalarEvaluator`.
    pub fn new() -> Self {
        Self {
            rpn_expr_builder: RpnExpressionBuilder::new_for_test(),
            return_field_type: None,
            context: None,
            metadata: None,
        }
    }

    /// Pushes a parameter as the value of an argument for evaluation. The field type will be auto
    /// inferred by choosing an arbitrary field type that matches the field type of the given
    /// value.
    #[must_use]
    pub fn push_param(mut self, value: impl Into<ScalarValue>) -> Self {
        self.rpn_expr_builder = self.rpn_expr_builder.push_constant_for_test(value);
        self
    }

    #[must_use]
    pub fn push_params(mut self, values: impl IntoIterator<Item = impl Into<ScalarValue>>) -> Self {
        for value in values {
            self.rpn_expr_builder = self.rpn_expr_builder.push_constant_for_test(value);
        }
        self
    }

    /// Pushes a parameter as the value of an argument for evaluation using a specified field type.
    #[must_use]
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
    #[must_use]
    pub fn return_field_type(mut self, field_type: impl Into<FieldType>) -> Self {
        self.return_field_type = Some(field_type.into());
        self
    }

    /// Sets the context to use during evaluation.
    ///
    /// If not set, a default `EvalContext` will be used.
    #[must_use]
    pub fn context(mut self, context: impl Into<EvalContext>) -> Self {
        self.context = Some(context.into());
        self
    }

    /// Sets the metadata to use during evaluation.
    #[must_use]
    pub fn metadata(mut self, metadata: Box<dyn Any + Send>) -> Self {
        self.metadata = Some(metadata);
        self
    }

    /// Evaluates the given function.
    ///
    /// Note that this function does not respect previous `return_field_type()` call.
    ///
    /// This function exposes low-level evaluate results. Prefer to use `evaluate()` instead for
    /// normal use case.
    pub fn evaluate_raw(
        self,
        ret_field_type: impl Into<FieldType>,
        sig: ScalarFuncSig,
    ) -> (Result<ScalarValue>, EvalContext) {
        let mut context = match self.context {
            Some(ctx) => ctx,
            None => EvalContext::default(),
        };

        // Children expr descriptors are needed to map the signature into the actual function impl.
        let children_ed: Vec<_> = self
            .rpn_expr_builder
            .as_ref()
            .iter()
            .map(|expr_node| {
                let mut ed = Expr::default();
                ed.set_field_type(expr_node.field_type().clone());
                ed.set_tp(expr_node.expr_tp());
                ed
            })
            .collect();

        let ret_field_type = ret_field_type.into();
        let mut fun_sig_expr = Expr::default();
        fun_sig_expr.set_sig(sig);
        fun_sig_expr.set_children(children_ed.clone().into());
        fun_sig_expr.set_field_type(ret_field_type.clone());
        fun_sig_expr.set_tp(ExprType::ScalarFunc);

        // use validator_ptr to testing the test arguments.
        let func: RpnFnMeta = super::super::map_expr_node_to_rpn_func(&fun_sig_expr).unwrap();

        if let Err(e) = (func.validator_ptr)(&fun_sig_expr) {
            return (Err(e), context);
        }

        let metadata = match self.metadata {
            Some(metadata) => metadata,
            None => match (func.metadata_expr_ptr)(&mut fun_sig_expr) {
                Ok(metadata) => metadata,
                Err(e) => {
                    return (Err(e), context);
                }
            },
        };
        let expr = self
            .rpn_expr_builder
            .push_fn_call_with_metadata(func, children_ed.len(), ret_field_type, metadata)
            .build_for_test();

        let mut columns = LazyBatchColumnVec::empty();
        let ret = expr.eval(&mut context, &[], &mut columns, &[0], 1);
        let result = ret.map(|ret| ret.get_logical_scalar_ref(0).to_owned());

        (result, context)
    }

    /// Evaluates the given function by using collected parameters.
    pub fn evaluate<T: EvaluableRet>(self, sig: ScalarFuncSig) -> Result<Option<T>>
    where
        Option<T>: From<ScalarValue>,
    {
        let return_field_type = match &self.return_field_type {
            Some(ft) => ft.clone(),
            None => T::EVAL_TYPE.into_certain_field_type_tp_for_test().into(),
        };
        let result = self.evaluate_raw(return_field_type, sig).0;
        result.map(|v| v.into())
    }
}

impl Default for RpnFnScalarEvaluator {
    fn default() -> Self {
        Self::new()
    }
}
