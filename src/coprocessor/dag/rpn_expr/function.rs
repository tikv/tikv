// Copyright 2018 PingCAP, Inc.
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

use std::convert::TryFrom;

use tipb::expression::{FieldType, ScalarFuncSig};

use super::types::{RpnExpressionEvalContext, RpnStackNode};
use super::{impl_compare, impl_dummy, impl_op};
use crate::coprocessor::codec::data_type::{Evaluable, ScalarValue, VectorValue};
use crate::coprocessor::Error;

// TODO: Merge into ScalarFuncSig
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum RpnFunction {
    Dummy0ArgFunc,  // Optimized for type expand
    Dummy1ArgFunc,  // Optimized for scalar expand + type expand
    Dummy2ArgsFunc, // Optimized for scalar expand + type expand
    //    Dummy3ArgsFunc,  // Optimized for type expand
    //    Dummy4OrMoreArgsFunc,   // Optimized for type
    //    DummyVariableArgsFunc,
    EQReal,
    EQInt,
    GTInt,
    LTInt,
    LogicalAnd,
    LogicalOr,
}

impl TryFrom<ScalarFuncSig> for RpnFunction {
    type Error = Error;

    fn try_from(value: ScalarFuncSig) -> Result<Self, Error> {
        match value {
            ScalarFuncSig::EQReal => Ok(RpnFunction::EQReal),
            ScalarFuncSig::EQInt => Ok(RpnFunction::EQInt),
            ScalarFuncSig::GTInt => Ok(RpnFunction::GTInt),
            ScalarFuncSig::LTInt => Ok(RpnFunction::LTInt),
            ScalarFuncSig::LogicalAnd => Ok(RpnFunction::LogicalAnd),
            ScalarFuncSig::LogicalOr => Ok(RpnFunction::LogicalOr),
            v => Err(box_err!(
                "ScalarFunction {:?} is not supported in batch mode",
                v
            )),
        }
    }
}

impl RpnFunction {
    pub fn args_len(self) -> usize {
        match self {
            RpnFunction::Dummy0ArgFunc => 0,
            RpnFunction::Dummy1ArgFunc => 1,
            RpnFunction::Dummy2ArgsFunc => 2,
            RpnFunction::EQReal => 2,
            RpnFunction::EQInt => 2,
            RpnFunction::GTInt => 2,
            RpnFunction::LTInt => 2,
            RpnFunction::LogicalAnd => 2,
            RpnFunction::LogicalOr => 2,
        }
    }

    pub fn eval(
        self,
        rows: usize,
        context: &mut RpnExpressionEvalContext,
        args: &[RpnStackNode],
    ) -> VectorValue {
        debug_assert_eq!(args.len(), self.args_len());
        match self {
            RpnFunction::Dummy0ArgFunc => {
                Self::eval_0_arg(rows, impl_dummy::dummy_0_arg_func, context)
            }
            RpnFunction::Dummy1ArgFunc => {
                Self::eval_1_arg(rows, impl_dummy::dummy_1_arg_func, context, &args[0])
            }
            RpnFunction::Dummy2ArgsFunc => Self::eval_2_args(
                rows,
                impl_dummy::dummy_2_args_func,
                context,
                &args[0],
                &args[1],
            ),
            RpnFunction::EQReal => {
                Self::eval_2_args(rows, impl_compare::eq_real, context, &args[0], &args[1])
            }
            RpnFunction::EQInt => {
                Self::eval_2_args(rows, impl_compare::eq_int, context, &args[0], &args[1])
            }
            RpnFunction::GTInt => {
                Self::eval_2_args(rows, impl_compare::gt_int, context, &args[0], &args[1])
            }
            RpnFunction::LTInt => {
                Self::eval_2_args(rows, impl_compare::lt_int, context, &args[0], &args[1])
            }
            RpnFunction::LogicalAnd => {
                Self::eval_2_args(rows, impl_op::logical_and, context, &args[0], &args[1])
            }
            RpnFunction::LogicalOr => {
                Self::eval_2_args(rows, impl_op::logical_or, context, &args[0], &args[1])
            }
        }
    }

    #[inline(always)]
    fn eval_0_arg<Ret, F>(
        rows: usize,
        mut f: F,
        context: &mut RpnExpressionEvalContext,
    ) -> VectorValue
    where
        Ret: Evaluable,
        F: FnMut(&mut RpnExpressionEvalContext) -> Ret,
    {
        let mut result = Vec::with_capacity(rows);
        for _ in 0..rows {
            result.push(f(context));
        }
        Ret::coerce_to_vector_value(result)
    }

    #[inline(always)]
    fn eval_1_arg<Arg0, Ret, F>(
        rows: usize,
        mut f: F,
        context: &mut RpnExpressionEvalContext,
        arg0: &RpnStackNode,
    ) -> VectorValue
    where
        Arg0: Evaluable,
        Ret: Evaluable,
        F: FnMut(&mut RpnExpressionEvalContext, &FieldType, &Arg0) -> Ret,
    {
        let mut result = Vec::with_capacity(rows);
        if arg0.is_scalar() {
            let v = Arg0::coerce_scalar_value_ref_as_slice(arg0.scalar_value());
            let field_type = arg0.field_type();
            for _ in 0..rows {
                result.push(f(context, field_type, v));
            }
        } else {
            let v = Arg0::coerce_vector_value_ref_as_slice(arg0.vector_value());
            let field_type = arg0.field_type();
            for i in 0..rows {
                result.push(f(context, field_type, &v[i]));
            }
        }
        Ret::coerce_to_vector_value(result)
    }

    #[inline(always)]
    fn eval_2_args<Arg0, Arg1, Ret, F>(
        rows: usize,
        f: F,
        context: &mut RpnExpressionEvalContext,
        arg0: &RpnStackNode,
        arg1: &RpnStackNode,
    ) -> VectorValue
    where
        Arg0: Evaluable,
        Arg1: Evaluable,
        Ret: Evaluable,
        F: FnMut(&mut RpnExpressionEvalContext, &FieldType, &Arg0, &FieldType, &Arg1) -> Ret,
    {
        if arg0.is_scalar() {
            if arg1.is_scalar() {
                Self::eval_2_args_scalar_scalar(
                    rows,
                    f,
                    context,
                    arg0.field_type(),
                    arg0.scalar_value(),
                    arg1.field_type(),
                    arg1.scalar_value(),
                )
            } else {
                Self::eval_2_args_scalar_vector(
                    rows,
                    f,
                    context,
                    arg0.field_type(),
                    arg0.scalar_value(),
                    arg1.field_type(),
                    arg1.vector_value(),
                )
            }
        } else {
            if arg1.is_scalar() {
                Self::eval_2_args_vector_scalar(
                    rows,
                    f,
                    context,
                    arg0.field_type(),
                    arg0.vector_value(),
                    arg1.field_type(),
                    arg1.scalar_value(),
                )
            } else {
                Self::eval_2_args_vector_vector(
                    rows,
                    f,
                    context,
                    arg0.field_type(),
                    arg0.vector_value(),
                    arg1.field_type(),
                    arg1.vector_value(),
                )
            }
        }
    }

    #[inline(always)]
    fn eval_2_args_scalar_scalar<Arg0, Arg1, Ret, F>(
        rows: usize,
        mut f: F,
        context: &mut RpnExpressionEvalContext,
        lhs_field_type: &FieldType,
        lhs: &ScalarValue,
        rhs_field_type: &FieldType,
        rhs: &ScalarValue,
    ) -> VectorValue
    where
        Arg0: Evaluable,
        Arg1: Evaluable,
        Ret: Evaluable,
        F: FnMut(&mut RpnExpressionEvalContext, &FieldType, &Arg0, &FieldType, &Arg1) -> Ret,
    {
        let mut result = Vec::with_capacity(rows);
        let lhs = Arg0::coerce_scalar_value_ref_as_slice(lhs);
        let rhs = Arg1::coerce_scalar_value_ref_as_slice(rhs);
        for _ in 0..rows {
            result.push(f(context, lhs_field_type, lhs, rhs_field_type, rhs));
        }
        Ret::coerce_to_vector_value(result)
    }

    #[inline(always)]
    fn eval_2_args_scalar_vector<Arg0, Arg1, Ret, F>(
        rows: usize,
        mut f: F,
        context: &mut RpnExpressionEvalContext,
        lhs_field_type: &FieldType,
        lhs: &ScalarValue,
        rhs_field_type: &FieldType,
        rhs: &VectorValue,
    ) -> VectorValue
    where
        Arg0: Evaluable,
        Arg1: Evaluable,
        Ret: Evaluable,
        F: FnMut(&mut RpnExpressionEvalContext, &FieldType, &Arg0, &FieldType, &Arg1) -> Ret,
    {
        debug_assert_eq!(rows, rhs.len());
        let mut result = Vec::with_capacity(rows);
        let lhs = Arg0::coerce_scalar_value_ref_as_slice(lhs);
        let rhs = Arg1::coerce_vector_value_ref_as_slice(rhs);
        for i in 0..rows {
            result.push(f(context, lhs_field_type, lhs, rhs_field_type, &rhs[i]));
        }
        Ret::coerce_to_vector_value(result)
    }

    #[inline(always)]
    fn eval_2_args_vector_scalar<Arg0, Arg1, Ret, F>(
        rows: usize,
        mut f: F,
        context: &mut RpnExpressionEvalContext,
        lhs_field_type: &FieldType,
        lhs: &VectorValue,
        rhs_field_type: &FieldType,
        rhs: &ScalarValue,
    ) -> VectorValue
    where
        Arg0: Evaluable,
        Arg1: Evaluable,
        Ret: Evaluable,
        F: FnMut(&mut RpnExpressionEvalContext, &FieldType, &Arg0, &FieldType, &Arg1) -> Ret,
    {
        debug_assert_eq!(rows, lhs.len());
        let mut result = Vec::with_capacity(rows);
        let lhs = Arg0::coerce_vector_value_ref_as_slice(lhs);
        let rhs = Arg1::coerce_scalar_value_ref_as_slice(rhs);
        for i in 0..rows {
            result.push(f(context, lhs_field_type, &lhs[i], rhs_field_type, rhs));
        }
        Ret::coerce_to_vector_value(result)
    }

    #[inline(always)]
    fn eval_2_args_vector_vector<Arg0, Arg1, Ret, F>(
        rows: usize,
        mut f: F,
        context: &mut RpnExpressionEvalContext,
        lhs_field_type: &FieldType,
        lhs: &VectorValue,
        rhs_field_type: &FieldType,
        rhs: &VectorValue,
    ) -> VectorValue
    where
        Arg0: Evaluable,
        Arg1: Evaluable,
        Ret: Evaluable,
        F: FnMut(&mut RpnExpressionEvalContext, &FieldType, &Arg0, &FieldType, &Arg1) -> Ret,
    {
        debug_assert_eq!(rows, lhs.len());
        debug_assert_eq!(rows, rhs.len());
        let mut result = Vec::with_capacity(rows);
        let lhs = Arg0::coerce_vector_value_ref_as_slice(lhs);
        let rhs = Arg1::coerce_vector_value_ref_as_slice(rhs);
        for i in 0..rows {
            result.push(f(context, lhs_field_type, &lhs[i], rhs_field_type, &rhs[i]));
        }
        Ret::coerce_to_vector_value(result)
    }
}
