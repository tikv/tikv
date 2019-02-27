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

use super::types::{RpnRuntimeContext, RpnStackNode};
use crate::coprocessor::codec::data_type::{Evaluable, ScalarValue, VectorValue};

pub trait RpnFunction: std::fmt::Debug + Send + Sync + 'static {
    /// The display name of the function.
    fn name(&self) -> &'static str;

    /// The accepted argument length of this RPN function.
    ///
    /// Currently we do not support variable arguments.
    fn args_len(&self) -> usize;

    /// Evaluates the function according to given arguments.
    fn eval(
        &self,
        rows: usize,
        context: &mut RpnRuntimeContext,
        args: &[RpnStackNode],
    ) -> VectorValue;
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
        context: &mut RpnRuntimeContext,
        args: &[RpnStackNode],
    ) -> VectorValue {
        (**self).eval(rows, context, args)
    }
}

pub struct Helper;

impl Helper {
    #[inline(always)]
    pub fn eval_0_arg<Ret, F>(
        rows: usize,
        mut f: F,
        context: &mut RpnRuntimeContext,
        args: &[RpnStackNode],
    ) -> VectorValue
    where
        Ret: Evaluable,
        F: FnMut(&mut RpnRuntimeContext) -> Ret,
    {
        debug_assert_eq!(args.len(), 0);

        let mut result = Vec::with_capacity(rows);
        for _ in 0..rows {
            result.push(f(context));
        }
        Ret::into_vector_value(result)
    }

    #[inline(always)]
    pub fn eval_1_arg<Arg0, Ret, F>(
        rows: usize,
        mut f: F,
        context: &mut RpnRuntimeContext,
        args: &[RpnStackNode],
    ) -> VectorValue
    where
        Arg0: Evaluable,
        Ret: Evaluable,
        F: FnMut(&mut RpnRuntimeContext, &FieldType, &Arg0) -> Ret,
    {
        debug_assert_eq!(args.len(), 1);

        let mut result = Vec::with_capacity(rows);
        if args[0].is_scalar() {
            let v = Arg0::borrow_scalar_value(args[0].scalar_value());
            let field_type = args[0].field_type();
            for _ in 0..rows {
                result.push(f(context, field_type, v));
            }
        } else {
            let v = Arg0::borrow_vector_value(args[0].vector_value());
            debug_assert_eq!(rows, v.len());
            let field_type = args[0].field_type();
            for i in 0..rows {
                result.push(f(context, field_type, &v[i]));
            }
        }
        Ret::into_vector_value(result)
    }

    #[inline(always)]
    pub fn eval_2_args<Arg0, Arg1, Ret, F>(
        rows: usize,
        f: F,
        context: &mut RpnRuntimeContext,
        args: &[RpnStackNode],
    ) -> VectorValue
    where
        Arg0: Evaluable,
        Arg1: Evaluable,
        Ret: Evaluable,
        F: FnMut(&mut RpnRuntimeContext, &FieldType, &Arg0, &FieldType, &Arg1) -> Ret,
    {
        debug_assert_eq!(args.len(), 2);

        if args[0].is_scalar() {
            if args[1].is_scalar() {
                Self::eval_2_args_scalar_scalar(
                    rows,
                    f,
                    context,
                    args[0].field_type(),
                    args[0].scalar_value(),
                    args[1].field_type(),
                    args[1].scalar_value(),
                )
            } else {
                Self::eval_2_args_scalar_vector(
                    rows,
                    f,
                    context,
                    args[0].field_type(),
                    args[0].scalar_value(),
                    args[1].field_type(),
                    args[1].vector_value(),
                )
            }
        } else {
            if args[1].is_scalar() {
                Self::eval_2_args_vector_scalar(
                    rows,
                    f,
                    context,
                    args[0].field_type(),
                    args[0].vector_value(),
                    args[1].field_type(),
                    args[1].scalar_value(),
                )
            } else {
                Self::eval_2_args_vector_vector(
                    rows,
                    f,
                    context,
                    args[0].field_type(),
                    args[0].vector_value(),
                    args[1].field_type(),
                    args[1].vector_value(),
                )
            }
        }
    }

    #[inline(always)]
    fn eval_2_args_scalar_scalar<Arg0, Arg1, Ret, F>(
        rows: usize,
        mut f: F,
        context: &mut RpnRuntimeContext,
        lhs_field_type: &FieldType,
        lhs: &ScalarValue,
        rhs_field_type: &FieldType,
        rhs: &ScalarValue,
    ) -> VectorValue
    where
        Arg0: Evaluable,
        Arg1: Evaluable,
        Ret: Evaluable,
        F: FnMut(&mut RpnRuntimeContext, &FieldType, &Arg0, &FieldType, &Arg1) -> Ret,
    {
        let mut result = Vec::with_capacity(rows);
        let lhs = Arg0::borrow_scalar_value(lhs);
        let rhs = Arg1::borrow_scalar_value(rhs);
        for _ in 0..rows {
            result.push(f(context, lhs_field_type, lhs, rhs_field_type, rhs));
        }
        Ret::into_vector_value(result)
    }

    #[inline(always)]
    fn eval_2_args_scalar_vector<Arg0, Arg1, Ret, F>(
        rows: usize,
        mut f: F,
        context: &mut RpnRuntimeContext,
        lhs_field_type: &FieldType,
        lhs: &ScalarValue,
        rhs_field_type: &FieldType,
        rhs: &VectorValue,
    ) -> VectorValue
    where
        Arg0: Evaluable,
        Arg1: Evaluable,
        Ret: Evaluable,
        F: FnMut(&mut RpnRuntimeContext, &FieldType, &Arg0, &FieldType, &Arg1) -> Ret,
    {
        debug_assert_eq!(rows, rhs.len());
        let mut result = Vec::with_capacity(rows);
        let lhs = Arg0::borrow_scalar_value(lhs);
        let rhs = Arg1::borrow_vector_value(rhs);
        for i in 0..rows {
            result.push(f(context, lhs_field_type, lhs, rhs_field_type, &rhs[i]));
        }
        Ret::into_vector_value(result)
    }

    #[inline(always)]
    fn eval_2_args_vector_scalar<Arg0, Arg1, Ret, F>(
        rows: usize,
        mut f: F,
        context: &mut RpnRuntimeContext,
        lhs_field_type: &FieldType,
        lhs: &VectorValue,
        rhs_field_type: &FieldType,
        rhs: &ScalarValue,
    ) -> VectorValue
    where
        Arg0: Evaluable,
        Arg1: Evaluable,
        Ret: Evaluable,
        F: FnMut(&mut RpnRuntimeContext, &FieldType, &Arg0, &FieldType, &Arg1) -> Ret,
    {
        debug_assert_eq!(rows, lhs.len());
        let mut result = Vec::with_capacity(rows);
        let lhs = Arg0::borrow_vector_value(lhs);
        let rhs = Arg1::borrow_scalar_value(rhs);
        for i in 0..rows {
            result.push(f(context, lhs_field_type, &lhs[i], rhs_field_type, rhs));
        }
        Ret::into_vector_value(result)
    }

    #[inline(always)]
    fn eval_2_args_vector_vector<Arg0, Arg1, Ret, F>(
        rows: usize,
        mut f: F,
        context: &mut RpnRuntimeContext,
        lhs_field_type: &FieldType,
        lhs: &VectorValue,
        rhs_field_type: &FieldType,
        rhs: &VectorValue,
    ) -> VectorValue
    where
        Arg0: Evaluable,
        Arg1: Evaluable,
        Ret: Evaluable,
        F: FnMut(&mut RpnRuntimeContext, &FieldType, &Arg0, &FieldType, &Arg1) -> Ret,
    {
        debug_assert_eq!(rows, lhs.len());
        debug_assert_eq!(rows, rhs.len());
        let mut result = Vec::with_capacity(rows);
        let lhs = Arg0::borrow_vector_value(lhs);
        let rhs = Arg1::borrow_vector_value(rhs);
        for i in 0..rows {
            result.push(f(context, lhs_field_type, &lhs[i], rhs_field_type, &rhs[i]));
        }
        Ret::into_vector_value(result)
    }

    #[inline(always)]
    pub fn eval_3_args<Arg0, Arg1, Arg2, Ret, F>(
        rows: usize,
        mut f: F,
        context: &mut RpnRuntimeContext,
        args: &[RpnStackNode],
    ) -> VectorValue
    where
        Arg0: Evaluable,
        Arg1: Evaluable,
        Arg2: Evaluable,
        Ret: Evaluable,
        F: FnMut(
            &mut RpnRuntimeContext,
            &FieldType,
            &Arg0,
            &FieldType,
            &Arg1,
            &FieldType,
            &Arg2,
        ) -> Ret,
    {
        debug_assert_eq!(args.len(), 0);

        let mut result = Vec::with_capacity(rows);
        let ft_arg0 = args[0].field_type();
        let ft_arg1 = args[1].field_type();
        let ft_arg2 = args[2].field_type();
        let s_arg0 = Arg0::borrow_vector_like_specialized(args[0].as_vector_like());
        let s_arg1 = Arg1::borrow_vector_like_specialized(args[1].as_vector_like());
        let s_arg2 = Arg2::borrow_vector_like_specialized(args[2].as_vector_like());
        for i in 0..rows {
            result.push(f(
                context, ft_arg0, &s_arg0[i], ft_arg1, &s_arg1[i], ft_arg2, &s_arg2[i],
            ));
        }
        Ret::into_vector_value(result)
    }
}

#[doc(hidden)]
#[macro_export]
macro_rules! impl_template_fn {
    (0 arg @ $name:ident) => {
        impl_template_fn! { @inner $name, 0, eval_0_arg }
    };
    (1 arg @ $name:ident) => {
        impl_template_fn! { @inner $name, 1, eval_1_arg }
    };
    (2 arg @ $name:ident) => {
        impl_template_fn! { @inner $name, 2, eval_2_args }
    };
    (3 arg @ $name:ident) => {
        impl_template_fn! { @inner $name, 3, eval_3_args }
    };
    (@inner $name:ident, $args:expr, $eval_fn:ident) => {
        impl $crate::coprocessor::dag::rpn_expr::RpnFunction for $name {
            #[inline]
            fn name(&self) -> &'static str {
                stringify!($name)
            }

            #[inline]
            fn args_len(&self) -> usize {
                $args
            }

            #[inline]
            fn eval(
                &self,
                rows: usize,
                context: &mut $crate::coprocessor::dag::rpn_expr::RpnRuntimeContext,
                args: &[$crate::coprocessor::dag::rpn_expr::types::RpnStackNode],
            ) -> $crate::coprocessor::codec::data_type::VectorValue {
                $crate::coprocessor::dag::rpn_expr::function::Helper::$eval_fn(
                    rows,
                    Self::call,
                    context,
                    args,
                )
            }
        }
    };
}
