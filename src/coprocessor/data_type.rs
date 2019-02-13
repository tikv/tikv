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

// FIXME: Move to cop_datatype.

use cop_datatype::EvalType;

pub type Int = i64;
pub type Real = f64;
pub type Bytes = Vec<u8>;
pub use crate::coprocessor::codec::batch::{
    BatchColumn as VectorValue, BatchColumnRef as VectorValueRef,
};
pub use crate::coprocessor::codec::mysql::{Decimal, Duration, Json, Time as DateTime};

pub trait AsBool {
    fn as_bool(&self) -> bool;
}

impl AsBool for Option<Int> {
    #[inline]
    fn as_bool(&self) -> bool {
        match self {
            None => false,
            Some(ref v) => *v != 0,
        }
    }
}

impl AsBool for Option<Real> {
    #[inline]
    fn as_bool(&self) -> bool {
        match self {
            None => false,
            Some(ref v) => v.round() != 0f64,
        }
    }
}

impl AsBool for Option<Decimal> {
    #[inline]
    fn as_bool(&self) -> bool {
        match self {
            None => false,
            // FIXME: No unwrap??
            Some(ref v) => v.as_f64().unwrap().round() != 0f64,
        }
    }
}

impl AsBool for Option<Bytes> {
    #[inline]
    fn as_bool(&self) -> bool {
        match self {
            None => false,
            // FIXME: No unwrap?? No without_context??
            Some(ref v) => {
                !v.is_empty()
                    && crate::coprocessor::codec::convert::bytes_to_int_without_context(v).unwrap()
                        != 0
            }
        }
    }
}

impl AsBool for Option<DateTime> {
    #[inline]
    fn as_bool(&self) -> bool {
        match self {
            None => false,
            Some(ref v) => !v.is_zero(),
        }
    }
}

impl AsBool for Option<Duration> {
    #[inline]
    fn as_bool(&self) -> bool {
        match self {
            None => false,
            Some(ref v) => !v.is_zero(),
        }
    }
}

impl AsBool for Option<Json> {
    #[inline]
    fn as_bool(&self) -> bool {
        // FIXME: Is it correct?
        false
    }
}

#[derive(Debug)]
pub enum ScalarValue {
    Int(Option<Int>),
    Real(Option<Real>),
    Decimal(Option<Decimal>),
    Bytes(Option<Bytes>),
    DateTime(Option<DateTime>),
    Duration(Option<Duration>),
    Json(Option<Json>),
}

#[derive(Debug, Clone, Copy)]
pub enum ScalarValueRef<'a> {
    Int(&'a Option<Int>),
    Real(&'a Option<Real>),
    Decimal(&'a Option<Decimal>),
    Bytes(&'a Option<Bytes>),
    DateTime(&'a Option<DateTime>),
    Duration(&'a Option<Duration>),
    Json(&'a Option<Json>),
}

impl ScalarValue {
    #[inline]
    pub fn eval_type(&self) -> EvalType {
        match self {
            ScalarValue::Int(_) => EvalType::Int,
            ScalarValue::Real(_) => EvalType::Real,
            ScalarValue::Decimal(_) => EvalType::Decimal,
            ScalarValue::Bytes(_) => EvalType::Bytes,
            ScalarValue::DateTime(_) => EvalType::DateTime,
            ScalarValue::Duration(_) => EvalType::Duration,
            ScalarValue::Json(_) => EvalType::Json,
        }
    }

    #[inline]
    pub fn borrow(&self) -> ScalarValueRef {
        match self {
            ScalarValue::Int(ref v) => ScalarValueRef::Int(v),
            ScalarValue::Real(ref v) => ScalarValueRef::Real(v),
            ScalarValue::Decimal(ref v) => ScalarValueRef::Decimal(v),
            ScalarValue::Bytes(ref v) => ScalarValueRef::Bytes(v),
            ScalarValue::DateTime(ref v) => ScalarValueRef::DateTime(v),
            ScalarValue::Duration(ref v) => ScalarValueRef::Duration(v),
            ScalarValue::Json(ref v) => ScalarValueRef::Json(v),
        }
    }
}

impl AsBool for ScalarValue {
    #[inline]
    fn as_bool(&self) -> bool {
        match self {
            ScalarValue::Int(ref v) => v.as_bool(),
            ScalarValue::Real(ref v) => v.as_bool(),
            ScalarValue::Decimal(ref v) => v.as_bool(),
            ScalarValue::Bytes(ref v) => v.as_bool(),
            ScalarValue::DateTime(ref v) => v.as_bool(),
            ScalarValue::Duration(ref v) => v.as_bool(),
            ScalarValue::Json(ref v) => v.as_bool(),
        }
    }
}

impl<'a> AsBool for ScalarValueRef<'a> {
    #[inline]
    fn as_bool(&self) -> bool {
        match self {
            ScalarValueRef::Int(ref v) => v.as_bool(),
            ScalarValueRef::Real(ref v) => v.as_bool(),
            ScalarValueRef::Decimal(ref v) => v.as_bool(),
            ScalarValueRef::Bytes(ref v) => v.as_bool(),
            ScalarValueRef::DateTime(ref v) => v.as_bool(),
            ScalarValueRef::Duration(ref v) => v.as_bool(),
            ScalarValueRef::Json(ref v) => v.as_bool(),
        }
    }
}

impl<'a> ScalarValueRef<'a> {
    #[inline]
    pub fn eval_type(&self) -> EvalType {
        match self {
            ScalarValueRef::Int(_) => EvalType::Int,
            ScalarValueRef::Real(_) => EvalType::Real,
            ScalarValueRef::Decimal(_) => EvalType::Decimal,
            ScalarValueRef::Bytes(_) => EvalType::Bytes,
            ScalarValueRef::DateTime(_) => EvalType::DateTime,
            ScalarValueRef::Duration(_) => EvalType::Duration,
            ScalarValueRef::Json(_) => EvalType::Json,
        }
    }
}

macro_rules! impl_scalar_value {
    ($ty:tt, $name:ident, $mut_name:ident) => {
        impl ScalarValue {
            #[inline]
            pub fn $name(&self) -> &Option<$ty> {
                match self {
                    ScalarValue::$ty(ref v) => v,
                    other => panic!(
                        "Cannot cast {} scalar value into {}",
                        other.eval_type(),
                        stringify!($tt),
                    ),
                }
            }

            #[inline]
            pub fn $mut_name(&mut self) -> &mut Option<$ty> {
                match self {
                    ScalarValue::$ty(ref mut v) => v,
                    other => panic!(
                        "Cannot cast {} scalar value into {}",
                        other.eval_type(),
                        stringify!($tt),
                    ),
                }
            }
        }

        impl AsRef<Option<$ty>> for ScalarValue {
            #[inline]
            fn as_ref(&self) -> &Option<$ty> {
                self.$name()
            }
        }

        impl AsMut<Option<$ty>> for ScalarValue {
            #[inline]
            fn as_mut(&mut self) -> &mut Option<$ty> {
                self.$mut_name()
            }
        }
    };
}

impl_scalar_value! { Int, as_int, as_mut_int }
impl_scalar_value! { Real, as_real, as_mut_real }
impl_scalar_value! { Decimal, as_decimal, as_mut_decimal }
impl_scalar_value! { Bytes, as_bytes, as_mut_bytes }
impl_scalar_value! { DateTime, as_date_time, as_mut_date_time }
impl_scalar_value! { Duration, as_duration, as_mut_duration }
impl_scalar_value! { Json, as_json, as_mut_json }

macro_rules! impl_scalar_value_ref {
    ($ty:tt) => {
        impl<'a> Into<&'a Option<$ty>> for ScalarValueRef<'a> {
            #[inline]
            fn into(self) -> &'a Option<$ty> {
                match self {
                    ScalarValueRef::$ty(ref v) => v,
                    other => panic!(
                        "Cannot cast {} scalar value ref into {}",
                        other.eval_type(),
                        stringify!($tt),
                    ),
                }
            }
        }
    };
}

impl_scalar_value_ref! { Int }
impl_scalar_value_ref! { Real }
impl_scalar_value_ref! { Decimal }
impl_scalar_value_ref! { Bytes }
impl_scalar_value_ref! { DateTime }
impl_scalar_value_ref! { Duration }
impl_scalar_value_ref! { Json }

pub trait Evaluable: Clone {
    fn coerce_scalar_value_ref_as_slice(v: ScalarValueRef) -> &Self;

    fn coerce_vector_value_ref_as_slice(v: VectorValueRef) -> &[Self];

    fn coerce_to_vector_value(vec: Vec<Self>) -> VectorValue;
}

macro_rules! impl_evaluable_type {
    ($ty:tt) => {
        impl Evaluable for Option<$ty> {
            #[inline]
            fn coerce_scalar_value_ref_as_slice(v: ScalarValueRef) -> &Self {
                v.into()
            }

            #[inline]
            fn coerce_vector_value_ref_as_slice(v: VectorValueRef) -> &[Self] {
                v.into()
            }

            #[inline]
            fn coerce_to_vector_value(vec: Vec<Self>) -> VectorValue {
                VectorValue::from(vec)
            }
        }
    };
}

impl_evaluable_type! { Int }
impl_evaluable_type! { Real }
impl_evaluable_type! { Decimal }
impl_evaluable_type! { Bytes }
impl_evaluable_type! { DateTime }
impl_evaluable_type! { Duration }
impl_evaluable_type! { Json }
