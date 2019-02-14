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
pub use crate::coprocessor::codec::batch::BatchColumn as VectorValue;
pub use crate::coprocessor::codec::mysql::{Decimal, Duration, Json, Time as DateTime};

/// A trait of evaluating current type into a MySQL logic value, represented by Rust's `bool` type.
pub trait AsMySQLBool {
    fn as_mysql_bool(&self) -> bool;
}

impl AsMySQLBool for Int {
    #[inline]
    fn as_mysql_bool(&self) -> bool {
        *self != 0
    }
}

impl AsMySQLBool for Real {
    #[inline]
    fn as_mysql_bool(&self) -> bool {
        self.round() != 0f64
    }
}

impl AsMySQLBool for Bytes {
    #[inline]
    fn as_mysql_bool(&self) -> bool {
        // FIXME: No unwrap?? No without_context??
        !self.is_empty()
            && crate::coprocessor::codec::convert::bytes_to_int_without_context(self).unwrap() != 0
    }
}

impl<T> AsMySQLBool for Option<T>
where
    T: AsMySQLBool,
{
    fn as_mysql_bool(&self) -> bool {
        match self {
            None => false,
            Some(ref v) => v.as_mysql_bool(),
        }
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
}

impl AsMySQLBool for ScalarValue {
    #[inline]
    fn as_mysql_bool(&self) -> bool {
        match self {
            ScalarValue::Int(ref v) => v.as_mysql_bool(),
            ScalarValue::Real(ref v) => v.as_mysql_bool(),
            ScalarValue::Decimal(ref v) => v.as_mysql_bool(),
            ScalarValue::Bytes(ref v) => v.as_mysql_bool(),
            ScalarValue::DateTime(ref v) => v.as_mysql_bool(),
            ScalarValue::Duration(ref v) => v.as_mysql_bool(),
            ScalarValue::Json(ref v) => v.as_mysql_bool(),
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

pub trait Evaluable: Clone {
    fn coerce_scalar_value_ref_as_slice(v: &ScalarValue) -> &Self;

    fn coerce_vector_value_ref_as_slice(v: &VectorValue) -> &[Self];

    fn coerce_to_vector_value(vec: Vec<Self>) -> VectorValue;
}

macro_rules! impl_evaluable_type {
    ($ty:tt) => {
        impl Evaluable for Option<$ty> {
            #[inline]
            fn coerce_scalar_value_ref_as_slice(v: &ScalarValue) -> &Self {
                v.as_ref()
            }

            #[inline]
            fn coerce_vector_value_ref_as_slice(v: &VectorValue) -> &[Self] {
                v.as_ref()
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
