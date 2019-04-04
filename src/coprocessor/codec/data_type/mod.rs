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

// FIXME: Move to cop_datatype. Currently it refers some types in `crate::coprocessor::codec::mysql`
// so that it is not possible to move.

mod scalar;
mod vector;
mod vector_like;

// Concrete eval types without a nullable wrapper.
pub type Int = i64;
pub type Real = f64;
pub type Bytes = Vec<u8>;
pub use crate::coprocessor::codec::mysql::{Decimal, Duration, Json, Time as DateTime};

// Dynamic eval types.
pub use self::scalar::ScalarValue;
pub use self::vector::{VectorValue, VectorValueExt};
pub use self::vector_like::{VectorLikeValueRef, VectorLikeValueRefSpecialized};

use crate::coprocessor::dag::expr::EvalContext;
use crate::coprocessor::Result;

/// A trait of evaluating current concrete eval type into a MySQL logic value, represented by
/// Rust's `bool` type.
pub trait AsMySQLBool {
    /// Evaluates into a MySQL logic value.
    fn as_mysql_bool(&self, context: &mut EvalContext) -> Result<bool>;
}

impl AsMySQLBool for Int {
    #[inline]
    fn as_mysql_bool(&self, _context: &mut EvalContext) -> Result<bool> {
        Ok(*self != 0)
    }
}

impl AsMySQLBool for Real {
    #[inline]
    fn as_mysql_bool(&self, _context: &mut EvalContext) -> Result<bool> {
        Ok(self.round() != 0f64)
    }
}

impl AsMySQLBool for Bytes {
    #[inline]
    fn as_mysql_bool(&self, context: &mut EvalContext) -> Result<bool> {
        Ok(!self.is_empty()
            && crate::coprocessor::codec::convert::bytes_to_int(context, self)? != 0)
    }
}

impl<T> AsMySQLBool for Option<T>
where
    T: AsMySQLBool,
{
    fn as_mysql_bool(&self, context: &mut EvalContext) -> Result<bool> {
        match self {
            None => Ok(false),
            Some(ref v) => v.as_mysql_bool(context),
        }
    }
}

/// A trait of all types that can be used during evaluation (eval type).
pub trait Evaluable: Clone + std::fmt::Debug + Send + 'static {
    /// Borrows this concrete type from a `ScalarValue` in the same type.
    fn borrow_scalar_value(v: &ScalarValue) -> &Option<Self>;

    /// Borrows a slice of this concrete type from a `VectorValue` in the same type.
    fn borrow_vector_value(v: &VectorValue) -> &[Option<Self>];

    /// Borrows a specialized reference from a `VectorLikeValueRef`. The specialized reference is
    /// also vector-like but contains the concrete type information, which doesn't need type
    /// checks (but needs vector/scalar checks) when accessing.
    fn borrow_vector_like_specialized(
        v: VectorLikeValueRef<'_>,
    ) -> VectorLikeValueRefSpecialized<'_, Self>;

    /// Converts a vector of this concrete type into a `VectorValue` in the same type.
    fn into_vector_value(vec: Vec<Option<Self>>) -> VectorValue;
}

macro_rules! impl_evaluable_type {
    ($ty:tt) => {
        impl Evaluable for $ty {
            #[inline]
            fn borrow_scalar_value(v: &ScalarValue) -> &Option<Self> {
                v.as_ref()
            }

            #[inline]
            fn borrow_vector_value(v: &VectorValue) -> &[Option<Self>] {
                v.as_ref()
            }

            #[inline]
            fn borrow_vector_like_specialized(
                v: VectorLikeValueRef<'_>,
            ) -> VectorLikeValueRefSpecialized<'_, Self> {
                v.into()
            }

            #[inline]
            fn into_vector_value(vec: Vec<Option<Self>>) -> VectorValue {
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
