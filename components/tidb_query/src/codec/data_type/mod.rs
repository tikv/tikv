// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

// FIXME: Move to tidb_query_datatype. Currently it refers some types in `crate::codec::mysql`
// so that it is not possible to move.

mod scalar;
mod vector;

// Concrete eval types without a nullable wrapper.
pub type Int = i64;
pub type Real = ordered_float::NotNan<f64>;
pub type Bytes = Vec<u8>;
pub use crate::codec::mysql::{Decimal, Duration, Json, Time as DateTime};

// Dynamic eval types.
pub use self::scalar::{ScalarValue, ScalarValueRef};
pub use self::vector::{VectorValue, VectorValueExt};

use tidb_query_datatype::{EvalType, FieldTypeTp};

use crate::codec::convert::ToInt;
use crate::expr::EvalContext;
use crate::Result;

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
        Ok(!self.is_empty() && self.to_int(context, FieldTypeTp::LongLong)? != 0)
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
pub trait Evaluable: Clone + std::fmt::Debug + Send + Sync + 'static {
    const EVAL_TYPE: EvalType;

    /// Borrows this concrete type from a `ScalarValue` in the same type.
    fn borrow_scalar_value(v: &ScalarValue) -> &Option<Self>;

    /// Borrows this concrete type from a `ScalarValueRef` in the same type.
    fn borrow_scalar_value_ref<'a>(v: &'a ScalarValueRef<'a>) -> &'a Option<Self>;

    /// Borrows a slice of this concrete type from a `VectorValue` in the same type.
    fn borrow_vector_value(v: &VectorValue) -> &[Option<Self>];

    /// Converts a vector of this concrete type into a `VectorValue` in the same type.
    fn into_vector_value(vec: Vec<Option<Self>>) -> VectorValue;
}

macro_rules! impl_evaluable_type {
    ($ty:tt) => {
        impl Evaluable for $ty {
            const EVAL_TYPE: EvalType = EvalType::$ty;

            #[inline]
            fn borrow_scalar_value(v: &ScalarValue) -> &Option<Self> {
                v.as_ref()
            }

            #[inline]
            fn borrow_scalar_value_ref<'a>(v: &'a ScalarValueRef<'a>) -> &'a Option<Self> {
                v.as_ref()
            }

            #[inline]
            fn borrow_vector_value(v: &VectorValue) -> &[Option<Self>] {
                v.as_ref()
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
