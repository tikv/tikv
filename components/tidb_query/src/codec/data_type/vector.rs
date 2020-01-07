// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use tidb_query_datatype::{EvalType, FieldTypeAccessor};
use tipb::FieldType;

use super::*;
use crate::codec::data_type::scalar::ScalarValueRef;
use crate::codec::mysql::decimal::DECIMAL_STRUCT_SIZE;
use crate::codec::Result;

/// A vector value container, a.k.a. column, for all concrete eval types.
///
/// The inner concrete value is immutable. However it is allowed to push and remove values from
/// this vector container.
#[derive(Debug, PartialEq, Clone)]
pub enum VectorValue {
    Int(Vec<Option<Int>>),
    Real(Vec<Option<Real>>),
    Decimal(Vec<Option<Decimal>>),
    // TODO: We need to improve its performance, i.e. store strings in adjacent memory places
    Bytes(Vec<Option<Bytes>>),
    DateTime(Vec<Option<DateTime>>),
    Duration(Vec<Option<Duration>>),
    Json(Vec<Option<Json>>),
}

impl VectorValue {
    /// Creates an empty `VectorValue` according to `eval_tp` and reserves capacity according
    /// to `capacity`.
    #[inline]
    pub fn with_capacity(capacity: usize, eval_tp: EvalType) -> Self {
        match_template_evaluable! {
            TT, match eval_tp {
                EvalType::TT => VectorValue::TT(Vec::with_capacity(capacity)),
            }
        }
    }

    /// Creates a new empty `VectorValue` with the same eval type.
    #[inline]
    pub fn clone_empty(&self, capacity: usize) -> Self {
        match_template_evaluable! {
            TT, match self {
                VectorValue::TT(_) => VectorValue::TT(Vec::with_capacity(capacity)),
            }
        }
    }

    /// Returns the `EvalType` used to construct current column.
    #[inline]
    pub fn eval_type(&self) -> EvalType {
        match_template_evaluable! {
            TT, match self {
                VectorValue::TT(_) => EvalType::TT,
            }
        }
    }

    /// Returns the number of datums contained in this column.
    #[inline]
    pub fn len(&self) -> usize {
        match_template_evaluable! {
            TT, match self {
                VectorValue::TT(v) => v.len(),
            }
        }
    }

    /// Returns whether this column is empty.
    ///
    /// Equals to `len() == 0`.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Shortens the column, keeping the first `len` datums and dropping the rest.
    ///
    /// If `len` is greater than the column's current length, this has no effect.
    #[inline]
    pub fn truncate(&mut self, len: usize) {
        match_template_evaluable! {
            TT, match self {
                VectorValue::TT(v) => v.truncate(len),
            }
        }
    }

    /// Clears the column, removing all datums.
    #[inline]
    pub fn clear(&mut self) {
        self.truncate(0);
    }

    /// Returns the number of elements this column can hold without reallocating.
    #[inline]
    pub fn capacity(&self) -> usize {
        match_template_evaluable! {
            TT, match self {
                VectorValue::TT(v) => v.capacity(),
            }
        }
    }

    /// Moves all the elements of `other` into `Self`, leaving `other` empty.
    ///
    /// # Panics
    ///
    /// Panics if `other` does not have the same `EvalType` as `Self`.
    #[inline]
    pub fn append(&mut self, other: &mut VectorValue) {
        match_template_evaluable! {
            TT, match self {
                VectorValue::TT(self_vec) => match other {
                    VectorValue::TT(other_vec) => {
                        self_vec.append(other_vec);
                    }
                    other => panic!("Cannot append {} to {} vector", other.eval_type(), self.eval_type())
                },
            }
        }
    }

    /// Evaluates values into MySQL logic values.
    ///
    /// The caller must provide an output buffer which is large enough for holding values.
    pub fn eval_as_mysql_bools(
        &self,
        ctx: &mut EvalContext,
        outputs: &mut [bool],
    ) -> crate::Result<()> {
        assert!(outputs.len() >= self.len());
        match_template_evaluable! {
            TT, match self {
                VectorValue::TT(v) => {
                    let l = self.len();
                    for i in 0..l {
                        outputs[i] = v[i].as_mysql_bool(ctx)?;
                    }
                },
            }
        }
        Ok(())
    }

    /// Gets a reference of the element in corresponding index.
    ///
    /// # Panics
    ///
    /// Panics if index is out of range.
    #[inline]
    pub fn get_scalar_ref(&self, index: usize) -> ScalarValueRef<'_> {
        match_template_evaluable! {
            TT, match self {
                VectorValue::TT(v) => ScalarValueRef::TT(&v[index]),
            }
        }
    }

    /// Returns maximum encoded size in binary format.
    pub fn maximum_encoded_size(&self, logical_rows: &[usize]) -> Result<usize> {
        match self {
            VectorValue::Int(_) => Ok(logical_rows.len() * 9),

            // Some elements might be NULLs which encoded size is 1 byte. However it's fine because
            // this function only calculates a maximum encoded size (for constructing buffers), not
            // actual encoded size.
            VectorValue::Real(_) => Ok(logical_rows.len() * 9),
            VectorValue::Decimal(vec) => {
                let mut size = 0;
                for idx in logical_rows {
                    let el = &vec[*idx];
                    match el {
                        Some(v) => {
                            // FIXME: We don't need approximate size. Maximum size is enough (so
                            // that we don't need to iterate each value).
                            size += 1 /* FLAG */ + v.approximate_encoded_size();
                        }
                        None => {
                            size += 1;
                        }
                    }
                }
                Ok(size)
            }
            VectorValue::Bytes(vec) => {
                let mut size = 0;
                for idx in logical_rows {
                    let el = &vec[*idx];
                    match el {
                        Some(v) => {
                            size += 1 /* FLAG */ + 10 /* MAX VARINT LEN */ + v.len();
                        }
                        None => {
                            size += 1;
                        }
                    }
                }
                Ok(size)
            }
            VectorValue::DateTime(_) => Ok(logical_rows.len() * 9),
            VectorValue::Duration(_) => Ok(logical_rows.len() * 9),
            VectorValue::Json(vec) => {
                let mut size = 0;
                for idx in logical_rows {
                    let el = &vec[*idx];
                    match el {
                        Some(v) => {
                            size += 1 /* FLAG */ + v.binary_len();
                        }
                        None => {
                            size += 1;
                        }
                    }
                }
                Ok(size)
            }
        }
    }

    /// Returns maximum encoded size in chunk format.
    pub fn maximum_encoded_size_chunk(&self, logical_rows: &[usize]) -> Result<usize> {
        match self {
            VectorValue::Int(_) => Ok(logical_rows.len() * 9 + 10),
            VectorValue::Real(_) => Ok(logical_rows.len() * 9 + 10),
            VectorValue::Decimal(_) => Ok(logical_rows.len() * (DECIMAL_STRUCT_SIZE + 1) + 10),
            VectorValue::DateTime(_) => Ok(logical_rows.len() * 21 + 10),
            VectorValue::Duration(_) => Ok(logical_rows.len() * 9 + 10),
            VectorValue::Bytes(vec) => {
                let mut size = logical_rows.len() + 10;
                for idx in logical_rows {
                    let el = &vec[*idx];
                    match el {
                        Some(v) => {
                            size += 8 /* Offset */ + v.len();
                        }
                        None => {
                            size +=  8 /* Offset */;
                        }
                    }
                }
                Ok(size)
            }
            VectorValue::Json(vec) => {
                let mut size = logical_rows.len() + 10;
                for idx in logical_rows {
                    let el = &vec[*idx];
                    match el {
                        Some(v) => {
                            size += 8 /* Offset */ + v.binary_len();
                        }
                        None => {
                            size += 8 /* Offset */;
                        }
                    }
                }
                Ok(size)
            }
        }
    }

    /// Encodes a single element into binary format.
    pub fn encode(
        &self,
        row_index: usize,
        field_type: &FieldType,
        ctx: &mut EvalContext,
        output: &mut Vec<u8>,
    ) -> Result<()> {
        use crate::codec::datum_codec::EvaluableDatumEncoder;

        match self {
            VectorValue::Int(ref vec) => {
                match vec[row_index] {
                    None => {
                        output.write_evaluable_datum_null()?;
                    }
                    Some(val) => {
                        // Always encode to INT / UINT instead of VAR INT to be efficient.
                        let is_unsigned = field_type.as_accessor().is_unsigned();
                        output.write_evaluable_datum_int(val, is_unsigned)?;
                    }
                }
                Ok(())
            }
            VectorValue::Real(ref vec) => {
                match vec[row_index] {
                    None => {
                        output.write_evaluable_datum_null()?;
                    }
                    Some(val) => {
                        output.write_evaluable_datum_real(val.into_inner())?;
                    }
                }
                Ok(())
            }
            VectorValue::Decimal(ref vec) => {
                match &vec[row_index] {
                    None => {
                        output.write_evaluable_datum_null()?;
                    }
                    Some(val) => {
                        output.write_evaluable_datum_decimal(val)?;
                    }
                }
                Ok(())
            }
            VectorValue::Bytes(ref vec) => {
                match &vec[row_index] {
                    None => {
                        output.write_evaluable_datum_null()?;
                    }
                    Some(ref val) => {
                        output.write_evaluable_datum_bytes(val)?;
                    }
                }
                Ok(())
            }
            VectorValue::DateTime(ref vec) => {
                match vec[row_index] {
                    None => {
                        output.write_evaluable_datum_null()?;
                    }
                    Some(val) => {
                        output.write_evaluable_datum_date_time(val, ctx)?;
                    }
                }
                Ok(())
            }
            VectorValue::Duration(ref vec) => {
                match vec[row_index] {
                    None => {
                        output.write_evaluable_datum_null()?;
                    }
                    Some(val) => {
                        output.write_evaluable_datum_duration(val)?;
                    }
                }
                Ok(())
            }
            VectorValue::Json(ref vec) => {
                match &vec[row_index] {
                    None => {
                        output.write_evaluable_datum_null()?;
                    }
                    Some(ref val) => {
                        output.write_evaluable_datum_json(val)?;
                    }
                }
                Ok(())
            }
        }
    }
}

macro_rules! impl_as_slice {
    ($ty:tt, $name:ident) => {
        impl VectorValue {
            /// Extracts a slice of values in specified concrete type from current column.
            ///
            /// # Panics
            ///
            /// Panics if the current column does not match the type.
            #[inline]
            pub fn $name(&self) -> &[Option<$ty>] {
                match self {
                    VectorValue::$ty(vec) => vec.as_slice(),
                    other => panic!(
                        "Cannot call `{}` over a {} column",
                        stringify!($name),
                        other.eval_type()
                    ),
                }
            }
        }

        impl AsRef<[Option<$ty>]> for VectorValue {
            #[inline]
            fn as_ref(&self) -> &[Option<$ty>] {
                self.$name()
            }
        }

        // TODO: We should only expose interface for push value, not the entire Vec.
        impl AsMut<Vec<Option<$ty>>> for VectorValue {
            #[inline]
            fn as_mut(&mut self) -> &mut Vec<Option<$ty>> {
                match self {
                    VectorValue::$ty(ref mut vec) => vec,
                    other => panic!(
                        "Cannot retrieve a mutable `{}` vector over a {} column",
                        stringify!($ty),
                        other.eval_type()
                    ),
                }
            }
        }
    };
}

impl_as_slice! { Int, as_int_slice }
impl_as_slice! { Real, as_real_slice }
impl_as_slice! { Decimal, as_decimal_slice }
impl_as_slice! { Bytes, as_bytes_slice }
impl_as_slice! { DateTime, as_date_time_slice }
impl_as_slice! { Duration, as_duration_slice }
impl_as_slice! { Json, as_json_slice }

/// Additional `VectorValue` methods available via generics. These methods support different
/// concrete types but have same names and should be specified via the generic parameter type.
pub trait VectorValueExt<T: Evaluable> {
    /// The generic version for `VectorValue::push_xxx()`.
    fn push(&mut self, v: Option<T>);
}

macro_rules! impl_ext {
    ($ty:tt, $push_name:ident) => {
        // Explicit version

        impl VectorValue {
            /// Pushes a value in specified concrete type into current column.
            ///
            /// # Panics
            ///
            /// Panics if the current column does not match the type.
            #[inline]
            pub fn $push_name(&mut self, v: Option<$ty>) {
                match self {
                    VectorValue::$ty(ref mut vec) => vec.push(v),
                    other => panic!(
                        "Cannot call `{}` over a {} column",
                        stringify!($push_name),
                        other.eval_type()
                    ),
                };
            }
        }

        // Implicit version

        impl VectorValueExt<$ty> for VectorValue {
            #[inline]
            fn push(&mut self, v: Option<$ty>) {
                self.$push_name(v);
            }
        }
    };
}

impl_ext! { Int, push_int }
impl_ext! { Real, push_real }
impl_ext! { Decimal, push_decimal }
impl_ext! { Bytes, push_bytes }
impl_ext! { DateTime, push_date_time }
impl_ext! { Duration, push_duration }
impl_ext! { Json, push_json }

macro_rules! impl_from {
    ($ty:tt) => {
        impl From<Vec<Option<$ty>>> for VectorValue {
            #[inline]
            fn from(s: Vec<Option<$ty>>) -> VectorValue {
                VectorValue::$ty(s)
            }
        }
    };
}

impl_from! { Int }
impl_from! { Real }
impl_from! { Decimal }
impl_from! { Bytes }
impl_from! { DateTime }
impl_from! { Duration }
impl_from! { Json }

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic() {
        let mut column = VectorValue::with_capacity(0, EvalType::Bytes);
        assert_eq!(column.eval_type(), EvalType::Bytes);
        assert_eq!(column.len(), 0);
        assert_eq!(column.capacity(), 0);
        assert!(column.is_empty());
        assert_eq!(column.as_bytes_slice(), &[]);

        column.push_bytes(None);
        assert_eq!(column.len(), 1);
        assert!(column.capacity() > 0);
        assert!(!column.is_empty());
        assert_eq!(column.as_bytes_slice(), &[None]);

        column.push_bytes(Some(vec![1, 2, 3]));
        assert_eq!(column.len(), 2);
        assert!(column.capacity() > 0);
        assert!(!column.is_empty());
        assert_eq!(column.as_bytes_slice(), &[None, Some(vec![1, 2, 3])]);

        let mut column = VectorValue::with_capacity(3, EvalType::Real);
        assert_eq!(column.eval_type(), EvalType::Real);
        assert_eq!(column.len(), 0);
        assert_eq!(column.capacity(), 3);
        assert!(column.is_empty());
        assert_eq!(column.as_real_slice(), &[]);
        assert_eq!(column.clone().capacity(), 0);
        assert_eq!(column.clone().as_real_slice(), column.as_real_slice());

        column.push_real(Real::new(1.0).ok());
        assert_eq!(column.len(), 1);
        assert_eq!(column.capacity(), 3);
        assert!(!column.is_empty());
        assert_eq!(column.as_real_slice(), &[Real::new(1.0).ok()]);
        assert_eq!(column.clone().capacity(), 1);
        assert_eq!(column.clone().as_real_slice(), column.as_real_slice());

        column.push_real(None);
        assert_eq!(column.len(), 2);
        assert_eq!(column.capacity(), 3);
        assert!(!column.is_empty());
        assert_eq!(column.as_real_slice(), &[Real::new(1.0).ok(), None]);
        assert_eq!(column.clone().capacity(), 2);
        assert_eq!(column.clone().as_real_slice(), column.as_real_slice());

        column.push_real(Real::new(4.5).ok());
        assert_eq!(column.len(), 3);
        assert_eq!(column.capacity(), 3);
        assert!(!column.is_empty());
        assert_eq!(
            column.as_real_slice(),
            &[Real::new(1.0).ok(), None, Real::new(4.5).ok()]
        );
        assert_eq!(column.clone().capacity(), 3);
        assert_eq!(column.clone().as_real_slice(), column.as_real_slice());

        column.push_real(None);
        assert_eq!(column.len(), 4);
        assert!(column.capacity() > 3);
        assert!(!column.is_empty());
        assert_eq!(
            column.as_real_slice(),
            &[Real::new(1.0).ok(), None, Real::new(4.5).ok(), None]
        );
        assert_eq!(column.clone().as_real_slice(), column.as_real_slice());

        column.truncate(2);
        assert_eq!(column.len(), 2);
        assert!(column.capacity() > 3);
        assert!(!column.is_empty());
        assert_eq!(column.as_real_slice(), &[Real::new(1.0).ok(), None]);
        assert_eq!(column.clone().as_real_slice(), column.as_real_slice());

        let column = VectorValue::with_capacity(10, EvalType::DateTime);
        assert_eq!(column.eval_type(), EvalType::DateTime);
        assert_eq!(column.len(), 0);
        assert_eq!(column.capacity(), 10);
        assert!(column.is_empty());
        assert_eq!(column.as_date_time_slice(), &[]);
        assert_eq!(
            column.clone().as_date_time_slice(),
            column.as_date_time_slice()
        );
    }

    #[test]
    fn test_append() {
        let mut column1 = VectorValue::with_capacity(0, EvalType::Real);
        let mut column2 = VectorValue::with_capacity(3, EvalType::Real);

        column1.append(&mut column2);
        assert_eq!(column1.len(), 0);
        assert_eq!(column1.capacity(), 0);
        assert_eq!(column2.len(), 0);
        assert_eq!(column2.capacity(), 3);

        column2.push_real(Real::new(1.0).ok());
        column2.append(&mut column1);
        assert_eq!(column1.len(), 0);
        assert_eq!(column1.capacity(), 0);
        assert_eq!(column1.as_real_slice(), &[]);
        assert_eq!(column2.len(), 1);
        assert_eq!(column2.capacity(), 3);
        assert_eq!(column2.as_real_slice(), &[Real::new(1.0).ok()]);

        column1.push_real(None);
        column1.push_real(None);
        column1.append(&mut column2);
        assert_eq!(column1.len(), 3);
        assert!(column1.capacity() > 0);
        assert_eq!(column1.as_real_slice(), &[None, None, Real::new(1.0).ok()]);
        assert_eq!(column2.len(), 0);
        assert_eq!(column2.capacity(), 3);
        assert_eq!(column2.as_real_slice(), &[]);

        column1.push_real(Real::new(1.1).ok());
        column2.push_real(Real::new(3.5).ok());
        column2.push_real(Real::new(4.1).ok());
        column2.truncate(1);
        column2.append(&mut column1);
        assert_eq!(column1.len(), 0);
        assert!(column1.capacity() > 0);
        assert_eq!(column1.as_real_slice(), &[]);
        assert_eq!(column2.len(), 5);
        assert!(column2.capacity() > 3);
        assert_eq!(
            column2.as_real_slice(),
            &[
                Real::new(3.5).ok(),
                None,
                None,
                Real::new(1.0).ok(),
                Real::new(1.1).ok()
            ]
        );
    }

    #[test]
    fn test_from() {
        let slice: &[_] = &[None, Real::new(1.0).ok()];
        let vec = slice.to_vec();
        let column = VectorValue::from(vec);
        assert_eq!(column.len(), 2);
        assert_eq!(column.as_real_slice(), slice);
    }
}
