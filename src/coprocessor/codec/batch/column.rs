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

use std::convert::{TryFrom, TryInto};

use cop_datatype::{EvalType, FieldTypeAccessor, FieldTypeTp};

use coprocessor::codec::datum;
use coprocessor::codec::mysql::Tz;
use coprocessor::codec::{Error, Result};
use util::codec::{bytes, number};

// TODO: Move these type alias and re-exports into cop_datatype.
// These types are ensured to be cheap to move. However clone can be expensive.
pub type Int = i64;
pub type Real = f64;
pub type Bytes = Vec<u8>;
pub use coprocessor::codec::mysql::{Decimal, Duration, Json, Time as DateTime};

/// An array of datums in the same data type and is column oriented.
///
/// Stores datums of multiple rows of one column.
#[derive(Debug, PartialEq)]
pub enum BatchColumn {
    Int(Vec<Option<Int>>),
    Real(Vec<Option<Real>>),
    Decimal(Vec<Option<Decimal>>),
    Bytes(Vec<Option<Bytes>>),
    DateTime(Vec<Option<DateTime>>),
    Duration(Vec<Option<Duration>>),
    Json(Vec<Option<Json>>),
}

macro_rules! match_self {
    (ref $self:ident, $var:ident, $expr:expr) => {{
        match_self!(INTERNAL ref, $self, $var, $expr)
    }};
    (ref mut $self:ident, $var:ident, $expr:expr) => {{
        match_self!(INTERNAL ref|mut, $self, $var, $expr)
    }};
    (INTERNAL $($ref:tt)|+, $self:ident, $var:ident, $expr:expr) => {{
        match $self {
            BatchColumn::Int($($ref)+ $var) => $expr,
            BatchColumn::Real($($ref)+ $var) => $expr,
            BatchColumn::Decimal($($ref)+ $var) => $expr,
            BatchColumn::Bytes($($ref)+ $var) => $expr,
            BatchColumn::DateTime($($ref)+ $var) => $expr,
            BatchColumn::Duration($($ref)+ $var) => $expr,
            BatchColumn::Json($($ref)+ $var) => $expr,
        }
    }};
}

#[inline]
fn clone_vec_with_capacity<T: Clone>(vec: &Vec<T>) -> Vec<T> {
    // According to benchmarks over rustc 1.30.0-nightly (39e6ba821 2018-08-25), `copy_from_slice`
    // has same performance as `extend_from_slice` when T: Copy. So we only use `extend_from_slice`
    // here.
    let mut new_vec = Vec::with_capacity(vec.capacity());
    new_vec.extend_from_slice(vec);
    new_vec
}

impl Clone for BatchColumn {
    #[inline]
    fn clone(&self) -> Self {
        // Implement `Clone` manually so that capacity can be preserved after clone.
        match self {
            BatchColumn::Int(ref vec) => BatchColumn::Int(clone_vec_with_capacity(vec)),
            BatchColumn::Real(ref vec) => BatchColumn::Real(clone_vec_with_capacity(vec)),
            BatchColumn::Decimal(ref vec) => BatchColumn::Decimal(clone_vec_with_capacity(vec)),
            BatchColumn::Bytes(ref vec) => BatchColumn::Bytes(clone_vec_with_capacity(vec)),
            BatchColumn::DateTime(ref vec) => BatchColumn::DateTime(clone_vec_with_capacity(vec)),
            BatchColumn::Duration(ref vec) => BatchColumn::Duration(clone_vec_with_capacity(vec)),
            BatchColumn::Json(ref vec) => BatchColumn::Json(clone_vec_with_capacity(vec)),
        }
    }
}

impl BatchColumn {
    /// Creates an empty `BatchColumn` according to `eval_tp` and reserves capacity according
    /// to `capacity`.
    #[inline]
    pub fn with_capacity(capacity: usize, eval_tp: EvalType) -> Self {
        match eval_tp {
            EvalType::Int => BatchColumn::Int(Vec::with_capacity(capacity)),
            EvalType::Real => BatchColumn::Real(Vec::with_capacity(capacity)),
            EvalType::Decimal => BatchColumn::Decimal(Vec::with_capacity(capacity)),
            EvalType::Bytes => BatchColumn::Bytes(Vec::with_capacity(capacity)),
            EvalType::DateTime => BatchColumn::DateTime(Vec::with_capacity(capacity)),
            EvalType::Duration => BatchColumn::Duration(Vec::with_capacity(capacity)),
            EvalType::Json => BatchColumn::Json(Vec::with_capacity(capacity)),
        }
    }

    /// Returns the `EvalType` used to construct current column.
    #[inline]
    pub fn eval_type(&self) -> EvalType {
        match self {
            BatchColumn::Int(_) => EvalType::Int,
            BatchColumn::Real(_) => EvalType::Real,
            BatchColumn::Decimal(_) => EvalType::Decimal,
            BatchColumn::Bytes(_) => EvalType::Bytes,
            BatchColumn::DateTime(_) => EvalType::DateTime,
            BatchColumn::Duration(_) => EvalType::Duration,
            BatchColumn::Json(_) => EvalType::Json,
        }
    }

    /// Returns the number of datums contained in this column.
    #[inline]
    pub fn len(&self) -> usize {
        match_self!(ref self, v, v.len())
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
        match_self!(ref mut self, v, v.truncate(len));
    }

    /// Clears the column, removing all datums.
    #[inline]
    pub fn clear(&mut self) {
        self.truncate(0);
    }

    /// Returns the number of elements this column can hold without reallocating.
    #[inline]
    pub fn capacity(&self) -> usize {
        match_self!(ref self, v, v.capacity())
    }

    /// Retains only the elements specified by the predicate, which accepts index only.
    ///
    /// In other words, remove all rows such that `f(element_index)` returns `false`.
    #[inline]
    pub fn retain_by_index<F>(&mut self, mut f: F)
    where
        F: FnMut(usize) -> bool,
    {
        match_self!(ref mut self, v, {
            let mut idx = 0;
            v.retain(|_| {
                let r = f(idx);
                idx += 1;
                r
            });
        });
    }

    /// Moves all the elements of `other` into `Self`, leaving `other` empty.
    ///
    /// # Panics
    ///
    /// Panics if `other` does not have the same `EvalType` as `Self`.
    #[inline]
    pub fn append(&mut self, other: &mut BatchColumn) {
        match self {
            BatchColumn::Int(ref mut self_vec) => match other {
                BatchColumn::Int(ref mut other_vec) => {
                    self_vec.append(other_vec);
                }
                other => panic!("Cannot append {} to Int column", other.eval_type()),
            },
            BatchColumn::Real(ref mut self_vec) => match other {
                BatchColumn::Real(ref mut other_vec) => {
                    self_vec.append(other_vec);
                }
                other => panic!("Cannot append {} to Real column", other.eval_type()),
            },
            BatchColumn::Decimal(ref mut self_vec) => match other {
                BatchColumn::Decimal(ref mut other_vec) => {
                    self_vec.append(other_vec);
                }
                other => panic!("Cannot append {} to Decimal column", other.eval_type()),
            },
            BatchColumn::Bytes(ref mut self_vec) => match other {
                BatchColumn::Bytes(ref mut other_vec) => {
                    self_vec.append(other_vec);
                }
                other => panic!("Cannot append {} to Bytes column", other.eval_type()),
            },
            BatchColumn::DateTime(ref mut self_vec) => match other {
                BatchColumn::DateTime(ref mut other_vec) => {
                    self_vec.append(other_vec);
                }
                other => panic!("Cannot append {} to DateTime column", other.eval_type()),
            },
            BatchColumn::Duration(ref mut self_vec) => match other {
                BatchColumn::Duration(ref mut other_vec) => {
                    self_vec.append(other_vec);
                }
                other => panic!("Cannot append {} to Duration column", other.eval_type()),
            },
            BatchColumn::Json(ref mut self_vec) => match other {
                BatchColumn::Json(ref mut other_vec) => {
                    self_vec.append(other_vec);
                }
                other => panic!("Cannot append {} to Json column", other.eval_type()),
            },
        }
    }

    /// Pushes a value into the column by decoding the datum and converting to current
    /// column's type.
    ///
    /// For values that needs a time zone, `time_zone` will be used.
    ///
    /// For values that current type's type is not sufficient, `field_type` will be used.
    ///
    /// # Panics
    ///
    /// Panics if `field_type` doesn't match current column's type.
    #[inline]
    #[cfg_attr(feature = "cargo-clippy", allow(cast_lossless))]
    pub fn push_datum(
        &mut self,
        mut raw_datum: &[u8],
        time_zone: Tz,
        field_type: &FieldTypeAccessor,
    ) -> Result<()> {
        #[inline]
        fn decode_int(v: &mut &[u8]) -> Result<i64> {
            number::decode_i64(v)
                .map_err(|_| Error::InvalidDataType("Failed to decode data as i64".to_owned()))
        }

        #[inline]
        fn decode_uint(v: &mut &[u8]) -> Result<u64> {
            number::decode_u64(v)
                .map_err(|_| Error::InvalidDataType("Failed to decode data as u64".to_owned()))
        }

        #[inline]
        fn decode_var_int(v: &mut &[u8]) -> Result<i64> {
            number::decode_var_i64(v)
                .map_err(|_| Error::InvalidDataType("Failed to decode data as var_i64".to_owned()))
        }

        #[inline]
        fn decode_var_uint(v: &mut &[u8]) -> Result<u64> {
            number::decode_var_u64(v)
                .map_err(|_| Error::InvalidDataType("Failed to decode data as var_u64".to_owned()))
        }

        #[inline]
        fn decode_float(v: &mut &[u8]) -> Result<f64> {
            number::decode_f64(v)
                .map_err(|_| Error::InvalidDataType("Failed to decode data as f64".to_owned()))
        }

        #[inline]
        fn decode_decimal(v: &mut &[u8]) -> Result<Decimal> {
            Decimal::decode(v)
                .map_err(|_| Error::InvalidDataType("Failed to decode data as decimal".to_owned()))
        }

        #[inline]
        fn decode_bytes(v: &mut &[u8]) -> Result<Vec<u8>> {
            bytes::decode_bytes(v, false)
                .map_err(|_| Error::InvalidDataType("Failed to decode data as bytes".to_owned()))
        }

        #[inline]
        fn decode_compact_bytes(v: &mut &[u8]) -> Result<Vec<u8>> {
            bytes::decode_compact_bytes(v).map_err(|_| {
                Error::InvalidDataType("Failed to decode data as compact bytes".to_owned())
            })
        }

        #[inline]
        fn decode_json(v: &mut &[u8]) -> Result<Json> {
            Json::decode(v)
                .map_err(|_| Error::InvalidDataType("Failed to decode data as json".to_owned()))
        }

        #[inline]
        fn decode_duration_from_i64(v: i64) -> Result<Duration> {
            Duration::from_nanos(v, 0)
                .map_err(|_| Error::InvalidDataType("Failed to decode i64 as duration".to_owned()))
        }

        #[inline]
        fn decode_date_time_from_uint(
            v: u64,
            time_zone: Tz,
            field_type: &FieldTypeAccessor,
        ) -> Result<DateTime> {
            let fsp = field_type.decimal() as i8;
            let time_type = field_type.tp().try_into()?;
            DateTime::from_packed_u64(v, time_type, fsp, time_zone)
        }

        // The inner implementation is much like `table::decode_col_value`, however it constructs
        // value directly without constructing a `Datum` to improve performance.

        // TODO: Use BufferReader.
        // TODO: Confirm correctness with TiDB team.

        // Make sure that the `field_type` given matches current column's type.
        let tp = field_type.tp();
        assert_eq!(EvalType::try_from(tp).unwrap(), self.eval_type());

        if raw_datum.is_empty() {
            return Err(Error::InvalidDataType(
                "Failed to decode datum flag".to_owned(),
            ));
        }

        let flag = raw_datum[0];
        raw_datum = &raw_datum[1..];

        match self {
            BatchColumn::Int(ref mut vec) => match flag {
                datum::NIL_FLAG => vec.push(None),
                datum::INT_FLAG => vec.push(Some(decode_int(&mut raw_datum)?)),
                datum::UINT_FLAG => vec.push(Some(decode_uint(&mut raw_datum)? as i64)),
                datum::VAR_INT_FLAG => vec.push(Some(decode_var_int(&mut raw_datum)?)),
                datum::VAR_UINT_FLAG => vec.push(Some(decode_var_uint(&mut raw_datum)? as i64)),
                flag => {
                    return Err(Error::InvalidDataType(format!(
                        "Unsupported datum flag {} for Int column",
                        flag
                    )))
                }
            },
            BatchColumn::Real(ref mut vec) => match flag {
                datum::NIL_FLAG => vec.push(None),
                // In both index and record, it's flag is `FLOAT`. See TiDB's `encode()`.
                datum::FLOAT_FLAG => {
                    let mut v = decode_float(&mut raw_datum)?;
                    if tp == FieldTypeTp::Float {
                        v = (v as f32) as f64;
                    }
                    vec.push(Some(v));
                }
                flag => {
                    return Err(Error::InvalidDataType(format!(
                        "Unsupported datum flag {} for Real column",
                        flag
                    )))
                }
            },
            BatchColumn::Decimal(ref mut vec) => match flag {
                datum::NIL_FLAG => vec.push(None),
                // In both index and record, it's flag is `DECIMAL`. See TiDB's `encode()`.
                datum::DECIMAL_FLAG => vec.push(Some(decode_decimal(&mut raw_datum)?)),
                flag => {
                    return Err(Error::InvalidDataType(format!(
                        "Unsupported datum flag {} for Decimal column",
                        flag
                    )))
                }
            },
            BatchColumn::Bytes(ref mut vec) => match flag {
                datum::NIL_FLAG => vec.push(None),
                // In index, it's flag is `BYTES`. See TiDB's `encode()`.
                datum::BYTES_FLAG => vec.push(Some(decode_bytes(&mut raw_datum)?)),
                // In record, it's flag is `COMPACT_BYTES`. See TiDB's `encode()`.
                datum::COMPACT_BYTES_FLAG => vec.push(Some(decode_compact_bytes(&mut raw_datum)?)),
                flag => {
                    return Err(Error::InvalidDataType(format!(
                        "Unsupported datum flag {} for Bytes column",
                        flag
                    )))
                }
            },
            BatchColumn::DateTime(ref mut vec) => match flag {
                datum::NIL_FLAG => vec.push(None),
                // In index, it's flag is `UINT`. See TiDB's `encode()`.
                datum::UINT_FLAG => {
                    let v = decode_uint(&mut raw_datum)?;
                    let v = decode_date_time_from_uint(v, time_zone, field_type)?;
                    vec.push(Some(v));
                }
                // In record, it's flag is `VAR_UINT`. See TiDB's `flatten()` and `encode()`.
                datum::VAR_UINT_FLAG => {
                    let v = decode_var_uint(&mut raw_datum)?;
                    let v = decode_date_time_from_uint(v, time_zone, field_type)?;
                    vec.push(Some(v));
                }
                flag => {
                    return Err(Error::InvalidDataType(format!(
                        "Unsupported datum flag {} for DateTime column",
                        flag
                    )))
                }
            },
            BatchColumn::Duration(ref mut vec) => match flag {
                datum::NIL_FLAG => vec.push(None),
                // In index, it's flag is `DURATION`. See TiDB's `encode()`.
                datum::DURATION_FLAG => {
                    let v = decode_int(&mut raw_datum)?;
                    let v = decode_duration_from_i64(v)?;
                    vec.push(Some(v));
                }
                // In record, it's flag is `VAR_INT`. See TiDB's `flatten()` and `encode()`.
                datum::VAR_INT_FLAG => {
                    let v = decode_var_int(&mut raw_datum)?;
                    let v = decode_duration_from_i64(v)?;
                    vec.push(Some(v));
                }
                flag => {
                    return Err(Error::InvalidDataType(format!(
                        "Unsupported datum flag {} for Duration column",
                        flag
                    )))
                }
            },
            BatchColumn::Json(ref mut vec) => match flag {
                datum::NIL_FLAG => vec.push(None),
                // In both index and record, it's flag is `JSON`. See TiDB's `encode()`.
                datum::JSON_FLAG => vec.push(Some(decode_json(&mut raw_datum)?)),
                flag => {
                    return Err(Error::InvalidDataType(format!(
                        "Unsupported datum flag {} for Json column",
                        flag
                    )))
                }
            },
        }

        Ok(())
    }
}

macro_rules! impl_as_slice {
    ($ty:tt, $name:ident, $mut_name:ident) => {
        impl BatchColumn {
            /// Extracts a slice containing the entire values of the column
            /// in specific type.
            ///
            /// # Panics
            ///
            /// Panics if current column is does not match the type.
            #[inline]
            pub fn $name(&self) -> &[Option<$ty>] {
                match self {
                    BatchColumn::$ty(ref vec) => vec.as_slice(),
                    other => panic!(
                        "Cannot call `{}` over a {} column",
                        stringify!($name),
                        other.eval_type()
                    ),
                }
            }

            /// Extracts a mutable slice containing the entire values of the column
            /// in specific type.
            ///
            /// # Panics
            ///
            /// Panics if current column is does not match the type.
            #[inline]
            pub fn $mut_name(&mut self) -> &mut [Option<$ty>] {
                match self {
                    BatchColumn::$ty(ref mut vec) => vec.as_mut_slice(),
                    other => panic!(
                        "Cannot call `{}` over a {} column",
                        stringify!($name),
                        other.eval_type()
                    ),
                }
            }
        }

        impl AsRef<[Option<$ty>]> for BatchColumn {
            #[inline]
            fn as_ref(&self) -> &[Option<$ty>] {
                self.$name()
            }
        }

        impl AsMut<[Option<$ty>]> for BatchColumn {
            #[inline]
            fn as_mut(&mut self) -> &mut [Option<$ty>] {
                self.$mut_name()
            }
        }
    };
}

impl_as_slice! { Int, as_int_slice, as_mut_int_slice }
impl_as_slice! { Real, as_real_slice, as_mut_real_slice }
impl_as_slice! { Decimal, as_decimal_slice, as_mut_decimal_slice }
impl_as_slice! { Bytes, as_bytes_slice, as_mut_bytes_slice }
impl_as_slice! { DateTime, as_date_time_slice, as_mut_date_time_slice }
impl_as_slice! { Duration, as_duration_slice, as_mut_duration_slice }
impl_as_slice! { Json, as_json_slice, as_mut_json_slice }

macro_rules! impl_push {
    ($ty:tt, $name:ident) => {
        impl BatchColumn {
            /// Pushes a value into the column in specific type.
            ///
            /// # Panics
            ///
            /// Panics if current column does not match the type.
            #[inline]
            pub fn $name(&mut self, v: Option<$ty>) {
                match self {
                    BatchColumn::$ty(ref mut vec) => vec.push(v),
                    other => panic!(
                        "Cannot call `{}` over to a {} column",
                        stringify!($name),
                        other.eval_type()
                    ),
                };
            }
        }
    };
}

impl_push! { Int, push_int }
impl_push! { Real, push_real }
impl_push! { Decimal, push_decimal }
impl_push! { Bytes, push_bytes }
impl_push! { DateTime, push_date_time }
impl_push! { Duration, push_duration }
impl_push! { Json, push_json }

macro_rules! impl_from {
    ($ty:tt) => {
        impl<'a> From<&'a [Option<$ty>]> for BatchColumn {
            #[inline]
            fn from(s: &'a [Option<$ty>]) -> BatchColumn {
                BatchColumn::$ty(s.to_vec())
            }
        }

        impl<'a> From<&'a mut [Option<$ty>]> for BatchColumn {
            #[inline]
            fn from(s: &'a mut [Option<$ty>]) -> BatchColumn {
                BatchColumn::$ty(s.to_vec())
            }
        }

        impl From<Vec<Option<$ty>>> for BatchColumn {
            #[inline]
            fn from(s: Vec<Option<$ty>>) -> BatchColumn {
                BatchColumn::$ty(s)
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
        let mut column = BatchColumn::with_capacity(0, EvalType::Bytes);
        assert_eq!(column.eval_type(), EvalType::Bytes);
        assert_eq!(column.len(), 0);
        assert_eq!(column.capacity(), 0);
        assert!(column.is_empty());
        assert_eq!(column.as_bytes_slice(), &[]);
        assert_eq!(column.as_mut_bytes_slice(), &mut []);

        column.push_bytes(None);
        assert_eq!(column.len(), 1);
        assert!(column.capacity() > 0);
        assert!(!column.is_empty());
        assert_eq!(column.as_bytes_slice(), &[None]);
        assert_eq!(column.as_mut_bytes_slice(), &mut [None]);

        column.push_bytes(Some(vec![1, 2, 3]));
        assert_eq!(column.len(), 2);
        assert!(column.capacity() > 0);
        assert!(!column.is_empty());
        assert_eq!(column.as_bytes_slice(), &[None, Some(vec![1, 2, 3])]);
        assert_eq!(
            column.as_mut_bytes_slice(),
            &mut [None, Some(vec![1, 2, 3])]
        );

        let mut column = BatchColumn::with_capacity(3, EvalType::Real);
        assert_eq!(column.eval_type(), EvalType::Real);
        assert_eq!(column.len(), 0);
        assert_eq!(column.capacity(), 3);
        assert!(column.is_empty());
        assert_eq!(column.as_real_slice(), &[]);
        assert_eq!(column.as_mut_real_slice(), &mut []);
        assert_eq!(column.clone().capacity(), column.capacity());
        assert_eq!(column.clone().as_real_slice(), column.as_real_slice());

        column.push_real(Some(1.0));
        assert_eq!(column.len(), 1);
        assert_eq!(column.capacity(), 3);
        assert!(!column.is_empty());
        assert_eq!(column.as_real_slice(), &[Some(1.0)]);
        assert_eq!(column.as_mut_real_slice(), &mut [Some(1.0)]);
        assert_eq!(column.clone().capacity(), column.capacity());
        assert_eq!(column.clone().as_real_slice(), column.as_real_slice());

        column.push_real(None);
        assert_eq!(column.len(), 2);
        assert_eq!(column.capacity(), 3);
        assert!(!column.is_empty());
        assert_eq!(column.as_real_slice(), &[Some(1.0), None]);
        assert_eq!(column.as_mut_real_slice(), &mut [Some(1.0), None]);
        assert_eq!(column.clone().capacity(), column.capacity());
        assert_eq!(column.clone().as_real_slice(), column.as_real_slice());

        column.push_real(Some(4.5));
        assert_eq!(column.len(), 3);
        assert_eq!(column.capacity(), 3);
        assert!(!column.is_empty());
        assert_eq!(column.as_real_slice(), &[Some(1.0), None, Some(4.5)]);
        assert_eq!(
            column.as_mut_real_slice(),
            &mut [Some(1.0), None, Some(4.5)]
        );
        assert_eq!(column.clone().capacity(), column.capacity());
        assert_eq!(column.clone().as_real_slice(), column.as_real_slice());

        column.push_real(None);
        assert_eq!(column.len(), 4);
        assert!(column.capacity() > 3);
        assert!(!column.is_empty());
        assert_eq!(column.as_real_slice(), &[Some(1.0), None, Some(4.5), None]);
        assert_eq!(
            column.as_mut_real_slice(),
            &mut [Some(1.0), None, Some(4.5), None]
        );
        assert_eq!(column.clone().capacity(), column.capacity());
        assert_eq!(column.clone().as_real_slice(), column.as_real_slice());

        column.as_mut_real_slice()[2] = None;
        assert_eq!(column.len(), 4);
        assert!(column.capacity() > 3);
        assert!(!column.is_empty());
        assert_eq!(column.as_real_slice(), &[Some(1.0), None, None, None]);
        assert_eq!(
            column.as_mut_real_slice(),
            &mut [Some(1.0), None, None, None]
        );
        assert_eq!(column.clone().capacity(), column.capacity());
        assert_eq!(column.clone().as_real_slice(), column.as_real_slice());

        column.truncate(2);
        assert_eq!(column.len(), 2);
        assert!(column.capacity() > 3);
        assert!(!column.is_empty());
        assert_eq!(column.as_real_slice(), &[Some(1.0), None]);
        assert_eq!(column.as_mut_real_slice(), &mut [Some(1.0), None]);
        assert_eq!(column.clone().capacity(), column.capacity());
        assert_eq!(column.clone().as_real_slice(), column.as_real_slice());

        column.as_mut_real_slice()[0] = Some(3.7);
        assert_eq!(column.len(), 2);
        assert!(column.capacity() > 3);
        assert!(!column.is_empty());
        assert_eq!(column.as_real_slice(), &[Some(3.7), None]);
        assert_eq!(column.as_mut_real_slice(), &mut [Some(3.7), None]);
        assert_eq!(column.clone().capacity(), column.capacity());
        assert_eq!(column.clone().as_real_slice(), column.as_real_slice());

        let mut column = BatchColumn::with_capacity(10, EvalType::DateTime);
        assert_eq!(column.eval_type(), EvalType::DateTime);
        assert_eq!(column.len(), 0);
        assert_eq!(column.capacity(), 10);
        assert!(column.is_empty());
        assert_eq!(column.as_date_time_slice(), &[]);
        assert_eq!(column.as_mut_date_time_slice(), &mut []);
        assert_eq!(column.clone().capacity(), column.capacity());
        assert_eq!(
            column.clone().as_date_time_slice(),
            column.as_date_time_slice()
        );
    }

    #[test]
    fn test_retain_by_index() {
        let mut column = BatchColumn::with_capacity(3, EvalType::Real);
        assert_eq!(column.len(), 0);
        assert_eq!(column.capacity(), 3);
        column.retain_by_index(|_| true);
        assert_eq!(column.len(), 0);
        assert_eq!(column.capacity(), 3);
        column.retain_by_index(|_| false);
        assert_eq!(column.len(), 0);
        assert_eq!(column.capacity(), 3);

        column.push_real(None);
        column.push_real(Some(2.0));
        column.push_real(Some(1.0));
        column.push_real(None);
        column.push_real(Some(5.0));
        column.push_real(None);

        let retain_map = &[true, true, false, false, true, false];
        column.retain_by_index(|idx| retain_map[idx]);

        assert_eq!(column.len(), 3);
        assert!(column.capacity() > 3);
        assert_eq!(column.as_real_slice(), &[None, Some(2.0), Some(5.0)]);

        column.push_real(None);
        column.push_real(Some(1.5));
        column.push_real(None);
        column.push_real(Some(4.0));

        assert_eq!(column.len(), 7);
        assert_eq!(
            column.as_real_slice(),
            &[None, Some(2.0), Some(5.0), None, Some(1.5), None, Some(4.0)]
        );

        let retain_map = &[true, false, true, false, false, true, true];
        column.retain_by_index(|idx| retain_map[idx]);

        assert_eq!(column.len(), 4);
        assert_eq!(column.as_real_slice(), &[None, Some(5.0), None, Some(4.0)]);

        column.retain_by_index(|_| true);
        assert_eq!(column.len(), 4);
        assert_eq!(column.as_real_slice(), &[None, Some(5.0), None, Some(4.0)]);

        column.retain_by_index(|_| false);
        assert_eq!(column.len(), 0);
        assert_eq!(column.as_real_slice(), &[]);

        column.push_real(None);
        column.push_real(Some(1.5));
        assert_eq!(column.as_real_slice(), &[None, Some(1.5)]);
    }

    #[test]
    fn test_append() {
        let mut column1 = BatchColumn::with_capacity(0, EvalType::Real);
        let mut column2 = BatchColumn::with_capacity(3, EvalType::Real);

        column1.append(&mut column2);
        assert_eq!(column1.len(), 0);
        assert_eq!(column1.capacity(), 0);
        assert_eq!(column2.len(), 0);
        assert_eq!(column2.capacity(), 3);

        column2.push_real(Some(1.0));
        column2.append(&mut column1);
        assert_eq!(column1.len(), 0);
        assert_eq!(column1.capacity(), 0);
        assert_eq!(column1.as_real_slice(), &[]);
        assert_eq!(column2.len(), 1);
        assert_eq!(column2.capacity(), 3);
        assert_eq!(column2.as_real_slice(), &[Some(1.0)]);

        column1.push_real(None);
        column1.push_real(None);
        column1.append(&mut column2);
        assert_eq!(column1.len(), 3);
        assert!(column1.capacity() > 0);
        assert_eq!(column1.as_real_slice(), &[None, None, Some(1.0)]);
        assert_eq!(column2.len(), 0);
        assert_eq!(column2.capacity(), 3);
        assert_eq!(column2.as_real_slice(), &[]);

        column1.push_real(Some(1.1));
        column2.push_real(Some(3.5));
        column2.push_real(Some(4.1));
        column2.truncate(1);
        column2.append(&mut column1);
        assert_eq!(column1.len(), 0);
        assert!(column1.capacity() > 0);
        assert_eq!(column1.as_real_slice(), &[]);
        assert_eq!(column2.len(), 5);
        assert!(column2.capacity() > 3);
        assert_eq!(
            column2.as_real_slice(),
            &[Some(3.5), None, None, Some(1.0), Some(1.1)]
        );
    }

    #[test]
    fn test_from() {
        let slice: &[_] = &[None, Some(1.0)];
        let column = BatchColumn::from(slice);
        assert_eq!(column.len(), 2);
        assert_eq!(column.as_real_slice(), slice);

        let vec = slice.to_vec();
        let column = BatchColumn::from(vec);
        assert_eq!(column.len(), 2);
        assert_eq!(column.as_real_slice(), slice);
    }
}

#[cfg(test)]
mod benches {
    use test;

    use super::*;

    #[bench]
    fn bench_push_datum_int(b: &mut test::Bencher) {
        use coprocessor::codec::datum::{Datum, DatumEncoder};

        let mut column = BatchColumn::with_capacity(1000, EvalType::Int);

        let mut datum_raw: Vec<u8> = Vec::new();
        DatumEncoder::encode(&mut datum_raw, &[Datum::U64(0xDEADBEEF)], true).unwrap();

        let col_info = {
            let mut col_info = ::tipb::schema::ColumnInfo::new();
            col_info.as_mut_accessor().set_tp(FieldTypeTp::LongLong);
            col_info
        };
        let tz = Tz::utc();

        b.iter(move || {
            for _ in 0..1000 {
                column
                    .push_datum(
                        test::black_box(&datum_raw),
                        test::black_box(tz),
                        test::black_box(&col_info),
                    )
                    .unwrap();
            }
            test::black_box(&column);
            column.clear();
        });
    }

    /// Bench performance of naively decoding multiple datums (without pushing into a vector).
    #[bench]
    fn bench_batch_decode(b: &mut test::Bencher) {
        use cop_datatype::FieldTypeTp;
        use coprocessor::codec::datum::{Datum, DatumEncoder};
        use coprocessor::codec::table;
        use coprocessor::dag::expr::EvalContext;

        let mut datum_raw: Vec<u8> = Vec::new();
        DatumEncoder::encode(&mut datum_raw, &[Datum::U64(0xDEADBEEF)], true).unwrap();

        let col_info = {
            let mut col_info = ::tipb::schema::ColumnInfo::new();
            col_info.as_mut_accessor().set_tp(FieldTypeTp::LongLong);
            col_info
        };
        let eval_ctx = EvalContext::default();

        b.iter(|| {
            for _ in 0..1000 {
                let mut raw = test::black_box(&datum_raw).as_slice();
                let datum = table::decode_col_value(
                    &mut raw,
                    test::black_box(&eval_ctx),
                    test::black_box(&col_info),
                ).unwrap();
                match datum {
                    Datum::I64(v) => {
                        test::black_box(v);
                    }
                    Datum::U64(v) => {
                        test::black_box(v);
                    }
                    _ => {
                        panic!();
                    }
                }
            }
        });
    }

    /// Bench performance of retain by array. It is used in Selection executor.
    #[bench]
    fn bench_retain(b: &mut test::Bencher) {
        use rand;

        let mut column = BatchColumn::with_capacity(1000, EvalType::Int);
        for _ in 0..1000 {
            column.push_int(Some(rand::random()));
        }

        // Filter out 20% elements
        let mut should_retain = vec![true; 1000];
        for retain in &mut should_retain {
            if rand::random::<f64>() >= 0.8 {
                *retain = false;
            }
        }

        b.iter(|| {
            let should_retain = test::black_box(&should_retain);
            let mut c = test::black_box(&column).clone();
            c.retain_by_index(|idx| should_retain[idx]);
            test::black_box(c);
        });
    }
}
