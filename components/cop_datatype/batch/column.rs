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

use tikv::coprocessor::codec::datum;
use tikv::coprocessor::codec::mysql::{Decimal, Duration, Json, Time, MAX_FSP};
use tikv::util::codec::{bytes, number};

/// An array of datums in the same data type and is column oriented.
///
/// Stores datums of multiple rows of one column.
#[derive(Clone)]
pub enum BatchColumn {
    Int(Vec<Option<i64>>),
    Real(Vec<Option<f64>>),
    Decimal(Vec<Option<Decimal>>),
    String(Vec<Option<Vec<u8>>>),
    DateTime(Vec<Option<Time>>),
    Duration(Vec<Option<Duration>>),
    Json(Vec<Option<Json>>),
}

impl BatchColumn {
    /// Creates an empty `BatchColumn` according to `eval_tp` and reserves capacity according
    /// to `capacity`.
    #[inline]
    pub fn with_capacity(capacity: usize, eval_tp: &::EvalTypeProvider) -> Self {
        match eval_tp.get_eval_type() {
            ::EvalType::Int => BatchColumn::Int(Vec::with_capacity(capacity)),
            ::EvalType::Real => BatchColumn::Real(Vec::with_capacity(capacity)),
            ::EvalType::Decimal => BatchColumn::Decimal(Vec::with_capacity(capacity)),
            ::EvalType::String => BatchColumn::String(Vec::with_capacity(capacity)),
            ::EvalType::DateTime => BatchColumn::DateTime(Vec::with_capacity(capacity)),
            ::EvalType::Duration => BatchColumn::Duration(Vec::with_capacity(capacity)),
            ::EvalType::Json => BatchColumn::Json(Vec::with_capacity(capacity)),
        }
    }

    /// Returns the number of datums contained in this column.
    #[inline]
    pub fn len(&self) -> usize {
        match self {
            BatchColumn::Int(ref v) => v.len(),
            BatchColumn::Real(ref v) => v.len(),
            BatchColumn::Decimal(ref v) => v.len(),
            BatchColumn::String(ref v) => v.len(),
            BatchColumn::DateTime(ref v) => v.len(),
            BatchColumn::Duration(ref v) => v.len(),
            BatchColumn::Json(ref v) => v.len(),
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
        match self {
            BatchColumn::Int(ref mut v) => v.truncate(len),
            BatchColumn::Real(ref mut v) => v.truncate(len),
            BatchColumn::Decimal(ref mut v) => v.truncate(len),
            BatchColumn::String(ref mut v) => v.truncate(len),
            BatchColumn::DateTime(ref mut v) => v.truncate(len),
            BatchColumn::Duration(ref mut v) => v.truncate(len),
            BatchColumn::Json(ref mut v) => v.truncate(len),
        };
    }

    /// Clears the column, removing all datums.
    #[inline]
    pub fn clear(&mut self) {
        self.truncate(0);
    }

    /// Pushes a value into the inner vector by decoding the datum and converting to current
    /// column's type.
    ///
    /// # Errors
    ///
    /// Returns `Error::InvalidData` if datum data is invalid or cannot be decoded in current
    /// column's type.
    #[inline]
    pub fn push_datum(&mut self, mut raw_datum: &[u8]) -> ::Result<()> {
        // The inner implementation is much like `decode_datum` + `as_xxx`, however it construct
        // value directly without constructing a `Datum` first to improve performance.

        // TODO: Use BufferReader.
        if raw_datum.is_empty() {
            return Err(::Error::InvalidData);
        }

        let flag = raw_datum[0];
        raw_datum = &raw_datum[1..];

        match self {
            BatchColumn::Int(ref mut vec) => match flag {
                datum::NIL_FLAG => vec.push(None),
                datum::INT_FLAG => {
                    let v = number::decode_i64(&mut raw_datum).map_err(|_| ::Error::InvalidData)?;
                    vec.push(Some(v));
                }
                datum::UINT_FLAG => {
                    let v = number::decode_u64(&mut raw_datum).map_err(|_| ::Error::InvalidData)?;
                    vec.push(Some(v as i64));
                }
                datum::VAR_INT_FLAG => {
                    let v =
                        number::decode_var_i64(&mut raw_datum).map_err(|_| ::Error::InvalidData)?;
                    vec.push(Some(v));
                }
                datum::VAR_UINT_FLAG => {
                    let v =
                        number::decode_var_u64(&mut raw_datum).map_err(|_| ::Error::InvalidData)?;
                    vec.push(Some(v as i64));
                }
                _ => return Err(::Error::InvalidData),
            },
            BatchColumn::Real(ref mut vec) => match flag {
                datum::NIL_FLAG => vec.push(None),
                datum::FLOAT_FLAG => {
                    let v = number::decode_f64(&mut raw_datum).map_err(|_| ::Error::InvalidData)?;
                    vec.push(Some(v));
                }
                _ => return Err(::Error::InvalidData),
            },
            BatchColumn::Decimal(ref mut vec) => match flag {
                datum::NIL_FLAG => vec.push(None),
                datum::DECIMAL_FLAG => {
                    let v = Decimal::decode(&mut raw_datum).map_err(|_| ::Error::InvalidData)?;
                    vec.push(Some(v));
                }
                _ => return Err(::Error::InvalidData),
            },
            BatchColumn::String(ref mut vec) => match flag {
                datum::NIL_FLAG => vec.push(None),
                datum::BYTES_FLAG => {
                    let v = bytes::decode_bytes(&mut raw_datum, false)
                        .map_err(|_| ::Error::InvalidData)?;
                    vec.push(Some(v));
                }
                datum::COMPACT_BYTES_FLAG => {
                    let v = bytes::decode_compact_bytes(&mut raw_datum)
                        .map_err(|_| ::Error::InvalidData)?;
                    vec.push(Some(v));
                }
                _ => return Err(::Error::InvalidData),
            },
            BatchColumn::DateTime(ref mut vec) => match flag {
                datum::NIL_FLAG => vec.push(None),
                // TODO
                // Datum::Time(v) => vec.push(Some(v)),
                _ => return Err(::Error::InvalidData),
            },
            BatchColumn::Duration(ref mut vec) => match flag {
                datum::NIL_FLAG => vec.push(None),
                datum::DURATION_FLAG => {
                    let nanos =
                        number::decode_i64(&mut raw_datum).map_err(|_| ::Error::InvalidData)?;
                    let v = Duration::from_nanos(nanos, MAX_FSP).map_err(|_| ::Error::InvalidData)?;
                    vec.push(Some(v));
                }
                _ => return Err(::Error::InvalidData),
            },
            BatchColumn::Json(ref mut vec) => match flag {
                datum::NIL_FLAG => vec.push(None),
                datum::JSON_FLAG => {
                    let v = Json::decode(&mut raw_datum).map_err(|_| ::Error::InvalidData)?;
                    vec.push(Some(v));
                }
                _ => return Err(::Error::InvalidData),
            },
        }

        Ok(())
    }
}

impl ::EvalTypeProvider for BatchColumn {
    #[inline]
    fn get_eval_type(&self) -> ::EvalType {
        match self {
            BatchColumn::Int(_) => ::EvalType::Int,
            BatchColumn::Real(_) => ::EvalType::Real,
            BatchColumn::Decimal(_) => ::EvalType::Decimal,
            BatchColumn::String(_) => ::EvalType::String,
            BatchColumn::DateTime(_) => ::EvalType::DateTime,
            BatchColumn::Duration(_) => ::EvalType::Duration,
            BatchColumn::Json(_) => ::EvalType::Json,
        }
    }
}
