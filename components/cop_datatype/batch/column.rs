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

use tikv::coprocessor::codec::mysql::{Decimal, Duration, Json, Time};
use tikv::coprocessor::codec::Datum;

/// An array of datums in the same data type and is column oriented.
///
/// Stores datums of multiple rows of one column.
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
    pub fn with_capacity(capacity: usize, eval_tp: &::EvalTypeProvider) -> ::Result<Self> {
        eval_tp.get_eval_type().map(|eval_tp| match eval_tp {
            ::EvalType::Int => BatchColumn::Int(Vec::with_capacity(capacity)),
            ::EvalType::Real => BatchColumn::Real(Vec::with_capacity(capacity)),
            ::EvalType::Decimal => BatchColumn::Decimal(Vec::with_capacity(capacity)),
            ::EvalType::String => BatchColumn::String(Vec::with_capacity(capacity)),
            ::EvalType::DateTime => BatchColumn::DateTime(Vec::with_capacity(capacity)),
            ::EvalType::Duration => BatchColumn::Duration(Vec::with_capacity(capacity)),
            ::EvalType::Json => BatchColumn::Json(Vec::with_capacity(capacity)),
        })
    }

    /// Returns the number of datums contained in this column.
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
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Pushes a value into the inner vector by decoding the datum and converting to current
    /// column's type.
    ///
    // TODO: We can remove the dependency of `Datum` and decode by our own.
    pub fn push_datum(&mut self, mut datum: &[u8]) -> ::Result<()> {
        // Note that there will be a memory copy when decoding.
        ::tikv::coprocessor::codec::datum::decode_datum(&mut datum)
            .map_err(|_| ::Error::InvalidData)
            .and_then(|datum| {
                match self {
                    BatchColumn::Int(ref mut vec) => match datum {
                        Datum::Null => vec.push(None),
                        Datum::I64(v) => vec.push(Some(v)),
                        Datum::U64(v) => vec.push(Some(v as i64)),
                        datum => {
                            // We want to call datum's Display interface,
                            // instead of its own "to_string".
                            return Err(::Error::InvalidConversion(
                                ::std::string::ToString::to_string(&datum),
                                "BatchColumn::Int".to_owned(),
                            ));
                        }
                    },
                    BatchColumn::Real(ref mut vec) => match datum {
                        Datum::Null => vec.push(None),
                        Datum::F64(v) => vec.push(Some(v)),
                        datum => {
                            return Err(::Error::InvalidConversion(
                                ::std::string::ToString::to_string(&datum),
                                "BatchColumn::Real".to_owned(),
                            ))
                        }
                    },
                    BatchColumn::Decimal(ref mut vec) => match datum {
                        Datum::Null => vec.push(None),
                        Datum::Dec(v) => vec.push(Some(v)),
                        datum => {
                            return Err(::Error::InvalidConversion(
                                ::std::string::ToString::to_string(&datum),
                                "BatchColumn::Decimal".to_owned(),
                            ))
                        }
                    },
                    BatchColumn::String(ref mut vec) => match datum {
                        Datum::Null => vec.push(None),
                        Datum::Bytes(v) => vec.push(Some(v)),
                        datum => {
                            return Err(::Error::InvalidConversion(
                                ::std::string::ToString::to_string(&datum),
                                "BatchColumn::String".to_owned(),
                            ))
                        }
                    },
                    BatchColumn::DateTime(ref mut vec) => match datum {
                        Datum::Null => vec.push(None),
                        Datum::Time(v) => vec.push(Some(v)),
                        datum => {
                            return Err(::Error::InvalidConversion(
                                ::std::string::ToString::to_string(&datum),
                                "BatchColumn::DateTime".to_owned(),
                            ))
                        }
                    },
                    BatchColumn::Duration(ref mut vec) => match datum {
                        Datum::Null => vec.push(None),
                        Datum::Dur(v) => vec.push(Some(v)),
                        datum => {
                            return Err(::Error::InvalidConversion(
                                ::std::string::ToString::to_string(&datum),
                                "BatchColumn::Duration".to_owned(),
                            ))
                        }
                    },
                    BatchColumn::Json(ref mut vec) => match datum {
                        Datum::Null => vec.push(None),
                        Datum::Json(v) => vec.push(Some(v)),
                        datum => {
                            return Err(::Error::InvalidConversion(
                                ::std::string::ToString::to_string(&datum),
                                "BatchColumn::Json".to_owned(),
                            ))
                        }
                    },
                };
                Ok(())
            })
    }
}

impl ::EvalTypeProvider for BatchColumn {
    fn get_eval_type(&self) -> ::Result<::EvalType> {
        let eval_tp = match self {
            BatchColumn::Int(_) => ::EvalType::Int,
            BatchColumn::Real(_) => ::EvalType::Real,
            BatchColumn::Decimal(_) => ::EvalType::Decimal,
            BatchColumn::String(_) => ::EvalType::String,
            BatchColumn::DateTime(_) => ::EvalType::DateTime,
            BatchColumn::Duration(_) => ::EvalType::Duration,
            BatchColumn::Json(_) => ::EvalType::Json,
        };
        Ok(eval_tp)
    }
}
