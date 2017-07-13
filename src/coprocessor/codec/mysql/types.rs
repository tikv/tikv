// Copyright 2016 PingCAP, Inc.
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

/// Field can't be NULL
const NOT_NULL_FLAG: u64 = 1;
/// The field is unsigned.
const UNSIGNED_FLAG: u64 = 32;

/// `has_unsigned_flag` checks if `UNSIGNED_FLAG` is set.
#[inline]
pub fn has_unsigned_flag(flag: u64) -> bool {
    flag & UNSIGNED_FLAG > 0
}

/// `has_not_null_flag` checks if `NOT_NULL_FLAG` is set.
#[inline]
pub fn has_not_null_flag(flag: u64) -> bool {
    flag & NOT_NULL_FLAG > 0
}

/// `MySQL` type informations.
pub const UNSPECIFIED: u8 = 0;
pub const TINY: u8 = 1;
pub const SHORT: u8 = 2;
pub const LONG: u8 = 3;
pub const FLOAT: u8 = 4;
pub const DOUBLE: u8 = 5;
pub const NULL: u8 = 6;
pub const TIMESTAMP: u8 = 7;
pub const LONG_LONG: u8 = 8;
pub const INT24: u8 = 9;
pub const DATE: u8 = 10;
pub const DURATION: u8 = 11;
pub const DATETIME: u8 = 12;
pub const YEAR: u8 = 13;
pub const NEWDATE: u8 = 14;
pub const VARCHAR: u8 = 15;
pub const BIT: u8 = 16;
pub const NEW_DECIMAL: u8 = 246;
pub const ENUM: u8 = 247;
pub const SET: u8 = 248;
pub const TINY_BLOB: u8 = 249;
pub const MEDIUM_BLOB: u8 = 250;
pub const LONG_BLOB: u8 = 251;
pub const BLOB: u8 = 252;
pub const VAR_STRING: u8 = 253;
pub const STRING: u8 = 254;
pub const GEOMETRY: u8 = 255;
