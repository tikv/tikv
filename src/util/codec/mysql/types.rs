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
pub const UNSPECIFIED: i32 = 0;
pub const TINY: i32 = 1;
pub const SHORT: i32 = 2;
pub const LONG: i32 = 3;
pub const FLOAT: i32 = 4;
pub const DOUBLE: i32 = 5;
pub const NULL: i32 = 6;
pub const TIMESTAMP: i32 = 7;
pub const LONG_LONG: i32 = 8;
pub const INT24: i32 = 9;
pub const DATE: i32 = 10;
pub const DURATION: i32 = 11;
pub const DATETIME: i32 = 12;
pub const YEAR: i32 = 13;
pub const NEWDATE: i32 = 14;
pub const VARCHAR: i32 = 15;
pub const BIT: i32 = 16;
pub const NEW_DECIMAL: i32 = 246;
pub const ENUM: i32 = 247;
pub const SET: i32 = 248;
pub const TINY_BLOB: i32 = 249;
pub const MEDIUM_BLOB: i32 = 250;
pub const LONG_BLOB: i32 = 251;
pub const BLOB: i32 = 252;
pub const VAR_STRING: i32 = 253;
pub const STRING: i32 = 254;
pub const GEOMETRY: i32 = 255;
