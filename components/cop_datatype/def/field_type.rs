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

use tipb::expression::FieldType;
use tipb::schema::ColumnInfo;

use num_traits::FromPrimitive;

/// Valid values of `tipb::expression::FieldType::tp` and `tipb::schema::ColumnInfo::tp`.
///
/// `FieldType` is the field type of a column defined by schema.
///
/// `ColumnInfo` describes a column. It contains `FieldType` and some other column specific
/// information. However for historical reasons, fields in `FieldType` (for example, `tp`)
/// are flattened into `ColumnInfo`. Semantically these fields are identical.
#[derive(Primitive)]
pub enum FieldTypeTp {
    Decimal = 0,
    Tiny = 1,
    Short = 2,
    Long = 3,
    Float = 4,
    Double = 5,
    Null = 6,
    Timestamp = 7,
    LongLong = 8,
    Int24 = 9,
    Date = 10,
    Duration = 11,
    DateTime = 12,
    Year = 13,
    NewDate = 14,
    VarChar = 15,
    Bit = 16,
    JSON = 0xf5,
    NewDecimal = 0xf6,
    Enum = 0xf7,
    Set = 0xf8,
    TinyBlob = 0xf9,
    MediumBlob = 0xfa,
    LongBlob = 0xfb,
    Blob = 0xfc,
    VarString = 0xfd,
    String = 0xfe,
    Geometry = 0xff,
}

#[derive(Primitive)]
pub enum Collation {
    Binary = 63,
    UTF8Bin = 83, // Default
}

bitflags! {
    pub struct FieldTypeFlag: u32 {
        /// Field can't be NULL.
        const NOT_NULL = 1;

        /// Field is unsigned.
        const UNSIGNED = 1 << 5;

        /// Field is binary.
        const BINARY = 1 << 7;

        /// Internal: Used when we want to parse string to JSON in CAST.
        const PARSE_TO_JSON = 1 << 18;

        /// Internal: Used for telling boolean literal from integer.
        const IS_BOOLEAN = 1 << 19;
    }
}

pub trait FieldTypeProvider {
    fn get_tp(&self) -> ::Result<FieldTypeTp>;

    fn get_flag(&self) -> FieldTypeFlag;

    fn get_flen(&self) -> i32;

    fn get_decimal(&self) -> i32;

    fn get_collation(&self) -> Collation;
}

impl FieldTypeProvider for FieldType {
    fn get_tp(&self) -> ::Result<FieldTypeTp> {
        FieldTypeTp::from_i32(FieldType::get_tp(self)).ok_or_else(|| ::Error::UnsupportedType)
    }

    fn get_flag(&self) -> FieldTypeFlag {
        FieldTypeFlag::from_bits_truncate(FieldType::get_flag(self))
    }

    fn get_flen(&self) -> i32 {
        FieldType::get_flen(self)
    }

    fn get_decimal(&self) -> i32 {
        FieldType::get_decimal(self)
    }

    fn get_collation(&self) -> Collation {
        if let Some(collation) = Collation::from_i32(FieldType::get_collate(self)) {
            return collation;
        }
        Collation::UTF8Bin
    }
}

impl FieldTypeProvider for ColumnInfo {
    fn get_tp(&self) -> ::Result<FieldTypeTp> {
        FieldTypeTp::from_i32(ColumnInfo::get_tp(self)).ok_or_else(|| ::Error::UnsupportedType)
    }

    fn get_flag(&self) -> FieldTypeFlag {
        FieldTypeFlag::from_bits_truncate(ColumnInfo::get_flag(self) as u32)
    }

    fn get_flen(&self) -> i32 {
        ColumnInfo::get_columnLen(self)
    }

    fn get_decimal(&self) -> i32 {
        ColumnInfo::get_decimal(self)
    }

    fn get_collation(&self) -> Collation {
        if let Some(collation) = Collation::from_i32(ColumnInfo::get_collation(self)) {
            return collation;
        }
        Collation::UTF8Bin
    }
}
