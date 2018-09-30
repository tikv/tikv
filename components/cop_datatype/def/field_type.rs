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
///
/// Please refer to `mysql/type.go` in TiDB.
#[derive(Primitive, PartialEq)]
pub enum FieldTypeTp {
    Unspecified = 0, // Default
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

/// Valid values of `tipv::expression::FieldType::collate` and
/// `tipb::schema::ColumnInfo::collation`.
///
/// The default value if `UTF8Bin`.
#[derive(Primitive, PartialEq)]
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
    fn get_tp(&self) -> FieldTypeTp;

    fn get_flag(&self) -> FieldTypeFlag;

    fn get_flen(&self) -> i32;

    fn get_decimal(&self) -> i32;

    fn get_collation(&self) -> Collation;

    /// Whether this type is unsigned.
    #[inline]
    fn is_unsigned(&self) -> bool {
        self.get_flag().contains(FieldTypeFlag::UNSIGNED)
    }

    /// Whether this type is a blob type.
    ///
    /// Please refer to `IsTypeBlob` in TiDB.
    #[inline]
    fn is_blob_like(&self) -> bool {
        let tp = self.get_tp();
        tp == FieldTypeTp::TinyBlob
            || tp == FieldTypeTp::MediumBlob
            || tp == FieldTypeTp::Blob
            || tp == FieldTypeTp::LongBlob
    }

    /// Whether this type is a char-like type like a string type or a varchar type.
    ///
    /// Please refer to `IsTypeChar` in TiDB.
    #[inline]
    fn is_char_like(&self) -> bool {
        let tp = self.get_tp();
        tp == FieldTypeTp::String || tp == FieldTypeTp::VarChar
    }

    /// Whether this type is a varchar-like type like a varstring type or a varchar type.
    ///
    /// Please refer to `IsTypeVarchar` in TiDB.
    #[inline]
    fn is_varchar_like(&self) -> bool {
        let tp = self.get_tp();
        tp == FieldTypeTp::VarString || tp == FieldTypeTp::VarChar
    }

    /// Whether this type is a string-like type.
    ///
    /// Please refer to `IsString` in TiDB.
    #[inline]
    fn is_string_like(&self) -> bool {
        self.is_blob_like()
            || self.is_char_like()
            || self.is_varchar_like()
            || self.get_tp() == FieldTypeTp::Unspecified
    }

    /// Whether this type is a binary-string-like type.
    ///
    /// Please refer to `IsBinaryStr` in TiDB.
    #[inline]
    fn is_binary_string_like(&self) -> bool {
        self.get_collation() == Collation::Binary && self.is_string_like()
    }

    /// Whether this type is a non-binary-string-like type.
    ///
    /// Please refer to `IsNonBinaryStr` in TiDB.
    #[inline]
    fn is_non_binary_string_like(&self) -> bool {
        self.get_collation() != Collation::Binary && self.is_string_like()
    }
}

impl FieldTypeProvider for FieldType {
    #[inline]
    fn get_tp(&self) -> FieldTypeTp {
        FieldTypeTp::from_i32(FieldType::get_tp(self)).unwrap_or(FieldTypeTp::Unspecified)
    }

    #[inline]
    fn get_flag(&self) -> FieldTypeFlag {
        FieldTypeFlag::from_bits_truncate(FieldType::get_flag(self))
    }

    #[inline]
    fn get_flen(&self) -> i32 {
        FieldType::get_flen(self)
    }

    #[inline]
    fn get_decimal(&self) -> i32 {
        FieldType::get_decimal(self)
    }

    #[inline]
    fn get_collation(&self) -> Collation {
        Collation::from_i32(FieldType::get_collate(self)).unwrap_or(Collation::UTF8Bin)
    }
}

impl FieldTypeProvider for ColumnInfo {
    #[inline]
    fn get_tp(&self) -> FieldTypeTp {
        FieldTypeTp::from_i32(ColumnInfo::get_tp(self)).unwrap_or(FieldTypeTp::Unspecified)
    }

    #[inline]
    fn get_flag(&self) -> FieldTypeFlag {
        FieldTypeFlag::from_bits_truncate(ColumnInfo::get_flag(self) as u32)
    }

    #[inline]
    fn get_flen(&self) -> i32 {
        ColumnInfo::get_columnLen(self)
    }

    #[inline]
    fn get_decimal(&self) -> i32 {
        ColumnInfo::get_decimal(self)
    }

    #[inline]
    fn get_collation(&self) -> Collation {
        Collation::from_i32(ColumnInfo::get_collation(self)).unwrap_or(Collation::UTF8Bin)
    }
}
