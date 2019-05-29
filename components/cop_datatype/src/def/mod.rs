// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

//! Type definitions

mod eval_type;
mod field_type;

pub use self::eval_type::EvalType;
pub use self::field_type::{Collation, FieldTypeAccessor, FieldTypeFlag, FieldTypeTp};

/// Length is unspecified, applicable to `FieldType`'s `flen` and `decimal`.
pub const UNSPECIFIED_LENGTH: isize = -1;

/// MySQL type maximum length
pub const MAX_BLOB_WIDTH: i32 = 16_777_216; // FIXME: Should be isize
pub const MAX_DECIMAL_WIDTH: isize = 65;
pub const MAX_REAL_WIDTH: isize = 23;
