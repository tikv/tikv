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

//! Type definitions

mod eval_type;
mod field_type;

pub use self::eval_type::EvalType;
pub use self::field_type::{Collation, FieldTypeAccessor, FieldTypeFlag, FieldTypeTp};

/// Length is unspecified, applicable to `FieldType`'s `flen` and `decimal`.
pub const UNSPECIFIED_LENGTH: isize = -1;

/// MySQL type maximum length
pub const MAX_BLOB_WIDTH: i32 = 16777216;
