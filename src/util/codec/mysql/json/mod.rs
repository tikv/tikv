// Copyright 2017 PingCAP, Inc.
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
// FIXME(shirly): remove following later
#![allow(dead_code)]
mod binary;
mod comparison;
mod serde;
// json functions
mod json_extract;
mod json_merge;
mod json_modify;
mod json_type;
mod json_unquote;
mod path_expr;

use std::collections::BTreeMap;
pub use self::binary::{JsonEncoder, JsonDecoder};
pub use self::path_expr::{PathExpression, parse_json_path_expr};
pub use self::json_modify::ModifyType;

const ERR_CONVERT_FAILED: &str = "Can not covert from ";

/// Json implements type json used in tikv, it specifies the following
/// implementations:
/// 1. Serialize `json` values into binary representation, and reading values
///  back from the binary representation.
/// 2. Serialize `json` values into readable string representation, and reading
/// values back from string representation.
/// 3. sql functions like `JSON_TYPE`, etc
#[derive(Clone, Debug)]
pub enum Json {
    Object(BTreeMap<String, Json>),
    Array(Vec<Json>),
    I64(i64),
    Double(f64),
    String(String),
    Boolean(bool),
    None,
}
