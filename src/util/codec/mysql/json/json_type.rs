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

use super::Json;

const JSON_TYPE_BOOLEAN: &'static [u8] = b"BOOLEAN";
const JSON_TYPE_NONE: &'static [u8] = b"NULL";
const JSON_TYPE_INTEGER: &'static [u8] = b"INTEGER";
const JSON_TYPE_DOUBLE: &'static [u8] = b"DOUBLE";
const JSON_TYPE_STRING: &'static [u8] = b"STRING";
const JSON_TYPE_OBJECT: &'static [u8] = b"OBJECT";
const JSON_TYPE_ARRAY: &'static [u8] = b"ARRAY";

impl Json {
    // json_type is the implementation for
    // https://dev.mysql.com/doc/refman/5.7/en/json-attribute-functions.html#function_json-type
    pub fn json_type(&self) -> &[u8] {
        match *self {
            Json::Object(_) => JSON_TYPE_OBJECT,
            Json::Array(_) => JSON_TYPE_ARRAY,
            Json::I64(_) => JSON_TYPE_INTEGER,
            Json::Double(_) => JSON_TYPE_DOUBLE,
            Json::String(_) => JSON_TYPE_STRING,
            Json::Boolean(_) => JSON_TYPE_BOOLEAN,
            Json::None => JSON_TYPE_NONE,
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_type() {
        let test_cases = vec![
            (r#"{"a": "b"}"#, JSON_TYPE_OBJECT),
            (r#"["a", "b"]"#, JSON_TYPE_ARRAY),
            ("5", JSON_TYPE_INTEGER),
            ("5.6", JSON_TYPE_DOUBLE),
            (r#""hello, world""#, JSON_TYPE_STRING),
            ("true", JSON_TYPE_BOOLEAN),
            ("null", JSON_TYPE_NONE),
        ];

        for (jstr, type_name) in test_cases {
            let json: Json = jstr.parse().unwrap();
            assert_eq!(json.json_type(), type_name);
        }
    }
}
