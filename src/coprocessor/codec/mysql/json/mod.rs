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
mod path_expr;
// json functions
mod json_extract;
mod json_merge;
mod json_modify;
mod json_type;
mod json_unquote;
mod json_remove;

use std::collections::BTreeMap;
pub use self::binary::{JsonDecoder, JsonEncoder};
pub use self::path_expr::{parse_json_path_expr, PathExpression};
pub use self::json_modify::ModifyType;

use util::is_even;
use super::super::datum::Datum;
use super::super::{Error, Result};

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
    U64(u64),
    Double(f64),
    String(String),
    Boolean(bool),
    None,
}

// https://dev.mysql.com/doc/refman/5.7/en/json-creation-functions.html#function_json-array
pub fn json_array(elems: Vec<Datum>) -> Result<Json> {
    let mut a = Vec::with_capacity(elems.len());
    for elem in elems {
        a.push(try!(elem.into_json()));
    }
    Ok(Json::Array(a))
}

// https://dev.mysql.com/doc/refman/5.7/en/json-creation-functions.html#function_json-object
pub fn json_object(kvs: Vec<Datum>) -> Result<Json> {
    let len = kvs.len();
    if !is_even(len) {
        return Err(Error::Other(box_err!(
            "Incorrect parameter count in the call to native \
             function 'JSON_OBJECT'"
        )));
    }
    let mut map = BTreeMap::new();
    let mut key = None;
    for elem in kvs {
        if key.is_none() {
            // take elem as key
            if elem == Datum::Null {
                return Err(invalid_type!(
                    "JSON documents may not contain NULL member names"
                ));
            }
            key = Some(try!(elem.into_string()));
        } else {
            // take elem as value
            let val = try!(elem.into_json());
            map.insert(key.take().unwrap(), val);
        }
    }
    Ok(Json::Object(map))
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_json_array() {
        let cases = vec![
            (
                vec![
                    Datum::I64(1),
                    Datum::Bytes(b"sdf".to_vec()),
                    Datum::U64(2),
                    Datum::Json(r#"[3,4]"#.parse().unwrap()),
                ],
                r#"[1,"sdf",2,[3,4]]"#.parse().unwrap(),
            ),
            (vec![], "[]".parse().unwrap()),
        ];
        for (d, ep_json) in cases {
            assert_eq!(json_array(d).unwrap(), ep_json);
        }
    }

    #[test]
    fn test_json_object() {
        let cases = vec![
            vec![Datum::I64(1)],
            vec![
                Datum::I64(1),
                Datum::Bytes(b"sdf".to_vec()),
                Datum::Null,
                Datum::U64(2),
            ],
        ];
        for d in cases {
            assert!(json_object(d).is_err());
        }

        let cases = vec![
            (
                vec![
                    Datum::I64(1),
                    Datum::Bytes(b"sdf".to_vec()),
                    Datum::Bytes(b"asd".to_vec()),
                    Datum::Bytes(b"qwe".to_vec()),
                    Datum::I64(2),
                    Datum::Json(r#"{"3":4}"#.parse().unwrap()),
                ],
                r#"{"1":"sdf","2":{"3":4},"asd":"qwe"}"#.parse().unwrap(),
            ),
            (vec![], "{}".parse().unwrap()),
        ];
        for (d, ep_json) in cases {
            assert_eq!(json_object(d).unwrap(), ep_json);
        }
    }
}
