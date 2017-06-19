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
use std::cmp::Ordering;
use std::f64;

use super::super::Result;
use super::{Json, JsonEncoder, ERR_CONVERT_FAILED};

const PRECEDENCE_BLOB: i32 = -1;
const PRECEDENCE_BIT: i32 = -2;
const PRECEDENCE_OPAQUE: i32 = -3;
const PRECEDENCE_DATETIME: i32 = -4;
const PRECEDENCE_TIME: i32 = -5;
const PRECEDENCE_DATE: i32 = -6;
const PRECEDENCE_BOOLEAN: i32 = -7;
const PRECEDENCE_ARRAY: i32 = -8;
const PRECEDENCE_OBJECT: i32 = -9;
const PRECEDENCE_STRING: i32 = -10;
const PRECEDENCE_NUMBER: i32 = -11;
const PRECEDENCE_NULL: i32 = -12;

impl Json {
    fn get_precedence(&self) -> i32 {
        match *self {
            Json::Object(_) => PRECEDENCE_OBJECT,
            Json::Array(_) => PRECEDENCE_ARRAY,
            Json::Boolean(_) => PRECEDENCE_BOOLEAN,
            Json::None => PRECEDENCE_NULL,
            Json::I64(_) | Json::Double(_) => PRECEDENCE_NUMBER,
            Json::String(_) => PRECEDENCE_STRING,
        }
    }

    fn as_f64(&self) -> Result<f64> {
        match *self {
            Json::Double(d) => Ok(d),
            Json::I64(d) => Ok(d as f64),
            Json::Boolean(_) => {
                let v = try!(self.as_literal());
                Ok(v as f64)
            }
            _ => Err(invalid_type!("{:?} from {} to f64", ERR_CONVERT_FAILED, self.to_string())),
        }
    }
}

impl Eq for Json {}
impl Ord for Json {
    fn cmp(&self, right: &Json) -> Ordering {
        self.partial_cmp(right).unwrap()
    }
}

impl PartialEq for Json {
    fn eq(&self, right: &Json) -> bool {
        self.partial_cmp(right).unwrap() == Ordering::Equal
    }
}

impl PartialOrd for Json {
    fn partial_cmp(&self, right: &Json) -> Option<Ordering> {
        let precedence_diff = self.get_precedence() - right.get_precedence();
        if precedence_diff == 0 {
            if self.get_precedence() == PRECEDENCE_NUMBER {
                let left_data = self.as_f64().unwrap();
                let right_data = right.as_f64().unwrap();
                if (left_data - right_data).abs() < f64::EPSILON {
                    return Some(Ordering::Equal);
                } else {
                    return left_data.partial_cmp(&right_data);
                }
            }

            return match (self, right) {
                (&Json::Boolean(left_data), &Json::Boolean(right_data)) => {
                    left_data.partial_cmp(&right_data)
                }
                (&Json::String(ref left_data), &Json::String(ref right_data)) => {
                    left_data.partial_cmp(right_data)
                }
                (&Json::Array(ref left_data), &Json::Array(ref right_data)) => {
                    left_data.partial_cmp(right_data)
                }
                (&Json::Object(_), &Json::Object(_)) => {
                    let mut left_data = vec![];
                    let mut right_data = vec![];
                    left_data.encode_json(self).unwrap();
                    right_data.encode_json(right).unwrap();
                    left_data.partial_cmp(&right_data)
                }
                _ => Some(Ordering::Equal),
            };
        }

        let left_data = self.as_f64();
        let right_data = right.as_f64();
        // tidb treats boolean as integer, but boolean is different from integer in JSON.
        // so we need convert them to same type and then compare.
        if left_data.is_ok() && right_data.is_ok() {
            let left = left_data.unwrap();
            let right = right_data.unwrap();
            return left.partial_cmp(&right);
        }

        if precedence_diff > 0 {
            Some(Ordering::Greater)
        } else {
            Some(Ordering::Less)
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_cmp_json_between_same_type() {
        let test_cases = vec![
            ("false", "true"),
            ("3", "5"),
            ("3.0", "4.9"),
            (r#""hello""#, r#""hello, world""#),
            (r#"["a", "b"]"#, r#"["a", "c"]"#),
            (r#"{"a": "b"}"#, r#"{"a": "c"}"#),
        ];
        for (left_str, right_str) in test_cases {
            let left: Json = left_str.parse().unwrap();
            let right: Json = right_str.parse().unwrap();
            assert!(left < right);
            assert_eq!(left, left);
        }
        assert_eq!(Json::None, Json::None);
    }

    #[test]
    fn test_cmp_json_between_diff_type() {
        let test_cases = vec![
            ("1.5", "2"),
            ("1.5", "false"),
            ("true", "1.5"),
            ("true", "2"),
            ("null", r#"{"a": "b"}"#),
            ("2", r#""hello, world""#),
            (r#""hello, world""#, r#"{"a": "b"}"#),
            (r#"{"a": "b"}"#, r#"["a", "b"]"#),
            (r#"["a", "b"]"#, "false"),
        ];

        for (left_str, right_str) in test_cases {
            let left: Json = left_str.parse().unwrap();
            let right: Json = right_str.parse().unwrap();
            assert!(left < right);
        }

        assert_eq!(Json::I64(2), Json::Boolean(false));
    }
}
