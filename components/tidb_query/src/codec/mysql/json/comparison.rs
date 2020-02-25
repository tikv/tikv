// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

use std::cmp::Ordering;
use std::f64;

use super::super::Result;
use super::constants::*;
use super::{Json, JsonRef, JsonType, ERR_CONVERT_FAILED};

impl<'a> JsonRef<'a> {
    fn get_precedence(&self) -> i32 {
        match self.get_type() {
            JsonType::Object => PRECEDENCE_OBJECT,
            JsonType::Array => PRECEDENCE_ARRAY,
            JsonType::Literal => self
                .get_literal()
                .map_or(PRECEDENCE_NULL, |_| PRECEDENCE_BOOLEAN),
            JsonType::I64 | JsonType::U64 | JsonType::Double => PRECEDENCE_NUMBER,
            JsonType::String => PRECEDENCE_STRING,
        }
    }

    fn as_f64(&self) -> Result<f64> {
        match self.get_type() {
            JsonType::I64 => Ok(self.get_i64() as f64),
            JsonType::U64 => Ok(self.get_u64() as f64),
            JsonType::Double => Ok(self.get_double()),
            JsonType::Literal => {
                let v = self.as_literal().unwrap();
                Ok(v.into())
            }
            _ => Err(invalid_type!(
                "{} from {} to f64",
                ERR_CONVERT_FAILED,
                self.to_string()
            )),
        }
    }
}

impl<'a> Eq for JsonRef<'a> {}

impl<'a> Ord for JsonRef<'a> {
    fn cmp(&self, right: &JsonRef<'_>) -> Ordering {
        self.partial_cmp(right).unwrap()
    }
}

impl<'a> PartialEq for JsonRef<'a> {
    fn eq(&self, right: &JsonRef<'_>) -> bool {
        self.partial_cmp(right)
            .map_or(false, |r| r == Ordering::Equal)
    }
}
impl<'a> PartialOrd for JsonRef<'a> {
    // See `CompareBinary` in TiDB `types/json/binary_functions.go`
    fn partial_cmp(&self, right: &JsonRef<'_>) -> Option<Ordering> {
        let precedence_diff = self.get_precedence() - right.get_precedence();
        if precedence_diff == 0 {
            if self.get_precedence() == PRECEDENCE_NULL {
                // for JSON null.
                return Some(Ordering::Equal);
            }

            return match self.get_type() {
                JsonType::I64 | JsonType::U64 | JsonType::Double => {
                    let left_data = self.as_f64().unwrap();
                    let right_data = right.as_f64().unwrap();
                    if (left_data - right_data).abs() < f64::EPSILON {
                        Some(Ordering::Equal)
                    } else {
                        left_data.partial_cmp(&right_data)
                    }
                }
                JsonType::Literal => {
                    // false is less than true.
                    self.get_literal().partial_cmp(&right.get_literal())
                }
                JsonType::Object => {
                    // only equal is defined on two json objects.
                    // larger and smaller are not defined.
                    self.value().partial_cmp(right.value())
                }
                JsonType::String => {
                    if let (Ok(left), Ok(right)) = (self.get_str_bytes(), right.get_str_bytes()) {
                        left.partial_cmp(right)
                    } else {
                        return None;
                    }
                }
                JsonType::Array => {
                    let left_count = self.get_elem_count();
                    let right_count = right.get_elem_count();
                    let mut i = 0;
                    while i < left_count && i < right_count {
                        if let (Ok(left_ele), Ok(right_ele)) =
                            (self.array_get_elem(i), right.array_get_elem(i))
                        {
                            match left_ele.partial_cmp(&right_ele) {
                                order @ None
                                | order @ Some(Ordering::Greater)
                                | order @ Some(Ordering::Less) => return order,
                                Some(Ordering::Equal) => i += 1,
                            }
                        } else {
                            return None;
                        }
                    }
                    Some(left_count.cmp(&right_count))
                }
            };
        }

        let left_data = self.as_f64();
        let right_data = right.as_f64();
        // tidb treats boolean as integer, but boolean is different from integer in JSON.
        // so we need convert them to same type and then compare.
        if let (Ok(left), Ok(right)) = (left_data, right_data) {
            return left.partial_cmp(&right);
        }

        if precedence_diff > 0 {
            Some(Ordering::Greater)
        } else {
            Some(Ordering::Less)
        }
    }
}

impl Eq for Json {}
impl Ord for Json {
    fn cmp(&self, right: &Json) -> Ordering {
        self.as_ref().partial_cmp(&right.as_ref()).unwrap()
    }
}

impl PartialEq for Json {
    fn eq(&self, right: &Json) -> bool {
        self.as_ref().partial_cmp(&right.as_ref()).unwrap() == Ordering::Equal
    }
}

impl PartialOrd for Json {
    fn partial_cmp(&self, right: &Json) -> Option<Ordering> {
        self.as_ref().partial_cmp(&right.as_ref())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cmp_json_between_same_type() {
        let test_cases = vec![
            ("false", "true"),
            ("-3", "3"),
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
        assert_eq!(Json::none().unwrap(), Json::none().unwrap());
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

        assert_eq!(Json::from_i64(2).unwrap(), Json::from_bool(false).unwrap());
    }
}
