// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

use super::{JsonRef, JsonType};

const JSON_TYPE_BOOLEAN: &[u8] = b"BOOLEAN";
const JSON_TYPE_NONE: &[u8] = b"NULL";
const JSON_TYPE_INTEGER: &[u8] = b"INTEGER";
const JSON_TYPE_UNSIGNED_INTEGER: &[u8] = b"UNSIGNED INTEGER";
const JSON_TYPE_DOUBLE: &[u8] = b"DOUBLE";
const JSON_TYPE_STRING: &[u8] = b"STRING";
const JSON_TYPE_OBJECT: &[u8] = b"OBJECT";
const JSON_TYPE_ARRAY: &[u8] = b"ARRAY";

impl<'a> JsonRef<'a> {
    /// `json_type` is the implementation for
    /// <https://dev.mysql.com/doc/refman/5.7/en/json-attribute-functions.html#function_json-type>
    pub fn json_type(&self) -> &'static [u8] {
        match self.get_type() {
            JsonType::Object => JSON_TYPE_OBJECT,
            JsonType::Array => JSON_TYPE_ARRAY,
            JsonType::I64 => JSON_TYPE_INTEGER,
            JsonType::U64 => JSON_TYPE_UNSIGNED_INTEGER,
            JsonType::Double => JSON_TYPE_DOUBLE,
            JsonType::String => JSON_TYPE_STRING,
            JsonType::Literal => match self.get_literal() {
                Some(_) => JSON_TYPE_BOOLEAN,
                None => JSON_TYPE_NONE,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{super::Json, *};

    #[test]
    fn test_type() {
        let test_cases = vec![
            (r#"{"a": "b"}"#, JSON_TYPE_OBJECT),
            (r#"["a", "b"]"#, JSON_TYPE_ARRAY),
            ("-5", JSON_TYPE_INTEGER),
            ("5", JSON_TYPE_INTEGER),
            ("18446744073709551615", JSON_TYPE_DOUBLE),
            ("5.6", JSON_TYPE_DOUBLE),
            (r#""hello, world""#, JSON_TYPE_STRING),
            ("true", JSON_TYPE_BOOLEAN),
            ("null", JSON_TYPE_NONE),
        ];

        for (jstr, type_name) in test_cases {
            let json: Json = jstr.parse().unwrap();
            assert_eq!(json.as_ref().json_type(), type_name);
        }
    }
}
