// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

use super::{JsonRef, JsonType};
use crate::FieldTypeTp;

const JSON_TYPE_BOOLEAN: &[u8] = b"BOOLEAN";
const JSON_TYPE_NONE: &[u8] = b"NULL";
const JSON_TYPE_INTEGER: &[u8] = b"INTEGER";
const JSON_TYPE_UNSIGNED_INTEGER: &[u8] = b"UNSIGNED INTEGER";
const JSON_TYPE_DOUBLE: &[u8] = b"DOUBLE";
const JSON_TYPE_STRING: &[u8] = b"STRING";
const JSON_TYPE_OBJECT: &[u8] = b"OBJECT";
const JSON_TYPE_ARRAY: &[u8] = b"ARRAY";
const JSON_TYPE_BIT: &[u8] = b"BIT";
const JSON_TYPE_BLOB: &[u8] = b"BLOB";
const JSON_TYPE_OPAQUE: &[u8] = b"OPAQUE";
const JSON_TYPE_DATE: &[u8] = b"DATE";
const JSON_TYPE_DATETIME: &[u8] = b"DATETIME";
const JSON_TYPE_TIME: &[u8] = b"TIME";

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
            JsonType::Opaque => match self.get_opaque_type() {
                Ok(
                    FieldTypeTp::TinyBlob
                    | FieldTypeTp::MediumBlob
                    | FieldTypeTp::LongBlob
                    | FieldTypeTp::Blob
                    | FieldTypeTp::String
                    | FieldTypeTp::VarString
                    | FieldTypeTp::VarChar,
                ) => JSON_TYPE_BLOB,
                Ok(FieldTypeTp::Bit) => JSON_TYPE_BIT,
                _ => JSON_TYPE_OPAQUE,
            },
            JsonType::Date => JSON_TYPE_DATE,
            JsonType::Datetime => JSON_TYPE_DATETIME,
            JsonType::Timestamp => JSON_TYPE_DATETIME,
            JsonType::Time => JSON_TYPE_TIME,
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
