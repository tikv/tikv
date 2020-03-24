// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

//! The binary JSON format from MySQL 5.7 is as follows:
//! ```text
//!   JSON doc ::= type value
//!   type ::=
//!       0x01 |       // large JSON object
//!       0x03 |       // large JSON array
//!       0x04 |       // literal (true/false/null)
//!       0x05 |       // int16
//!       0x06 |       // uint16
//!       0x07 |       // int32
//!       0x08 |       // uint32
//!       0x09 |       // int64
//!       0x0a |       // uint64
//!       0x0b |       // double
//!       0x0c |       // utf8mb4 string
//!   value ::=
//!       object  |
//!       array   |
//!       literal |
//!       number  |
//!       string  |
//!   object ::= element-count size key-entry* value-entry* key* value*
//!   array ::= element-count size value-entry* value*
//!
//!   // the number of members in object or number of elements in array
//!   element-count ::= uint32
//!
//!   //number of bytes in the binary representation of the object or array
//!   size ::= uint32
//!   key-entry ::= key-offset key-length
//!   key-offset ::= uint32
//!   key-length ::= uint16    // key length must be less than 64KB
//!   value-entry ::= type offset-or-inlined-value
//!
//!   // This field holds either the offset to where the value is stored,
//!   // or the value itself if it is small enough to be inlined (that is,
//!   // if it is a JSON literal or a small enough [u]int).
//!   offset-or-inlined-value ::= uint32
//!   key ::= utf8mb4-data
//!   literal ::=
//!       0x00 |   // JSON null literal
//!       0x01 |   // JSON true literal
//!       0x02 |   // JSON false literal
//!   number ::=  ....    // little-endian format for [u]int(16|32|64), whereas
//!                       // double is stored in a platform-independent, eight-byte
//!                       // format using float8store()
//!   string ::= data-length utf8mb4-data
//!   data-length ::= uint8*    // If the high bit of a byte is 1, the length
//!                             // field is continued in the next byte,
//!                             // otherwise it is the last byte of the length
//!                             // field. So we need 1 byte to represent
//!                             // lengths up to 127, 2 bytes to represent
//!                             // lengths up to 16383, and so on...
//! ```
//!

mod binary;
mod comparison;
// FIXME(shirly): remove following later
#[allow(dead_code)]
mod constants;
mod jcodec;
mod modifier;
mod path_expr;
mod serde;
// json functions
mod json_depth;
mod json_extract;
mod json_keys;
mod json_length;
mod json_merge;
mod json_modify;
mod json_remove;
mod json_type;
mod json_unquote;

pub use self::jcodec::{JsonDatumPayloadChunkEncoder, JsonDecoder, JsonEncoder};
pub use self::json_modify::ModifyType;
pub use self::path_expr::{parse_json_path_expr, PathExpression};

use std::collections::BTreeMap;
use std::convert::TryFrom;
use std::str;
use tikv_util::is_even;

use super::super::datum::Datum;
use super::super::{Error, Result};
use crate::codec::convert::ConvertTo;
use crate::codec::data_type::{Decimal, Real};
use crate::codec::mysql;
use crate::codec::mysql::{Duration, Time, TimeType};
use crate::expr::EvalContext;
use codec::number::{NumberCodec, F64_SIZE, I64_SIZE};
use constants::{JSON_LITERAL_FALSE, JSON_LITERAL_NIL, JSON_LITERAL_TRUE};

const ERR_CONVERT_FAILED: &str = "Can not covert from ";

/// The types of `Json` which follows https://tools.ietf.org/html/rfc7159#section-3
#[derive(Eq, PartialEq, FromPrimitive, Clone, Debug, Copy)]
pub enum JsonType {
    Object = 0x01,
    Array = 0x03,
    Literal = 0x04,
    I64 = 0x09,
    U64 = 0x0a,
    Double = 0x0b,
    String = 0x0c,
}

impl TryFrom<u8> for JsonType {
    type Error = Error;
    fn try_from(src: u8) -> Result<JsonType> {
        num_traits::FromPrimitive::from_u8(src)
            .ok_or_else(|| Error::InvalidDataType("unexpected JSON type".to_owned()))
    }
}

/// Represents a reference of JSON value aiming to reduce memory copy.
#[derive(Clone, Copy, Debug)]
pub struct JsonRef<'a> {
    type_code: JsonType,
    // Referred value
    value: &'a [u8],
}

impl<'a> JsonRef<'a> {
    pub fn new(type_code: JsonType, value: &[u8]) -> JsonRef<'_> {
        JsonRef { type_code, value }
    }

    /// Returns an owned Json via copying
    pub fn to_owned(&self) -> Json {
        Json {
            type_code: self.type_code,
            value: self.value.to_owned(),
        }
    }

    /// Returns the JSON type
    pub fn get_type(&self) -> JsonType {
        self.type_code
    }

    /// Returns the underlying value slice
    pub fn value(&self) -> &'a [u8] {
        &self.value
    }

    // Returns the JSON value as u64
    //
    // See `GetUint64()` in TiDB `json/binary.go`
    pub(crate) fn get_u64(&self) -> u64 {
        assert_eq!(self.type_code, JsonType::U64);
        NumberCodec::decode_u64_le(self.value())
    }

    // Returns the JSON value as i64
    //
    // See `GetInt64()` in TiDB `json/binary.go`
    pub(crate) fn get_i64(&self) -> i64 {
        assert_eq!(self.type_code, JsonType::I64);
        NumberCodec::decode_i64_le(self.value())
    }

    // Returns the JSON value as f64
    //
    // See `GetFloat64()` in TiDB `json/binary.go`
    pub(crate) fn get_double(&self) -> f64 {
        assert_eq!(self.type_code, JsonType::Double);
        NumberCodec::decode_f64_le(self.value())
    }

    // Gets the count of Object or Array
    //
    // See `GetElemCount()` in TiDB `json/binary.go`
    pub(crate) fn get_elem_count(&self) -> usize {
        assert!((self.type_code == JsonType::Object) | (self.type_code == JsonType::Array));
        NumberCodec::decode_u32_le(self.value()) as usize
    }

    // Returns `None` if the JSON value is `null`. Otherwise, returns
    // `Some(bool)`
    pub(crate) fn get_literal(&self) -> Option<bool> {
        assert_eq!(self.type_code, JsonType::Literal);
        match self.value()[0] {
            JSON_LITERAL_FALSE => Some(false),
            JSON_LITERAL_TRUE => Some(true),
            _ => None,
        }
    }

    // Returns the string value in bytes
    pub(crate) fn get_str_bytes(&self) -> Result<&'a [u8]> {
        assert_eq!(self.type_code, JsonType::String);
        let val = self.value();
        let (str_len, len_len) = NumberCodec::try_decode_var_u64(val)?;
        Ok(&val[len_len..len_len + str_len as usize])
    }

    // Returns the value as a &str
    pub(crate) fn get_str(&self) -> Result<&'a str> {
        Ok(str::from_utf8(self.get_str_bytes()?)?)
    }
}

/// Json implements type json used in tikv by Binary Json.
/// The Binary Json format from `MySQL` 5.7 is in the following link:
/// (https://github.com/mysql/mysql-server/blob/5.7/sql/json_binary.h#L52)
/// The only difference is that we use large `object` or large `array` for
/// the small corresponding ones. That means in our implementation there
/// is no difference between small `object` and big `object`, so does `array`.
#[derive(Clone, Debug)]
pub struct Json {
    type_code: JsonType,
    /// The binary encoded json data in bytes
    pub value: Vec<u8>,
}

use std::fmt::{Display, Formatter};

impl Display for Json {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "{}", self.to_string())
    }
}

impl Json {
    /// Creates a new JSON from the type and encoded bytes
    pub fn new(tp: JsonType, value: Vec<u8>) -> Self {
        Self {
            type_code: tp.into(),
            value,
        }
    }

    /// Creates a `string` JSON from a `String`
    pub fn from_string(s: String) -> Result<Self> {
        let mut value = vec![];
        value.write_json_str(s.as_str())?;
        Ok(Self::new(JsonType::String, value))
    }

    /// Creates a `string` JSON from a `&str`
    pub fn from_str_val(s: &str) -> Result<Self> {
        let mut value = vec![];
        value.write_json_str(s)?;
        Ok(Self::new(JsonType::String, value))
    }

    /// Creates a `literal` JSON from a `bool`
    pub fn from_bool(b: bool) -> Result<Self> {
        let mut value = vec![];
        value.write_json_literal(if b {
            JSON_LITERAL_TRUE
        } else {
            JSON_LITERAL_FALSE
        })?;
        Ok(Self::new(JsonType::Literal, value))
    }

    /// Creates a `number` JSON from a `u64`
    pub fn from_u64(v: u64) -> Result<Self> {
        let mut value = vec![];
        value.write_json_u64(v)?;
        Ok(Self::new(JsonType::U64, value))
    }

    /// Creates a `number` JSON from a `f64`
    pub fn from_f64(v: f64) -> Result<Self> {
        let mut value = vec![];
        value.write_json_f64(v)?;
        Ok(Self::new(JsonType::Double, value))
    }

    /// Creates a `number` JSON from an `i64`
    pub fn from_i64(v: i64) -> Result<Self> {
        let mut value = vec![];
        value.write_json_i64(v)?;
        Ok(Self::new(JsonType::I64, value))
    }

    /// Creates a `array` JSON from a collection of `JsonRef`
    pub fn from_ref_array(array: Vec<JsonRef<'_>>) -> Result<Self> {
        let mut value = vec![];
        value.write_json_ref_array(&array)?;
        Ok(Self::new(JsonType::Array, value))
    }

    /// Creates a `array` JSON from a collection of `Json`
    pub fn from_array(array: Vec<Json>) -> Result<Self> {
        let mut value = vec![];
        value.write_json_array(&array)?;
        Ok(Self::new(JsonType::Array, value))
    }

    /// Creates a `object` JSON from key-value pairs
    pub fn from_kv_pairs<'a>(entries: Vec<(&[u8], JsonRef<'a>)>) -> Result<Self> {
        let mut value = vec![];
        value.write_json_obj_from_keys_values(entries)?;
        Ok(Self::new(JsonType::Object, value))
    }

    /// Creates a `object` JSON from key-value pairs in BTreeMap
    pub fn from_object(map: BTreeMap<String, Json>) -> Result<Self> {
        let mut value = vec![];
        // TODO(fullstop000): use write_json_obj_from_keys_values instead
        value.write_json_obj(&map)?;
        Ok(Self::new(JsonType::Object, value))
    }

    /// Creates a `null` JSON
    pub fn none() -> Result<Self> {
        let mut value = vec![];
        value.write_json_literal(JSON_LITERAL_NIL)?;
        Ok(Self::new(JsonType::Literal, value))
    }

    /// Returns a `JsonRef` points to the starting of encoded bytes
    pub fn as_ref(&self) -> JsonRef<'_> {
        JsonRef {
            type_code: self.type_code,
            value: self.value.as_slice(),
        }
    }
}

/// Create JSON arrayy by given elements
/// https://dev.mysql.com/doc/refman/5.7/en/json-creation-functions.html#function_json-array
pub fn json_array(elems: Vec<Datum>) -> Result<Json> {
    let mut a = Vec::with_capacity(elems.len());
    for elem in elems {
        a.push(elem.into_json()?);
    }
    Json::from_array(a)
}

/// Create JSON object by given key-value pairs
/// https://dev.mysql.com/doc/refman/5.7/en/json-creation-functions.html#function_json-object
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
            key = Some(elem.into_string()?);
        } else {
            // take elem as value
            let val = elem.into_json()?;
            map.insert(key.take().unwrap(), val);
        }
    }
    Json::from_object(map)
}

impl ConvertTo<f64> for Json {
    ///  Keep compatible with TiDB's `ConvertJSONToFloat` function.
    #[inline]
    fn convert(&self, ctx: &mut EvalContext) -> Result<f64> {
        let d = match self.as_ref().get_type() {
            JsonType::Array | JsonType::Object => 0f64,
            JsonType::U64 => self.as_ref().get_u64() as f64,
            JsonType::I64 => self.as_ref().get_i64() as f64,
            JsonType::Double => self.as_ref().get_double(),
            JsonType::Literal => {
                self.as_ref()
                    .get_literal()
                    .map_or(0f64, |x| if x { 1f64 } else { 0f64 })
            }
            JsonType::String => self.as_ref().get_str_bytes()?.convert(ctx)?,
        };
        Ok(d)
    }
}

impl ConvertTo<Json> for i64 {
    #[inline]
    fn convert(&self, _: &mut EvalContext) -> Result<Json> {
        let mut value = vec![0; I64_SIZE];
        NumberCodec::encode_i64_le(&mut value, *self);
        Ok(Json {
            type_code: JsonType::I64,
            value,
        })
    }
}

impl ConvertTo<Json> for f64 {
    #[inline]
    fn convert(&self, _: &mut EvalContext) -> Result<Json> {
        // FIXME: `select json_type(cast(1111.11 as json))` should return `DECIMAL`, we return `DOUBLE` now.
        let mut value = vec![0; F64_SIZE];
        NumberCodec::encode_f64_le(&mut value, *self);
        Ok(Json {
            type_code: JsonType::Double,
            value,
        })
    }
}

impl ConvertTo<Json> for Real {
    #[inline]
    fn convert(&self, _: &mut EvalContext) -> Result<Json> {
        // FIXME: `select json_type(cast(1111.11 as json))` should return `DECIMAL`, we return `DOUBLE` now.
        let mut value = vec![0; F64_SIZE];
        NumberCodec::encode_f64_le(&mut value, self.into_inner());
        Ok(Json {
            type_code: JsonType::Double,
            value,
        })
    }
}

impl ConvertTo<Json> for Decimal {
    #[inline]
    fn convert(&self, ctx: &mut EvalContext) -> Result<Json> {
        // FIXME: `select json_type(cast(1111.11 as json))` should return `DECIMAL`, we return `DOUBLE` now.
        let val: f64 = self.convert(ctx)?;
        val.convert(ctx)
    }
}

impl ConvertTo<Json> for Time {
    #[inline]
    fn convert(&self, ctx: &mut EvalContext) -> Result<Json> {
        let tp = self.get_time_type();
        let s = if tp == TimeType::DateTime || tp == TimeType::Timestamp {
            self.round_frac(ctx, mysql::MAX_FSP)?
        } else {
            *self
        };
        Json::from_string(s.to_string())
    }
}

impl ConvertTo<Json> for Duration {
    #[inline]
    fn convert(&self, _: &mut EvalContext) -> Result<Json> {
        let d = self.maximize_fsp();
        Json::from_string(d.to_string())
    }
}

impl crate::codec::data_type::AsMySQLBool for Json {
    #[inline]
    fn as_mysql_bool(
        &self,
        _context: &mut crate::expr::EvalContext,
    ) -> tidb_query_common::error::Result<bool> {
        // TODO: This logic is not correct. See pingcap/tidb#9593
        Ok(false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc;

    use crate::expr::{EvalConfig, EvalContext};

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

    #[test]
    fn test_cast_to_real() {
        let test_cases = vec![
            ("{}", 0f64),
            ("[]", 0f64),
            ("3", 3f64),
            ("-3", -3f64),
            ("4.5", 4.5),
            ("true", 1f64),
            ("false", 0f64),
            ("null", 0f64),
            (r#""hello""#, 0f64),
            (r#""1234""#, 1234f64),
        ];
        let mut ctx = EvalContext::new(Arc::new(EvalConfig::default_for_test()));
        for (jstr, exp) in test_cases {
            let json: Json = jstr.parse().unwrap();
            let get: f64 = json.convert(&mut ctx).unwrap();
            assert!(
                (get - exp).abs() < std::f64::EPSILON,
                "json.as_f64 get: {}, exp: {}",
                get,
                exp
            );
        }
    }
}
