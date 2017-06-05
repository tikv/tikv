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

use super::Result;
use std::{str, f64};
use std::cmp::{Ord, Ordering, PartialEq, PartialOrd};
use std::collections::BTreeMap;
use std::result::Result as SResult;
use serde_json::{self, Value};
use std::io::Write;
use util::codec::number::{NumberDecoder, NumberEncoder};
use serde::ser::{Serialize, Serializer, SerializeTuple, SerializeMap};

const TYPE_CODE_OBJECT: u8 = 0x01;
const TYPE_CODE_ARRAY: u8 = 0x03;
const TYPE_CODE_LITERAL: u8 = 0x04;
const TYPE_CODE_I64: u8 = 0x09;
const TYPE_CODE_DOUBLE: u8 = 0x0b;
const TYPE_CODE_STRING: u8 = 0x0c;

#[allow(dead_code)]
const PRECEDENCE_BLOB: i32 = -1;
#[allow(dead_code)]
const PRECEDENCE_BIT: i32 = -2;
#[allow(dead_code)]
const PRECEDENCE_OPAQUE: i32 = -3;
#[allow(dead_code)]
const PRECEDENCE_DATETIME: i32 = -4;
#[allow(dead_code)]
const PRECEDENCE_TIME: i32 = -5;
#[allow(dead_code)]
const PRECEDENCE_DATE: i32 = -6;
const PRECEDENCE_BOOLEAN: i32 = -7;
const PRECEDENCE_ARRAY: i32 = -8;
const PRECEDENCE_OBJECT: i32 = -9;
const PRECEDENCE_STRING: i32 = -10;
const PRECEDENCE_NUMBER: i32 = -11;
const PRECEDENCE_NULL: i32 = -12;

const JSON_LITERAL_NIL: u8 = 0x00;
const JSON_LITERAL_TRUE: u8 = 0x01;
const JSON_LITERAL_FALSE: u8 = 0x02;

const LENGTH_TYPE: usize = 1;
const LENGTH_U16: usize = 2;
const LENGTH_U32: usize = 4;
const LENGTH_KEY_ENTRY: usize = LENGTH_U32 + LENGTH_U16;
const LENGTH_VALUE_ENTRY: usize = LENGTH_TYPE + LENGTH_U32;

const ERR_EMPTY_DOCUMENT: &str = "The document is empty";
const ERR_CONVERT_FAILED: &str = "Can not covert from ";

// Json implement type json used in tikv, it specifies the following
// implementations:
// 1. Serialize `json` values into binary representation, and reading values
//  back from the binary representation.
// 2. Serialize `json` values into readable string representation, and reading
// values back from string representation.
// The binary Json format from MySQL 5.7 is in the following link:
// https://github.com/mysql/mysql-server/blob/5.7/sql/json_binary.h#L52
// The only difference is that we use large `object` or large `array` for
// the small corresponding ones. That means in our implementation there
// is no difference between small `object` and big `object`, so does `array`.
#[derive(Clone, Debug)]
pub enum Json {
    Object(BTreeMap<String, Json>),
    Array(Vec<Json>),
    Literal(u8),
    I64(i64),
    Double(f64),
    String(String),
}

#[allow(dead_code)]
impl Json {
    pub fn deserialize(data: &[u8]) -> Result<Json> {
        if data.is_empty() {
            return Err(invalid_type!(ERR_EMPTY_DOCUMENT));
        }
        Json::decode(data[0], &data[LENGTH_TYPE..])
    }

    pub fn parse_from_string(s: &str) -> Result<Json> {
        if s.is_empty() {
            return Err(invalid_type!(ERR_EMPTY_DOCUMENT));
        }
        match serde_json::from_str(s) {
            Ok(value) => Ok(normalize(value)),
            Err(e) => Err(invalid_type!("Illegal Json text:{:?}", e)),
        }
    }

    pub fn serialize(&self) -> Vec<u8> {
        let mut data = vec![self.get_type_code()];
        self.encode(&mut data);
        data
    }

    fn encode(&self, buf: &mut Vec<u8>) -> usize {
        let start_len = buf.len();
        match *self {
            Json::Object(ref d) => encode_obj(d, buf),
            Json::Array(ref d) => encode_array(d, buf),
            Json::Literal(d) => encode_literal(d, buf),
            Json::I64(d) => encode_i64(d, buf),
            Json::Double(d) => encode_double(d, buf),
            Json::String(ref d) => encode_str(d, buf),
        };
        buf.len() - start_len
    }

    fn decode(code_type: u8, data: &[u8]) -> Result<Json> {
        match code_type {
            TYPE_CODE_OBJECT => decode_obj(data),
            TYPE_CODE_ARRAY => decode_array(data),
            TYPE_CODE_LITERAL => decode_literal(data),
            TYPE_CODE_I64 => decode_i64(data),
            TYPE_CODE_DOUBLE => decode_double(data),
            TYPE_CODE_STRING => decode_str(data),
            _ => Err(invalid_type!("unsupported type {:?}", code_type)),
        }
    }

    fn get_type_code(&self) -> u8 {
        match *self {
            Json::Object(_) => TYPE_CODE_OBJECT,
            Json::Array(_) => TYPE_CODE_ARRAY,
            Json::Literal(_) => TYPE_CODE_LITERAL,
            Json::I64(_) => TYPE_CODE_I64,
            Json::Double(_) => TYPE_CODE_DOUBLE,
            Json::String(_) => TYPE_CODE_STRING,
        }
    }

    fn get_precedence(&self) -> i32 {
        match *self {
            Json::Object(_) => PRECEDENCE_OBJECT,
            Json::Array(_) => PRECEDENCE_ARRAY,
            Json::Literal(d) => {
                if d == JSON_LITERAL_NIL {
                    PRECEDENCE_NULL
                } else {
                    PRECEDENCE_BOOLEAN
                }
            }
            Json::I64(_) | Json::Double(_) => PRECEDENCE_NUMBER,
            Json::String(_) => PRECEDENCE_STRING,
        }
    }

    pub fn to_string(&self) -> String {
        serde_json::to_string(self).unwrap()
    }

    fn as_literal(&self) -> Result<u8> {
        if let Json::Literal(d) = *self {
            Ok(d)
        } else {
            Err(invalid_type!("{} from {} to literal",
                              ERR_CONVERT_FAILED,
                              self.get_type_code()))
        }
    }

    fn as_f64(&self) -> Result<f64> {
        match *self {
            Json::Double(d) => Ok(d),
            Json::I64(d) => Ok(d as f64),
            Json::Literal(d) if d != JSON_LITERAL_NIL => Ok(d as f64),
            _ => {
                Err(invalid_type!("{} from {} to f64",
                                  ERR_CONVERT_FAILED,
                                  self.get_type_code()))
            }
        }
    }

    fn as_str(&self) -> Result<&str> {
        if let Json::String(ref s) = *self {
            return Ok(s);
        }
        Err(invalid_type!("{} from {} to string",
                          ERR_CONVERT_FAILED,
                          self.get_type_code()))
    }

    fn as_array(&self) -> Result<&[Json]> {
        if let Json::Array(ref s) = *self {
            return Ok(s);
        }
        Err(invalid_type!("{} from {} to array",
                          ERR_CONVERT_FAILED,
                          self.get_type_code()))
    }
}

impl Serialize for Json {
    fn serialize<S>(&self, serializer: S) -> SResult<S::Ok, S::Error>
        where S: Serializer
    {
        match *self {
            Json::Literal(l) => {
                match l {
                    JSON_LITERAL_NIL => serializer.serialize_none(),
                    JSON_LITERAL_TRUE => serializer.serialize_bool(true),
                    _ => serializer.serialize_bool(false),
                }
            }
            Json::String(ref s) => serializer.serialize_str(s),
            Json::Object(ref obj) => {
                let mut map = try!(serializer.serialize_map(Some(obj.len())));
                for (k, v) in obj {
                    try!(map.serialize_entry(k, v));
                }
                map.end()
            }
            Json::Array(ref array) => {
                let mut tup = try!(serializer.serialize_tuple(array.len()));
                for item in array {
                    try!(tup.serialize_element(item));
                }
                tup.end()
            }
            Json::Double(d) => serializer.serialize_f64(d),
            Json::I64(d) => serializer.serialize_i64(d),
        }
    }
}

impl PartialEq for Json {
    fn eq(&self, right: &Json) -> bool {
        self.cmp(right) == Ordering::Equal
    }
}

impl PartialOrd for Json {
    fn partial_cmp(&self, right: &Json) -> Option<Ordering> {
        let precedence_diff = self.get_precedence() - right.get_precedence();
        if precedence_diff == 0 {
            return match self.get_type_code() {
                TYPE_CODE_LITERAL => {
                    let left_data = self.as_literal().unwrap();
                    let right_data = right.as_literal().unwrap();
                    right_data.partial_cmp(&left_data)
                }
                TYPE_CODE_DOUBLE | TYPE_CODE_I64 => {
                    let left_data = self.as_f64().unwrap();
                    let right_data = right.as_f64().unwrap();
                    left_data.partial_cmp(&right_data)
                }
                TYPE_CODE_STRING => {
                    let left_data = self.as_str().unwrap();
                    let right_data = right.as_str().unwrap();
                    left_data.partial_cmp(right_data)
                }
                TYPE_CODE_ARRAY => {
                    let left_data = self.as_array().unwrap();
                    let right_data = right.as_array().unwrap();
                    left_data.partial_cmp(right_data)
                }
                TYPE_CODE_OBJECT => {
                    let left_data = self.serialize();
                    let right_data = right.serialize();
                    left_data.partial_cmp(&right_data)
                }
                _ => Some(Ordering::Equal),
            };
        }

        let left_data = self.as_f64();
        let right_data = right.as_f64();
        // tidb treat boolean as integer, but boolean is different from integer in JSON.
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

impl Eq for Json {}

impl Ord for Json {
    fn cmp(&self, right: &Json) -> Ordering {
        self.partial_cmp(right).unwrap()
    }
}

fn encode_literal(data: u8, buf: &mut Vec<u8>) {
    buf.push(data);
}

fn decode_literal(data: &[u8]) -> Result<Json> {
    Ok(Json::Literal(data[0]))
}

fn encode_str(data: &str, buf: &mut Vec<u8>) {
    let bytes = data.as_bytes();
    buf.encode_var_u64(bytes.len() as u64).unwrap();
    buf.write_all(bytes).unwrap();
}

fn decode_str(mut data: &[u8]) -> Result<Json> {
    let length = try!(data.decode_var_u64());
    let encode_value = &data[0..(length as usize)];
    match str::from_utf8(encode_value) {
        Ok(v) => {
            let value = String::from(v);
            Ok(Json::String(value))
        }
        Err(e) => Err(invalid_type!("Invalid UTF-8 sequence:{}", e)),
    }
}

fn encode_double(data: f64, buf: &mut Vec<u8>) {
    buf.encode_f64_le(data).unwrap();
}

fn decode_double(mut data: &[u8]) -> Result<Json> {
    let value = try!(data.decode_f64_le());
    Ok(Json::Double(value))
}

fn encode_i64(data: i64, buf: &mut Vec<u8>) {
    buf.encode_i64_le(data).unwrap();
}

fn decode_i64(mut data: &[u8]) -> Result<Json> {
    let value = try!(data.decode_i64_le());
    Ok(Json::I64(value))
}

fn encode_obj(data: &BTreeMap<String, Json>, buf: &mut Vec<u8>) {
    // object: element-count size key-entry* value-entry* key* value*
    // element-count ::= uint32 number of members in object or number of elements in array
    let element_count = data.len();
    let element_count_len = LENGTH_U32;
    // size ::= uint32 number of bytes in the binary representation of the object or array
    let size_len = LENGTH_U32;
    // key-entry ::= key-offset(uint32) key-length(uint16)
    let key_entries_len = LENGTH_KEY_ENTRY * element_count;
    // value-entry ::= type(byte) offset-or-inlined-value(uint32)
    let value_entries_len = LENGTH_VALUE_ENTRY * element_count;
    let mut key_entries = vec![];
    let mut encode_keys = Vec::new();
    let mut key_offset = element_count_len + size_len + key_entries_len + value_entries_len;
    for key in data.keys() {
        let encode_key = key.as_bytes();
        let key_len = encode_keys.write(encode_key).unwrap();
        key_entries.encode_u32_le(key_offset as u32).unwrap();
        key_entries.encode_u16_le(key_len as u16).unwrap();
        key_offset += key_len;
    }

    let mut value_offset = key_offset as u32;
    let mut value_entries = vec![];
    let mut encode_values = vec![];
    for value in data.values() {
        encode_item_json(value,
                         &mut value_offset,
                         &mut value_entries,
                         &mut encode_values);
    }
    let size = value_offset;
    buf.encode_u32_le(element_count as u32).unwrap();
    buf.encode_u32_le(size as u32).unwrap();
    buf.write_all(key_entries.as_mut()).unwrap();
    buf.write_all(value_entries.as_mut()).unwrap();
    buf.write_all(encode_keys.as_mut()).unwrap();
    buf.write_all(encode_values.as_mut()).unwrap();
}

fn decode_obj(mut data: &[u8]) -> Result<Json> {
    // count size key_entries value_entries keys values
    let element_count = data.decode_u32_le().unwrap() as usize;
    data.decode_u32_le().unwrap();
    // already removed count and size
    let mut obj = BTreeMap::new();
    if element_count == 0 {
        return Ok(Json::Object(obj));
    }
    // key_entries
    let key_entries_len = LENGTH_KEY_ENTRY * element_count;
    let mut key_entries_data = &data[0..key_entries_len];

    // value-entry ::= type(byte) offset-or-inlined-value(uint32)
    let value_entries_len = LENGTH_VALUE_ENTRY * element_count;
    let mut value_entries_data = &data[key_entries_len..(key_entries_len + value_entries_len)];

    let mut key_offset = key_entries_len + value_entries_len;
    for _ in 0..element_count {
        let key_real_offset = key_entries_data.decode_u32_le().unwrap();
        let key_len = key_entries_data.decode_u16_le().unwrap();
        let key_data = &data[key_offset..(key_offset + key_len as usize)];
        let key = String::from(str::from_utf8(key_data).unwrap());
        let value =
            try!(decode_item_json(value_entries_data, &data[key_offset..], key_real_offset));
        obj.insert(key, value);
        key_offset += key_len as usize;
        value_entries_data = &value_entries_data[LENGTH_VALUE_ENTRY..];
    }
    Ok(Json::Object(obj))
}

fn encode_array(data: &[Json], buf: &mut Vec<u8>) {
    // array ::= element-count size value-entry* value*
    let element_count = data.len();
    let count_len = LENGTH_U32;
    let size_len = LENGTH_U32;
    let value_entries_len = LENGTH_VALUE_ENTRY * element_count;
    let mut value_offset = (count_len + size_len + value_entries_len) as u32;
    let mut value_entries = vec![];
    let mut encode_values = vec![];
    for value in data {
        encode_item_json(value,
                         &mut value_offset,
                         &mut value_entries,
                         &mut encode_values);
    }
    let total_size = value_offset;
    buf.encode_u32_le(element_count as u32).unwrap();
    buf.encode_u32_le(total_size as u32).unwrap();
    buf.write_all(value_entries.as_mut()).unwrap();
    buf.write_all(encode_values.as_mut()).unwrap();
}

fn decode_array(mut data: &[u8]) -> Result<Json> {
    // count size value_entries values
    let element_count = data.decode_u32_le().unwrap() as usize;
    data.decode_u32_le().unwrap();
    // already removed count and size
    let value_entries_len = LENGTH_VALUE_ENTRY * element_count;
    let mut value_entries_data = &data[0..value_entries_len];
    let values_data = &data[value_entries_len..];
    let mut array_data = Vec::with_capacity(element_count);
    let data_start_offset = (LENGTH_U32 + LENGTH_U32 + value_entries_len) as u32;
    for _ in 0..element_count {
        let value = try!(decode_item_json(value_entries_data, values_data, data_start_offset));
        value_entries_data = &value_entries_data[LENGTH_VALUE_ENTRY..];
        array_data.push(value);
    }
    Ok(Json::Array(array_data))
}

fn encode_item_json(data: &Json,
                    offset: &mut u32,
                    index_buf: &mut Vec<u8>,
                    data_buf: &mut Vec<u8>) {
    let code = data.get_type_code();
    index_buf.push(code);
    match *data {
        // If the data has length in (0, 4], it could be inline here.
        // And padding 0x00 to 4 bytes if needed.
        Json::Literal(ref v) => {
            let value_len = index_buf.write(&[*v]).unwrap();
            let left = LENGTH_U32 - value_len;
            for _ in 0..left {
                index_buf.push(JSON_LITERAL_NIL);
            }
        }
        _ => {
            index_buf.encode_u32_le(*offset).unwrap();
            let cur_value_len = data.encode(data_buf.as_mut()) as u32;
            *offset += cur_value_len;
        }
    }
}

fn decode_item_json(value_entries_data: &[u8],
                    values_data: &[u8],
                    data_start_position: u32)
                    -> Result<Json> {
    let entry = &value_entries_data[0..LENGTH_VALUE_ENTRY];
    let code = entry[0];
    let mut entry_value = &entry[LENGTH_TYPE..];
    match code {
        TYPE_CODE_LITERAL => Json::decode(code, entry_value),
        _ => {
            let real_offset = entry_value.decode_u32_le().unwrap();
            let offset_in_values = real_offset - data_start_position;
            Json::decode(code, &values_data[offset_in_values as usize..])
        }
    }
}

fn normalize(data: Value) -> Json {
    match data {
        Value::Null => Json::Literal(JSON_LITERAL_NIL),
        Value::Bool(data) => {
            if !data {
                Json::Literal(JSON_LITERAL_FALSE)
            } else {
                Json::Literal(JSON_LITERAL_TRUE)
            }
        }
        Value::Number(ref data) => {
            if data.is_f64() {
                Json::Double(data.as_f64().unwrap())
            } else {
                Json::I64(data.as_i64().unwrap())
            }
        }
        Value::String(data) => Json::String(data),
        Value::Array(data) => {
            let mut array = Vec::with_capacity(data.len());
            for item in data {
                array.push(normalize(item));
            }
            Json::Array(array)
        }
        Value::Object(data) => {
            let mut obj = BTreeMap::new();
            for (key, value) in data {
                let value_item = normalize(value);
                obj.insert(key, value_item);
            }
            Json::Object(obj)
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_json_serialize() {
        let jstr1 =
            r#"{"aaaaaaaaaaa": [1, "2", {"aa": "bb"}, 4.0], "bbbbbbbbbb": true, "ccccccccc": "d"}"#;
        let j1 = Json::parse_from_string(jstr1).unwrap();
        let jstr2 = r#"[{"a": 1, "b": true}, 3, 3.5, "hello, world", null, true]"#;
        let j2 = Json::parse_from_string(jstr2).unwrap();

        let json_nil = Json::Literal(0x00);
        let json_bool = Json::Literal(0x01);
        let json_double = Json::Double(3.24);
        let json_str = Json::String(String::from("hello, 世界"));
        let test_cases = vec![json_nil, json_bool, json_double, json_str, j1, j2];
        for json in test_cases {
            let data = json.serialize();
            let output = Json::deserialize(data.as_slice()).unwrap();
            let input_str = json.to_string();
            let output_str = output.to_string();
            assert_eq!(input_str, output_str);
        }
    }


    #[test]
    fn test_parse_from_string_for_object() {
        let jstr1 = r#"{"a": [1, "2", {"aa": "bb"}, 4.0, null], "c": null,"b": true}"#;
        let j1 = Json::parse_from_string(jstr1).unwrap();
        let jstr2 = j1.to_string();
        let expect_str = r#"{"a":[1,"2",{"aa":"bb"},4.0,null],"b":true,"c":null}"#;
        assert_eq!(jstr2, expect_str);
    }

    #[test]
    fn test_parse_from_string() {
        let legal_cases = vec!{
            (r#"{"key":"value"}"#, TYPE_CODE_OBJECT),
            (r#"["d1","d2"]"#, TYPE_CODE_ARRAY),
            (r#"3"#, TYPE_CODE_I64),
            (r#"3.0"#, TYPE_CODE_DOUBLE),
            (r#"null"#, TYPE_CODE_LITERAL),
            (r#"true"#, TYPE_CODE_LITERAL),
            (r#"false"#, TYPE_CODE_LITERAL),
        };

        for (json_str, code) in legal_cases {
            let json = Json::parse_from_string(json_str).unwrap();
            assert_eq!(json.get_type_code(), code);
        }

        let illegal_cases = vec!["[pxx,apaa]", "hpeheh", ""];
        for json_str in illegal_cases {
            let resp = Json::parse_from_string(json_str);
            assert!(resp.is_err());
        }
    }

    #[test]
    fn test_cmp_json() {
        let json_null = Json::parse_from_string("null").unwrap();
        let json_bool_true = Json::parse_from_string("true").unwrap();
        let json_bool_false = Json::parse_from_string("false").unwrap();
        let json_integer_big = Json::parse_from_string("5").unwrap();
        let json_integer_small = Json::parse_from_string("3").unwrap();
        let json_str_big = Json::parse_from_string(r#""hello, world""#).unwrap();
        let json_str_small = Json::parse_from_string(r#""hello""#).unwrap();
        let json_array_big = Json::parse_from_string(r#"["a", "c"]"#).unwrap();
        let json_array_small = Json::parse_from_string(r#"["a", "b"]"#).unwrap();
        let json_object = Json::parse_from_string(r#"{"a": "b"}"#).unwrap();
        let test_cases = vec![
            (&json_null, &json_integer_small),
            (&json_integer_small, &json_integer_big),
            (&json_integer_big, &json_str_small),
            (&json_str_small, &json_str_big),
            (&json_str_big, &json_object),
            (&json_object, &json_array_small),
            (&json_array_small, &json_array_big),
            (&json_array_big, &json_bool_false),
            (&json_bool_false, &json_bool_true),
        ];

        for (left, right) in test_cases {
            println!("left:{:?},right:{:?}", *left, *right);
            assert!(*left < *right);
        }
    }
}
