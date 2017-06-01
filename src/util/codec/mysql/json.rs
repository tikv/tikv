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
use std::collections::BTreeMap;
use std::result::Result as SResult;
use serde_json::{self, Value};
use util::codec::number::{NumberDecoder, NumberEncoder};
use serde::ser::{Serialize, Serializer, SerializeTuple, SerializeMap};

const TYPE_CODE_OBJECT: u8 = 0x01;
const TYPE_CODE_ARRAY: u8 = 0x03;
const TYPE_CODE_LITERAL: u8 = 0x04;
const TYPE_CODE_I64: u8 = 0x09;
const TYPE_CODE_DOUBLE: u8 = 0x0b;
const TYPE_CODE_STRING: u8 = 0x0c;

const JSON_LITERAL_NIL: u8 = 0x00;
const JSON_LITERAL_TRUE: u8 = 0x01;
const JSON_LITERAL_FALSE: u8 = 0x02;

const LENGTH_TYPE: usize = 1;
const LENGTH_U16: usize = 2;
const LENGTH_U32: usize = 4;
const LENGTH_KEY_ENTRY: usize = LENGTH_U32 + LENGTH_U16;
const LENGTH_VALUE_ENTRY: usize = LENGTH_TYPE + LENGTH_U32;

const ERR_EMPTY_DOCUMENT: &str = "The document is empty";

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
#[derive(Clone,PartialEq,Debug)]
pub enum Json {
    Literal(u8),
    String(String),
    Object(BTreeMap<String, Json>),
    Array(Vec<Json>),
    Double(f64),
    I64(i64),
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

    pub fn serialize(self) -> Vec<u8> {
        let mut data = vec![self.get_type_code()];
        self.encode(&mut data);
        data
    }

    fn encode(self, buf: &mut Vec<u8>) -> usize {
        let start_len = buf.len();
        match self {
            Json::Literal(d) => encode_literal(d, buf),
            Json::String(d) => encode_str(d, buf),
            Json::Object(d) => encode_obj(d, buf),
            Json::Array(d) => encode_array(d, buf),
            Json::Double(d) => encode_double(d, buf),
            Json::I64(d) => encode_i64(d, buf),
        };
        buf.len() - start_len
    }

    fn decode(code_type: u8, data: &[u8]) -> Result<Json> {
        match code_type {
            TYPE_CODE_LITERAL => decode_literal(data),
            TYPE_CODE_STRING => decode_str(data),
            TYPE_CODE_OBJECT => decode_obj(data),
            TYPE_CODE_ARRAY => decode_array(data),
            TYPE_CODE_DOUBLE => decode_double(data),
            TYPE_CODE_I64 => decode_i64(data),
            _ => Err(invalid_type!("unsupported type {:?}", code_type)),
        }
    }

    fn get_type_code(&self) -> u8 {
        match *self {
            Json::Literal(_) => TYPE_CODE_LITERAL,
            Json::String(_) => TYPE_CODE_STRING,
            Json::Object(_) => TYPE_CODE_OBJECT,
            Json::Array(_) => TYPE_CODE_ARRAY,
            Json::Double(_) => TYPE_CODE_DOUBLE,
            Json::I64(_) => TYPE_CODE_I64,
        }
    }

    pub fn to_string(&self) -> String {
        serde_json::to_string(self).unwrap()
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

fn encode_literal(data: u8, buf: &mut Vec<u8>) {
    buf.push(data);
}

fn decode_literal(data: &[u8]) -> Result<Json> {
    Ok(Json::Literal(data[0]))
}

fn encode_str(data: String, buf: &mut Vec<u8>) {
    buf.encode_var_u64(data.len() as u64).unwrap();
    buf.append(&mut data.into_bytes());
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
    buf.encode_f64_with_little_endian(data).unwrap();
}

fn decode_double(mut data: &[u8]) -> Result<Json> {
    let value = try!(data.decode_f64_with_little_endian());
    Ok(Json::Double(value))
}

fn encode_i64(data: i64, buf: &mut Vec<u8>) {
    buf.encode_i64_with_little_endian(data).unwrap();
}

fn decode_i64(mut data: &[u8]) -> Result<Json> {
    let value = try!(data.decode_i64_with_little_endian());
    Ok(Json::I64(value))
}

fn encode_obj(data: BTreeMap<String, Json>, buf: &mut Vec<u8>) {
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
    let mut keys = Vec::with_capacity(element_count);
    let mut values = Vec::with_capacity(element_count);
    for (key, value) in &data {
        keys.push(key.to_owned());
        values.push(value.to_owned());
    }
    let mut key_entries = vec![];
    let mut encode_keys = Vec::new();
    let mut key_offset = element_count_len + size_len + key_entries_len + value_entries_len;
    for key in keys {
        let mut encode_key = key.into_bytes();
        let key_len = encode_key.len();
        key_entries.encode_u32_with_little_endian(key_offset as u32).unwrap();
        key_entries.encode_u16_with_little_endian(key_len as u16).unwrap();
        encode_keys.append(&mut encode_key);
        key_offset += key_len;
    }

    let value_offset = key_offset;
    let (mut value_entries, mut encode_values) = encode_vec_json(values, value_offset as u32);
    let size = key_offset + encode_values.len();
    buf.encode_u32_with_little_endian(element_count as u32).unwrap();
    buf.encode_u32_with_little_endian(size as u32).unwrap();
    buf.append(key_entries.as_mut());
    buf.append(value_entries.as_mut());
    buf.append(encode_keys.as_mut());
    buf.append(encode_values.as_mut());
}

fn decode_obj(mut data: &[u8]) -> Result<Json> {
    // count size key_entries value_entries keys values
    let element_count = data.decode_u32_with_little_endian().unwrap() as usize;
    data.decode_u32_with_little_endian().unwrap();
    // already removed count and size
    let mut obj = BTreeMap::new();
    if element_count == 0 {
        return Ok(Json::Object(obj));
    }
    // decode key_entries
    let key_entries_len = LENGTH_KEY_ENTRY * element_count;
    let key_entries_data = &data[0..key_entries_len];
    let key_entries = decode_key_entries(key_entries_data, element_count);

    // value-entry ::= type(byte) offset-or-inlined-value(uint32)
    let value_entries_len = LENGTH_VALUE_ENTRY * element_count;
    let value_entries_data = &data[key_entries_len..(key_entries_len + value_entries_len)];

    // decode keys
    let mut keys = Vec::with_capacity(element_count);
    let mut offset = key_entries_len + value_entries_len;
    for &(_, length) in &key_entries {
        let key_data = &data[offset..(offset + length as usize)];
        offset += length as usize;
        let key = String::from(str::from_utf8(key_data).unwrap());
        keys.push(key);
    }

    // parse values
    let values = try!(decode_vec_json(value_entries_data, &data[offset..], element_count));
    for (key, value) in keys.iter().zip(values.iter()) {
        obj.insert(key.to_owned(), value.to_owned());
    }

    Ok(Json::Object(obj))
}

fn decode_key_entries(mut data: &[u8], element_count: usize) -> Vec<(u32, u16)> {
    let mut entries = Vec::with_capacity(element_count);
    for _ in 0..element_count {
        let offset = data.decode_u32_with_little_endian().unwrap();
        let length = data.decode_u16_with_little_endian().unwrap();
        entries.push((offset, length));
    }
    entries
}

fn encode_array(data: Vec<Json>, buf: &mut Vec<u8>) {
    // array ::= element-count size value-entry* value*
    let element_count = data.len();
    let count_len = LENGTH_U32;
    let size_len = LENGTH_U32;
    let value_entries_len = LENGTH_VALUE_ENTRY * element_count;
    let value_offset = count_len + size_len + value_entries_len;
    let (mut value_entries, mut encode_values) = encode_vec_json(data, value_offset as u32);
    let total_size = value_offset + encode_values.len();
    buf.encode_u32_with_little_endian(element_count as u32).unwrap();
    buf.encode_u32_with_little_endian(total_size as u32).unwrap();
    buf.append(value_entries.as_mut());
    buf.append(encode_values.as_mut());
}

fn decode_array(mut data: &[u8]) -> Result<Json> {
    // count size value_entries values
    let element_count = data.decode_u32_with_little_endian().unwrap() as usize;
    data.decode_u32_with_little_endian().unwrap();
    // already removed count and size
    let value_entries_len = LENGTH_VALUE_ENTRY * element_count;
    let value_entries_data = &data[0..value_entries_len];
    let array_data = try!(decode_vec_json(value_entries_data,
                                          &data[value_entries_len..],
                                          element_count));
    Ok(Json::Array(array_data))
}

fn encode_vec_json(data: Vec<Json>, offset: u32) -> (Vec<u8>, Vec<u8>) {
    let mut value_offset = offset;
    let mut value_entries = vec![];
    let mut encode_values = vec![];
    for value in data {
        let code = value.get_type_code();
        value_entries.push(code);
        match value {
            // If the value has length in (0, 4], it could be inline here.
            // And padding 0x00 to 4 bytes if needed.
            Json::Literal(v) => {
                let last_len = value_entries.len();
                value_entries.push(v);
                let value_len = value_entries.len() - last_len;
                let left = LENGTH_U32 - value_len;
                for _ in 0..left {
                    value_entries.push(JSON_LITERAL_NIL);
                }
            }
            _ => {
                value_entries.encode_u32_with_little_endian(value_offset).unwrap();
                let cur_value_len = value.encode(encode_values.as_mut()) as u32;
                value_offset += cur_value_len;
            }
        }
    }
    (value_entries, encode_values)
}

fn decode_vec_json(value_entries_data: &[u8],
                   mut values_data: &[u8],
                   element_count: usize)
                   -> Result<Vec<Json>> {
    let mut values = Vec::with_capacity(element_count);
    let mut entry_offset = 0;
    let mut last_value_offset = 0;
    for _ in 0..element_count {
        let entry = &value_entries_data[entry_offset..(entry_offset + LENGTH_VALUE_ENTRY)];
        entry_offset += LENGTH_VALUE_ENTRY;
        let code = entry[0];
        let mut entry_value = &entry[LENGTH_TYPE..];
        let value = match code {
            TYPE_CODE_LITERAL => try!(Json::decode(code, entry_value)),
            _ => {
                let cur_offset = entry_value.decode_u32_with_little_endian().unwrap();
                if last_value_offset > 0 {
                    let last_value_len = (cur_offset - last_value_offset) as usize;
                    values_data = &values_data[last_value_len..];
                }
                last_value_offset = cur_offset;
                try!(Json::decode(code, values_data))
            }
        };
        values.push(value);
    }
    Ok(values)
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
        let jstr1 = r#"{"a": [1, "2", {"aa": "bb"}, 4.0], "b": true}"#;
        let j1 = Json::parse_from_string(jstr1).unwrap();
        let jstr2 = r#"[{"a": 1, "b": true}, 3, 3.5, "hello, world", null, true]"#;
        let j2 = Json::parse_from_string(jstr2).unwrap();

        let json_nil = Json::Literal(0x00);
        let json_bool = Json::Literal(0x01);
        let json_double = Json::Double(3.24);
        let json_str = Json::String(String::from("hello, 世界"));
        let test_cases = vec![json_nil, json_bool, json_double, json_str, j1, j2];
        for case in test_cases {
            let json = case.clone();
            let data = json.serialize();
            let output = Json::deserialize(data.as_slice()).unwrap();
            let input_str = case.to_string();
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
}
