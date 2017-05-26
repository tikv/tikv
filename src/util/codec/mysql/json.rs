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

use std::string::String;
use super::Result;
use std::str;
use serde_json::{self, Value};
use std::collections::BTreeMap;
use util::codec::number::{NumberDecoder, NumberEncoder};

// The binary jSON format from MySQL 5.7 is as follows:
//
// JSON doc ::= type value
// type ::=
// 0x01 |       // large JSON object
// 0x03 |       // large JSON array
// 0x04 |       // literal (true/false/null)
// 0x05 |       // int16
// 0x06 |       // uint16
// 0x07 |       // int32
// 0x08 |       // uint32
// 0x09 |       // int64
// 0x0a |       // uint64
// 0x0b |       // double
// 0x0c |       // utf8mb4 string
//
// value ::=
// object  |
// array   |
// literal |
// number  |
// string  |
//
// object ::= element-count size key-entry* value-entry* key* value*
//
// array ::= element-count size value-entry* value*
//
// number of members in object or number of elements in array
// element-count ::= uint32
//
// number of bytes in the binary representation of the object or array
// size ::= uint32
//
// key-entry ::= key-offset key-length
//
// key-offset ::= uint32
//
// key-length ::= uint16    // key length must be less than 64KB
//
// value-entry ::= type offset-or-inlined-value
//
// This field holds either the offset to where the value is stored,
// or the value itself if it is small enough to be inlined (that is,
// if it is a JSON literal or a small enough [u]int).
// offset-or-inlined-value ::= uint32
//
// key ::= utf8mb4-data
//
// literal ::=
// 0x00 |   // JSON null literal
// 0x01 |   // JSON true literal
// 0x02 |   // JSON false literal
//
// number ::=  ....    // little-endian format for [u]int(16|32|64), whereas
// double is stored in a platform-independent, eight-byte
// format using float8store()
//
// string ::= data-length utf8mb4-data
//
// data-length ::= uint8*    // If the high bit of a byte is 1, the length
// field is continued in the next byte,
// otherwise it is the last byte of the length
// field. So we need 1 byte to represent
// lengths up to 127, 2 bytes to represent
// lengths up to 16383, and so on...
//


const TYPE_CODE_OBJECT: u8 = 0x01;
const TYPE_CODE_ARRAY: u8 = 0x03;
const TYPE_CODE_LITERAL: u8 = 0x04;
const TYPE_CODE_DOUBLE: u8 = 0x0b;
const TYPE_CODE_STRING: u8 = 0x0c;

const JSON_LITERAL_NIL: u8 = 0x00;
const JSON_LITERAL_TRUE: u8 = 0x01;
const JSON_LITERAL_FALSE: u8 = 0x02;

#[derive(Serialize,Clone,PartialEq)]
pub enum JSON {
    JLiteral(u8),
    JString(String),
    JObject(BTreeMap<String, JSON>),
    JArray(Vec<JSON>),
    JDouble(f64),
}

impl JSON {
    pub fn deserialize(mut data: &[u8]) -> Result<JSON> {
        if data.is_empty() {
            return Err(invalid_type!("The document is empty"));
        }
        let code_type = take_left_bytes(data, LENGTH_TYPE);
        JSON::decode(code_type[0], data)
    }

    fn serialize(self) -> Vec<u8> {
        let mut data = vec![self.get_type_code()];
        self.encode(&mut data);
        data
    }

    fn encode(self, data: &mut Vec<u8>) -> usize {
        let mut encode_data = match self {
            JSON::JLiteral(d) => vec![d],
            JSON::JString(d) => encode_str(d),
            JSON::JObject(d) => encode_obj(d),
            JSON::JArray(d) => encode_array(d),
            JSON::JDouble(d) => encode_double(d),
        };
        let len = encode_data.len();
        data.append(&mut encode_data);
        len
    }

    fn decode(code_type: u8, mut data: &[u8]) -> Result<JSON> {
        match code_type {
            TYPE_CODE_LITERAL => {
                let literal = take_left_bytes(data, LENGTH_TYPE);
                Ok(JSON::JLiteral(literal[0]))
            }
            TYPE_CODE_STRING => decode_str(data),
            TYPE_CODE_OBJECT => decode_obj(data),
            TYPE_CODE_ARRAY => decode_array(data),
            TYPE_CODE_DOUBLE => decode_double(data),
            _ => Err(invalid_type!("unsupported type {:?}", code_type)),
        }
    }

    fn get_type_code(&self) -> u8 {
        match *self {
            JSON::JLiteral(_) => TYPE_CODE_LITERAL,
            JSON::JString(_) => TYPE_CODE_STRING,
            JSON::JObject(_) => TYPE_CODE_OBJECT,
            JSON::JArray(_) => TYPE_CODE_ARRAY,
            JSON::JDouble(_) => TYPE_CODE_DOUBLE,
        }
    }

    pub fn to_string(&self) -> String {
        if let JSON::JLiteral(ref d) = *self {
            match *d {
                JSON_LITERAL_NIL => return String::from("null"),
                JSON_LITERAL_TRUE => return String::from("true"),
                _ => return String::from("false"),
            };
        }
        serde_json::to_string(self).unwrap()
    }
}


fn encode_str(data: String) -> Vec<u8> {
    let mut buf = vec![];
    buf.encode_var_u64(data.len() as u64).unwrap();
    buf.append(&mut data.into_bytes());
    buf
}

fn decode_str(mut data: &[u8]) -> Result<JSON> {
    let length = try!(data.decode_var_u64());
    let encode_value = &data[0..(length as usize)];
    data = &data[(length as usize)..];
    match str::from_utf8(encode_value) {
        Ok(v) => {
            let value = String::from(v);
            Ok(JSON::JString(value))
        }
        Err(e) => Err(invalid_type!("Invalid UTF-8 sequence:{}", e)),
    }
}

const LENGTH_TYPE: usize = 1;
const LENGTH_U16: usize = 2;
const LENGTH_U32: usize = 4;
const LENGTH_U64: usize = 8;
const LENGTH_KEY_ENTRY: usize = LENGTH_U32 + LENGTH_U16;
const LENGTH_VALUE_ENTRY: usize = LENGTH_TYPE + LENGTH_U32;

fn encode_double(data: f64) -> Vec<u8> {
    let mut encode_data = vec![];
    encode_data.encode_f64_with_little_endian(data).unwrap();
    encode_data
}

fn decode_double(mut data: &[u8]) -> Result<JSON> {
    let value = try!(data.decode_f64_with_little_endian());
    Ok(JSON::JDouble(value))
}

fn encode_obj(data: BTreeMap<String, JSON>) -> Vec<u8> {
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
    for (key, value) in data.iter() {
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

    let mut value_offset = key_offset;
    let (mut value_entries, mut encode_values) = encode_vec_json(values, value_offset as u32);
    let size = key_offset + encode_values.len();
    let mut encode_data = vec![];
    encode_data.encode_u32_with_little_endian(element_count as u32).unwrap();
    encode_data.encode_u32_with_little_endian(size as u32).unwrap();
    encode_data.append(key_entries.as_mut());
    encode_data.append(value_entries.as_mut());
    encode_data.append(encode_keys.as_mut());
    encode_data.append(encode_values.as_mut());
    encode_data
}

fn take_left_bytes(mut data: &[u8], len: usize) -> &[u8] {
    let left_data = &data[0..len];
    data = &data[len..];
    left_data
}

fn decode_obj(mut data: &[u8]) -> Result<JSON> {
    // count size key_entries value_entries keys values
    let element_count = data.decode_u32_with_little_endian().unwrap() as usize;
    let total_size = data.decode_u32_with_little_endian().unwrap() as usize;
    // already removed count and size
    let left_size = total_size - LENGTH_U32 - LENGTH_U32;
    let mut obj_data = take_left_bytes(data, left_size);

    let mut obj = BTreeMap::new();
    if element_count == 0 {
        return Ok(JSON::JObject(obj));
    }

    let key_entries_len = LENGTH_KEY_ENTRY * element_count;
    let mut key_entries_data = take_left_bytes(obj_data, key_entries_len);
    // value-entry ::= type(byte) offset-or-inlined-value(uint32)
    let value_entries_len = LENGTH_VALUE_ENTRY * element_count;
    let mut value_entries_data = take_left_bytes(obj_data, value_entries_len);
    let key_entries = decode_key_entries(key_entries_data, element_count);

    let mut keys = Vec::with_capacity(element_count);
    for &(_, length) in &key_entries {
        let key_data = take_left_bytes(obj_data, length as usize);
        let key = str::from_utf8(key_data).unwrap();
        keys.push(key);
    }

    let mut values = try!(decode_vec_json(value_entries_data, obj_data, element_count));
    for (key, value) in keys.iter().zip(values.iter()) {
        obj.insert(String::from(*key), value.to_owned());
    }

    Ok(JSON::JObject(obj))
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

fn encode_array(data: Vec<JSON>) -> Vec<u8> {
    // array ::= element-count size value-entry* value*
    let element_count = data.len();
    let count_len = LENGTH_U32;
    let size_len = LENGTH_U32;
    let value_entries_len = LENGTH_VALUE_ENTRY * element_count;
    let value_offset = count_len + size_len + value_entries_len;
    let (mut value_entries, mut encode_values) = encode_vec_json(data, value_offset as u32);
    let total_size = value_offset + encode_values.len();
    let mut encode_data = vec![];
    encode_data.encode_u32_with_little_endian(element_count as u32).unwrap();
    encode_data.encode_u32_with_little_endian(total_size as u32).unwrap();
    encode_data.append(value_entries.as_mut());
    encode_data.append(encode_values.as_mut());
    encode_data
}

fn decode_array(mut data: &[u8]) -> Result<JSON> {
    // count size value_entries values
    let element_count = data.decode_u32_with_little_endian().unwrap() as usize;
    let total_size = data.decode_u32_with_little_endian().unwrap() as usize;
    // already removed count and size
    let left_size = total_size - LENGTH_U32 - LENGTH_U32;
    let left_data = take_left_bytes(data, left_size);
    let value_entries_len = LENGTH_VALUE_ENTRY * element_count;
    let mut value_entries_data = take_left_bytes(left_data, value_entries_len);
    let array_data = try!(decode_vec_json(value_entries_data, left_data, element_count));
    Ok(JSON::JArray(array_data))
}

fn encode_vec_json(data: Vec<JSON>, offset: u32) -> (Vec<u8>, Vec<u8>) {
    let mut value_offset = offset;
    let mut value_entries = vec![];
    let mut encode_values = vec![];
    for value in data {
        let code = value.get_type_code();
        value_entries.push(code);
        match value {
            // If the value has length in (0, 4], it could be inline here.
            // And padding 0x00 to 4 bytes if needed.
            JSON::JLiteral(v) => {
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
                let cur_value_len = encode(value, encode_values.as_mut()) as u32;
                value_offset += cur_value_len;
            }
        }
    }
    (value_entries, encode_values)
}

fn decode_vec_json(mut value_entries_data: &[u8],
                   mut values_data: &[u8],
                   element_count: usize)
                   -> Result<Vec<JSON>> {
    let mut values = Vec::with_capacity(element_count);
    for _ in 0..element_count {
        let mut entry = take_left_bytes(value_entries_data, LENGTH_VALUE_ENTRY);
        let code = take_left_bytes(entry, LENGTH_TYPE);
        let value = match code[0] {
            TYPE_CODE_LITERAL => try!(JSON::decode(code[0], entry)),
            _ => try!(JSON::decode(code[0], values_data)),
        };
        values.push(value);
    }
    Ok(values)
}

fn normalize(data: Value) -> Option<JSON> {
    match data {
        Value::Null => Some(JSON::JLiteral(JSON_LITERAL_NIL)),
        Value::Bool(data) => {
            if data {
                Some(JSON::JLiteral(JSON_LITERAL_TRUE))
            } else {
                Some(JSON::JLiteral(JSON_LITERAL_FALSE))
            }
        }
        Value::Number(ref data) if data.is_f64() => Some(JSON::JDouble(data.as_f64().unwrap())),
        Value::String(data) => Some(JSON::JString(data)),
        Value::Array(data) => {
            let mut array = Vec::with_capacity(data.len());
            for item in data {
                Some(array.push(normalize(item).unwrap()));
            }
            Some(JSON::JArray(array))
        }
        Value::Object(data) => {
            let mut obj = BTreeMap::new();
            for (key, value) in data {
                let value_item = normalize(value).unwrap();
                obj.insert(key, value_item);
            }
            Some(JSON::JObject(obj))
        }
        _ => None,
    }
}

fn encode(json: JSON, data: &mut Vec<u8>) -> usize {
    let mut encode_data = match json {
        JSON::JLiteral(d) => vec![d],
        JSON::JString(d) => encode_str(d),
        JSON::JObject(d) => encode_obj(d),
        JSON::JArray(d) => encode_array(d),
        JSON::JDouble(d) => encode_double(d),
    };
    let len = encode_data.len();
    data.append(&mut encode_data);
    len
}


pub fn parse_from_string(s: &str) -> Result<Option<JSON>> {
    if s.len() == 0 {
        return Err(invalid_type!("The document is empty"));
    }
    match serde_json::from_str(s) {
        Ok(value) => Ok(normalize(value)),
        Err(err) => Err(invalid_type!("Invalid document {:?}", err)),
    }
}

// pub fn serialize(json:JSON)->Vec<u8> {
//     let mut data = vec![json.get_type_code()];
//     encode(json,&mut data);
//     data
// }

// pub fn deserialize(mut data:&[u8])->Result<JSON> {
//       if data.is_empty() {
//             return Err(invalid_type!("The document is empty"));
//       }
//       let code_type = take_left_bytes(data,LENGTH_TYPE);
//       JSON::decode(code_type[0],data)
// }
