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


use byteorder::{ReadBytesExt, WriteBytesExt};
use std::{str, f64};
use std::collections::BTreeMap;
use std::io::{Read, Write};
use util::codec::number::{NumberDecoder, NumberEncoder};

use super::super::Result;
use super::{Json, ERR_CONVERT_FAILED};

const TYPE_CODE_OBJECT: u8 = 0x01;
const TYPE_CODE_ARRAY: u8 = 0x03;
const TYPE_CODE_LITERAL: u8 = 0x04;
const TYPE_CODE_I64: u8 = 0x09;
const TYPE_CODE_DOUBLE: u8 = 0x0b;
const TYPE_CODE_STRING: u8 = 0x0c;

const JSON_LITERAL_NIL: u8 = 0x00;
const JSON_LITERAL_TRUE: u8 = 0x01;
const JSON_LITERAL_FALSE: u8 = 0x02;

const TYPE_LEN: usize = 1;
const LITERAL_LEN: usize = 1;
const U16_LEN: usize = 2;
const U32_LEN: usize = 4;
const NUMBER_LEN: usize = 8;
const KEY_ENTRY_LEN: usize = U32_LEN + U16_LEN;
const VALUE_ENTRY_LEN: usize = TYPE_LEN + U32_LEN;
const ELEMENT_COUNT_LEN: usize = U32_LEN;
const SIZE_LEN: usize = U32_LEN;

// The binary Json format from `MySQL` 5.7 is in the following link:
// (https://github.com/mysql/mysql-server/blob/5.7/sql/json_binary.h#L52)
// The only difference is that we use large `object` or large `array` for
// the small corresponding ones. That means in our implementation there
// is no difference between small `object` and big `object`, so does `array`.
impl Json {
    pub fn as_literal(&self) -> Result<u8> {
        match *self {
            Json::Boolean(d) => {
                if d {
                    Ok(JSON_LITERAL_TRUE)
                } else {
                    Ok(JSON_LITERAL_FALSE)
                }
            }
            Json::None => Ok(JSON_LITERAL_NIL),
            _ => {
                Err(invalid_type!("{:?} from {} to literal",
                                  ERR_CONVERT_FAILED,
                                  self.to_string()))
            }
        }
    }

    pub fn binary_len(&self) -> usize {
        TYPE_LEN + self.body_binary_len()
    }

    fn body_binary_len(&self) -> usize {
        match *self {
            Json::Object(ref d) => get_obj_binary_len(d),
            Json::Array(ref d) => get_array_binary_len(d),
            Json::Boolean(_) | Json::None => LITERAL_LEN,
            Json::I64(_) | Json::Double(_) => NUMBER_LEN,
            Json::String(ref d) => get_str_binary_len(d),
        }
    }

    fn get_type_code(&self) -> u8 {
        match *self {
            Json::Object(_) => TYPE_CODE_OBJECT,
            Json::Array(_) => TYPE_CODE_ARRAY,
            Json::Boolean(_) | Json::None => TYPE_CODE_LITERAL,
            Json::I64(_) => TYPE_CODE_I64,
            Json::Double(_) => TYPE_CODE_DOUBLE,
            Json::String(_) => TYPE_CODE_STRING,
        }
    }
}

pub trait JsonEncoder: NumberEncoder {
    fn encode_json(&mut self, data: &Json) -> Result<()> {
        try!(self.write_u8(data.get_type_code()));
        self.encode_json_body(data)
    }

    fn encode_json_body(&mut self, data: &Json) -> Result<()> {
        match *data {
            Json::Object(ref d) => self.encode_obj(d),
            Json::Array(ref d) => self.encode_array(d),
            Json::Boolean(_) | Json::None => {
                let v = try!(data.as_literal());
                self.encode_literal(v)
            }
            Json::I64(d) => self.encode_json_i64(d),
            Json::Double(d) => self.encode_json_f64(d),
            Json::String(ref d) => self.encode_str(d),
        }
    }

    fn encode_obj(&mut self, data: &BTreeMap<String, Json>) -> Result<()> {
        // object: element-count size key-entry* value-entry* key* value*
        let element_count = data.len();
        // key-entry ::= key-offset(uint32) key-length(uint16)
        let key_entries_len = KEY_ENTRY_LEN * element_count;
        // value-entry ::= type(byte) offset-or-inlined-value(uint32)
        let value_entries_len = VALUE_ENTRY_LEN * element_count;
        let mut key_entries = Vec::with_capacity(key_entries_len);
        let mut encode_keys = Vec::new();
        let mut key_offset = ELEMENT_COUNT_LEN + SIZE_LEN + key_entries_len + value_entries_len;
        for key in data.keys() {
            let encode_key = key.as_bytes();
            let key_len = try!(encode_keys.write(encode_key));
            try!(key_entries.encode_u32_le(key_offset as u32));
            try!(key_entries.encode_u16_le(key_len as u16));
            key_offset += key_len;
        }

        let mut value_offset = key_offset as u32;
        let mut value_entries = Vec::with_capacity(value_entries_len);
        let mut encode_values = vec![];
        for value in data.values() {
            try!(value_entries.encode_json_item(value, &mut value_offset, &mut encode_values));
        }
        let size = ELEMENT_COUNT_LEN + SIZE_LEN + key_entries_len + value_entries_len +
                   encode_keys.len() + encode_values.len();
        try!(self.encode_u32_le(element_count as u32));
        try!(self.encode_u32_le(size as u32));
        try!(self.write_all(key_entries.as_mut()));
        try!(self.write_all(value_entries.as_mut()));
        try!(self.write_all(encode_keys.as_mut()));
        try!(self.write_all(encode_values.as_mut()));
        Ok(())
    }

    fn encode_array(&mut self, data: &[Json]) -> Result<()> {
        // array ::= element-count size value-entry* value*
        let element_count = data.len();
        let value_entries_len = VALUE_ENTRY_LEN * element_count;
        let mut value_offset = (ELEMENT_COUNT_LEN + SIZE_LEN + value_entries_len) as u32;
        let mut value_entries = Vec::with_capacity(value_entries_len);
        let mut encode_values = vec![];
        for value in data {
            try!(value_entries.encode_json_item(value, &mut value_offset, &mut encode_values));
        }
        let total_size = ELEMENT_COUNT_LEN + SIZE_LEN + value_entries_len + encode_values.len();
        try!(self.encode_u32_le(element_count as u32));
        try!(self.encode_u32_le(total_size as u32));
        try!(self.write_all(value_entries.as_mut()));
        try!(self.write_all(encode_values.as_mut()));
        Ok(())
    }

    fn encode_literal(&mut self, data: u8) -> Result<()> {
        try!(self.write_u8(data));
        Ok(())
    }

    fn encode_json_i64(&mut self, data: i64) -> Result<()> {
        self.encode_i64_le(data)
    }

    fn encode_json_f64(&mut self, data: f64) -> Result<()> {
        self.encode_f64_le(data)
    }

    fn encode_str(&mut self, data: &str) -> Result<()> {
        let bytes = data.as_bytes();
        let bytes_len = bytes.len() as u64;
        try!(self.encode_var_u64(bytes_len));
        try!(self.write_all(bytes));
        Ok(())
    }

    fn encode_json_item(&mut self,
                        data: &Json,
                        offset: &mut u32,
                        data_buf: &mut Vec<u8>)
                        -> Result<()> {
        let code = data.get_type_code();
        try!(self.write_u8(code));
        match *data {
            // If the data has length in (0, 4], it could be inline here.
            // And padding 0x00 to 4 bytes if needed.
            Json::Boolean(_) | Json::None => {
                let v = try!(data.as_literal());
                try!(self.write_u8(v));
                let left = U32_LEN - LITERAL_LEN;
                for _ in 0..left {
                    try!(self.write_u8(JSON_LITERAL_NIL));
                }
            }
            _ => {
                try!(self.encode_u32_le(*offset));
                let start_len = data_buf.len();
                try!(data_buf.encode_json_body(data));
                *offset += (data_buf.len() - start_len) as u32;
            }
        };
        Ok(())
    }
}

impl<T: Write> JsonEncoder for T {}

pub trait JsonDecoder: NumberDecoder {
    fn decode_json(&mut self) -> Result<Json> {
        let code = try!(self.read_u8());
        self.decode_json_body(code)
    }

    fn decode_json_body(&mut self, code_type: u8) -> Result<Json> {
        match code_type {
            TYPE_CODE_OBJECT => self.decode_json_obj(),
            TYPE_CODE_ARRAY => self.decode_json_array(),
            TYPE_CODE_LITERAL => self.decode_json_literal(),
            TYPE_CODE_I64 => self.decode_json_i64(),
            TYPE_CODE_DOUBLE => self.decode_json_double(),
            TYPE_CODE_STRING => self.decode_json_str(),
            _ => Err(invalid_type!("unsupported type {:?}", code_type)),
        }
    }

    fn decode_json_obj(&mut self) -> Result<Json> {
        // count size key_entries value_entries keys values
        let element_count = try!(self.decode_u32_le()) as usize;
        let total_size = try!(self.decode_u32_le()) as usize;
        let left_size = total_size - ELEMENT_COUNT_LEN - SIZE_LEN;
        let mut data = vec![0; left_size];
        try!(self.read_exact(&mut data));
        let mut obj = BTreeMap::new();
        if element_count == 0 {
            return Ok(Json::Object(obj));
        }
        // key_entries
        let key_entries_len = KEY_ENTRY_LEN * element_count;
        let mut key_entries_data = &data[0..key_entries_len];

        // value-entry ::= type(byte) offset-or-inlined-value(uint32)
        let value_entries_len = VALUE_ENTRY_LEN * element_count;
        let mut value_entries_data = &data[key_entries_len..(key_entries_len + value_entries_len)];
        let mut key_offset = key_entries_len + value_entries_len;
        for _ in 0..element_count {
            let key_real_offset = try!(key_entries_data.decode_u32_le());
            let key_len = try!(key_entries_data.decode_u16_le());
            let key_data = &data[key_offset..(key_offset + key_len as usize)];
            let key = String::from(str::from_utf8(key_data).unwrap());
            let value =
                try!(value_entries_data.decode_json_item(&data[key_offset..], key_real_offset));
            obj.insert(key, value);
            key_offset += key_len as usize;
        }
        Ok(Json::Object(obj))
    }

    fn decode_json_array(&mut self) -> Result<Json> {
        // count size value_entries values
        let element_count = try!(self.decode_u32_le()) as usize;
        let total_size = try!(self.decode_u32_le());
        // already removed count and size
        let left_size = total_size as usize - ELEMENT_COUNT_LEN - SIZE_LEN;
        let mut data = vec![0; left_size];
        try!(self.read_exact(&mut data));
        let value_entries_len = VALUE_ENTRY_LEN * element_count;
        let mut value_entries_data = &data[0..value_entries_len];
        let values_data = &data[value_entries_len..];
        let mut array_data = Vec::with_capacity(element_count);
        let data_start_offset = (U32_LEN + U32_LEN + value_entries_len) as u32;
        for _ in 0..element_count {
            let value = try!(value_entries_data.decode_json_item(values_data, data_start_offset));
            array_data.push(value);
        }
        Ok(Json::Array(array_data))
    }

    fn decode_json_str(&mut self) -> Result<Json> {
        let length = try!(self.decode_var_u64());
        let mut encode_value = vec![0; length as usize];
        try!(self.read_exact(&mut encode_value));
        let value = try!(String::from_utf8(encode_value));
        Ok(Json::String(value))
    }

    fn decode_json_literal(&mut self) -> Result<Json> {
        match try!(self.read_u8()) {
            JSON_LITERAL_TRUE => Ok(Json::Boolean(true)),
            JSON_LITERAL_FALSE => Ok(Json::Boolean(false)),
            _ => Ok(Json::None),
        }
    }

    fn decode_json_double(&mut self) -> Result<Json> {
        let value = try!(self.decode_f64_le());
        Ok(Json::Double(value))
    }

    fn decode_json_i64(&mut self) -> Result<Json> {
        let value = try!(self.decode_i64_le());
        Ok(Json::I64(value))
    }

    fn decode_json_item(&mut self, values_data: &[u8], data_start_position: u32) -> Result<Json> {
        let mut entry = vec![0; VALUE_ENTRY_LEN];
        try!(self.read_exact(&mut entry));
        let mut entry = entry.as_slice();
        let code = try!(entry.read_u8());
        match code {
            TYPE_CODE_LITERAL => entry.decode_json_body(code),
            _ => {
                let real_offset = entry.decode_u32_le().unwrap();
                let offset_in_values = real_offset - data_start_position;
                let mut value = &values_data[offset_in_values as usize..];
                value.decode_json_body(code)
            }
        }
    }
}

impl<T: Read> JsonDecoder for T {}

fn get_obj_binary_len(data: &BTreeMap<String, Json>) -> usize {
    let element_count = data.len();
    let key_entries_len = element_count * KEY_ENTRY_LEN;
    let value_entries_len = element_count * VALUE_ENTRY_LEN;
    let mut keys_len = 0;
    let mut values_len = 0;
    for (k, v) in data {
        keys_len += k.len();
        if v.get_type_code() != TYPE_CODE_LITERAL {
            values_len += v.body_binary_len();
        }
    }
    // object: element-count size key-entry* value-entry* key* value*
    ELEMENT_COUNT_LEN + SIZE_LEN + key_entries_len + value_entries_len + keys_len + values_len
}

fn get_array_binary_len(data: &[Json]) -> usize {
    let element_count = data.len();
    let value_entries_len = element_count * VALUE_ENTRY_LEN;
    let mut values_len = 0;
    for v in data {
        if v.get_type_code() != TYPE_CODE_LITERAL {
            values_len += v.body_binary_len();
        }
    }
    // array ::= element-count size value-entry* value*
    ELEMENT_COUNT_LEN + SIZE_LEN + value_entries_len + values_len
}

fn get_str_binary_len(data: &str) -> usize {
    let len = data.as_bytes().len();
    get_var_u64_binary_len(len as u64) + len
}

fn get_var_u64_binary_len(mut v: u64) -> usize {
    let mut len = 1;
    while v >= 0x80 {
        v >>= 7;
        len += 1;
    }
    len
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_json_binary() {
        let jstr1 =
            r#"{"aaaaaaaaaaa": [1, "2", {"aa": "bb"}, 4.0], "bbbbbbbbbb": true, "ccccccccc": "d"}"#;
        let j1: Json = jstr1.parse().unwrap();
        let jstr2 = r#"[{"a": 1, "b": true}, 3, 3.5, "hello, world", null, true]"#;
        let j2: Json = jstr2.parse().unwrap();

        let json_nil = Json::None;
        let json_bool = Json::Boolean(true);
        let json_double = Json::Double(3.24);
        let json_str = Json::String(String::from("hello, 世界"));
        let test_cases = vec![json_nil, json_bool, json_double, json_str, j1, j2];
        for json in test_cases {
            let mut data = vec![];
            data.encode_json(&json).unwrap();
            let output = data.as_slice().decode_json().unwrap();
            let input_str = json.to_string();
            let output_str = output.to_string();
            assert_eq!(input_str, output_str);
        }
    }

    #[test]
    fn test_type_code() {
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
            let json: Json = json_str.parse().unwrap();
            assert_eq!(json.get_type_code(), code);
        }
    }
}
