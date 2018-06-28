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

use std::collections::BTreeMap;
use std::io::Write;
use std::{f64, str};

use super::{Json, ERR_CONVERT_FAILED};
use byteorder::WriteBytesExt;
use coprocessor::codec::{Error, Result};
use util::codec::number::{self, NumberEncoder};
use util::codec::{read_slice, BytesSlice};
const TYPE_CODE_OBJECT: u8 = 0x01;
const TYPE_CODE_ARRAY: u8 = 0x03;
const TYPE_CODE_LITERAL: u8 = 0x04;
const TYPE_CODE_I64: u8 = 0x09;
const TYPE_CODE_U64: u8 = 0x0a;
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
            Json::Boolean(d) => if d {
                Ok(JSON_LITERAL_TRUE)
            } else {
                Ok(JSON_LITERAL_FALSE)
            },
            Json::None => Ok(JSON_LITERAL_NIL),
            _ => Err(invalid_type!(
                "{:?} from {} to literal",
                ERR_CONVERT_FAILED,
                self.to_string()
            )),
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
            Json::I64(_) | Json::U64(_) | Json::Double(_) => NUMBER_LEN,
            Json::String(ref d) => get_str_binary_len(d),
        }
    }

    fn get_type_code(&self) -> u8 {
        match *self {
            Json::Object(_) => TYPE_CODE_OBJECT,
            Json::Array(_) => TYPE_CODE_ARRAY,
            Json::Boolean(_) | Json::None => TYPE_CODE_LITERAL,
            Json::I64(_) => TYPE_CODE_I64,
            Json::U64(_) => TYPE_CODE_U64,
            Json::Double(_) => TYPE_CODE_DOUBLE,
            Json::String(_) => TYPE_CODE_STRING,
        }
    }
}

pub trait JsonEncoder: NumberEncoder {
    fn encode_json(&mut self, data: &Json) -> Result<()> {
        self.write_u8(data.get_type_code())?;
        self.encode_json_body(data)
    }

    fn encode_json_body(&mut self, data: &Json) -> Result<()> {
        match *data {
            Json::Object(ref d) => self.encode_obj(d),
            Json::Array(ref d) => self.encode_array(d),
            Json::Boolean(_) | Json::None => {
                let v = data.as_literal()?;
                self.encode_literal(v)
            }
            Json::I64(d) => self.encode_json_i64(d),
            Json::U64(d) => self.encode_json_u64(d),
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
            let key_len = encode_keys.write(encode_key)?;
            key_entries.encode_u32_le(key_offset as u32)?;
            key_entries.encode_u16_le(key_len as u16)?;
            key_offset += key_len;
        }

        let mut value_offset = key_offset as u32;
        let mut value_entries = Vec::with_capacity(value_entries_len);
        let mut encode_values = vec![];
        for value in data.values() {
            value_entries.encode_json_item(value, &mut value_offset, &mut encode_values)?;
        }
        let size = ELEMENT_COUNT_LEN
            + SIZE_LEN
            + key_entries_len
            + value_entries_len
            + encode_keys.len()
            + encode_values.len();
        self.encode_u32_le(element_count as u32)?;
        self.encode_u32_le(size as u32)?;
        self.write_all(key_entries.as_mut())?;
        self.write_all(value_entries.as_mut())?;
        self.write_all(encode_keys.as_mut())?;
        self.write_all(encode_values.as_mut())?;
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
            value_entries.encode_json_item(value, &mut value_offset, &mut encode_values)?;
        }
        let total_size = ELEMENT_COUNT_LEN + SIZE_LEN + value_entries_len + encode_values.len();
        self.encode_u32_le(element_count as u32)?;
        self.encode_u32_le(total_size as u32)?;
        self.write_all(value_entries.as_mut())?;
        self.write_all(encode_values.as_mut())?;
        Ok(())
    }

    fn encode_literal(&mut self, data: u8) -> Result<()> {
        self.write_u8(data)?;
        Ok(())
    }

    fn encode_json_i64(&mut self, data: i64) -> Result<()> {
        self.encode_i64_le(data).map_err(Error::from)
    }

    fn encode_json_u64(&mut self, data: u64) -> Result<()> {
        self.encode_u64_le(data).map_err(Error::from)
    }

    fn encode_json_f64(&mut self, data: f64) -> Result<()> {
        self.encode_f64_le(data).map_err(Error::from)
    }

    fn encode_str(&mut self, data: &str) -> Result<()> {
        let bytes = data.as_bytes();
        let bytes_len = bytes.len() as u64;
        self.encode_var_u64(bytes_len)?;
        self.write_all(bytes)?;
        Ok(())
    }

    fn encode_json_item(
        &mut self,
        data: &Json,
        offset: &mut u32,
        data_buf: &mut Vec<u8>,
    ) -> Result<()> {
        let code = data.get_type_code();
        self.write_u8(code)?;
        match *data {
            // If the data has length in (0, 4], it could be inline here.
            // And padding 0x00 to 4 bytes if needed.
            Json::Boolean(_) | Json::None => {
                let v = data.as_literal()?;
                self.write_u8(v)?;
                let left = U32_LEN - LITERAL_LEN;
                for _ in 0..left {
                    self.write_u8(JSON_LITERAL_NIL)?;
                }
            }
            _ => {
                self.encode_u32_le(*offset)?;
                let start_len = data_buf.len();
                data_buf.encode_json_body(data)?;
                *offset += (data_buf.len() - start_len) as u32;
            }
        };
        Ok(())
    }
}

impl<T: Write> JsonEncoder for T {}

impl Json {
    // `decode` decodes value encoded by `encode_json` before.
    pub fn decode(buf: &mut BytesSlice) -> Result<Json> {
        let code = number::read_u8(buf)?;
        Json::decode_body(buf, code)
    }

    fn decode_body(buf: &mut BytesSlice, code_type: u8) -> Result<Json> {
        match code_type {
            TYPE_CODE_OBJECT => Json::decode_obj(buf),
            TYPE_CODE_ARRAY => Json::decode_array(buf),
            TYPE_CODE_LITERAL => Json::decode_literal(buf),
            TYPE_CODE_I64 => Json::decode_i64(buf),
            TYPE_CODE_U64 => Json::decode_u64(buf),
            TYPE_CODE_DOUBLE => Json::decode_double(buf),
            TYPE_CODE_STRING => Json::decode_str(buf),
            _ => Err(Error::InvalidDataType("unsupported type".into())),
        }
    }

    fn decode_obj(buf: &mut BytesSlice) -> Result<Json> {
        // count size key_entries value_entries keys values
        let element_count = number::decode_u32_le(buf)? as usize;
        let total_size = number::decode_u32_le(buf)? as usize;
        let left_size = total_size - ELEMENT_COUNT_LEN - SIZE_LEN;
        let mut obj = BTreeMap::new();
        if element_count == 0 {
            return Ok(Json::Object(obj));
        }
        // key_entries
        let key_entries_len = KEY_ENTRY_LEN * element_count;
        let mut key_entries_data = read_slice(buf, key_entries_len)?; //&data[0..key_entries_len];

        // value-entry ::= type(byte) offset-or-inlined-value(uint32)
        let value_entries_len = VALUE_ENTRY_LEN * element_count;
        let mut value_entries_data = read_slice(buf, value_entries_len)?;
        let mut data = read_slice(buf, left_size - key_entries_len - value_entries_len)?;
        for _ in 0..element_count {
            let key_real_offset = number::decode_u32_le(&mut key_entries_data)?;
            let key_len = number::decode_u16_le(&mut key_entries_data)?;
            let key_data = read_slice(&mut data, key_len as usize)?;
            let key = String::from(str::from_utf8(key_data).unwrap());
            let value = Json::decode_item(
                &mut value_entries_data,
                data,
                key_real_offset + u32::from(key_len),
            )?;
            obj.insert(key, value);
        }
        Ok(Json::Object(obj))
    }

    fn decode_array(buf: &mut BytesSlice) -> Result<Json> {
        // count size value_entries values
        let element_count = number::decode_u32_le(buf)? as usize;
        let total_size = number::decode_u32_le(buf)?;
        // already removed count and size
        let left_size = total_size as usize - ELEMENT_COUNT_LEN - SIZE_LEN;
        let value_entries_len = VALUE_ENTRY_LEN * element_count;
        let mut value_entries_data = read_slice(buf, value_entries_len)?;
        let values_data = read_slice(buf, left_size - value_entries_len)?;
        let mut array_data = Vec::with_capacity(element_count);
        let data_start_offset = (U32_LEN + U32_LEN + value_entries_len) as u32;
        for _ in 0..element_count {
            let value = Json::decode_item(&mut value_entries_data, values_data, data_start_offset)?;
            array_data.push(value);
        }
        Ok(Json::Array(array_data))
    }

    fn decode_str(buf: &mut BytesSlice) -> Result<Json> {
        let length = number::decode_var_u64(buf)?;
        let encode_value = read_slice(buf, length as usize)?;
        let value = str::from_utf8(encode_value)?;
        Ok(Json::String(value.into()))
    }

    fn decode_literal(buf: &mut BytesSlice) -> Result<Json> {
        match number::read_u8(buf)? {
            JSON_LITERAL_TRUE => Ok(Json::Boolean(true)),
            JSON_LITERAL_FALSE => Ok(Json::Boolean(false)),
            _ => Ok(Json::None),
        }
    }

    fn decode_double(buf: &mut BytesSlice) -> Result<Json> {
        let value = number::decode_f64_le(buf)?;
        Ok(Json::Double(value))
    }

    fn decode_i64(buf: &mut BytesSlice) -> Result<Json> {
        let value = number::decode_i64_le(buf)?;
        Ok(Json::I64(value))
    }

    fn decode_u64(buf: &mut BytesSlice) -> Result<Json> {
        let value = number::decode_u64_le(buf)?;
        Ok(Json::U64(value))
    }

    fn decode_item(
        buf: &mut BytesSlice,
        values_data: &[u8],
        data_start_position: u32,
    ) -> Result<Json> {
        let entry = read_slice(buf, VALUE_ENTRY_LEN)?;
        let code = entry[0];
        match code {
            TYPE_CODE_LITERAL => Json::decode_literal(&mut &entry[1..]),
            _ => {
                let real_offset = number::decode_u32_le(&mut &entry[1..])?;
                let offset_in_values = real_offset - data_start_position;
                let mut value = &values_data[offset_in_values as usize..];
                Json::decode_body(&mut value, code)
            }
        }
    }
}

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
        let json_int = Json::I64(30);
        let json_uint = Json::U64(30);
        let json_double = Json::Double(3.24);
        let json_str = Json::String(String::from("hello, 世界"));
        let test_cases = vec![
            json_nil,
            json_bool,
            json_int,
            json_uint,
            json_double,
            json_str,
            j1,
            j2,
        ];
        for json in test_cases {
            let mut data = vec![];
            data.encode_json(&json).unwrap();
            let output = Json::decode(&mut data.as_slice()).unwrap();
            let input_str = json.to_string();
            let output_str = output.to_string();
            assert_eq!(input_str, output_str);
        }
    }

    #[test]
    fn test_type_code() {
        let legal_cases = vec![
            (r#"{"key":"value"}"#, TYPE_CODE_OBJECT),
            (r#"["d1","d2"]"#, TYPE_CODE_ARRAY),
            (r#"-3"#, TYPE_CODE_I64),
            (r#"3"#, TYPE_CODE_U64),
            (r#"18446744073709551615"#, TYPE_CODE_U64),
            (r#"3.0"#, TYPE_CODE_DOUBLE),
            (r#"null"#, TYPE_CODE_LITERAL),
            (r#"true"#, TYPE_CODE_LITERAL),
            (r#"false"#, TYPE_CODE_LITERAL),
        ];

        for (json_str, code) in legal_cases {
            let json: Json = json_str.parse().unwrap();
            assert_eq!(json.get_type_code(), code);
        }
    }
}
