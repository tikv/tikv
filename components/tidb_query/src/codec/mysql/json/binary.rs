// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

use std::collections::BTreeMap;
use std::io::Write;
use std::{f64, str};

use super::{Json, ERR_CONVERT_FAILED};
use crate::codec::{Error, Result};
use codec::prelude::*;

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
            Json::Boolean(d) => {
                if d {
                    Ok(JSON_LITERAL_TRUE)
                } else {
                    Ok(JSON_LITERAL_FALSE)
                }
            }
            Json::None => Ok(JSON_LITERAL_NIL),
            _ => Err(invalid_type!(
                "{} from {} to literal",
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
    fn write_json(&mut self, data: &Json) -> Result<()> {
        self.write_u8(data.get_type_code())?;
        self.write_json_body(data)
    }

    fn write_json_body(&mut self, data: &Json) -> Result<()> {
        match *data {
            Json::Object(ref d) => self.write_json_obj(d),
            Json::Array(ref d) => self.write_json_array(d),
            Json::Boolean(_) | Json::None => {
                let v = data.as_literal()?;
                self.write_json_literal(v)
            }
            Json::I64(d) => self.write_json_i64(d),
            Json::U64(d) => self.write_json_u64(d),
            Json::Double(d) => self.write_json_f64(d),
            Json::String(ref d) => self.write_json_str(d),
        }
    }

    fn write_json_obj(&mut self, data: &BTreeMap<String, Json>) -> Result<()> {
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
            key_entries.write_u32_le(key_offset as u32)?;
            key_entries.write_u16_le(key_len as u16)?;
            key_offset += key_len;
        }

        let mut value_offset = key_offset as u32;
        let mut value_entries = Vec::with_capacity(value_entries_len);
        let mut encode_values = vec![];
        for value in data.values() {
            value_entries.write_json_item(value, &mut value_offset, &mut encode_values)?;
        }
        let size = ELEMENT_COUNT_LEN
            + SIZE_LEN
            + key_entries_len
            + value_entries_len
            + encode_keys.len()
            + encode_values.len();
        self.write_u32_le(element_count as u32)?;
        self.write_u32_le(size as u32)?;
        self.write_bytes(key_entries.as_mut())?;
        self.write_bytes(value_entries.as_mut())?;
        self.write_bytes(encode_keys.as_mut())?;
        self.write_bytes(encode_values.as_mut())?;
        Ok(())
    }

    fn write_json_array(&mut self, data: &[Json]) -> Result<()> {
        // array ::= element-count size value-entry* value*
        let element_count = data.len();
        let value_entries_len = VALUE_ENTRY_LEN * element_count;
        let mut value_offset = (ELEMENT_COUNT_LEN + SIZE_LEN + value_entries_len) as u32;
        let mut value_entries = Vec::with_capacity(value_entries_len);
        let mut encode_values = vec![];
        for value in data {
            value_entries.write_json_item(value, &mut value_offset, &mut encode_values)?;
        }
        let total_size = ELEMENT_COUNT_LEN + SIZE_LEN + value_entries_len + encode_values.len();
        self.write_u32_le(element_count as u32)?;
        self.write_u32_le(total_size as u32)?;
        self.write_bytes(value_entries.as_mut())?;
        self.write_bytes(encode_values.as_mut())?;
        Ok(())
    }

    fn write_json_literal(&mut self, data: u8) -> Result<()> {
        self.write_u8(data).map_err(Error::from)
    }

    fn write_json_i64(&mut self, data: i64) -> Result<()> {
        self.write_i64_le(data).map_err(Error::from)
    }

    fn write_json_u64(&mut self, data: u64) -> Result<()> {
        self.write_u64_le(data).map_err(Error::from)
    }

    fn write_json_f64(&mut self, data: f64) -> Result<()> {
        self.write_f64_le(data).map_err(Error::from)
    }

    fn write_json_str(&mut self, data: &str) -> Result<()> {
        let bytes = data.as_bytes();
        let bytes_len = bytes.len() as u64;
        self.write_var_u64(bytes_len)?;
        self.write_bytes(bytes)?;
        Ok(())
    }

    fn write_json_item(
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
                self.write_u32_le(*offset)?;
                let start_len = data_buf.len();
                data_buf.write_json_body(data)?;
                *offset += (data_buf.len() - start_len) as u32;
            }
        };
        Ok(())
    }
}

impl<T: BufferWriter> JsonEncoder for T {}

pub trait JsonDecoder: NumberDecoder {
    // `read_json` decodes value encoded by `write_json` before.
    fn read_json(&mut self) -> Result<Json> {
        let code = self.read_u8()?;
        self.read_json_body(code)
    }

    fn read_json_body(&mut self, code_type: u8) -> Result<Json> {
        match code_type {
            TYPE_CODE_OBJECT => self.read_json_obj(),
            TYPE_CODE_ARRAY => self.read_json_array(),
            TYPE_CODE_LITERAL => self.read_json_literal(),
            TYPE_CODE_I64 => self.read_json_i64(),
            TYPE_CODE_U64 => self.read_json_u64(),
            TYPE_CODE_DOUBLE => self.read_json_double(),
            TYPE_CODE_STRING => self.read_json_str(),
            _ => Err(Error::InvalidDataType("unsupported type".into())),
        }
    }

    fn read_json_obj(&mut self) -> Result<Json> {
        // count size key_entries value_entries keys values
        let element_count = self.read_u32_le()? as usize;
        let total_size = self.read_u32_le()? as usize;
        let left_size = total_size - ELEMENT_COUNT_LEN - SIZE_LEN;
        let mut obj = BTreeMap::new();
        if element_count == 0 {
            return Ok(Json::Object(obj));
        }
        let buf = self.bytes();
        if buf.len() < left_size {
            return Err(Error::unexpected_eof());
        }
        let buf = &buf[..left_size];
        // key_entries
        let key_entries_len = KEY_ENTRY_LEN * element_count;
        let (mut key_entries_data, buf) = buf.split_at(key_entries_len);

        // value-entry ::= type(byte) offset-or-inlined-value(uint32)
        let value_entries_len = VALUE_ENTRY_LEN * element_count;
        let (mut value_entries_data, mut data) = buf.split_at(value_entries_len);
        for _ in 0..element_count {
            let key_real_offset = key_entries_data.read_u32_le()?;
            let key_len = key_entries_data.read_u16_le()? as usize;
            let key_data = data.read_bytes(key_len)?;
            let key = str::from_utf8(key_data)?.to_owned();
            let value =
                value_entries_data.read_json_item(data, key_real_offset + key_len as u32)?;
            obj.insert(key, value);
        }
        self.advance(left_size);
        Ok(Json::Object(obj))
    }

    fn read_json_array(&mut self) -> Result<Json> {
        // count size value_entries values
        let element_count = self.read_u32_le()? as usize;
        let total_size = self.read_u32_le()? as usize;
        // already removed count and size
        let left_size = total_size - ELEMENT_COUNT_LEN - SIZE_LEN;
        let buf = self.bytes();
        if buf.len() < left_size {
            return Err(Error::unexpected_eof());
        }
        let buf = &buf[..left_size];
        let value_entries_len = VALUE_ENTRY_LEN * element_count;
        let (mut value_entries_data, values_data) = buf.split_at(value_entries_len);
        let mut array_data = Vec::with_capacity(element_count);
        let data_start_offset = (U32_LEN + U32_LEN + value_entries_len) as u32;
        for _ in 0..element_count {
            let value = value_entries_data.read_json_item(values_data, data_start_offset)?;
            array_data.push(value);
        }
        self.advance(left_size);
        Ok(Json::Array(array_data))
    }

    fn read_json_str(&mut self) -> Result<Json> {
        let length = self.read_var_u64()? as usize;
        let encode_value = self.read_bytes(length)?;
        let value = str::from_utf8(encode_value)?.to_owned();
        Ok(Json::String(value))
    }

    fn read_json_literal(&mut self) -> Result<Json> {
        match self.read_u8()? {
            JSON_LITERAL_TRUE => Ok(Json::Boolean(true)),
            JSON_LITERAL_FALSE => Ok(Json::Boolean(false)),
            _ => Ok(Json::None),
        }
    }

    fn read_json_double(&mut self) -> Result<Json> {
        let value = self.read_f64_le()?;
        Ok(Json::Double(value))
    }

    fn read_json_i64(&mut self) -> Result<Json> {
        let value = self.read_i64_le()?;
        Ok(Json::I64(value))
    }

    fn read_json_u64(&mut self) -> Result<Json> {
        let value = self.read_u64_le()?;
        Ok(Json::U64(value))
    }

    fn read_json_item(&mut self, values_data: &[u8], data_start_position: u32) -> Result<Json> {
        let mut entry = self.read_bytes(VALUE_ENTRY_LEN)?;
        let code = entry.read_u8()?;
        match code {
            TYPE_CODE_LITERAL => entry.read_json_literal(),
            _ => {
                let real_offset = entry.read_u32_le()?;
                let offset_in_values = real_offset - data_start_position;
                let mut value = &values_data[offset_in_values as usize..];
                value.read_json_body(code)
            }
        }
    }
}

impl<T: BufferReader> JsonDecoder for T {}

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
mod tests {
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
            data.write_json(&json).unwrap();
            let output = data.as_slice().read_json().unwrap();
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
