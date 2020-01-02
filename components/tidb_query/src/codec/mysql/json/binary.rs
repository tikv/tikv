// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::io::Write;
use std::{f64, str};

use super::constants::*;
use super::{Json, JsonRef, JsonType, ERR_CONVERT_FAILED};
use crate::codec::{Error, Result};
use codec::number::NumberCodec;
use codec::prelude::*;

// The binary Json format from `MySQL` 5.7 is in the following link:
// (https://github.com/mysql/mysql-server/blob/5.7/sql/json_binary.h#L52)
// The only difference is that we use large `object` or large `array` for
// the small corresponding ones. That means in our implementation there
// is no difference between small `object` and big `object`, so does `array`.
impl<'a> JsonRef<'a> {
    // Get the ith element in JsonRef
    pub fn array_get_elem(&self, idx: usize) -> JsonRef<'a> {
        self.val_entry_get(HEADER_LEN + idx * VALUE_ENTRY_LEN)
    }

    // Return the ith key in current Object json
    pub fn object_get_key(&self, i: usize) -> &'a [u8] {
        let key_off_start = HEADER_LEN + i * KEY_ENTRY_LEN;
        let key_off = NumberCodec::decode_u32_le(&self.value()[key_off_start..]) as usize;
        let key_len = NumberCodec::decode_u16_le(&self.value()[key_off_start + U32_LEN..]) as usize;
        &self.value()[key_off..key_off + key_len]
    }

    // Return the JsonRef of ith value in current Object json
    pub fn object_get_val(&self, i: usize) -> JsonRef<'a> {
        let ele_count = self.get_elem_count() as usize;
        let val_entry_off = HEADER_LEN + ele_count * KEY_ENTRY_LEN + i * VALUE_ENTRY_LEN;
        self.val_entry_get(val_entry_off)
    }

    // Try to get the value index by the give `key` in Object.
    pub fn object_search_key(&self, key: &[u8]) -> Option<usize> {
        let len = self.get_elem_count() as usize;
        let mut j = len;
        let mut i = 0;
        while i < j {
            let mid = (i + j) >> 1;
            if self.object_get_key(mid).cmp(key) == Ordering::Less {
                i = mid + 1;
            } else {
                j = mid;
            }
        }
        if i < len && self.object_get_key(i).cmp(key) == Ordering::Equal {
            return Some(i);
        }
        None
    }

    // Get the value (JsonRef) by the given offset of the value entry
    pub fn val_entry_get(&self, val_entry_off: usize) -> JsonRef<'a> {
        let val_type: JsonType = self.value()[val_entry_off].into();
        let val_offset =
            NumberCodec::decode_u32_le(&self.value()[val_entry_off + TYPE_LEN as usize..]) as usize;
        match val_type {
            JsonType::Literal => {
                let offset = val_entry_off + TYPE_LEN;
                JsonRef::new(val_type, &self.value()[offset..offset + LITERAL_LEN])
            }
            JsonType::U64 | JsonType::I64 | JsonType::Double => {
                JsonRef::new(val_type, &self.value()[val_offset..val_offset + NUMBER_LEN])
            }
            JsonType::String => {
                let (str_len, len_len) =
                    NumberCodec::try_decode_var_u64(&self.value()[val_offset..]).unwrap();
                JsonRef::new(
                    val_type,
                    &self.value()[val_offset..val_offset + str_len as usize + len_len],
                )
            }
            _ => {
                let data_size =
                    NumberCodec::decode_u32_le(&self.value()[val_offset + ELEMENT_COUNT_LEN..])
                        as usize;
                JsonRef::new(val_type, &self.value()[val_offset..val_offset + data_size])
            }
        }
    }

    pub fn as_ptr(&self) -> *const u8 {
        self.value.as_ptr()
    }

    pub fn as_literal(&self) -> Result<u8> {
        match self.get_type() {
            JsonType::Literal => Ok(self.value()[0]),
            _ => Err(invalid_type!(
                "{} from {} to literal",
                ERR_CONVERT_FAILED,
                self.to_string()
            )),
        }
    }

    pub fn binary_len(&self) -> usize {
        TYPE_LEN + self.value.len()
    }
}

pub trait JsonEncoder: NumberEncoder {
    fn write_json<'a>(&mut self, data: JsonRef<'a>) -> Result<()> {
        self.write_u8(data.get_type() as u8)?;
        self.write_bytes(data.value()).map_err(Error::from)
    }

    fn write_json_obj_from_keys_values<'a>(
        &mut self,
        keys: Vec<&[u8]>,
        values: Vec<JsonRef<'a>>,
    ) -> Result<()> {
        // object: element-count size key-entry* value-entry* key* value*
        let element_count = keys.len();
        // key-entry ::= key-offset(uint32) key-length(uint16)
        let key_entries_len = KEY_ENTRY_LEN * element_count;
        // value-entry ::= type(byte) offset-or-inlined-value(uint32)
        let value_entries_len = VALUE_ENTRY_LEN * element_count;
        let mut key_entries = Vec::with_capacity(key_entries_len);
        let mut encode_keys = Vec::new();
        let mut key_offset = ELEMENT_COUNT_LEN + SIZE_LEN + key_entries_len + value_entries_len;
        for key in keys {
            let key_len = encode_keys.write(key)?;
            key_entries.write_u32_le(key_offset as u32)?;
            key_entries.write_u16_le(key_len as u16)?;
            key_offset += key_len;
        }

        let mut value_offset = key_offset as u32;
        let mut value_entries = Vec::with_capacity(value_entries_len);
        let mut encode_values = vec![];
        for value in values {
            value_entries.write_json_item(&value, &mut value_offset, &mut encode_values)?;
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
            value_entries.write_json_item(
                &value.as_ref(),
                &mut value_offset,
                &mut encode_values,
            )?;
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

    fn write_json_ref_array<'a>(&mut self, data: &[JsonRef<'a>]) -> Result<()> {
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

    fn write_json_array(&mut self, data: &[Json]) -> Result<()> {
        // array ::= element-count size value-entry* value*
        let element_count = data.len();
        let value_entries_len = VALUE_ENTRY_LEN * element_count;
        let mut value_offset = (ELEMENT_COUNT_LEN + SIZE_LEN + value_entries_len) as u32;
        let mut value_entries = Vec::with_capacity(value_entries_len);
        let mut encode_values = vec![];
        for value in data {
            value_entries.write_json_item(
                &value.as_ref(),
                &mut value_offset,
                &mut encode_values,
            )?;
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

    fn write_json_item<'a>(
        &mut self,
        data: &JsonRef<'a>,
        offset: &mut u32,
        data_buf: &mut Vec<u8>,
    ) -> Result<()> {
        let code = data.get_type();
        self.write_u8(code as u8)?;
        match code {
            // If the data has length in (0, 4], it could be inline here.
            // And padding 0x00 to 4 bytes if needed.
            JsonType::Literal => {
                let v = data.as_literal()?;
                self.write_u8(v)?;
                let left = U32_LEN - LITERAL_LEN;
                for _ in 0..left {
                    self.write_u8(JSON_LITERAL_NIL)?;
                }
            }
            _ => {
                self.write_u32_le(*offset)?;
                data_buf.write_bytes(data.value)?;
                *offset += data.value.len() as u32;
            }
        };
        Ok(())
    }
}

impl<T: BufferWriter> JsonEncoder for T {}

pub trait JsonDatumPayloadChunkEncoder: BufferWriter {
    fn write_json_to_chunk_by_datum_payload(&mut self, src_payload: &[u8]) -> Result<()> {
        self.write_bytes(src_payload)?;
        Ok(())
    }
}

impl<T: BufferWriter> JsonDatumPayloadChunkEncoder for T {}

pub trait JsonDecoder: NumberDecoder {
    // `read_json` decodes value encoded by `write_json` before.
    fn read_json(&mut self) -> Result<Json> {
        if self.bytes().is_empty() {
            return Err(box_err!("Cant read json from empty bytes"));
        }
        let tp: JsonType = self.read_u8()?.into();
        let value = match tp {
            JsonType::Object | JsonType::Array => {
                let value = self.bytes();
                let data_size = NumberCodec::decode_u32_le(&value[ELEMENT_COUNT_LEN..]) as usize;
                self.read_bytes(data_size)?
            }
            JsonType::String => {
                let value = self.bytes();
                let (str_len, len_len) = NumberCodec::try_decode_var_u64(&value).unwrap();
                self.read_bytes(str_len as usize + len_len)?
            }
            JsonType::I64 | JsonType::U64 | JsonType::Double => self.read_bytes(NUMBER_LEN)?,
            JsonType::Literal => self.read_bytes(LITERAL_LEN)?,
        };
        Ok(Json::new(tp, Vec::from(value)))
    }
}

impl<T: BufferReader> JsonDecoder for T {}

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

        let json_nil = Json::none();
        let json_bool = Json::from_bool(true);
        let json_int = Json::from_i64(30);
        let json_uint = Json::from_u64(30);
        let json_double = Json::from_f64(3.24);
        let json_str = Json::from_string(String::from("hello, 世界"));
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
            data.write_json(json.as_ref()).unwrap();
            let output = data.as_slice().read_json().unwrap();
            let input_str = json.to_string();
            let output_str = output.to_string();
            assert_eq!(input_str, output_str);
        }
    }

    #[test]
    fn test_type() {
        let legal_cases = vec![
            (r#"{"key":"value"}"#, JsonType::Object),
            (r#"["d1","d2"]"#, JsonType::Array),
            (r#"-3"#, JsonType::I64),
            (r#"3"#, JsonType::I64),
            (r#"18446744073709551615"#, JsonType::Double),
            (r#"3.0"#, JsonType::Double),
            (r#"null"#, JsonType::Literal),
            (r#"true"#, JsonType::Literal),
            (r#"false"#, JsonType::Literal),
        ];

        for (json_str, tp) in legal_cases {
            let json: Json = json_str.parse().unwrap();
            assert_eq!(json.as_ref().get_type(), tp, "{:?}", json_str);
        }
    }
}
