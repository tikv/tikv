// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

use std::collections::BTreeMap;
use std::convert::TryInto;
use std::{f64, str};

use super::constants::*;
use super::{Json, JsonRef, JsonType, ERR_CONVERT_FAILED};
use crate::codec::{Error, Result};
use codec::number::NumberCodec;
use codec::prelude::*;

///   The binary JSON format from MySQL 5.7 is as follows:
///   JSON doc ::= type value
///   type ::=
///       0x01 |       // large JSON object
///       0x03 |       // large JSON array
///       0x04 |       // literal (true/false/null)
///       0x05 |       // int16
///       0x06 |       // uint16
///       0x07 |       // int32
///       0x08 |       // uint32
///       0x09 |       // int64
///       0x0a |       // uint64
///       0x0b |       // double
///       0x0c |       // utf8mb4 string
///   value ::=
///       object  |
///       array   |
///       literal |
///       number  |
///       string  |
///   object ::= element-count size key-entry* value-entry* key* value*
///   array ::= element-count size value-entry* value*
///   // number of members in object or number of elements in array
///   element-count ::= uint32
///   // number of bytes in the binary representation of the object or array
///   size ::= uint32
///   key-entry ::= key-offset key-length
///   key-offset ::= uint32
///   key-length ::= uint16    // key length must be less than 64KB
///   value-entry ::= type offset-or-inlined-value
///   // This field holds either the offset to where the value is stored,
///   // or the value itself if it is small enough to be inlined (that is,
///   // if it is a JSON literal or a small enough [u]int).
///   offset-or-inlined-value ::= uint32
///   key ::= utf8mb4-data
///   literal ::=
///       0x00 |   // JSON null literal
///       0x01 |   // JSON true literal
///       0x02 |   // JSON false literal
///   number ::=  ....    // little-endian format for [u]int(16|32|64), whereas
///                       // double is stored in a platform-independent, eight-byte
///                       // format using float8store()
///   string ::= data-length utf8mb4-data
///   data-length ::= uint8*    // If the high bit of a byte is 1, the length
///                             // field is continued in the next byte,
///                             // otherwise it is the last byte of the length
///                             // field. So we need 1 byte to represent
///                             // lengths up to 127, 2 bytes to represent
///                             // lengths up to 16383, and so on...
///
impl<'a> JsonRef<'a> {
    /// Gets the ith element in JsonRef
    pub fn array_get_elem(&self, idx: usize) -> Result<JsonRef<'a>> {
        self.val_entry_get(HEADER_LEN + idx * VALUE_ENTRY_LEN)
    }

    /// Return the `i`th key in current Object json
    pub fn object_get_key(&self, i: usize) -> &'a [u8] {
        let key_off_start = HEADER_LEN + i * KEY_ENTRY_LEN;
        let key_off = NumberCodec::decode_u32_le(&self.value()[key_off_start..]) as usize;
        let key_len = NumberCodec::decode_u16_le(&self.value()[key_off_start + U32_LEN..]) as usize;
        &self.value()[key_off..key_off + key_len]
    }

    /// Returns the JsonRef of `i`th value in current Object json
    pub fn object_get_val(&self, i: usize) -> Result<JsonRef<'a>> {
        let ele_count = self.get_elem_count() as usize;
        let val_entry_off = HEADER_LEN + ele_count * KEY_ENTRY_LEN + i * VALUE_ENTRY_LEN;
        self.val_entry_get(val_entry_off)
    }

    /// Searches the value index by the give `key` in Object.
    pub fn object_search_key(&self, key: &[u8]) -> Option<usize> {
        let len = self.get_elem_count() as usize;
        let mut j = len;
        let mut i = 0;
        while i < j {
            let mid = (i + j) >> 1;
            if self.object_get_key(mid) < key {
                i = mid + 1;
            } else {
                j = mid;
            }
        }
        if i < len && self.object_get_key(i) == key {
            return Some(i);
        }
        None
    }

    /// Gets the value (JsonRef) by the given offset of the value entry
    pub fn val_entry_get(&self, val_entry_off: usize) -> Result<JsonRef<'a>> {
        let val_type: JsonType = self.value()[val_entry_off].try_into()?;
        let val_offset =
            NumberCodec::decode_u32_le(&self.value()[val_entry_off + TYPE_LEN as usize..]) as usize;
        Ok(match val_type {
            JsonType::Literal => {
                let offset = val_entry_off + TYPE_LEN;
                #[allow(clippy::range_plus_one)]
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
        })
    }

    // Returns a raw pointer to the underlying values buffer.
    pub(super) fn as_ptr(&self) -> *const u8 {
        self.value.as_ptr()
    }

    // Returns the literal value of JSON document
    pub(super) fn as_literal(&self) -> Result<u8> {
        match self.get_type() {
            JsonType::Literal => Ok(self.value()[0]),
            _ => Err(invalid_type!(
                "{} from {} to literal",
                ERR_CONVERT_FAILED,
                self.to_string()
            )),
        }
    }

    /// Returns the encoding binary length of self
    pub fn binary_len(&self) -> usize {
        TYPE_LEN + self.value.len()
    }

    fn encoded_len(&self) -> usize {
        match self.type_code {
            // Literal is encoded inline with value-entry, so nothing will be
            // appended in value part
            JsonType::Literal => 0,
            _ => self.value.len(),
        }
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
        let kv_encoded_len = keys
            .iter()
            .zip(&values)
            .fold(0, |acc, (k, v)| acc + k.len() + v.encoded_len());
        let size =
            ELEMENT_COUNT_LEN + SIZE_LEN + key_entries_len + value_entries_len + kv_encoded_len;
        self.write_u32_le(element_count as u32)?;
        self.write_u32_le(size as u32)?;
        let mut key_offset = ELEMENT_COUNT_LEN + SIZE_LEN + key_entries_len + value_entries_len;

        // Write key entries
        for key in keys.iter() {
            let key_len = key.len();
            self.write_u32_le(key_offset as u32)?;
            self.write_u16_le(key_len as u16)?;
            key_offset += key_len;
        }

        let mut value_offset = key_offset as u32;
        // Write value entries
        for v in values.iter() {
            self.write_value_entry(&mut value_offset, v)?;
        }

        // Write keys
        for key in keys {
            self.write_bytes(key)?;
        }

        // Write values
        for v in values {
            if v.get_type() != JsonType::Literal {
                self.write_bytes(v.value)?;
            }
        }
        Ok(())
    }

    fn write_json_obj(&mut self, data: &BTreeMap<String, Json>) -> Result<()> {
        // object: element-count size key-entry* value-entry* key* value*
        let element_count = data.len();
        // key-entry ::= key-offset(uint32) key-length(uint16)
        let key_entries_len = KEY_ENTRY_LEN * element_count;
        // value-entry ::= type(byte) offset-or-inlined-value(uint32)
        let value_entries_len = VALUE_ENTRY_LEN * element_count;
        let kv_encoded_len = data
            .iter()
            .fold(0, |acc, (k, v)| acc + k.len() + v.as_ref().encoded_len());
        let size =
            ELEMENT_COUNT_LEN + SIZE_LEN + key_entries_len + value_entries_len + kv_encoded_len;
        self.write_u32_le(element_count as u32)?;
        self.write_u32_le(size as u32)?;
        let mut key_offset = ELEMENT_COUNT_LEN + SIZE_LEN + key_entries_len + value_entries_len;

        // Write key entries
        for key in data.keys() {
            let key_len = key.len();
            self.write_u32_le(key_offset as u32)?;
            self.write_u16_le(key_len as u16)?;
            key_offset += key_len;
        }

        let mut value_offset = key_offset as u32;
        // Write value entries
        for v in data.values() {
            self.write_value_entry(&mut value_offset, &v.as_ref())?;
        }

        // Write keys
        for key in data.keys() {
            self.write_bytes(key.as_bytes())?;
        }

        // Write values
        for v in data.values() {
            if v.as_ref().get_type() != JsonType::Literal {
                self.write_bytes(v.as_ref().value())?;
            }
        }
        Ok(())
    }

    fn write_json_ref_array<'a>(&mut self, data: &[JsonRef<'a>]) -> Result<()> {
        let element_count = data.len();
        let value_entries_len = VALUE_ENTRY_LEN * element_count;
        let values_len = data.iter().fold(0, |acc, v| acc + v.encoded_len());
        let total_size = ELEMENT_COUNT_LEN + SIZE_LEN + value_entries_len + values_len;
        self.write_u32_le(element_count as u32)?;
        self.write_u32_le(total_size as u32)?;
        let mut value_offset = (ELEMENT_COUNT_LEN + SIZE_LEN + value_entries_len) as u32;
        // Write value entries
        for v in data {
            self.write_value_entry(&mut value_offset, v)?;
        }
        // Write value data
        for v in data {
            if v.get_type() != JsonType::Literal {
                self.write_bytes(v.value())?;
            }
        }
        Ok(())
    }

    fn write_json_array(&mut self, data: &[Json]) -> Result<()> {
        // array ::= element-count size value-entry* value*
        let element_count = data.len();
        let value_entries_len = VALUE_ENTRY_LEN * element_count;
        let values_len = data.iter().fold(0, |acc, v| acc + v.as_ref().encoded_len());
        let total_size = ELEMENT_COUNT_LEN + SIZE_LEN + value_entries_len + values_len;
        self.write_u32_le(element_count as u32)?;
        self.write_u32_le(total_size as u32)?;
        let mut value_offset = (ELEMENT_COUNT_LEN + SIZE_LEN + value_entries_len) as u32;
        // Write value entries
        for v in data {
            self.write_value_entry(&mut value_offset, &v.as_ref())?;
        }
        // Write value data
        for v in data {
            if v.as_ref().get_type() != JsonType::Literal {
                self.write_bytes(v.as_ref().value())?;
            }
        }
        Ok(())
    }

    fn write_value_entry<'a>(&mut self, value_offset: &mut u32, v: &JsonRef<'a>) -> Result<()> {
        let tp = v.get_type();
        self.write_u8(tp as u8)?;
        match tp {
            JsonType::Literal => {
                self.write_u8(v.value()[0])?;
                let left = U32_LEN - LITERAL_LEN;
                for _ in 0..left {
                    self.write_u8(JSON_LITERAL_NIL)?;
                }
            }
            _ => {
                self.write_u32_le(*value_offset)?;
                *value_offset += v.value().len() as u32;
            }
        }
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
        let tp: JsonType = self.read_u8()?.try_into()?;
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
