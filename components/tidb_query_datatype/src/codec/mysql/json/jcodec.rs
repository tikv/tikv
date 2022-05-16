// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::{collections::BTreeMap, convert::TryInto, f64, str};

use codec::{number::NumberCodec, prelude::*};

use super::{constants::*, Json, JsonRef, JsonType};
use crate::codec::{Error, Result};

impl<'a> JsonRef<'a> {
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
    fn write_json(&mut self, data: JsonRef<'_>) -> Result<()> {
        self.write_u8(data.get_type() as u8)?;
        self.write_bytes(data.value()).map_err(Error::from)
    }

    // See `appendBinaryObject` in TiDB `types/json/binary.go`
    fn write_json_obj_from_keys_values<'a>(
        &mut self,
        mut entries: Vec<(&[u8], JsonRef<'a>)>,
    ) -> Result<()> {
        entries.sort_by(|a, b| a.0.cmp(b.0));
        // object: element-count size key-entry* value-entry* key* value*
        let element_count = entries.len();
        // key-entry ::= key-offset(uint32) key-length(uint16)
        let key_entries_len = KEY_ENTRY_LEN * element_count;
        // value-entry ::= type(byte) offset-or-inlined-value(uint32)
        let value_entries_len = VALUE_ENTRY_LEN * element_count;
        let kv_encoded_len = entries
            .iter()
            .fold(0, |acc, (k, v)| acc + k.len() + v.encoded_len());
        let size =
            ELEMENT_COUNT_LEN + SIZE_LEN + key_entries_len + value_entries_len + kv_encoded_len;
        self.write_u32_le(element_count as u32)?;
        self.write_u32_le(size as u32)?;
        let mut key_offset = ELEMENT_COUNT_LEN + SIZE_LEN + key_entries_len + value_entries_len;

        // Write key entries
        for (key, _) in entries.iter() {
            let key_len = key.len();
            self.write_u32_le(key_offset as u32)?;
            self.write_u16_le(key_len as u16)?;
            key_offset += key_len;
        }

        let mut value_offset = key_offset as u32;
        // Write value entries
        for (_, v) in entries.iter() {
            self.write_value_entry(&mut value_offset, v)?;
        }

        // Write keys
        for (key, _) in entries.iter() {
            self.write_bytes(key)?;
        }

        // Write values
        for (_, v) in entries.iter() {
            if v.get_type() != JsonType::Literal {
                self.write_bytes(v.value)?;
            }
        }
        Ok(())
    }

    // See `appendBinaryObject` in TiDB `types/json/binary.go`
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

    // See `appendBinaryArray` in TiDB `types/json/binary.go`
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

    // See `appendBinaryArray` in TiDB `types/json/binary.go`
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

    // See `appendBinaryValElem` in TiDB `types/json/binary.go`
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

    // See `appendBinary` in TiDB `types/json/binary.go`
    fn write_json_literal(&mut self, data: u8) -> Result<()> {
        self.write_u8(data).map_err(Error::from)
    }

    // See `appendBinary` in TiDB `types/json/binary.go`
    fn write_json_i64(&mut self, data: i64) -> Result<()> {
        self.write_i64_le(data).map_err(Error::from)
    }

    // See `appendBinaryUint64` in TiDB `types/json/binary.go`
    fn write_json_u64(&mut self, data: u64) -> Result<()> {
        self.write_u64_le(data).map_err(Error::from)
    }

    // See `appendBinaryFloat64` in TiDB `types/json/binary.go`
    fn write_json_f64(&mut self, data: f64) -> Result<()> {
        self.write_f64_le(data).map_err(Error::from)
    }

    // See `appendBinaryString` in TiDB `types/json/binary.go`
    fn write_json_str(&mut self, data: &str) -> Result<()> {
        let bytes = data.as_bytes();
        let bytes_len = bytes.len() as u64;
        self.write_var_u64(bytes_len)?;
        self.write_bytes(bytes)?;
        Ok(())
    }
}

pub trait JsonDatumPayloadChunkEncoder: BufferWriter {
    fn write_json_to_chunk_by_datum_payload(&mut self, src_payload: &[u8]) -> Result<()> {
        self.write_bytes(src_payload)?;
        Ok(())
    }
}
impl<T: BufferWriter> JsonDatumPayloadChunkEncoder for T {}

impl<T: BufferWriter> JsonEncoder for T {}

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
                let (str_len, len_len) = NumberCodec::try_decode_var_u64(value)?;
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

        let json_nil = Json::none().unwrap();
        let json_bool = Json::from_bool(true).unwrap();
        let json_int = Json::from_i64(30).unwrap();
        let json_uint = Json::from_u64(30).unwrap();
        let json_double = Json::from_f64(3.24).unwrap();
        let json_str = Json::from_string(String::from("hello, 世界")).unwrap();
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
}
