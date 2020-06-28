// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

use std::convert::TryInto;

use super::constants::*;
use super::{JsonRef, JsonType, ERR_CONVERT_FAILED};
use crate::codec::Result;
use codec::number::NumberCodec;

impl<'a> JsonRef<'a> {
    /// Gets the ith element in JsonRef
    ///
    /// See `arrayGetElem()` in TiDB `json/binary.go`
    pub fn array_get_elem(&self, idx: usize) -> Result<JsonRef<'a>> {
        self.val_entry_get(HEADER_LEN + idx * VALUE_ENTRY_LEN)
    }

    /// Return the `i`th key in current Object json
    ///
    /// See `arrayGetElem()` in TiDB `json/binary.go`
    pub fn object_get_key(&self, i: usize) -> &'a [u8] {
        let key_off_start = HEADER_LEN + i * KEY_ENTRY_LEN;
        let key_off = NumberCodec::decode_u32_le(&self.value()[key_off_start..]) as usize;
        let key_len =
            NumberCodec::decode_u16_le(&self.value()[key_off_start + KEY_OFFSET_LEN..]) as usize;
        &self.value()[key_off..key_off + key_len]
    }

    /// Returns the JsonRef of `i`th value in current Object json
    ///
    /// See `arrayGetElem()` in TiDB `json/binary.go`
    pub fn object_get_val(&self, i: usize) -> Result<JsonRef<'a>> {
        let ele_count = self.get_elem_count();
        let val_entry_off = HEADER_LEN + ele_count * KEY_ENTRY_LEN + i * VALUE_ENTRY_LEN;
        self.val_entry_get(val_entry_off)
    }

    /// Searches the value index by the give `key` in Object.
    ///
    /// See `objectSearchKey()` in TiDB `json/binary_function.go`
    pub fn object_search_key(&self, key: &[u8]) -> Option<usize> {
        let len = self.get_elem_count();
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
    ///
    /// See `arrayGetElem()` in TiDB `json/binary.go`
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
                    NumberCodec::try_decode_var_u64(&self.value()[val_offset..])?;
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

    /// Returns a raw pointer to the underlying values buffer.
    pub(super) fn as_ptr(&self) -> *const u8 {
        self.value.as_ptr()
    }

    /// Returns the literal value of JSON document
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
}

#[cfg(test)]
mod tests {
    use super::super::Json;
    use super::*;

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
