// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

use std::convert::TryInto;

use codec::number::NumberCodec;

use super::{constants::*, JsonRef, JsonType, ERR_CONVERT_FAILED};
use crate::codec::{mysql::json::path_expr::ArrayIndex, Result};

impl<'a> JsonRef<'a> {
    /// Gets the index from the ArrayIndex
    ///
    /// If the idx is greater than the count and is from right, it will return
    /// `None`
    ///
    /// See `jsonPathArrayIndex.getIndexFromStart()` in TiDB
    /// `types/json_path_expr.go`
    pub fn array_get_index(&self, idx: ArrayIndex) -> Option<usize> {
        match idx {
            ArrayIndex::Left(idx) => Some(idx as usize),
            ArrayIndex::Right(idx) => {
                if self.get_elem_count() < 1 + (idx as usize) {
                    None
                } else {
                    Some(self.get_elem_count() - 1 - (idx as usize))
                }
            }
        }
    }

    /// Gets the ith element in JsonRef
    ///
    /// See `arrayGetElem()` in TiDB `json/binary.go`
    pub fn array_get_elem(&self, idx: usize) -> Result<JsonRef<'a>> {
        self.val_entry_get(HEADER_LEN + idx * VALUE_ENTRY_LEN)
    }

    /// Return the `i`th key in current Object json
    ///
    /// See `objectGetKey()` in TiDB `types/json_binary.go`
    pub fn object_get_key(&self, i: usize) -> &'a [u8] {
        let key_off_start = HEADER_LEN + i * KEY_ENTRY_LEN;
        let key_off = NumberCodec::decode_u32_le(&self.value()[key_off_start..]) as usize;
        let key_len =
            NumberCodec::decode_u16_le(&self.value()[key_off_start + KEY_OFFSET_LEN..]) as usize;
        &self.value()[key_off..key_off + key_len]
    }

    /// Returns the JsonRef of `i`th value in current Object json
    ///
    /// See `objectGetVal()` in TiDB `types/json_binary.go`
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
            NumberCodec::decode_u32_le(&self.value()[val_entry_off + TYPE_LEN..]) as usize;
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
            JsonType::Opaque => {
                let (opaque_bytes_len, len_len) =
                    NumberCodec::try_decode_var_u64(&self.value()[val_offset + 1..])?;
                JsonRef::new(
                    val_type,
                    &self.value()[val_offset..val_offset + opaque_bytes_len as usize + len_len + 1],
                )
            }
            JsonType::Date | JsonType::Datetime | JsonType::Timestamp => {
                JsonRef::new(val_type, &self.value()[val_offset..val_offset + TIME_LEN])
            }
            JsonType::Time => JsonRef::new(
                val_type,
                &self.value()[val_offset..val_offset + DURATION_LEN],
            ),
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
    use std::collections::BTreeMap;

    use super::*;
    use crate::{
        codec::{
            data_type::Duration,
            mysql::{Json, Time, TimeType},
        },
        expr::EvalContext,
    };

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

    #[test]
    fn test_array_get_elem() {
        let mut ctx = EvalContext::default();

        let time = Time::parse(
            &mut ctx,
            "1998-06-13 12:13:14",
            TimeType::DateTime,
            0,
            false,
        )
        .unwrap();
        let duration = Duration::parse(&mut ctx, "12:13:14", 0).unwrap();
        let array = vec![
            Json::from_u64(1).unwrap(),
            Json::from_str_val("abcdefg").unwrap(),
        ];
        let object = BTreeMap::from([
            ("key1".to_string(), Json::from_u64(1).unwrap()),
            ("key2".to_string(), Json::from_str_val("abcdefg").unwrap()),
        ]);

        let json_array = Json::from_array(vec![
            Json::from_u64(1).unwrap(),
            Json::from_time(time).unwrap(),
            Json::from_duration(duration).unwrap(),
            Json::from_array(array).unwrap(),
            Json::from_str_val("abcdefg").unwrap(),
            Json::from_bool(false).unwrap(),
            Json::from_object(object).unwrap(),
        ])
        .unwrap();
        let json_array_ref = json_array.as_ref();

        assert_eq!(json_array_ref.array_get_elem(0).unwrap().get_u64(), 1);
        assert_eq!(
            json_array_ref
                .array_get_elem(1)
                .unwrap()
                .get_time()
                .unwrap(),
            time
        );
        assert_eq!(
            json_array_ref
                .array_get_elem(2)
                .unwrap()
                .get_duration()
                .unwrap(),
            duration
        );
        assert_eq!(
            json_array_ref
                .array_get_elem(3)
                .unwrap()
                .array_get_elem(0)
                .unwrap()
                .get_u64(),
            1
        );
        assert_eq!(
            json_array_ref
                .array_get_elem(3)
                .unwrap()
                .array_get_elem(1)
                .unwrap()
                .get_str()
                .unwrap(),
            "abcdefg"
        );
        assert_eq!(
            json_array_ref.array_get_elem(4).unwrap().get_str().unwrap(),
            "abcdefg"
        );
        assert_eq!(
            json_array_ref
                .array_get_elem(5)
                .unwrap()
                .get_literal()
                .unwrap(),
            false
        );
        assert_eq!(
            json_array_ref.array_get_elem(6).unwrap().object_get_key(0),
            b"key1"
        );
        assert_eq!(
            json_array_ref.array_get_elem(6).unwrap().object_get_key(1),
            b"key2"
        );
        assert_eq!(
            json_array_ref
                .array_get_elem(6)
                .unwrap()
                .object_get_val(0)
                .unwrap()
                .get_u64(),
            1
        );
        assert_eq!(
            json_array_ref
                .array_get_elem(6)
                .unwrap()
                .object_get_val(1)
                .unwrap()
                .get_str()
                .unwrap(),
            "abcdefg"
        );
    }

    #[test]
    fn test_object_get_val() {
        let mut ctx = EvalContext::default();

        let time = Time::parse(
            &mut ctx,
            "1998-06-13 12:13:14",
            TimeType::DateTime,
            0,
            false,
        )
        .unwrap();
        let duration = Duration::parse(&mut ctx, "12:13:14", 0).unwrap();
        let array = vec![
            Json::from_u64(1).unwrap(),
            Json::from_str_val("abcdefg").unwrap(),
        ];
        let object = BTreeMap::from([
            ("key1".to_string(), Json::from_u64(1).unwrap()),
            ("key2".to_string(), Json::from_str_val("abcdefg").unwrap()),
        ]);

        let json_object = Json::from_object(BTreeMap::from([
            ("0".to_string(), Json::from_u64(1).unwrap()),
            ("1".to_string(), Json::from_time(time).unwrap()),
            ("2".to_string(), Json::from_duration(duration).unwrap()),
            ("3".to_string(), Json::from_array(array).unwrap()),
            ("4".to_string(), Json::from_str_val("abcdefg").unwrap()),
            ("5".to_string(), Json::from_bool(false).unwrap()),
            ("6".to_string(), Json::from_object(object).unwrap()),
        ]))
        .unwrap();
        let json_object_ref = json_object.as_ref();

        assert_eq!(json_object_ref.object_get_key(0), b"0");
        assert_eq!(json_object_ref.object_get_key(1), b"1");
        assert_eq!(json_object_ref.object_get_key(2), b"2");
        assert_eq!(json_object_ref.object_get_key(3), b"3");

        assert_eq!(json_object_ref.object_get_val(0).unwrap().get_u64(), 1);
        assert_eq!(
            json_object_ref
                .object_get_val(1)
                .unwrap()
                .get_time()
                .unwrap(),
            time
        );
        assert_eq!(
            json_object_ref
                .object_get_val(2)
                .unwrap()
                .get_duration()
                .unwrap(),
            duration
        );
        assert_eq!(
            json_object_ref
                .object_get_val(3)
                .unwrap()
                .array_get_elem(0)
                .unwrap()
                .get_u64(),
            1
        );
        assert_eq!(
            json_object_ref
                .object_get_val(3)
                .unwrap()
                .array_get_elem(1)
                .unwrap()
                .get_str()
                .unwrap(),
            "abcdefg"
        );
        assert_eq!(
            json_object_ref
                .object_get_val(4)
                .unwrap()
                .get_str()
                .unwrap(),
            "abcdefg"
        );
        assert_eq!(
            json_object_ref
                .object_get_val(5)
                .unwrap()
                .get_literal()
                .unwrap(),
            false
        );
        assert_eq!(
            json_object_ref.object_get_val(6).unwrap().object_get_key(0),
            b"key1"
        );
        assert_eq!(
            json_object_ref.object_get_val(6).unwrap().object_get_key(1),
            b"key2"
        );
        assert_eq!(
            json_object_ref
                .object_get_val(6)
                .unwrap()
                .object_get_val(0)
                .unwrap()
                .get_u64(),
            1
        );
        assert_eq!(
            json_object_ref
                .object_get_val(6)
                .unwrap()
                .object_get_val(1)
                .unwrap()
                .get_str()
                .unwrap(),
            "abcdefg"
        );
    }
}
