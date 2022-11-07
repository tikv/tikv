// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

use std::{collections::BTreeMap, f64, fmt, str, str::FromStr, string::ToString};

use serde::{
    de::{self, Deserialize, Deserializer, MapAccess, SeqAccess, Visitor},
    ser::{Error as SerError, Serialize, SerializeMap, SerializeTuple, Serializer},
};
use serde_json::Serializer as JsonSerializer;

use super::{Json, JsonRef, JsonType};
use crate::codec::Error;

/// MySQL formatter follows the implementation in TiDB
/// https://github.com/pingcap/tidb/blob/master/types/json/binary.go
/// We add a space after `,` and `:`.
#[derive(Clone, Debug)]
pub struct MySqlFormatter {}

impl serde_json::ser::Formatter for MySqlFormatter {
    #[inline]
    fn begin_object_value<W>(&mut self, writer: &mut W) -> std::io::Result<()>
    where
        W: ?Sized + std::io::Write,
    {
        writer.write_all(b": ")
    }

    #[inline]
    fn begin_array_value<W>(&mut self, writer: &mut W, first: bool) -> std::io::Result<()>
    where
        W: ?Sized + std::io::Write,
    {
        if first {
            Ok(())
        } else {
            writer.write_all(b", ")
        }
    }

    #[inline]
    fn begin_object_key<W>(&mut self, writer: &mut W, first: bool) -> std::io::Result<()>
    where
        W: ?Sized + std::io::Write,
    {
        if first {
            Ok(())
        } else {
            writer.write_all(b", ")
        }
    }
}

impl MySqlFormatter {
    pub fn new() -> Self {
        MySqlFormatter {}
    }
}

impl<'a> ToString for JsonRef<'a> {
    /// This function is a simple combination and rewrite of serde_json's
    /// `to_writer_pretty`
    fn to_string(&self) -> String {
        let mut writer = Vec::with_capacity(128);
        let mut ser = JsonSerializer::with_formatter(&mut writer, MySqlFormatter::new());
        self.serialize(&mut ser).unwrap();
        unsafe {
            // serde_json will not emit invalid UTF-8
            String::from_utf8_unchecked(writer)
        }
    }
}

impl<'a> Serialize for JsonRef<'a> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self.get_type() {
            JsonType::Literal => match self.get_literal() {
                Some(b) => serializer.serialize_bool(b),
                None => serializer.serialize_none(),
            },
            JsonType::String => match self.get_str() {
                Ok(s) => serializer.serialize_str(s),
                Err(_) => Err(SerError::custom("json contains invalid UTF-8 characters")),
            },
            JsonType::Double => serializer.serialize_f64(self.get_double()),
            JsonType::I64 => serializer.serialize_i64(self.get_i64()),
            JsonType::U64 => serializer.serialize_u64(self.get_u64()),
            JsonType::Object => {
                let elem_count = self.get_elem_count();
                let mut map = serializer.serialize_map(Some(elem_count))?;
                for i in 0..elem_count {
                    let key = self.object_get_key(i);
                    let val = self.object_get_val(i).map_err(SerError::custom)?;
                    map.serialize_entry(str::from_utf8(key).unwrap(), &val)?;
                }
                map.end()
            }
            JsonType::Array => {
                let elem_count = self.get_elem_count();
                let mut tup = serializer.serialize_tuple(elem_count)?;
                for i in 0..elem_count {
                    let item = self.array_get_elem(i).map_err(SerError::custom)?;
                    tup.serialize_element(&item)?;
                }
                tup.end()
            }
            JsonType::Opaque => {
                let bytes = self
                    .get_opaque_bytes()
                    .map_err(|_| SerError::custom("invalid opaque value"))?;
                let typ = self
                    .get_opaque_type()
                    .map_err(|_| SerError::custom("invalid opaque type code"))?;

                let str = format!(
                    "base64:type{}:{}",
                    typ.to_u8().unwrap(),
                    base64::encode(bytes)
                );
                serializer.serialize_str(&str)
            }
            JsonType::Date | JsonType::Datetime | JsonType::Timestamp => {
                let mut time = self
                    .get_time()
                    .map_err(|_| SerError::custom("invalid time data"))?;
                // Printing json datetime/duration will always keep 6 fsp
                time.maximize_fsp();

                serializer.serialize_str(&time.to_string())
            }
            JsonType::Time => {
                let duration = self
                    .get_duration()
                    .map_err(|_| SerError::custom("invalid duration data"))?;
                // Printing json datetime/duration will always keep 6 fsp
                let duration = duration.maximize_fsp();

                serializer.serialize_str(&duration.to_string())
            }
        }
    }
}

impl ToString for Json {
    fn to_string(&self) -> String {
        self.as_ref().to_string()
    }
}

impl FromStr for Json {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match serde_json::from_str(s) {
            Ok(value) => Ok(value),
            Err(e) => Err(invalid_type!("Illegal Json text: {:?}", e)),
        }
    }
}

struct JsonVisitor;
impl<'de> Visitor<'de> for JsonVisitor {
    type Value = Json;
    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "a json value")
    }

    fn visit_unit<E>(self) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Json::none().map_err(de::Error::custom)
    }

    fn visit_bool<E>(self, v: bool) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Json::from_bool(v).map_err(de::Error::custom)
    }

    fn visit_i64<E>(self, v: i64) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Json::from_i64(v).map_err(de::Error::custom)
    }

    fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        if v > (i64::MAX as u64) {
            Ok(Json::from_f64(v as f64).map_err(de::Error::custom)?)
        } else {
            Ok(Json::from_i64(v as i64).map_err(de::Error::custom)?)
        }
    }

    fn visit_f64<E>(self, v: f64) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Json::from_f64(v).map_err(de::Error::custom)
    }

    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Json::from_string(String::from(v)).map_err(de::Error::custom)
    }

    fn visit_seq<M>(self, mut seq: M) -> Result<Self::Value, M::Error>
    where
        M: SeqAccess<'de>,
    {
        let size = seq.size_hint().unwrap_or_default();
        let mut value = Vec::with_capacity(size);
        while let Some(v) = seq.next_element()? {
            value.push(v);
        }
        Json::from_array(value).map_err(de::Error::custom)
    }

    fn visit_map<M>(self, mut access: M) -> Result<Self::Value, M::Error>
    where
        M: MapAccess<'de>,
    {
        let mut map = BTreeMap::new();
        while let Some((key, value)) = access.next_entry()? {
            map.insert(key, value);
        }
        Json::from_object(map).map_err(de::Error::custom)
    }
}

impl<'de> Deserialize<'de> for Json {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_any(JsonVisitor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::FieldTypeTp;

    #[test]
    fn test_from_str_for_object() {
        let jstr1 = r#"{"a": [1, "2", {"aa": "bb"}, 4.0, null], "c": null,"b": true}"#;
        let j1: Json = jstr1.parse().unwrap();
        let jstr2 = j1.to_string();
        let expect_str = r#"{"a": [1, "2", {"aa": "bb"}, 4.0, null], "b": true, "c": null}"#;
        assert_eq!(jstr2, expect_str);
    }

    #[test]
    fn test_from_str() {
        let legal_cases = vec![
            (r#"{"key":"value"}"#),
            (r#"["d1","d2"]"#),
            (r#"-3"#),
            (r#"3"#),
            (r#"3.0"#),
            (r#"null"#),
            (r#"true"#),
            (r#"false"#),
        ];

        for json_str in legal_cases {
            Json::from_str(json_str).unwrap();
        }

        let cases = vec![
            (
                r#"9223372036854776000"#,
                Json::from_f64(9223372036854776000.0),
            ),
            (
                r#"9223372036854775807"#,
                Json::from_i64(9223372036854775807),
            ),
        ];

        for (json_str, json) in cases {
            let resp = Json::from_str(json_str).unwrap();
            assert_eq!(resp, json.unwrap());
        }

        let illegal_cases = vec!["[pxx,apaa]", "hpeheh", ""];
        for json_str in illegal_cases {
            let resp = Json::from_str(json_str);
            resp.unwrap_err();
        }
    }

    #[test]
    fn test_to_str() {
        let legal_cases = vec![
            (
                Json::from_kv_pairs(vec![(
                    b"key",
                    Json::from_str_val("value").unwrap().as_ref(),
                )])
                .unwrap(),
                r#"{"key": "value"}"#,
            ),
            (
                Json::from_array(vec![
                    Json::from_str_val("d1").unwrap(),
                    Json::from_str_val("d2").unwrap(),
                ])
                .unwrap(),
                r#"["d1", "d2"]"#,
            ),
            (Json::from_i64(-3).unwrap(), r#"-3"#),
            (Json::from_i64(3).unwrap(), r#"3"#),
            (Json::from_f64(3.0).unwrap(), r#"3.0"#),
            (Json::none().unwrap(), r#"null"#),
            (Json::from_bool(true).unwrap(), r#"true"#),
            (Json::from_bool(false).unwrap(), r#"false"#),
            (
                Json::from_opaque(FieldTypeTp::VarString, &[0xAB, 0xCD]).unwrap(),
                r#""base64:type253:q80=""#,
            ),
        ];

        for (json, json_str) in legal_cases {
            assert_eq!(json.to_string(), json_str);
        }
    }
}
