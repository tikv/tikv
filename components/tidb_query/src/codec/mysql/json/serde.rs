// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

use serde::de::{self, Deserialize, Deserializer, MapAccess, SeqAccess, Visitor};
use serde::ser::{Error as SerError, Serialize, SerializeMap, SerializeTuple, Serializer};
use serde_json;
use std::collections::BTreeMap;
use std::fmt;
use std::str::FromStr;
use std::string::ToString;
use std::{f64, str};

use super::{Json, JsonRef, JsonType};
use crate::codec::Error;

impl<'a> ToString for JsonRef<'a> {
    fn to_string(&self) -> String {
        serde_json::to_string(self).unwrap()
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
        }
    }
}

impl ToString for Json {
    fn to_string(&self) -> String {
        serde_json::to_string(&self.as_ref()).unwrap()
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
        Ok(Json::none().map_err(de::Error::custom)?)
    }

    fn visit_bool<E>(self, v: bool) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(Json::from_bool(v).map_err(de::Error::custom)?)
    }

    fn visit_i64<E>(self, v: i64) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(Json::from_i64(v).map_err(de::Error::custom)?)
    }

    fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        if v > (std::i64::MAX as u64) {
            Ok(Json::from_f64(v as f64).map_err(de::Error::custom)?)
        } else {
            Ok(Json::from_i64(v as i64).map_err(de::Error::custom)?)
        }
    }

    fn visit_f64<E>(self, v: f64) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(Json::from_f64(v).map_err(de::Error::custom)?)
    }

    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(Json::from_string(String::from(v)).map_err(de::Error::custom)?)
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
        Ok(Json::from_array(value).map_err(de::Error::custom)?)
    }

    fn visit_map<M>(self, mut access: M) -> Result<Self::Value, M::Error>
    where
        M: MapAccess<'de>,
    {
        let mut map = BTreeMap::new();
        while let Some((key, value)) = access.next_entry()? {
            map.insert(key, value);
        }
        Ok(Json::from_object(map).map_err(de::Error::custom)?)
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

    #[test]
    fn test_from_str_for_object() {
        let jstr1 = r#"{"a": [1, "2", {"aa": "bb"}, 4.0, null], "c": null,"b": true}"#;
        let j1: Json = jstr1.parse().unwrap();
        let jstr2 = j1.to_string();
        let expect_str = r#"{"a":[1,"2",{"aa":"bb"},4.0,null],"b":true,"c":null}"#;
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
            let resp = Json::from_str(json_str);
            assert!(resp.is_ok());
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
            let resp = Json::from_str(json_str);
            assert!(resp.is_ok());
            assert_eq!(resp.unwrap(), json.unwrap());
        }

        let illegal_cases = vec!["[pxx,apaa]", "hpeheh", ""];
        for json_str in illegal_cases {
            let resp = Json::from_str(json_str);
            assert!(resp.is_err());
        }
    }
}
