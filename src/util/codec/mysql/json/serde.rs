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

use super::Json;
use std::{str, f64};
use std::collections::BTreeMap;
use serde_json;
use std::fmt;
use util::codec::Error;
use std::str::FromStr;
use serde::ser::{Serialize, Serializer, SerializeTuple, SerializeMap};
use serde::de::{self, Deserialize, Deserializer, Visitor, SeqAccess, MapAccess};

const ERR_CONVERT_FAILED: &str = "Can not covert from ";

impl Json {
    pub fn to_string(&self) -> String {
        serde_json::to_string(self).unwrap()
    }
}

impl FromStr for Json {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match serde_json::from_str(s) {
            Ok(value) => Ok(value),
            Err(e) => Err(invalid_type!("Illegal Json text:{:?}", e)),
        }
    }
}

impl Serialize for Json {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where S: Serializer
    {
        match *self {
            Json::None => serializer.serialize_none(),
            Json::Boolean(d) => serializer.serialize_bool(d),
            Json::String(ref s) => serializer.serialize_str(s),
            Json::Object(ref obj) => {
                let mut map = try!(serializer.serialize_map(Some(obj.len())));
                for (k, v) in obj {
                    try!(map.serialize_entry(k, v));
                }
                map.end()
            }
            Json::Array(ref array) => {
                let mut tup = try!(serializer.serialize_tuple(array.len()));
                for item in array {
                    try!(tup.serialize_element(item));
                }
                tup.end()
            }
            Json::Double(d) => serializer.serialize_f64(d),
            Json::I64(d) => serializer.serialize_i64(d),
        }
    }
}

struct JsonVisitor;
impl<'de> Visitor<'de> for JsonVisitor {
    type Value = Json;
    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        write!(formatter, "a json value")
    }

    fn visit_unit<E>(self) -> Result<Self::Value, E>
        where E: de::Error
    {
        Ok(Json::None)
    }

    fn visit_bool<E>(self, v: bool) -> Result<Self::Value, E>
        where E: de::Error
    {
        Ok(Json::Boolean(v))
    }

    fn visit_i64<E>(self, v: i64) -> Result<Self::Value, E>
        where E: de::Error
    {
        Ok(Json::I64(v))
    }

    fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E>
        where E: de::Error
    {
        self.visit_i64(v as i64)
    }

    fn visit_f64<E>(self, v: f64) -> Result<Self::Value, E>
        where E: de::Error
    {
        Ok(Json::Double(v))
    }

    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
        where E: de::Error
    {
        Ok(Json::String(String::from(v)))
    }

    fn visit_seq<M>(self, mut seq: M) -> Result<Self::Value, M::Error>
        where M: SeqAccess<'de>
    {
        let mut seqs = Vec::with_capacity(seq.size_hint().unwrap_or(0));
        while let Some(value) = seq.next_element()? {
            seqs.push(value);
        }
        Ok(Json::Array(seqs))
    }

    fn visit_map<M>(self, mut access: M) -> Result<Self::Value, M::Error>
        where M: MapAccess<'de>
    {
        let mut map = BTreeMap::new();
        while let Some((key, value)) = access.next_entry()? {
            map.insert(key, value);
        }
        Ok(Json::Object(map))
    }
}

impl<'de> Deserialize<'de> for Json {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where D: Deserializer<'de>
    {
        deserializer.deserialize_any(JsonVisitor)
    }
}

#[cfg(test)]
mod test {
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
        let legal_cases = vec!{
            (r#"{"key":"value"}"#),
            (r#"["d1","d2"]"#),
            (r#"3"#),
            (r#"3.0"#),
            (r#"null"#),
            (r#"true"#),
            (r#"false"#),
        };

        for json_str in legal_cases {
            let resp = Json::from_str(json_str);
            assert!(resp.is_ok());
        }

        let illegal_cases = vec!["[pxx,apaa]", "hpeheh", ""];
        for json_str in illegal_cases {
            let resp = Json::from_str(json_str);
            assert!(resp.is_err());
        }
    }
}
