// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

use super::{Json, JsonRef, JsonType};
use crate::codec::{Error, Result};
use std::collections::BTreeMap;

impl Json {
    /// `merge` is the implementation for JSON_MERGE in mysql
    /// https://dev.mysql.com/doc/refman/5.7/en/json-modification-functions.html#function_json-merge
    ///
    /// The merge rules are listed as following:
    /// 1. adjacent arrays are merged to a single array;
    /// 2. adjacent object are merged to a single object;
    /// 3. a scalar value is autowrapped as an array before merge;
    /// 4. an adjacent array and object are merged by autowrapping the object as an array.
    ///
    /// See `MergeBinary()` in TiDB `json/binary_function.go`
    #[allow(clippy::comparison_chain)]
    pub fn merge<'a>(bjs: Vec<JsonRef<'a>>) -> Result<Json> {
        let mut result = vec![];
        let mut objects = vec![];
        for j in bjs {
            if j.get_type() != JsonType::Object {
                if objects.len() == 1 {
                    let o = objects.pop().unwrap();
                    result.push(MergeUnit::Ref(o));
                } else if objects.len() > 1 {
                    // We have adjacent JSON objects, merge them into a single object
                    result.push(MergeUnit::Owned(merge_binary_object(&mut objects)?));
                }
                result.push(MergeUnit::Ref(j));
            } else {
                objects.push(j);
            }
        }
        // Resolve the possibly remained objects
        if !objects.is_empty() {
            result.push(MergeUnit::Owned(merge_binary_object(&mut objects)?));
        }
        if result.len() == 1 {
            return Ok(result.pop().unwrap().into_owned());
        }
        merge_binary_array(&result)
    }
}

enum MergeUnit<'a> {
    Ref(JsonRef<'a>),
    Owned(Json),
}

impl<'a> MergeUnit<'a> {
    fn as_ref(&self) -> JsonRef<'_> {
        match self {
            MergeUnit::Ref(r) => *r,
            MergeUnit::Owned(o) => o.as_ref(),
        }
    }
    fn into_owned(self) -> Json {
        match self {
            MergeUnit::Ref(r) => r.to_owned(),
            MergeUnit::Owned(o) => o,
        }
    }
}

// See `mergeBinaryArray()` in TiDB `json/binary_function.go`
fn merge_binary_array<'a>(elems: &[MergeUnit<'a>]) -> Result<Json> {
    let mut buf = vec![];
    for j in elems.iter() {
        let j = j.as_ref();
        if j.get_type() != JsonType::Array {
            buf.push(j)
        } else {
            let child_count = j.get_elem_count();
            for i in 0..child_count {
                buf.push(j.array_get_elem(i)?);
            }
        }
    }
    Json::from_ref_array(buf)
}

// See `mergeBinaryObject()` in TiDB `json/binary_function.go`
fn merge_binary_object<'a>(objects: &mut Vec<JsonRef<'a>>) -> Result<Json> {
    let mut kv_map: BTreeMap<String, Json> = BTreeMap::new();
    for j in objects.drain(..) {
        let elem_count = j.get_elem_count();
        for i in 0..elem_count {
            let key = j.object_get_key(i);
            let val = j.object_get_val(i)?;
            let key = String::from_utf8(key.to_owned()).map_err(Error::from)?;
            if let Some(old) = kv_map.remove(&key) {
                let new = Json::merge(vec![old.as_ref(), val])?;
                kv_map.insert(key, new);
            } else {
                kv_map.insert(key, val.to_owned());
            }
        }
    }
    Json::from_object(kv_map)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_merge() {
        let test_cases = vec![
            vec![r#"{"a": 1}"#, r#"{"b": 2}"#, r#"{"a": 1, "b": 2}"#],
            vec![r#"{"a": 1}"#, r#"{"a": 2}"#, r#"{"a": [1, 2]}"#],
            vec![r#"{"a": 1}"#, r#"{"a": [2, 3]}"#, r#"{"a": [1, 2, 3]}"#],
            vec![
                r#"{"a": 1}"#,
                r#"{"a": {"b": [2, 3]}}"#,
                r#"{"a": [1, {"b": [2, 3]}]}"#,
            ],
            vec![
                r#"{"a": {"b": [2, 3]}}"#,
                r#"{"a": 1}"#,
                r#"{"a": [{"b": [2, 3]}, 1]}"#,
            ],
            vec![
                r#"{"a": [1, 2]}"#,
                r#"{"a": {"b": [3, 4]}}"#,
                r#"{"a": [1, 2, {"b": [3, 4]}]}"#,
            ],
            vec![
                r#"{"b": {"c": 2}}"#,
                r#"{"a": 1, "b": {"d": 1}}"#,
                r#"{"a": 1, "b": {"c": 2, "d": 1}}"#,
            ],
            vec![r#"[1]"#, r#"[2]"#, r#"[1, 2]"#],
            vec![r#"{"a": 1}"#, r#"[1]"#, r#"[{"a": 1}, 1]"#],
            vec![r#"[1]"#, r#"{"a": 1}"#, r#"[1, {"a": 1}]"#],
            vec![r#"{"a": 1}"#, r#"4"#, r#"[{"a": 1}, 4]"#],
            vec![r#"[1]"#, r#"4"#, r#"[1, 4]"#],
            vec![r#"4"#, r#"{"a": 1}"#, r#"[4, {"a": 1}]"#],
            vec![r#"1"#, r#"[4]"#, r#"[1, 4]"#],
            vec![r#"4"#, r#"1"#, r#"[4, 1]"#],
            vec!["1", "2", "3", "[1, 2, 3]"],
            vec!["[1, 2]", "3", "[4, 5, 6]", "[1, 2, 3, 4, 5, 6]"],
            vec![
                r#"{"a": 1, "b": {"c": 3, "d": 4}, "e": [5, 6]}"#,
                r#"{"c": 7, "b": {"a": 8, "c": 9}, "f": [1, 2]}"#,
                r#"{"d": 9, "b": {"b": 10, "c": 11}, "e": 8}"#,
                r#"{
                    "a": 1,
                    "b": {"a": 8, "b": 10, "c": [3, 9, 11], "d": 4},
                    "c": 7,
                    "d": 9,
                    "e": [5, 6, 8],
                    "f": [1, 2]
                }"#,
            ],
        ];
        for case in test_cases {
            let (to_be_merged, expect) = case.split_at(case.len() - 1);
            let jsons = to_be_merged
                .iter()
                .map(|s| s.parse::<Json>().unwrap())
                .collect::<Vec<Json>>();
            let refs = jsons.iter().map(|j| j.as_ref()).collect::<Vec<_>>();
            let res = Json::merge(refs).unwrap();
            let expect: Json = expect[0].parse().unwrap();
            assert_eq!(res, expect);
        }
    }
}
