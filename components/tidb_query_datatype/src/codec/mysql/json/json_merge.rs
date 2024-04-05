// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

use std::collections::BTreeMap;

use super::{Json, JsonRef, JsonType};
use crate::codec::{Error, Result};

impl Json {
    /// `merge` is the implementation for JSON_MERGE in mysql
    /// <https://dev.mysql.com/doc/refman/5.7/en/json-modification-functions.html#function_json-merge>
    ///
    /// The merge rules are listed as following:
    /// 1. adjacent arrays are merged to a single array;
    /// 2. adjacent object are merged to a single object;
    /// 3. a scalar value is autowrapped as an array before merge;
    /// 4. an adjacent array and object are merged by autowrapping the object as
    /// an array.
    ///
    /// See `MergeBinary()` in TiDB `json/binary_function.go`
    #[allow(clippy::comparison_chain)]
    pub fn merge(bjs: Vec<JsonRef<'_>>) -> Result<Json> {
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

    /// `merge_patch` is the implementation for JSON_MERGE_PATCH in mysql
    /// <https://dev.mysql.com/doc/refman/8.3/en/json-modification-functions.html#function_json-merge-patch>
    ///
    /// The merge_patch rules are listed as following:
    /// 1. If the first argument is not an object, the result of the merge is
    ///    the same as if an empty object had been merged with the second
    ///    argument.
    /// 2. If the second argument is not an object, the result of the merge is
    ///    the second argument.
    /// 3. If both arguments are objects, the result of the merge is an object
    ///    with the following members: 3.1. All members of the first object
    ///    which do not have a corresponding member with the same key in the
    ///    second object. 3.2. All members of the second object which do not
    ///    have a corresponding key in the first object, and whose value is not
    ///    the JSON null literal. 3.3. All members with a key that exists in
    ///    both the first and the second object, and whose value in the second
    ///    object is not the JSON null literal. The values of these members are
    ///    the results of recursively merging the value in the first object with
    ///    the value in the second object.
    /// See `MergePatchBinaryJSON()` in TiDB
    /// `pkg/types/json_binary_functions.go`
    #[allow(clippy::comparison_chain)]
    pub fn merge_patch(bjs: Vec<JsonRef<'_>>) -> Result<Json> {
        let mut objects: Vec<JsonRef<'_>> = vec![];
        let mut index = 0;

        // according to the implements of RFC7396
        // when the last item is not object
        // we can return the last item directly
        for (i, element) in bjs.iter().rev().enumerate() {
            // check element type or if it is null
            if element.get_type() != JsonType::Object {
                index = i;
                break;
            }
        }

        for i in (0..=bjs.len() - 1).rev() {
            if bjs[i].get_type() != JsonType::Object {
                index = i;
                break;
            }
        }

        // assign sub array from index to bjs.len() from bjs to objects
        objects.extend(&bjs[index..]);
        let mut target = objects[0].to_owned();

        // if the lenth of objects is 1, return the target directly
        if objects.len() > 1 {
            for i in 1..objects.len() {
                target = merge_patch_binary_object(target.as_ref(), objects[i])?;
            }
        }
        Ok(target)
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
fn merge_binary_array(elems: &[MergeUnit<'_>]) -> Result<Json> {
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
fn merge_binary_object(objects: &mut Vec<JsonRef<'_>>) -> Result<Json> {
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

// See `mergePatchBinaryJSON()` in TiDB `pkg/types/json_binary_functions.go`
fn merge_patch_binary_object(target: JsonRef<'_>, patch: JsonRef<'_>) -> Result<Json> {
    // translate the function from go to rust
    if patch.get_type() != JsonType::Object {
        return Ok(patch.to_owned());
    }

    if target.get_type() != JsonType::Object {
        return Ok(patch.to_owned());
    }

    let mut key_val_map: BTreeMap<String, Json> = BTreeMap::new();
    let elem_count = target.get_elem_count();
    for i in 0..elem_count {
        let key = target.object_get_key(i);
        let val = target.object_get_val(i)?;
        let key = String::from_utf8(key.to_owned()).map_err(Error::from)?;
        key_val_map.insert(key, val.to_owned());
    }

    let mut tmp: Json;
    let elem_count = patch.get_elem_count();
    for i in 0..elem_count {
        let key = patch.object_get_key(i);
        let val = patch.object_get_val(i)?;
        let k = String::from_utf8(key.to_owned()).map_err(Error::from)?;

        if val.get_type() == JsonType::Literal && val.get_literal().is_none() {
            if key_val_map.contains_key(&k) {
                key_val_map.remove(&k);
            }
        } else {
            let target_kv = key_val_map.get(&k);
            if let Some(target_kv) = target_kv {
                tmp = merge_patch_binary_object(target_kv.as_ref(), val)?;
                key_val_map.insert(k, tmp);
            } else {
                key_val_map.insert(k, val.to_owned());
            }
        }
    }

    Json::from_object(key_val_map)
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

    #[test]
    fn test_merge_patch() {
        let test_cases = vec![
            vec![r#"[1, 2]"#, r#"[true, false]"#, r#"[true, false]"#],
            vec![
                r#"{"name": "x"}"#,
                r#"{"id": 47}"#,
                r#"{"id": 47, "name": "x"}"#,
            ],
            vec![r#"1"#, r#"true"#, r#"true"#],
            vec![r#"1"#, r#"null"#, r#"null"#],
            vec![r#"{"a": 1}"#, r#"{"b": 2}"#, r#"null"#, r#"null"#],
            vec![r#"[1, 2]"#, r#"{"id": 47}"#, r#"{"id": 47}"#],
            vec![r#"{"a":1, "b":2}"#, r#"{"b":null}"#, r#"{"a": 1}"#],
            vec![r#"{"a": 1}"#, r#"{"b": 2}"#, r#"{"a": 1, "b": 2}"#],
            vec![r#"{"a": 1}"#, r#"{"a": 2}"#, r#"{"a": 2}"#],
            vec![r#"{"a": 1}"#, r#"{"a": [2, 3]}"#, r#"{"a": [2, 3]}"#],
            vec![
                r#"{"a": 1}"#,
                r#"{"a": {"b": [2, 3]}}"#,
                r#"{"a": {"b": [2, 3]}}"#,
            ],
            vec![r#"{"a": {"b": [2, 3]}}"#, r#"{"a": 1}"#, r#"{"a": 1}"#],
            vec![
                r#"{"a": [1, 2]}"#,
                r#"{"a": {"b": [3, 4]}}"#,
                r#"{"a": {"b": [3, 4]}}"#,
            ],
            vec![
                r#"{"b": {"c": 2}}"#,
                r#"{"a": 1, "b": {"d": 1}}"#,
                r#"{"a": 1, "b": {"c": 2, "d": 1}}"#,
            ],
            vec![r#"[1]"#, r#"[2]"#, r#"[2]"#],
            vec![r#"{"a": 1}"#, r#"[1]"#, r#"[1]"#],
            vec![r#"[1]"#, r#"{"a": 1}"#, r#"{"a": 1}"#],
            vec![r#"{"a": 1}"#, r#"4"#, r#"4"#],
            vec![r#"[1]"#, r#"4"#, r#"4"#],
            vec![r#"4"#, r#"{"a": 1}"#, r#"{"a": 1}"#],
            vec![r#"1"#, r#"[4]"#, r#"[4]"#],
            vec![r#"4"#, r#"1"#, r#"1"#],
            vec!["1", "2", "3", "3"],
            vec!["[1, 2]", "3", "[4, 5, 6]", "[4, 5, 6]"],
            vec![
                r#"{"a": 1, "b": {"c": 3, "d": 4}, "e": [5, 6]}"#,
                r#"{"c": 7, "b": {"a": 8, "c": 9}, "f": [1, 2]}"#,
                r#"{"d": 9, "b": {"b": 10, "c": 11}, "e": 8}"#,
                r#"{
                    "a": 1,
                    "b": {"a": 8, "b": 10, "c": 11, "d": 4},
                    "c": 7,
                    "d": 9,
                    "e": 8,
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
            let res = Json::merge_patch(refs).unwrap();
            let expect: Json = expect[0].parse().unwrap();
            assert_eq!(res, expect);
        }
    }
}
