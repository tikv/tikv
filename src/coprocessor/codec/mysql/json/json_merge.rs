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

impl Json {
    // merge is the implementation for JSON_MERGE in mysql
    // https://dev.mysql.com/doc/refman/5.7/en/json-modification-functions.html#function_json-merge
    // It merges suffixes into self according the following rules:
    // 1. adjacent arrays are merged to a single array;
    // 2. adjacent object are merged to a single object;
    // 3. a scalar value is autowrapped as an array before merge;
    // 4. an adjacent array and object are merged by autowrapping the object as an array.
    pub fn merge(self, suffixes: Vec<Json>) -> Json {
        let base_data: Json = match self {
            Json::Object(obj) => Json::Object(obj),
            Json::Array(array) => Json::Array(array),
            json => Json::Array(vec![json]),
        };

        suffixes.into_iter().fold(base_data, |data, suffix| {
            match (data, suffix) {
                // rule 1
                (Json::Array(mut array), Json::Array(mut sub_array)) => {
                    array.append(&mut sub_array);
                    Json::Array(array)
                }
                // rule 3, 4
                (Json::Array(mut array), suffix) => {
                    array.push(suffix);
                    Json::Array(array)
                }
                // rule 2
                (Json::Object(mut obj), Json::Object(sub_obj)) => {
                    for (sub_key, sub_value) in sub_obj {
                        let v = if let Some(value) = obj.remove(&sub_key) {
                            value.merge(vec![sub_value])
                        } else {
                            sub_value
                        };
                        obj.insert(sub_key, v);
                    }
                    Json::Object(obj)
                }
                // rule 4
                (obj, Json::Array(mut sub_array)) => {
                    let mut array = vec![obj];
                    array.append(&mut sub_array);
                    Json::Array(array)
                }
                // rule 3, 4
                (obj, suffix) => Json::Array(vec![obj, suffix]),
            }
        })
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_merge() {
        let test_cases = vec![
            (r#"{"a": 1}"#, r#"{"b": 2}"#, r#"{"a": 1, "b": 2}"#),
            (r#"{"a": 1}"#, r#"{"a": 2}"#, r#"{"a": [1, 2]}"#),
            (r#"[1]"#, r#"[2]"#, r#"[1, 2]"#),
            (r#"{"a": 1}"#, r#"[1]"#, r#"[{"a": 1}, 1]"#),
            (r#"[1]"#, r#"{"a": 1}"#, r#"[1, {"a": 1}]"#),
            (r#"{"a": 1}"#, r#"4"#, r#"[{"a": 1}, 4]"#),
            (r#"[1]"#, r#"4"#, r#"[1, 4]"#),
            (r#"4"#, r#"{"a": 1}"#, r#"[4, {"a": 1}]"#),
            (r#"4"#, r#"1"#, r#"[4, 1]"#),
        ];
        for (left, right, expect_str) in test_cases {
            let left_json: Json = left.parse().unwrap();
            let right_json: Json = right.parse().unwrap();
            let get = left_json.merge(vec![right_json]);
            let expect: Json = expect_str.parse().unwrap();
            assert_eq!(get, expect);
        }
    }
}
