// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

use super::Json;

impl Json {
    // `merge` is the implementation for JSON_MERGE in mysql
    // https://dev.mysql.com/doc/refman/5.7/en/json-modification-functions.html#function_json-merge
    //
    // The merge rules are listed as following:
    // 1. adjacent arrays are merged to a single array;
    // 2. adjacent object are merged to a single object;
    // 3. a scalar value is autowrapped as an array before merge;
    // 4. an adjacent array and object are merged by autowrapping the object as an array.
    pub fn merge(self, to_merge: Json) -> Json {
        match (self, to_merge) {
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
                        value.merge(sub_value)
                    } else {
                        sub_value
                    };
                    obj.insert(sub_key, v);
                }
                Json::Object(obj)
            }
            // rule 4
            (obj, Json::Array(mut sub_array)) => {
                let mut array = Vec::with_capacity(sub_array.len() + 1);
                array.push(obj);
                array.append(&mut sub_array);
                Json::Array(array)
            }
            // rule 3, 4
            (obj, suffix) => Json::Array(vec![obj, suffix]),
        }
    }
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
            let base: Json = case[0].parse().unwrap();
            let res = case[1..case.len() - 1]
                .iter()
                .map(|s| s.parse().unwrap())
                .fold(base, Json::merge);
            let expect: Json = case[case.len() - 1].parse().unwrap();
            assert_eq!(res, expect);
        }
    }
}
