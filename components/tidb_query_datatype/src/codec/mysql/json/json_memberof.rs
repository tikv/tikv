// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use std::cmp::Ordering;

use super::{super::Result, JsonRef, JsonType};

impl<'a> JsonRef<'a> {
    /// `member_of` is the implementation for MEMBER OF in mysql
    /// <https://dev.mysql.com/doc/refman/8.0/en/json-search-functions.html#operator_member-of>
    /// See `builtinJSONMemberOfSig` in TiDB `expression/builtin_json.go`
    pub fn member_of(&self, json_array: JsonRef<'_>) -> Result<bool> {
        match json_array.type_code {
            JsonType::Array => {
                let elem_count = json_array.get_elem_count();
                for i in 0..elem_count {
                    if json_array.array_get_elem(i)?.partial_cmp(self).unwrap() == Ordering::Equal {
                        return Ok(true);
                    };
                }
            }
            _ => {
                // If `json_array` is not a JSON_ARRAY, compare the two JSON directly.
                return match json_array.partial_cmp(self).unwrap() {
                    Ordering::Equal => Ok(true),
                    _ => Ok(false),
                };
            }
        };
        Ok(false)
    }
}

#[cfg(test)]
mod tests {
    use super::super::Json;
    #[test]
    fn test_json_member_of() {
        let mut test_cases = vec![
            (r#"1"#, r#"[1,2]"#, true),
            (r#"1"#, r#"[1]"#, true),
            (r#"1"#, r#"[0]"#, false),
            (r#"1"#, r#"[[1]]"#, false),
            (r#""1""#, r#"[1]"#, false),
            (r#""1""#, r#"["1"]"#, true),
            (r#""{\"a\":1}""#, r#"{"a":1}"#, false),
            (r#""{\"a\":1}""#, r#"[{"a":1}]"#, false),
            (r#""{\"a\":1}""#, r#"[{"a":1}, 1]"#, false),
            (r#""{\"a\":1}""#, r#"["{\"a\":1}"]"#, true),
            (r#""{\"a\":1}""#, r#"["{\"a\":1}",1]"#, true),
            (r#"1"#, r#"1"#, true),
            (r#"[4,5]"#, r#"[[3,4],[4,5]]"#, true),
            (r#""[4,5]""#, r#"[[3,4],"[4,5]"]"#, true),
            (r#"{"a":1}"#, r#"{"a":1}"#, true),
            (r#"{"a":1}"#, r#"{"a":1, "b":2}"#, false),
            (r#"{"a":1}"#, r#"[{"a":1}]"#, true),
            (r#"{"a":1}"#, r#"{"b": {"a":1}}"#, false),
            (r#"1"#, r#"1"#, true),
            (r#"[1,2]"#, r#"[1,2]"#, false),
            (r#"[1,2]"#, r#"[[1,2]]"#, true),
            (r#"[[1,2]]"#, r#"[[1,2]]"#, false),
            (r#"[[1,2]]"#, r#"[[[1,2]]]"#, true),
        ];
        for (i, (js, value, expected)) in test_cases.drain(..).enumerate() {
            let j = js.parse();
            assert!(j.is_ok(), "#{} expect parse ok but got {:?}", i, j);
            let j: Json = j.unwrap();
            let value = value.parse();
            assert!(value.is_ok(), "#{} expect parse ok but got {:?}", i, j);
            let value: Json = value.unwrap();
            let got = j.as_ref().member_of(value.as_ref()).unwrap();
            assert_eq!(
                got, expected,
                "#{} expect {:?}, but got {:?}",
                i, expected, got
            );
        }
    }
}
