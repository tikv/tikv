// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::cmp::Ordering;

use super::{super::Result, JsonRef, JsonType};

impl<'a> JsonRef<'a> {
    /// `json_contains` is the implementation for JSON_CONTAINS in mysql
    /// <https://dev.mysql.com/doc/refman/5.7/en/json-search-functions.html#function_json-contains>
    /// See `ContainsBinaryJSON()` in TiDB `types/json_binary_functions.go`
    pub fn json_contains(&self, target: JsonRef<'_>) -> Result<bool> {
        match self.type_code {
            JsonType::Object => {
                if target.type_code == JsonType::Object {
                    let elem_count = target.get_elem_count();
                    for i in 0..elem_count {
                        let key = target.object_get_key(i);
                        let val = target.object_get_val(i)?;
                        let idx = self.object_search_key(key);
                        match idx {
                            None => {
                                return Ok(false);
                            }
                            Some(idx) => {
                                let exp = self.object_get_val(idx)?;
                                if !(exp.json_contains(val)?) {
                                    return Ok(false);
                                }
                            }
                        }
                    }
                    return Ok(true);
                }
            }
            JsonType::Array => {
                if target.type_code == JsonType::Array {
                    let elem_count = target.get_elem_count();
                    for i in 0..elem_count {
                        if !(self.json_contains(target.array_get_elem(i)?)?) {
                            return Ok(false);
                        }
                    }
                    return Ok(true);
                }
                let elem_count = self.get_elem_count();
                for i in 0..elem_count {
                    if self.array_get_elem(i)?.json_contains(target)? {
                        return Ok(true);
                    }
                }
            }
            _ => {
                return match self.partial_cmp(&target).unwrap() {
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
    fn test_json_contains() {
        let mut test_cases = vec![
            (r#"{"a":{"a":1},"b":2}"#, r#"{"b":2}"#, true),
            (r#"{}"#, r#"{}"#, true),
            (r#"{"a":1}"#, r#"{}"#, true),
            (r#"{"a":1}"#, r#"1"#, false),
            (r#"{"a":[1]}"#, r#"[1]"#, false),
            (r#"{"b":2, "c":3}"#, r#"{"c":3}"#, true),
            (r#"1"#, r#"1"#, true),
            (r#"[1]"#, r#"1"#, true),
            (r#"[1,2]"#, r#"[1]"#, true),
            (r#"[1,2]"#, r#"[1,3]"#, false),
            (r#"[1,2]"#, r#"["1"]"#, false),
            (r#"[1,2,[1,3]]"#, r#"[1,3]"#, true),
            (r#"[1,2,[1,[5,[3]]]]"#, r#"[1,3]"#, true),
            (r#"[1,2,[1,[5,{"a":[2,3]}]]]"#, r#"[1,{"a":[3]}]"#, true),
            (r#"[{"a":1}]"#, r#"{"a":1}"#, true),
            (r#"[{"a":1,"b":2}]"#, r#"{"a":1}"#, true),
            (r#"[{"a":{"a":1},"b":2}]"#, r#"{"a":1}"#, false),
            (r#"{"a":{"a":1},"b":2}"#, r#"{"b":3}"#, false),
            (r#"[1,2,[1,[5,{"a":[2,3]}]]]"#, r#"[1,{"a":[3]}]"#, true),
            (r#"[1,2,[1,[5,{"a":[2,3]}]]]"#, r#"[10,{"a":[3]}]"#, false),
        ];
        for (i, (js, value, expected)) in test_cases.drain(..).enumerate() {
            let j = js.parse();
            assert!(j.is_ok(), "#{} expect parse ok but got {:?}", i, j);
            let j: Json = j.unwrap();
            let value = value.parse();
            assert!(value.is_ok(), "#{} expect parse ok but got {:?}", i, j);
            let value: Json = value.unwrap();

            let got = j.as_ref().json_contains(value.as_ref()).unwrap();
            assert_eq!(
                got, expected,
                "#{} expect {:?}, but got {:?}",
                i, expected, got
            );
        }
    }
}
