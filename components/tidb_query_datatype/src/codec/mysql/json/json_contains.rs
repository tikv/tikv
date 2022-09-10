// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.


use std::cmp::Ordering;
use super::{super::Result, JsonRef, JsonType};

impl<'a> JsonRef<'a> {

    /// `json_contains` is the implementation for JSON_CONTAINS in mysql
    /// <https://dev.mysql.com/doc/refman/5.7/en/json-search-functions.html#function_json-contains>
    /// See `ContainsBinary()` in TiDB `json/binary_function.go`
    pub fn json_contains(&self, target: JsonRef) -> Result<Option<bool>> {
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
                                return Ok(None);
                            }
                            Some(idx) => {
                                let exp = self.object_get_val(idx)?;
                                if exp.json_contains(val)? == None {
                                    return Ok(None);
                                }
                            }
                        }
                    }
                    return Ok(Some(true));
                }
                return Ok(None);
            }
            JsonType::Array => {
                if target.type_code == JsonType::Array {
                    let elem_count = target.get_elem_count();
                    for i in 0..elem_count {
                        if self.json_contains(target.array_get_elem(i)?)? == None {
                            return Ok(None)
                        }
                    }
                    return Ok(Some(true));
                }
                let elem_count =self.get_elem_count();
                for i in 0..elem_count {
                    if let Some(_) = self.array_get_elem(i)?.json_contains(target)? {
                        return Ok(Some(true));
                    }
                }
                return Ok(None)
            }
            _ => {
                return match self.partial_cmp(&target).unwrap() {
                    Ordering::Equal => {
                        Ok(Some(true))
                    },
                    _ => {
                        Ok(None)
                    },
                }
            }
        };
    }
}

#[cfg(test)]
mod tests {
    use super::super::Json;
    #[test]
    fn test_json_contains() {
        let mut test_cases = vec![
            (r#"{"a":{"a":1},"b":2}"#, r#"{"b":2}"#, Some(true)),
            (r#"{"a":{"a":1},"b":2}"#, r#"{"b":3}"#, None),
            (r#"[1,2,[1,[5,{"a":[2,3]}]]]"#, r#"[1,{"a":[3]}]"#, Some(true)),
            (r#"[1,2,[1,[5,{"a":[2,3]}]]]"#, r#"[10,{"a":[3]}]"#, None),
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
