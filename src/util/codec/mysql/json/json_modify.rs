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
use super::super::Result;
use super::path_expr::{PathLeg, PathExpression, contains_any_asterisk};

/// `ModifyType` is for modify a JSON.
#[derive(Clone, Debug)]
pub enum ModifyType {
    /// `Insert` is for inserting a new element into a JSON.
    Insert,
    /// `Replace` is for replacing a old element from a JSON.
    Replace,
    /// `Set` = `Insert` | `Replace`
    Set,
}

impl Json {
    // Modifies a Json object by insert, replace or set.
    // All path expressions cannot contain * or ** wildcard.
    // If any error occurs, the input won't be changed.
    pub fn modify(self,
                  path_expr_list: &[PathExpression],
                  mut values: Vec<Json>,
                  mt: ModifyType)
                  -> (Json, Result<()>) {
        if path_expr_list.len() != values.len() {
            return (self, Err(box_err!("Incorrect parameter count")));
        }
        for expr in path_expr_list {
            if contains_any_asterisk(expr.flags) {
                return (self, Err(box_err!("Invalid path expression")));
            }
        }
        let mut j = self;
        for (expr, value) in path_expr_list.iter().zip(values.drain(..)) {
            j = set_json(j, &expr.legs, value, mt.clone());
        }
        (j, Ok(()))
    }
}

// `set_json` is used in Json::modify().
fn set_json(j: Json, path_legs: &[PathLeg], value: Json, mt: ModifyType) -> Json {
    if path_legs.is_empty() {
        match mt {
            ModifyType::Replace | ModifyType::Set => return value,
            _ => return j,
        }
    }
    let (current_leg, sub_path_legs) = (&path_legs[0], &path_legs[1..]);
    match *current_leg {
        PathLeg::Index(i) => {
            let index = i as usize;
            // If `j` is not an array, we should autowrap it to be an array.
            // Then if the length of result array equals to 1, it's unwraped.
            let (mut array, wrapped) = match j {
                Json::Array(array) => (array, false),
                _ => (vec![j], true),
            };
            if array.len() > index {
                // e.g. json_replace('[1, 2, 3]', '$[0]', "x") => '["x", 2, 3]'
                if index == (array.len() - 1) {
                    let chosen = array.pop().unwrap();
                    array.push(set_json(chosen, sub_path_legs, value, mt));
                } else {
                    let mut right = array.split_off(index + 1);
                    let chosen = array.pop().unwrap();
                    array.push(set_json(chosen, sub_path_legs, value, mt));
                    array.append(&mut right);
                }
            } else if sub_path_legs.is_empty() {
                match mt {
                    ModifyType::Insert | ModifyType::Set => {
                        // e.g. json_insert('[1, 2, 3]', '$[3]', "x") => '[1, 2, 3, "x"]'
                        array.push(value)
                    }
                    _ => {}
                }
            }
            if (array.len() == 1) && wrapped {
                return array.pop().unwrap();
            }
            return Json::Array(array);
        }
        PathLeg::Key(ref key) => {
            if let Json::Object(mut map) = j {
                let r = map.remove(key);
                if let Some(v) = r {
                    // e.g. json_replace('{"a": 1}', '$.a', 2) => '{"a": 2}'
                    map.insert(key.clone(), set_json(v, sub_path_legs, value, mt));
                } else if sub_path_legs.is_empty() {
                    match mt {
                        ModifyType::Insert | ModifyType::Set => {
                            // e.g. json_insert('{"a": 1}', '$.b', 2) => '{"a": 1, "b": 2}'
                            map.insert(key.clone(), value);
                        }
                        _ => {}
                    }
                }
                return Json::Object(map);
            }
        }
        _ => {}
    }
    j
}

#[cfg(test)]
mod test {
    use super::*;
    use super::super::path_expr::parse_json_path_expr;

    #[test]
    fn test_json_modify() {
        let mut test_cases = vec![
            (r#"null"#, "$", r#"{}"#, ModifyType::Set, r#"{}"#, true),
            (r#"{}"#, "$.a", r#"3"#, ModifyType::Set, r#"{"a": 3}"#, true),
            (r#"{"a": 3}"#, "$.a", r#"[]"#, ModifyType::Replace, r#"{"a": []}"#, true),
            (r#"{"a": []}"#, "$.a[0]", r#"3"#, ModifyType::Set, r#"{"a": [3]}"#, true),
            (r#"{"a": [3]}"#, "$.a[1]", r#"4"#, ModifyType::Insert, r#"{"a": [3, 4]}"#, true),
            (r#"{"a": [3]}"#, "$[0]", r#"4"#, ModifyType::Set, r#"4"#, true),
            (r#"{"a": [3]}"#, "$[1]", r#"4"#, ModifyType::Set, r#"[{"a": [3]}, 4]"#, true),

            // Nothing changed because the path is empty and we want to insert.
            (r#"{}"#, "$", r#"1"#, ModifyType::Insert, r#"{}"#, true),
            // Nothing changed because the path without last leg doesn't exist.
            (r#"{"a": [3, 4]}"#, "$.b[1]", r#"3"#, ModifyType::Set, r#"{"a": [3, 4]}"#, true),
            // Nothing changed because the path without last leg doesn't exist.
            (r#"{"a": [3, 4]}"#, "$.a[2].b", r#"3"#, ModifyType::Set, r#"{"a": [3, 4]}"#, true),
            // Nothing changed because we want to insert but the full path exists.
            (r#"{"a": [3, 4]}"#, "$.a[0]", r#"30"#, ModifyType::Insert, r#"{"a": [3, 4]}"#, true),
            // Nothing changed because we want to replace but the full path doesn't exist.
            (r#"{"a": [3, 4]}"#, "$.a[2]", r#"30"#, ModifyType::Replace, r#"{"a": [3, 4]}"#, true),

            // Bad path expression.
            (r#"null"#, "$.*", r#"{}"#, ModifyType::Set, r#"null"#, false),
            (r#"null"#, "$[*]", r#"{}"#, ModifyType::Set, r#"null"#, false),
            (r#"null"#, "$**.a", r#"{}"#, ModifyType::Set, r#"null"#, false),
            (r#"null"#, "$**[3]", r#"{}"#, ModifyType::Set, r#"null"#, false),
        ];
        for (i, (json, path, value, mt, expected, success)) in test_cases.drain(..).enumerate() {
            let j: Result<Json> = json.parse();
            assert!(j.is_ok(), "#{} expect json parse ok but got {:?}", i, j);
            let p = parse_json_path_expr(path);
            assert!(p.is_ok(), "#{} expect path parse ok but got {:?}", i, p);
            let v = value.parse();
            assert!(v.is_ok(), "#{} expect value parse ok but got {:?}", i, v);
            let e: Result<Json> = expected.parse();
            assert!(e.is_ok(),
                    "#{} expect expected value parse ok but got {:?}",
                    i,
                    e);
            let (j, p, v, e) = (j.unwrap(), p.unwrap(), v.unwrap(), e.unwrap());
            let (m, r) = j.modify(vec![p].as_slice(), vec![v], mt);
            if success {
                assert!(r.is_ok(), "#{} expect modify ok but got {:?}", i, r);
            } else {
                assert!(r.is_err(), "#{} expect modify error but got {:?}", i, r);
            }
            assert_eq!(e, m, "#{} expect modified json {:?} == {:?}", i, m, e);
        }
    }
}
