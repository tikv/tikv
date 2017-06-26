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
        if let ModifyType::Replace = mt {
            return value;
        }
        return j;
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
                if let ModifyType::Insert = mt {
                    // e.g. json_insert('[1, 2, 3]', '$[3]', "x") => '[1, 2, 3, "x"]'
                    array.push(value)
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
                    if let ModifyType::Insert = mt {
                        // e.g. json_insert('{"a": 1}', '$.b', 2) => '{"a": 1, "b": 2}'
                        map.insert(key.clone(), value);
                    }
                }
                return Json::Object(map);
            }
        }
        _ => {}
    }
    j
}
