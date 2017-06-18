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

// FIXME: remove following later
#![allow(dead_code)]

use std::collections::BTreeMap;
use super::super::Result;
use super::json::Json;
use super::path_expr::{PathLeg, PathExpression, PATH_EXPR_ASTERISK, PATH_EXPR_ARRAY_INDEX_ASTERISK};

impl Json {
    // extract receives several path expressions as arguments, matches them in j, and returns
    // the target JSON matched any path expressions, which may be autowrapped as an array.
    // If there is no any expression matched, it returns None.
    pub fn extract(&self, path_expr_list: &[PathExpression]) -> Option<Json> {
        let mut elem_list = Vec::with_capacity(path_expr_list.len());
        for path_expr in path_expr_list {
            elem_list.append(&mut extract_json(self.clone(), path_expr))
        }
        if elem_list.is_empty() {
            return None;
        }
        if path_expr_list.len() == 1 && elem_list.len() == 1 {
            // If path_expr contains asterisks, elem_list.length won't be 1
            // even if path_expr_list.len() equals to 1.
            return Some(elem_list.remove(0));
        }
        Some(Json::Array(elem_list))
    }
}

// unquote_string recognizes the escape sequences shown in:
// https://dev.mysql.com/doc/refman/5.7/en/json-modification-functions.html#
// json-unquote-character-escape-sequences
pub fn unquote_string(s: &[u8]) -> Result<String> {
    let mut ret = String::with_capacity(s.len());
    let mut i = 0;
    while i < s.len() {
        if char::from(s[i]) == '\\' {
            if i + 1 == s.len() {
                return Err(box_err!("Missing a closing quotation mark in string"));
            }
            match char::from(s[i]) {
                '"' => ret.push('"'),
                'b' => ret.push('\x08'),
                'f' => ret.push('\x0C'),
                'n' => ret.push('\x0A'),
                'r' => ret.push('\x0D'),
                't' => ret.push('\x0B'),
                '\\' => ret.push('\\'),
                'u' => {
                    if i + 4 >= s.len() {
                        return Err(box_err!("Invalid unicode"));
                    }
                    let unicode = try!(String::from_utf8(s[i..i + 5].to_vec()));
                    ret += &unicode;
                    i += 4;
                }
                _ => ret.push(char::from(s[i])),
            }
        } else {
            ret.push(char::from(s[i]))
        }
        i += 1;
    }
    Ok(ret)
}

// extract_json is used by JSON::extract().
pub fn extract_json(j: Json, path_expr: &PathExpression) -> Vec<Json> {
    if path_expr.legs.is_empty() {
        return vec![j.clone()];
    }
    let (current_leg, sub_path_expr) = path_expr.pop_one_leg();
    let mut ret = vec![];
    match current_leg {
        PathLeg::Index(i) => {
            // If j is not an array, autowrap that into array.
            let array = match j {
                Json::Array(array) => array,
                _ => wrap_to_array(j),
            };
            if i == PATH_EXPR_ARRAY_INDEX_ASTERISK {
                for child in array {
                    ret.append(&mut extract_json(child, &sub_path_expr))
                }
            } else if (i as usize) < array.len() {
                ret.append(&mut extract_json(array[i as usize].clone(), &sub_path_expr))
            }
        }
        PathLeg::Key(key) => {
            if let Json::Object(map) = j {
                if key == PATH_EXPR_ASTERISK {
                    let sorted_keys = get_sorted_keys(&map);
                    for key in sorted_keys {
                        ret.append(&mut extract_json(map[&key].clone(), &sub_path_expr))
                    }
                } else if map.contains_key(&key) {
                    ret.append(&mut extract_json(map[&key].clone(), &sub_path_expr))
                }
            }
        }
        PathLeg::DoubleAsterisk => {
            match j {
                Json::Array(array) => {
                    for child in array {
                        ret.append(&mut extract_json(child.clone(), path_expr))
                    }
                }
                Json::Object(map) => {
                    let sorted_keys = get_sorted_keys(&map);
                    for key in sorted_keys {
                        ret.append(&mut extract_json(map[&key].clone(), path_expr))
                    }
                }
                _ => {}
            }
        }
    }
    ret
}

fn wrap_to_array(j: Json) -> Vec<Json> {
    let mut array = Vec::with_capacity(1);
    array.push(j.clone());
    array
}

// get_sorted_keys returns sorted keys of a map.
fn get_sorted_keys(m: &BTreeMap<String, Json>) -> Vec<String> {
    let mut keys = Vec::with_capacity(m.len());
    for k in m.keys() {
        keys.push(k.clone());
    }
    keys.sort();
    keys
}
