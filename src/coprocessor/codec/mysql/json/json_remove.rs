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

use super::super::Result;
use super::path_expr::{PathExpression, PathLeg};
use super::Json;

impl Json {
    // Remove elements from Json,
    // All path expressions cannot contain * or ** wildcard.
    // If any error occurs, the input won't be changed.
    pub fn remove(&mut self, path_expr_list: &[PathExpression]) -> Result<()> {
        if path_expr_list
            .iter()
            .any(|expr| expr.legs.is_empty() || expr.contains_any_asterisk())
        {
            return Err(box_err!("Invalid path expression"));
        }

        for expr in path_expr_list {
            self.remove_path(&expr.legs);
        }
        Ok(())
    }

    fn remove_path(&mut self, path_legs: &[PathLeg]) {
        let (current_leg, sub_path_legs) = path_legs.split_first().unwrap();

        if let PathLeg::Index(index) = *current_leg {
            if let Json::Array(ref mut array) = *self {
                let index = index as usize;
                if array.len() <= index {
                    return;
                }
                if sub_path_legs.is_empty() {
                    array.remove(index);
                    return;
                }
                array[index].remove_path(sub_path_legs);
                return;
            }
        }

        if let PathLeg::Key(ref key) = *current_leg {
            if let Json::Object(ref mut obj) = *self {
                if sub_path_legs.is_empty() {
                    obj.remove(key);
                    return;
                }
                if let Some(sub_obj) = obj.get_mut(key) {
                    sub_obj.remove_path(sub_path_legs);
                }
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::super::path_expr::parse_json_path_expr;
    use super::*;

    #[test]
    fn test_json_remove() {
        let test_cases = vec![
            (r#"{"a": [3, 4]}"#, "$.a[0]", r#"{"a": [4]}"#, true),
            (r#"{"a": [3, 4]}"#, "$.a", r#"{}"#, true),
            (
                r#"{"a": [3, 4], "b":1, "c":{"a":1}}"#,
                "$.c.a",
                r#"{"a": [3, 4],"b":1, "c":{}}"#,
                true,
            ),
            // Nothing changed because the path without last leg doesn't exist.
            (r#"{"a": [3, 4]}"#, "$.b[1]", r#"{"a": [3, 4]}"#, true),
            // Nothing changed because the path without last leg doesn't exist.
            (r#"{"a": [3, 4]}"#, "$.a[0].b", r#"{"a": [3, 4]}"#, true),
            // Bad path expression.
            (r#"null"#, "$.*", r#"null"#, false),
            (r#"null"#, "$[*]", r#"null"#, false),
            (r#"null"#, "$**.a", r#"null"#, false),
            (r#"null"#, "$**[3]", r#"null"#, false),
        ];

        for (i, (json, path, expected, success)) in test_cases.into_iter().enumerate() {
            let j: Result<Json> = json.parse();
            assert!(j.is_ok(), "#{} expect json parse ok but got {:?}", i, j);
            let p = parse_json_path_expr(path);
            assert!(p.is_ok(), "#{} expect path parse ok but got {:?}", i, p);
            let e: Result<Json> = expected.parse();
            assert!(
                e.is_ok(),
                "#{} expect expected value parse ok but got {:?}",
                i,
                e
            );
            let (mut j, p, e) = (j.unwrap(), p.unwrap(), e.unwrap());
            let r = j.remove(vec![p].as_slice());
            if success {
                assert!(r.is_ok(), "#{} expect remove ok but got {:?}", i, r);
            } else {
                assert!(r.is_err(), "#{} expect remove error but got {:?}", i, r);
            }
            assert_eq!(e, j, "#{} expect remove json {:?} == {:?}", i, j, e);
        }
    }
}
