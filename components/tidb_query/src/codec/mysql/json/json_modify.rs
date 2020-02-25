// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

use super::super::Result;
use super::modifier::BinaryModifier;
use super::path_expr::PathExpression;
use super::{Json, JsonRef};

/// `ModifyType` is for modify a JSON.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum ModifyType {
    /// `Insert` is for inserting a new element into a JSON.
    Insert,
    /// `Replace` is for replacing a old element from a JSON.
    Replace,
    /// `Set` = `Insert` | `Replace`
    Set,
}

impl<'a> JsonRef<'a> {
    /// Modifies a Json object by insert, replace or set.
    /// All path expressions cannot contain * or ** wildcard.
    /// If any error occurs, the input won't be changed.
    ///
    /// See `Modify()` in TiDB `json/binary_function.go`
    pub fn modify(
        &self,
        path_expr_list: &[PathExpression],
        values: Vec<Json>,
        mt: ModifyType,
    ) -> Result<Json> {
        if path_expr_list.len() != values.len() {
            return Err(box_err!(
                "Incorrect number of parameters: expected: {:?}, found {:?}",
                values.len(),
                path_expr_list.len()
            ));
        }
        for expr in path_expr_list {
            if expr.contains_any_asterisk() {
                return Err(box_err!(
                    "Invalid path expression: expected no asterisk, found {:?}",
                    expr
                ));
            }
        }
        let mut res = self.to_owned();
        for (expr, value) in path_expr_list.iter().zip(values.into_iter()) {
            let modifier = BinaryModifier::new(res.as_ref());
            res = match mt {
                ModifyType::Insert => modifier.insert(&expr, value)?,
                ModifyType::Replace => modifier.replace(&expr, value)?,
                ModifyType::Set => modifier.set(&expr, value)?,
            };
        }
        Ok(res)
    }
}

#[cfg(test)]
mod tests {
    use super::super::path_expr::parse_json_path_expr;
    use super::*;

    #[test]
    fn test_json_modify() {
        let mut test_cases = vec![
            (r#"null"#, "$", r#"{}"#, ModifyType::Set, r#"{}"#, true),
            (r#"{}"#, "$.a", r#"3"#, ModifyType::Set, r#"{"a": 3}"#, true),
            (
                r#"{"a": 3}"#,
                "$.a",
                r#"[]"#,
                ModifyType::Replace,
                r#"{"a": []}"#,
                true,
            ),
            (
                r#"{"a": []}"#,
                "$.a[0]",
                r#"3"#,
                ModifyType::Set,
                r#"{"a": [3]}"#,
                true,
            ),
            (
                r#"{"a": [3]}"#,
                "$.a[1]",
                r#"4"#,
                ModifyType::Insert,
                r#"{"a": [3, 4]}"#,
                true,
            ),
            (
                r#"{"a": [3]}"#,
                "$[0]",
                r#"4"#,
                ModifyType::Set,
                r#"4"#,
                true,
            ),
            (
                r#"{"a": [3]}"#,
                "$[1]",
                r#"4"#,
                ModifyType::Set,
                r#"[{"a": [3]}, 4]"#,
                true,
            ),
            // Nothing changed because the path is empty and we want to insert.
            (r#"{}"#, "$", r#"1"#, ModifyType::Insert, r#"{}"#, true),
            // Nothing changed because the path without last leg doesn't exist.
            (
                r#"{"a": [3, 4]}"#,
                "$.b[1]",
                r#"3"#,
                ModifyType::Set,
                r#"{"a": [3, 4]}"#,
                true,
            ),
            // Nothing changed because the path without last leg doesn't exist.
            (
                r#"{"a": [3, 4]}"#,
                "$.a[2].b",
                r#"3"#,
                ModifyType::Set,
                r#"{"a": [3, 4]}"#,
                true,
            ),
            // Nothing changed because we want to insert but the full path exists.
            (
                r#"{"a": [3, 4]}"#,
                "$.a[0]",
                r#"30"#,
                ModifyType::Insert,
                r#"{"a": [3, 4]}"#,
                true,
            ),
            // Nothing changed because we want to replace but the full path doesn't exist.
            (
                r#"{"a": [3, 4]}"#,
                "$.a[2]",
                r#"30"#,
                ModifyType::Replace,
                r#"{"a": [3, 4]}"#,
                true,
            ),
            // Bad path expression.
            (r#"null"#, "$.*", r#"{}"#, ModifyType::Set, r#"null"#, false),
            (
                r#"null"#,
                "$[*]",
                r#"{}"#,
                ModifyType::Set,
                r#"null"#,
                false,
            ),
            (
                r#"null"#,
                "$**.a",
                r#"{}"#,
                ModifyType::Set,
                r#"null"#,
                false,
            ),
            (
                r#"null"#,
                "$**[3]",
                r#"{}"#,
                ModifyType::Set,
                r#"null"#,
                false,
            ),
        ];
        for (i, (json, path, value, mt, expected, success)) in test_cases.drain(..).enumerate() {
            let j: Result<Json> = json.parse();
            assert!(j.is_ok(), "#{} expect json parse ok but got {:?}", i, j);
            let p = parse_json_path_expr(path);
            assert!(p.is_ok(), "#{} expect path parse ok but got {:?}", i, p);
            let v = value.parse();
            assert!(v.is_ok(), "#{} expect value parse ok but got {:?}", i, v);
            let e: Result<Json> = expected.parse();
            assert!(
                e.is_ok(),
                "#{} expect expected value parse ok but got {:?}",
                i,
                e
            );
            let (j, p, v, e) = (j.unwrap(), p.unwrap(), v.unwrap(), e.unwrap());
            let r = j.as_ref().modify(vec![p].as_slice(), vec![v], mt);
            if success {
                assert!(r.is_ok(), "#{} expect modify ok but got {:?}", i, r);
                let j = r.unwrap();
                assert_eq!(
                    e,
                    j,
                    "#{} expect modified json {:?} == {:?}",
                    i,
                    j.to_string(),
                    e.to_string()
                );
            } else {
                assert!(r.is_err(), "#{} expect modify error but got {:?}", i, r);
            }
        }
    }
}
