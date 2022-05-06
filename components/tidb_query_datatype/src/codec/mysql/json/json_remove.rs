// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

use super::{super::Result, modifier::BinaryModifier, path_expr::PathExpression, Json, JsonRef};

impl<'a> JsonRef<'a> {
    /// Removes elements from Json,
    /// All path expressions cannot contain * or ** wildcard.
    /// If any error occurs, the input won't be changed.
    pub fn remove(&self, path_expr_list: &[PathExpression]) -> Result<Json> {
        if path_expr_list
            .iter()
            .any(|expr| expr.legs.is_empty() || expr.contains_any_asterisk())
        {
            return Err(box_err!("Invalid path expression"));
        }

        let mut res = self.to_owned();
        for expr in path_expr_list {
            let modifier = BinaryModifier::new(res.as_ref());
            res = modifier.remove(&expr.legs)?;
        }
        Ok(res)
    }
}

#[cfg(test)]
mod tests {
    use super::{super::path_expr::parse_json_path_expr, *};

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
            let (j, p, e) = (j.unwrap(), p.unwrap(), e.unwrap());
            let r = j.as_ref().remove(vec![p].as_slice());
            if success {
                assert!(r.is_ok(), "#{} expect remove ok but got {:?}", i, r);
                let j = r.unwrap();
                assert_eq!(e, j, "#{} expect remove json {:?} == {:?}", i, j, e);
            } else {
                assert!(r.is_err(), "#{} expect remove error but got {:?}", i, r);
            }
        }
    }
}
