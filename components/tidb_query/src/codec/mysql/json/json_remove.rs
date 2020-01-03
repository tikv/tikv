// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

use super::super::Result;
use super::modifier::BinaryModifier;
use super::path_expr::PathExpression;
use super::Json;

impl Json {
    /// Removes elements from Json,
    /// All path expressions cannot contain * or ** wildcard.
    /// If any error occurs, the input won't be changed.
    pub fn remove(&mut self, path_expr_list: &[PathExpression]) -> Result<()> {
        if path_expr_list
            .iter()
            .any(|expr| expr.legs.is_empty() || expr.contains_any_asterisk())
        {
            return Err(box_err!("Invalid path expression"));
        }

        for expr in path_expr_list {
            let modifier = BinaryModifier::new(self.as_ref());
            *self = modifier.remove(&expr.legs)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
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
