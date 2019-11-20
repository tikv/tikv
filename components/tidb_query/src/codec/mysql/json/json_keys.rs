// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use super::super::Result;
use super::path_expr::PathExpression;
use super::Json;

impl Json {
    pub fn keys(&self, path_expr_list: &[PathExpression]) -> Result<Option<Json>> {
        if !path_expr_list.is_empty() {
            if path_expr_list.len() > 1 {
                return Err(box_err!(
                    "Incorrect number of parameters: expected: 0 or 1, get {:?}",
                    path_expr_list.len()
                ));
            }
            if path_expr_list
                .iter()
                .any(|expr| expr.contains_any_asterisk())
            {
                return Err(box_err!(
                    "Invalid path expression: expected no asterisk, but {:?}",
                    path_expr_list
                ));
            }
            return Ok(self.extract(path_expr_list).and_then(|j| json_keys(&j)));
        }
        Ok(json_keys(self))
    }
}

fn json_keys(j: &Json) -> Option<Json> {
    match *j {
        Json::Object(ref map) => Some(Json::Array(
            map.keys()
                .map(|key| Json::String(key.to_string()))
                .collect(),
        )),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::super::path_expr::parse_json_path_expr;
    use super::*;
    use std::str::FromStr;
    #[test]
    fn test_json_keys() {
        let mut test_cases = vec![
            // Tests nil arguments
            ("null", None, None, true),
            ("null", Some("$.c"), None, true),
            ("null", None, None, true),
            // Tests with other type
            ("1", None, None, true),
            (r#""str""#, None, None, true),
            ("true", None, None, true),
            ("null", None, None, true),
            (r#"[1, 2]"#, None, None, true),
            (r#"["1", "2"]"#, None, None, true),
            // Tests without path expression
            (r#"{}"#, None, Some("[]"), true),
            (r#"{"a": 1}"#, None, Some(r#"["a"]"#), true),
            (r#"{"a": 1, "b": 2}"#, None, Some(r#"["a", "b"]"#), true),
            (
                r#"{"a": {"c": 3}, "b": 2}"#,
                None,
                Some(r#"["a", "b"]"#),
                true,
            ),
            // Tests with path expression
            (r#"{"a": 1}"#, Some("$.a"), None, true),
            (
                r#"{"a": {"c": 3}, "b": 2}"#,
                Some("$.a"),
                Some(r#"["c"]"#),
                true,
            ),
            (r#"{"a": {"c": 3}, "b": 2}"#, Some("$.a.c"), None, true),
            // Tests path expression contains any asterisk
            (r#"{}"#, Some("$.*"), None, false),
            (r#"{"a": 1}"#, Some("$.*"), None, false),
            (r#"{"a": {"c": 3}, "b": 2}"#, Some("$.*"), None, false),
            (r#"{"a": {"c": 3}, "b": 2}"#, Some("$.a.*"), None, false),
            // Tests path expression does not identify a section of the target document
            (r#"{"a": 1}"#, Some("$.b"), None, true),
            (r#"{"a": {"c": 3}, "b": 2}"#, Some("$.c"), None, true),
            (r#"{"a": {"c": 3}, "b": 2}"#, Some("$.a.d"), None, true),
        ];
        for (i, (js, param, expected, success)) in test_cases.drain(..).enumerate() {
            let j = js.parse();
            assert!(j.is_ok(), "#{} expect parse ok but got {:?}", i, j);
            let j: Json = j.unwrap();
            let exprs = match param {
                Some(p) => vec![parse_json_path_expr(p).unwrap()],
                None => vec![],
            };
            let got = j.keys(&exprs[..]);
            if success {
                assert!(got.is_ok(), "#{} expect modify ok but got {:?}", i, got);
                let result = got.unwrap();
                let expected = match expected {
                    Some(es) => {
                        let e = Json::from_str(es);
                        assert!(e.is_ok(), "#{} expect parse json ok but got {:?}", i, e);
                        Some(e.unwrap())
                    }
                    None => None,
                };
                assert_eq!(
                    result, expected,
                    "#{} expect {:?}, but got {:?}",
                    i, expected, result,
                );
            } else {
                assert!(got.is_err(), "#{} expect modify error but got {:?}", i, got);
            }
        }
    }
}
