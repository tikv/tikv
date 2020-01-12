// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use super::path_expr::PathExpression;
use super::Json;

impl Json {
    pub fn len(&self) -> Option<i64> {
        match self {
            Json::Array(array) => Some(array.len() as i64),
            Json::Object(obj) => Some(obj.len() as i64),
            Json::None
            | Json::String(_)
            | Json::Boolean(_)
            | Json::U64(_)
            | Json::I64(_)
            | Json::Double(_) => Some(1),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len().or_else(|| Some(0)).unwrap() == 0
    }

    // `json_length` is the implementation for JSON_LENGTH in mysql
    // https://dev.mysql.com/doc/refman/5.7/en/json-attribute-functions.html#function_json-length
    pub fn json_length(&self, path_expr_list: &[PathExpression]) -> Option<i64> {
        if path_expr_list.is_empty() {
            return self.len();
        }
        if path_expr_list.len() == 1 && path_expr_list[0].contains_any_asterisk() {
            return None;
        }
        if let Some(json) = self.extract(path_expr_list) {
            return json.len();
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::super::path_expr::parse_json_path_expr;
    use super::*;
    #[test]
    fn test_json_length() {
        let mut test_cases = vec![
            ("null", None, Some(1)),
            ("false", None, Some(1)),
            ("true", None, Some(1)),
            ("1", None, Some(1)),
            ("-1", None, Some(1)),
            ("1.1", None, Some(1)),
            // Tests with path expression
            (r#"[1,2,[1,[5,[3]]]]"#, Some("$[2]"), Some(2)),
            (r#"[{"a":1}]"#, Some("$"), Some(1)),
            (r#"[{"a":1,"b":2}]"#, Some("$[0].a"), Some(1)),
            (r#"{"a":{"a":1},"b":2}"#, Some("$"), Some(2)),
            (r#"{"a":{"a":1},"b":2}"#, Some("$.a"), Some(1)),
            (r#"{"a":{"a":1},"b":2}"#, Some("$.a.a"), Some(1)),
            (r#"{"a": [1, 2, {"aa": "xx"}]}"#, Some("$.a[2].aa"), Some(1)),
            // Tests without path expression
            (r#"{}"#, None, Some(0)),
            (r#"{"a":1}"#, None, Some(1)),
            (r#"{"a":[1]}"#, None, Some(1)),
            (r#"{"b":2, "c":3}"#, None, Some(2)),
            (r#"[1]"#, None, Some(1)),
            (r#"[1,2]"#, None, Some(2)),
            (r#"[1,2,[1,3]]"#, None, Some(3)),
            (r#"[1,2,[1,[5,[3]]]]"#, None, Some(3)),
            (r#"[1,2,[1,[5,{"a":[2,3]}]]]"#, None, Some(3)),
            (r#"[{"a":1}]"#, None, Some(1)),
            (r#"[{"a":1,"b":2}]"#, None, Some(1)),
            (r#"[{"a":{"a":1},"b":2}]"#, None, Some(1)),
            // Tests path expression contains any asterisk
            (r#"{"a": [1, 2, {"aa": "xx"}]}"#, Some("$.*"), None),
            (r#"{"a": [1, 2, {"aa": "xx"}]}"#, Some("$[*]"), None),
            (r#"{"a": [1, 2, {"aa": "xx"}]}"#, Some("$**.a"), None),
            // Tests path expression does not identify a section of the target document
            (r#"{"a": [1, 2, {"aa": "xx"}]}"#, Some("$.c"), None),
            (r#"{"a": [1, 2, {"aa": "xx"}]}"#, Some("$.a[3]"), None),
            (r#"{"a": [1, 2, {"aa": "xx"}]}"#, Some("$.a[2].b"), None),
        ];
        for (i, (js, param, expected)) in test_cases.drain(..).enumerate() {
            let j = js.parse();
            assert!(j.is_ok(), "#{} expect parse ok but got {:?}", i, j);
            let j: Json = j.unwrap();
            let exprs = match param {
                Some(p) => vec![parse_json_path_expr(p).unwrap()],
                None => vec![],
            };
            let got = j.json_length(&exprs[..]);
            assert_eq!(
                got, expected,
                "#{} expect {:?}, but got {:?}",
                i, expected, got
            );
        }
    }
}
