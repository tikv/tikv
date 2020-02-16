// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

use super::super::Result;
use super::path_expr::{
    PathExpression, PathLeg, PATH_EXPR_ARRAY_INDEX_ASTERISK, PATH_EXPR_ASTERISK,
};
use super::{Json, JsonRef, JsonType};

impl<'a> JsonRef<'a> {
    /// `extract` receives several path expressions as arguments, matches them in j, and returns
    /// the target JSON matched any path expressions, which may be autowrapped as an array.
    /// If there is no any expression matched, it returns None.
    ///
    /// See `Extract()` in TiDB `json.binary_function.go`
    pub fn extract(&self, path_expr_list: &[PathExpression]) -> Result<Option<Json>> {
        let mut elem_list = Vec::with_capacity(path_expr_list.len());
        for path_expr in path_expr_list {
            elem_list.append(&mut extract_json(*self, &path_expr.legs)?)
        }
        if elem_list.is_empty() {
            return Ok(None);
        }
        if path_expr_list.len() == 1 && elem_list.len() == 1 {
            // If path_expr contains asterisks, elem_list.len() won't be 1
            // even if path_expr_list.len() equals to 1.
            return Ok(Some(elem_list.remove(0).to_owned()));
        }
        Ok(Some(Json::from_array(
            elem_list.drain(..).map(|j| j.to_owned()).collect(),
        )?))
    }
}

/// `extract_json` is used by JSON::extract().
pub fn extract_json<'a>(j: JsonRef<'a>, path_legs: &[PathLeg]) -> Result<Vec<JsonRef<'a>>> {
    if path_legs.is_empty() {
        return Ok(vec![j]);
    }
    let (current_leg, sub_path_legs) = (&path_legs[0], &path_legs[1..]);
    let mut ret = vec![];
    match *current_leg {
        PathLeg::Index(i) => match j.get_type() {
            JsonType::Array => {
                let elem_count = j.get_elem_count();
                if i == PATH_EXPR_ARRAY_INDEX_ASTERISK {
                    for k in 0..elem_count {
                        ret.append(&mut extract_json(j.array_get_elem(k)?, sub_path_legs)?)
                    }
                } else if (i as usize) < elem_count {
                    ret.append(&mut extract_json(
                        j.array_get_elem(i as usize)?,
                        sub_path_legs,
                    )?)
                }
            }
            _ => {
                if i as usize == 0 {
                    ret.append(&mut extract_json(j, sub_path_legs)?)
                }
            }
        },
        PathLeg::Key(ref key) => {
            if j.get_type() == JsonType::Object {
                if key == PATH_EXPR_ASTERISK {
                    let elem_count = j.get_elem_count();
                    for i in 0..elem_count {
                        ret.append(&mut extract_json(j.object_get_val(i)?, sub_path_legs)?)
                    }
                } else if let Some(idx) = j.object_search_key(key.as_bytes()) {
                    let val = j.object_get_val(idx)?;
                    ret.append(&mut extract_json(val, sub_path_legs)?)
                }
            }
        }
        PathLeg::DoubleAsterisk => {
            ret.append(&mut extract_json(j, sub_path_legs)?);
            match j.get_type() {
                JsonType::Array => {
                    let elem_count = j.get_elem_count();
                    for k in 0..elem_count {
                        ret.append(&mut extract_json(j.array_get_elem(k)?, sub_path_legs)?)
                    }
                }
                JsonType::Object => {
                    let elem_count = j.get_elem_count();
                    for i in 0..elem_count {
                        ret.append(&mut extract_json(j.object_get_val(i)?, sub_path_legs)?)
                    }
                }
                _ => {}
            }
        }
    }
    Ok(ret)
}

#[cfg(test)]
mod tests {
    use super::super::path_expr::{
        PathExpressionFlag, PATH_EXPRESSION_CONTAINS_ASTERISK,
        PATH_EXPRESSION_CONTAINS_DOUBLE_ASTERISK, PATH_EXPR_ARRAY_INDEX_ASTERISK,
    };
    use super::*;
    use std::str::FromStr;

    #[test]
    fn test_json_extract() {
        let mut test_cases = vec![
            // no path expression
            ("null", vec![], None),
            // Index
            (
                "[true, 2017]",
                vec![PathExpression {
                    legs: vec![PathLeg::Index(0)],
                    flags: PathExpressionFlag::default(),
                }],
                Some("true"),
            ),
            (
                "[true, 2017]",
                vec![PathExpression {
                    legs: vec![PathLeg::Index(PATH_EXPR_ARRAY_INDEX_ASTERISK)],
                    flags: PATH_EXPRESSION_CONTAINS_ASTERISK,
                }],
                Some("[true, 2017]"),
            ),
            (
                "[true, 2107]",
                vec![PathExpression {
                    legs: vec![PathLeg::Index(2)],
                    flags: PathExpressionFlag::default(),
                }],
                None,
            ),
            (
                "6.18",
                vec![PathExpression {
                    legs: vec![PathLeg::Index(0)],
                    flags: PathExpressionFlag::default(),
                }],
                Some("6.18"),
            ),
            (
                "6.18",
                vec![PathExpression {
                    legs: vec![PathLeg::Index(PATH_EXPR_ARRAY_INDEX_ASTERISK)],
                    flags: PathExpressionFlag::default(),
                }],
                None,
            ),
            (
                "true",
                vec![PathExpression {
                    legs: vec![PathLeg::Index(0)],
                    flags: PathExpressionFlag::default(),
                }],
                Some("true"),
            ),
            (
                "true",
                vec![PathExpression {
                    legs: vec![PathLeg::Index(PATH_EXPR_ARRAY_INDEX_ASTERISK)],
                    flags: PathExpressionFlag::default(),
                }],
                None,
            ),
            (
                "6",
                vec![PathExpression {
                    legs: vec![PathLeg::Index(0)],
                    flags: PathExpressionFlag::default(),
                }],
                Some("6"),
            ),
            (
                "6",
                vec![PathExpression {
                    legs: vec![PathLeg::Index(PATH_EXPR_ARRAY_INDEX_ASTERISK)],
                    flags: PathExpressionFlag::default(),
                }],
                None,
            ),
            (
                "-6",
                vec![PathExpression {
                    legs: vec![PathLeg::Index(0)],
                    flags: PathExpressionFlag::default(),
                }],
                Some("-6"),
            ),
            (
                "-6",
                vec![PathExpression {
                    legs: vec![PathLeg::Index(PATH_EXPR_ARRAY_INDEX_ASTERISK)],
                    flags: PathExpressionFlag::default(),
                }],
                None,
            ),
            (
                r#"{"a": [1, 2, {"aa": "xx"}]}"#,
                vec![PathExpression {
                    legs: vec![PathLeg::Index(PATH_EXPR_ARRAY_INDEX_ASTERISK)],
                    flags: PathExpressionFlag::default(),
                }],
                None,
            ),
            (
                r#"{"a": [1, 2, {"aa": "xx"}]}"#,
                vec![PathExpression {
                    legs: vec![PathLeg::Index(0)],
                    flags: PathExpressionFlag::default(),
                }],
                Some(r#"{"a": [1, 2, {"aa": "xx"}]}"#),
            ),
            // Key
            (
                r#"{"a": "a1", "b": 20.08, "c": false}"#,
                vec![PathExpression {
                    legs: vec![PathLeg::Key(String::from("c"))],
                    flags: PathExpressionFlag::default(),
                }],
                Some("false"),
            ),
            (
                r#"{"a": "a1", "b": 20.08, "c": false}"#,
                vec![PathExpression {
                    legs: vec![PathLeg::Key(String::from(PATH_EXPR_ASTERISK))],
                    flags: PATH_EXPRESSION_CONTAINS_ASTERISK,
                }],
                Some(r#"["a1", 20.08, false]"#),
            ),
            (
                r#"{"a": "a1", "b": 20.08, "c": false}"#,
                vec![PathExpression {
                    legs: vec![PathLeg::Key(String::from("d"))],
                    flags: PathExpressionFlag::default(),
                }],
                None,
            ),
            // Double asterisks
            (
                "21",
                vec![PathExpression {
                    legs: vec![PathLeg::DoubleAsterisk, PathLeg::Key(String::from("c"))],
                    flags: PATH_EXPRESSION_CONTAINS_DOUBLE_ASTERISK,
                }],
                None,
            ),
            (
                r#"{"g": {"a": "a1", "b": 20.08, "c": false}}"#,
                vec![PathExpression {
                    legs: vec![PathLeg::DoubleAsterisk, PathLeg::Key(String::from("c"))],
                    flags: PATH_EXPRESSION_CONTAINS_DOUBLE_ASTERISK,
                }],
                Some("false"),
            ),
            (
                r#"[{"a": "a1", "b": 20.08, "c": false}, true]"#,
                vec![PathExpression {
                    legs: vec![PathLeg::DoubleAsterisk, PathLeg::Key(String::from("c"))],
                    flags: PATH_EXPRESSION_CONTAINS_DOUBLE_ASTERISK,
                }],
                Some("false"),
            ),
        ];
        for (i, (js, exprs, expected)) in test_cases.drain(..).enumerate() {
            let j = js.parse();
            assert!(j.is_ok(), "#{} expect parse ok but got {:?}", i, j);
            let j: Json = j.unwrap();
            let expected = match expected {
                Some(es) => {
                    let e = Json::from_str(es);
                    assert!(e.is_ok(), "#{} expect parse json ok but got {:?}", i, e);
                    Some(e.unwrap())
                }
                None => None,
            };
            let got = j.as_ref().extract(&exprs[..]).unwrap();
            assert_eq!(
                got, expected,
                "#{} expect {:?}, but got {:?}",
                i, expected, got
            );
        }
    }
}
