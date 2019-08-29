// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use tidb_query_codegen::rpn_fn;
use tidb_query_datatype::EvalType;

use crate::codec::data_type::*;
use crate::codec::mysql::json::*;
use crate::{Error, Result};

#[rpn_fn]
#[inline]
fn json_type(arg: &Option<Json>) -> Result<Option<Bytes>> {
    Ok(arg
        .as_ref()
        .map(|json_arg| Bytes::from(json_arg.json_type())))
}

#[rpn_fn(varg)]
#[inline]
fn json_array(args: &[&Option<Json>]) -> Result<Option<Json>> {
    Ok(Some(Json::Array(
        args.iter()
            .map(|json| match json {
                None => Json::None,
                Some(json) => json.to_owned(),
            })
            .collect(),
    )))
}

// According to mysql 5.7,
// arguments of json_merge should not be less than 2.
#[rpn_fn(varg, min_args = 2)]
#[inline]
pub fn json_merge(args: &[&Option<Json>]) -> Result<Option<Json>> {
    // min_args = 2, so it's ok to call args[0]
    let base_json = match args[0] {
        None => return Ok(None),
        Some(json) => json.to_owned(),
    };

    Ok(args[1..]
        .iter()
        .try_fold(base_json, move |base, json_to_merge| {
            json_to_merge
                .as_ref()
                .map(|json| base.merge(json.to_owned()))
        }))
}

#[rpn_fn]
#[inline]
fn json_unquote(arg: &Option<Json>) -> Result<Option<Bytes>> {
    arg.as_ref().map_or(Ok(None), |json_arg| {
        Ok(Some(Bytes::from(json_arg.unquote()?)))
    })
}

fn json_with_bytes_validator(expr: &tipb::Expr) -> Result<()> {
    let children = expr.get_children();
    // min_args = 2
    // args should be like `&Option<Json> , &[&Option<Bytes>]`.
    super::function::validate_expr_return_type(&children[0], EvalType::Json)?;
    for i in 1..children.len() {
        super::function::validate_expr_return_type(&children[i], EvalType::Bytes)?;
    }
    Ok(())
}

/// args should be like `&Option<Json> , &[&Option<Bytes>]`.
#[rpn_fn(raw_varg, min_args = 2, extra_validator = json_with_bytes_validator)]
#[inline]
fn json_extract(args: &[ScalarValueRef]) -> Result<Option<Json>> {
    // args should be at least 2
    let j: &Option<Json> = Evaluable::borrow_scalar_value_ref(&args[0]);
    let j = match j.as_ref() {
        None => return Ok(None),
        Some(j) => j.to_owned(),
    };

    let mut path_expr_list = vec![];
    for i in 1..args.len() {
        let json_path: &Option<Bytes> = Evaluable::borrow_scalar_value_ref(&args[i]);

        let json_path = match json_path.as_ref() {
            None => return Ok(None),
            Some(p) => p.to_owned(),
        };
        let json_path = eval_string_and_decode(json_path)?;

        let path_expr = parse_json_path_expr(&json_path)?;

        path_expr_list.push(path_expr);
    }

    Ok(j.extract(&path_expr_list))
}

/// args should be like `&Option<Json> , &[&Option<Bytes>]`.
#[rpn_fn(raw_varg, min_args = 2, extra_validator = json_with_bytes_validator)]
#[inline]
fn json_remove(args: &[ScalarValueRef]) -> Result<Option<Json>> {
    // args should be at least 2
    let j: &Option<Json> = Evaluable::borrow_scalar_value_ref(&args[0]);
    let mut j = match j.as_ref() {
        None => return Ok(None),
        Some(j) => j.to_owned(),
    };

    let mut path_expr_list = vec![];
    for i in 1..args.len() {
        let json_path: &Option<Bytes> = Evaluable::borrow_scalar_value_ref(&args[i]);

        let json_path = match json_path.as_ref() {
            None => return Ok(None),
            Some(p) => p.to_owned(),
        };
        let json_path = eval_string_and_decode(json_path)?;

        let path_expr = parse_json_path_expr(&json_path)?;

        path_expr_list.push(path_expr);
    }

    j.remove(&path_expr_list)?;
    Ok(Some(j))
}

#[inline]
fn eval_string_and_decode(b: Bytes) -> Result<String> {
    String::from_utf8(b)
        .map_err(crate::codec::Error::from)
        .map_err(Error::from)
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use tipb::ScalarFuncSig;

    use super::*;
    use crate::rpn_expr::types::test_util::RpnFnScalarEvaluator;

    #[test]
    fn test_json_type() {
        let cases = vec![
            (None, None),
            (Some(r#"true"#), Some("BOOLEAN")),
            (Some(r#"null"#), Some("NULL")),
            (Some(r#"-3"#), Some("INTEGER")),
            (Some(r#"3"#), Some("UNSIGNED INTEGER")),
            (Some(r#"3.14"#), Some("DOUBLE")),
            (Some(r#"[1, 2, 3]"#), Some("ARRAY")),
            (Some(r#"{"name": 123}"#), Some("OBJECT")),
        ];

        for (arg, expect_output) in cases {
            let arg = arg.map(|input| Json::from_str(input).unwrap());
            let expect_output = expect_output.map(|s| Bytes::from(s));

            let output = RpnFnScalarEvaluator::new()
                .push_param(arg.clone())
                .evaluate(ScalarFuncSig::JsonTypeSig)
                .unwrap();
            assert_eq!(output, expect_output, "{:?}", arg);
        }
    }

    #[test]
    fn test_json_array() {
        let cases = vec![
            (vec![], Some(r#"[]"#)),
            (vec![Some(r#"1"#), None], Some(r#"[1, null]"#)),
            (
                vec![
                    Some(r#"1"#),
                    None,
                    Some(r#"2"#),
                    Some(r#""sdf""#),
                    Some(r#""k1""#),
                    Some(r#""v1""#),
                ],
                Some(r#"[1, null, 2, "sdf", "k1", "v1"]"#),
            ),
        ];

        for (vargs, expected) in cases {
            let vargs = vargs
                .into_iter()
                .map(|input| input.map(|s| Json::from_str(s).unwrap()))
                .collect::<Vec<_>>();
            let expected = expected.map(|s| Json::from_str(s).unwrap());

            let output = RpnFnScalarEvaluator::new()
                .push_params(vargs.clone())
                .evaluate(ScalarFuncSig::JsonArraySig)
                .unwrap();
            assert_eq!(output, expected, "{:?}", vargs);
        }
    }

    #[test]
    fn test_json_merge() {
        let cases = vec![
            (vec![None, None], None),
            (vec![Some("{}"), Some("[]")], Some("[{}]")),
            (
                vec![Some(r#"{}"#), Some(r#"[]"#), Some(r#"3"#), Some(r#""4""#)],
                Some(r#"[{}, 3, "4"]"#),
            ),
            (
                vec![Some("[1, 2]"), Some("[3, 4]")],
                Some(r#"[1, 2, 3, 4]"#),
            ),
        ];

        for (vargs, expected) in cases {
            let vargs = vargs
                .into_iter()
                .map(|input| input.map(|s| Json::from_str(s).unwrap()))
                .collect::<Vec<_>>();
            let expected = expected.map(|s| Json::from_str(s).unwrap());

            let output = RpnFnScalarEvaluator::new()
                .push_params(vargs.clone())
                .evaluate(ScalarFuncSig::JsonMergeSig)
                .unwrap();
            assert_eq!(output, expected, "{:?}", vargs);
        }
    }

    #[test]
    fn test_json_unquote() {
        let cases = vec![
            (None, false, None),
            (Some(r"a"), false, Some("a")),
            (Some(r#""3""#), false, Some(r#""3""#)),
            (Some(r#""3""#), true, Some(r#"3"#)),
            (Some(r#"{"a":  "b"}"#), false, Some(r#"{"a":  "b"}"#)),
            (Some(r#"{"a":  "b"}"#), true, Some(r#"{"a":"b"}"#)),
            (
                Some(r#"hello,\"quoted string\",world"#),
                false,
                Some(r#"hello,"quoted string",world"#),
            ),
        ];

        for (arg, parse, expect_output) in cases {
            let arg = arg.map(|input| {
                if parse {
                    input.parse().unwrap()
                } else {
                    Json::String(input.to_string())
                }
            });
            let expect_output = expect_output.map(Bytes::from);

            let output = RpnFnScalarEvaluator::new()
                .push_param(arg.clone())
                .evaluate(ScalarFuncSig::JsonUnquoteSig)
                .unwrap();
            assert_eq!(output, expect_output, "{:?}", arg);
        }
    }

    #[test]
    fn test_json_extract() {
        let cases: Vec<(Vec<ScalarValue>, _)> = vec![
            (vec![None::<Json>.into(), None::<Bytes>.into()], None),
            (
                vec![
                    Some(Json::from_str("[10, 20, [30, 40]]").unwrap()).into(),
                    Some(b"$[1]".to_vec()).into(),
                ],
                Some("20"),
            ),
            (
                vec![
                    Some(Json::from_str("[10, 20, [30, 40]]").unwrap()).into(),
                    Some(b"$[1]".to_vec()).into(),
                    Some(b"$[0]".to_vec()).into(),
                ],
                Some("[20, 10]"),
            ),
            (
                vec![
                    Some(Json::from_str("[10, 20, [30, 40]]").unwrap()).into(),
                    Some(b"$[2][*]".to_vec()).into(),
                ],
                Some("[30, 40]"),
            ),
            (
                vec![
                    Some(Json::from_str("[10, 20, [30, 40]]").unwrap()).into(),
                    Some(b"$[2][*]".to_vec()).into(),
                    None::<Bytes>.into(),
                ],
                None,
            ),
        ];

        for (vargs, expected) in cases {
            let expected = expected.map(|s| Json::from_str(s).unwrap());

            let output = RpnFnScalarEvaluator::new()
                .push_params(vargs.clone())
                .evaluate(ScalarFuncSig::JsonExtractSig)
                .unwrap();
            assert_eq!(output, expected, "{:?}", vargs);
        }
    }

    #[test]
    fn test_json_remove() {
        let cases: Vec<(Vec<ScalarValue>, _)> = vec![
            (
                vec![
                    Some(Json::from_str(r#"["a", ["b", "c"], "d"]"#).unwrap()).into(),
                    Some(b"$[1]".to_vec()).into(),
                ],
                Some(r#"["a", "d"]"#),
            )
        ];

        for (vargs, expected) in cases {
            let expected = expected.map(|s| Json::from_str(s).unwrap());

            let output = RpnFnScalarEvaluator::new()
                .push_params(vargs.clone())
                .evaluate(ScalarFuncSig::JsonRemoveSig)
                .unwrap();
            assert_eq!(output, expected, "{:?}", vargs);
        }
    }
}
