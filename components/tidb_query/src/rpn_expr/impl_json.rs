// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use tidb_query_codegen::rpn_fn;

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

#[rpn_fn(raw_varg, min_args=2, extra_validator = json_modify_validator)]
#[inline]
fn json_set(args: &[ScalarValueRef]) -> Result<Option<Json>> {
    json_modify(args, ModifyType::Set)
}

#[rpn_fn(raw_varg, min_args=2, extra_validator = json_modify_validator)]
#[inline]
fn json_insert(args: &[ScalarValueRef]) -> Result<Option<Json>> {
    json_modify(args, ModifyType::Insert)
}

#[rpn_fn(raw_varg, min_args=2, extra_validator = json_modify_validator)]
#[inline]
fn json_replace(args: &[ScalarValueRef]) -> Result<Option<Json>> {
    json_modify(args, ModifyType::Replace)
}

#[inline]
fn json_modify(args: &[ScalarValueRef], mt: ModifyType) -> Result<Option<Json>> {
    // args >= 2
    // base Json argument
    let base: &Option<Json> = args[0].as_ref();
    let mut base = base.as_ref().map_or(Json::None, |json| json.to_owned());

    // args.len() / 2
    let sz = args.len() / 2;

    let mut path_expr_list = Vec::with_capacity(sz);
    let mut values = Vec::with_capacity(sz);

    for chunk in args[1..].chunks(2) {
        let path: &Option<Bytes> = chunk[0].as_ref();
        let value: &Option<Json> = chunk[1].as_ref();

        let path = match path.as_ref() {
            None => return Ok(None),
            Some(p) => p.to_owned(),
        };

        let json_path = eval_string_and_decode(path)?;
        let path_expr = parse_json_path_expr(&json_path)?;

        path_expr_list.push(path_expr);

        let value = value.as_ref().map_or(Json::None, |json| json.to_owned());
        values.push(value);
    }
    base.modify(&path_expr_list, values, mt)?;

    Ok(Some(base))
}

/// validate the arguments are `(&Option<Json>, &[(Option<Bytes>, Option<Json>)])`
fn json_modify_validator(expr: &tipb::Expr) -> Result<()> {
    let children = expr.get_children();
    if children.len() % 2 != 1 {
        return Err(other_err!(
            "Incorrect parameter count in the call to native function 'JSON_OBJECT'"
        ));
    }
    // min_args will be validated before extra_validator, so expr.get_children() must be larger than 2
    super::function::validate_expr_return_type(&children[0], Json::EVAL_TYPE)?;
    for chunk in children[1..].chunks(2) {
        super::function::validate_expr_return_type(&chunk[0], Bytes::EVAL_TYPE)?;
        super::function::validate_expr_return_type(&chunk[1], Json::EVAL_TYPE)?;
    }
    Ok(())
}

#[inline]
fn eval_string_and_decode(b: Bytes) -> Result<String> {
    String::from_utf8(b)
        .map_err(crate::codec::Error::from)
        .map_err(Error::from)
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

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;

    use tipb::ScalarFuncSig;

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
    fn test_json_modify() {
        let cases: Vec<(_, Vec<ScalarValue>, _)> = vec![
            (
                ScalarFuncSig::JsonSetSig,
                vec![
                    None::<Json>.into(),
                    None::<Bytes>.into(),
                    None::<Json>.into(),
                ],
                None::<Json>,
            ),
            (
                ScalarFuncSig::JsonSetSig,
                vec![
                    Some(Json::I64(9)).into(),
                    Some(b"$[1]".to_vec()).into(),
                    Some(Json::U64(3)).into(),
                ],
                Some(r#"[9,3]"#.parse().unwrap()),
            ),
            (
                ScalarFuncSig::JsonInsertSig,
                vec![
                    Some(Json::I64(9)).into(),
                    Some(b"$[1]".to_vec()).into(),
                    Some(Json::U64(3)).into(),
                ],
                Some(r#"[9,3]"#.parse().unwrap()),
            ),
            (
                ScalarFuncSig::JsonReplaceSig,
                vec![
                    Some(Json::I64(9)).into(),
                    Some(b"$[1]".to_vec()).into(),
                    Some(Json::U64(3)).into(),
                ],
                Some(r#"9"#.parse().unwrap()),
            ),
            (
                ScalarFuncSig::JsonSetSig,
                vec![
                    Some(Json::from_str(r#"{"a":"x"}"#).unwrap()).into(),
                    Some(b"$.a".to_vec()).into(),
                    None::<Json>.into(),
                ],
                Some(r#"{"a":null}"#.parse().unwrap()),
            ),
        ];
        for (sig, args, expect_output) in cases {
            let output: Option<Json> = RpnFnScalarEvaluator::new()
                .push_params(args.clone())
                .evaluate(sig)
                .unwrap();
            assert_eq!(output, expect_output, "{:?}", args);
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
}
