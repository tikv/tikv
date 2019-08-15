// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use tidb_query_codegen::rpn_fn;

use crate::codec::data_type::*;
use crate::codec::mysql::json::*;
use crate::Result;

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

/// TODO: think about can macros be used in this function.
/// TODO: implement the functions.
/// TODO: make clear how json handles None.
#[inline]
fn json_modify(args: &[ScalarValueRef], mt: ModifyType) -> Result<Option<Json>> {
    // args >= 2
    // base Json argument
    let base: &Option<Json> = Evaluable::borrow_scalar_value_ref(&args[0]);
    let base = base.as_ref().map_or(Json::None, |json| json.to_owned());

    // args.len() / 2
    let sz = args.len() / 2;

    let mut path_expr_list = Vec::with_capacity(sz);
    let mut values = Vec::with_capacity(sz);

    for chunk in args[1..].chunks(2) {
        let path: &Option<Bytes> = Evaluable::borrow_scalar_value_ref(&chunk[0]);
        let value: &Option<Json> = Evaluable::borrow_scalar_value_ref(&chunk[1]);
        if path.is_none() {
            return Ok(None);
        }
        let path = path.unwrap();
        let value = value.as_ref().map_or(Json::None, |json| json.to_owned());

        // TODO: check path with
        let path = PathExpression::try_from(path);

        path_expr_list.push(try_opt!(path));
        values.push(value);
    }
    base.modify(&path_expr_list, values, mt)?;

    base
}

/// validate the arguments are `(&Option<Json>, &[(Option<Bytes>, Option<Json>)])`
fn json_modify_validator(expr: &tipb::Expr) -> Result<()> {
    let children = expr.get_children();
    // min_args will be validated before extra_validator, so expr.get_children() must be larger than 2
    super::function::validate_expr_return_type(&children[0], Json::EVAL_TYPE)?;
    if chunk.len() == 1 {
        return Err(other_err!(
                "Incorrect parameter count in the call to native function 'JSON_OBJECT'"
            ));
    } else {
        super::function::validate_expr_return_type(&chunk[0], Bytes::EVAL_TYPE)?;
        super::function::validate_expr_return_type(&chunk[1], Json::EVAL_TYPE)?;
    }
    Ok(())
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
    use super::*;

    use tipb::ScalarFuncSig;

    use crate::rpn_expr::types::test_util::RpnFnScalarEvaluator;

    #[test]
    fn test_json_type() {
        use std::str::FromStr;

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
                vec![None::<Json>.into(), None::<Bytes>.into(), None::<Json>.into()],
                None::<Json>,
            ),
            (
                ScalarFuncSig::JsonSetSig,
                vec![
                    Some(Json::I64(9)).into(),
                    Some(Bytes::from(b"$[1]".to_vec())).into(),
                    Some(Json::U64(3)).into(),
                ],
                Some(Json::from(r#"[9,3]"#.parse().unwrap())),
            ),
            (
                ScalarFuncSig::JsonInsertSig,
                vec![
                    Some(Json::I64(9)).into(),
                    Some(Bytes::from(b"$[1]".to_vec())).into(),
                    Some(Json::U64(3)).into(),
                ],
                Some(Json::from(r#"[9,3]"#.parse().unwrap())),
            ),
            (
                ScalarFuncSig::JsonReplaceSig,
                vec![
                    Some(Json::I64(9)).into(),
                    Some(Bytes::from(b"$[1]".to_vec())).into(),
                    Some(Json::U64(3)).into(),
                ],
                Some(Json::from(r#"9"#.parse().unwrap())),
            ),
            (
                ScalarFuncSig::JsonSetSig,
                vec![
                    Some(Json::from(r#"{"a":"x"}"#.parse().unwrap())).into(),
                    Some(Bytes::from(b"$.a".to_vec())).into(),
                    None::<Json>.into(),
                ],
                Some(Json::from(r#"{"a":null}"#.parse().unwrap())),
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
