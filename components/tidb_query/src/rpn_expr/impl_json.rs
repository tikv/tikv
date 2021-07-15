// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::collections::BTreeMap;

use tidb_query_codegen::rpn_fn;
use tidb_query_datatype::EvalType;

use crate::codec::data_type::*;
use crate::codec::mysql::json::*;
use crate::Result;

#[rpn_fn]
#[inline]
fn json_depth(arg: &Option<Json>) -> Result<Option<i64>> {
    match arg {
        Some(j) => Ok(Some(j.as_ref().depth()?)),
        None => Ok(None),
    }
}

#[rpn_fn]
#[inline]
fn json_type(arg: &Option<Json>) -> Result<Option<Bytes>> {
    Ok(arg
        .as_ref()
        .map(|json_arg| Bytes::from(json_arg.as_ref().json_type())))
}

#[rpn_fn(raw_varg, min_args = 2, extra_validator = json_modify_validator)]
#[inline]
fn json_set(args: &[ScalarValueRef]) -> Result<Option<Json>> {
    json_modify(args, ModifyType::Set)
}

#[rpn_fn(raw_varg, min_args = 2, extra_validator = json_modify_validator)]
#[inline]
fn json_insert(args: &[ScalarValueRef]) -> Result<Option<Json>> {
    json_modify(args, ModifyType::Insert)
}

#[rpn_fn(raw_varg, min_args = 2, extra_validator = json_modify_validator)]
#[inline]
fn json_replace(args: &[ScalarValueRef]) -> Result<Option<Json>> {
    json_modify(args, ModifyType::Replace)
}

#[inline]
fn json_modify(args: &[ScalarValueRef], mt: ModifyType) -> Result<Option<Json>> {
    assert!(args.len() >= 2);
    // base Json argument
    let base: &Option<Json> = args[0].as_ref();
    let base = base
        .as_ref()
        .map_or(Json::none(), |json| Ok(json.to_owned()))?;

    let buf_size = args.len() / 2;

    let mut path_expr_list = Vec::with_capacity(buf_size);
    let mut values = Vec::with_capacity(buf_size);

    for chunk in args[1..].chunks(2) {
        let path: &Option<Bytes> = chunk[0].as_ref();
        let value: &Option<Json> = chunk[1].as_ref();

        path_expr_list.push(try_opt!(parse_json_path(path)));

        let value = value
            .as_ref()
            .map_or(Json::none(), |json| Ok(json.to_owned()))?;
        values.push(value);
    }
    Ok(Some(base.as_ref().modify(&path_expr_list, values, mt)?))
}

/// validate the arguments are `(&Option<Json>, &[(Option<Bytes>, Option<Json>)])`
fn json_modify_validator(expr: &tipb::Expr) -> Result<()> {
    let children = expr.get_children();
    assert!(children.len() >= 2);
    if children.len() % 2 != 1 {
        return Err(other_err!(
            "Incorrect parameter count in the call to native function 'JSON_OBJECT'"
        ));
    }
    super::function::validate_expr_return_type(&children[0], EvalType::Json)?;
    for chunk in children[1..].chunks(2) {
        super::function::validate_expr_return_type(&chunk[0], EvalType::Bytes)?;
        super::function::validate_expr_return_type(&chunk[1], EvalType::Json)?;
    }
    Ok(())
}

#[rpn_fn(varg)]
#[inline]
fn json_array(args: &[&Option<Json>]) -> Result<Option<Json>> {
    let mut jsons = vec![];
    for arg in args {
        match arg {
            None => jsons.push(Json::none()?),
            Some(j) => jsons.push(j.to_owned()),
        }
    }
    Ok(Some(Json::from_array(jsons)?))
}

fn json_object_validator(expr: &tipb::Expr) -> Result<()> {
    let chunks = expr.get_children();
    if chunks.len() % 2 == 1 {
        return Err(other_err!(
            "Incorrect parameter count in the call to native function 'JSON_OBJECT'"
        ));
    }
    for chunk in chunks.chunks(2) {
        super::function::validate_expr_return_type(&chunk[0], EvalType::Bytes)?;
        super::function::validate_expr_return_type(&chunk[1], EvalType::Json)?;
    }
    Ok(())
}

/// Required args like `&[(&Option<Byte>, &Option<Json>)]`.
#[rpn_fn(raw_varg, extra_validator = json_object_validator)]
#[inline]
fn json_object(raw_args: &[ScalarValueRef]) -> Result<Option<Json>> {
    let mut pairs = BTreeMap::new();
    for chunk in raw_args.chunks(2) {
        assert_eq!(chunk.len(), 2);
        let key: &Option<Bytes> = chunk[0].as_ref();
        if key.is_none() {
            return Err(other_err!(
                "Data truncation: JSON documents may not contain NULL member names."
            ));
        }
        let key = String::from_utf8(key.as_ref().unwrap().to_owned())
            .map_err(|e| crate::codec::Error::from(e))?;

        let value: &Option<Json> = chunk[1].as_ref();
        let value = match value {
            None => Json::none()?,
            Some(v) => v.to_owned(),
        };

        pairs.insert(key, value);
    }
    Ok(Some(Json::from_object(pairs)?))
}

// According to mysql 5.7,
// arguments of json_merge should not be less than 2.
#[rpn_fn(varg, min_args = 2)]
#[inline]
pub fn json_merge(args: &[&Option<Json>]) -> Result<Option<Json>> {
    // min_args = 2, so it's ok to call args[0]
    if args[0].is_none() {
        return Ok(None);
    }
    let mut jsons = vec![];
    let json_none = Json::none()?;
    for arg in args {
        match arg {
            None => jsons.push(json_none.as_ref()),
            Some(j) => jsons.push(j.as_ref()),
        }
    }
    Ok(Some(Json::merge(jsons)?))
}

#[rpn_fn]
#[inline]
fn json_unquote(arg: &Option<Bytes>) -> Result<Option<Bytes>> {
    arg.as_ref().map_or(Ok(None), |json_arg| {
        let tmp_str = std::str::from_utf8(&json_arg).map_err(crate::codec::Error::from)?;
        Ok(Some(Bytes::from(self::unquote_string(&tmp_str)?)))
    })
}

fn unquote_string(s: &str) -> Result<String> {
    let first_char = s.chars().next();
    let last_char = s.chars().last();
    if s.len() >= 2 && first_char == Some('"') && last_char == Some('"') {
        Ok(json_unquote::unquote_string(&s[1..s.len() - 1])?)
    } else {
        Ok(String::from(s))
    }
}

// Args should be like `(&Option<Json> , &[&Option<Bytes>])`.
fn json_with_paths_validator(expr: &tipb::Expr) -> Result<()> {
    assert!(expr.get_children().len() >= 2);
    // args should be like `&Option<Json> , &[&Option<Bytes>]`.
    valid_paths(expr)
}

fn valid_paths(expr: &tipb::Expr) -> Result<()> {
    let children = expr.get_children();
    super::function::validate_expr_return_type(&children[0], EvalType::Json)?;
    for i in 1..children.len() {
        super::function::validate_expr_return_type(&children[i], EvalType::Bytes)?;
    }
    Ok(())
}

#[rpn_fn(raw_varg, min_args = 2, extra_validator = json_with_paths_validator)]
#[inline]
fn json_extract(args: &[ScalarValueRef]) -> Result<Option<Json>> {
    assert!(args.len() >= 2);
    let j: &Option<Json> = args[0].as_ref();
    let j = match j.as_ref() {
        None => return Ok(None),
        Some(j) => j.to_owned(),
    };

    let path_expr_list = try_opt!(parse_json_path_list(&args[1..]));

    Ok(j.as_ref().extract(&path_expr_list)?)
}

// Args should be like `(&Option<Json> , &[&Option<Bytes>])`.
fn json_with_path_validator(expr: &tipb::Expr) -> Result<()> {
    assert!(expr.get_children().len() == 2 || expr.get_children().len() == 1);
    valid_paths(expr)
}

#[rpn_fn(raw_varg,min_args= 1, max_args = 2, extra_validator = json_with_path_validator)]
#[inline]
fn json_keys(args: &[ScalarValueRef]) -> Result<Option<Json>> {
    assert!(!args.is_empty() && args.len() <= 2);
    if let Some(j) = args[0].as_json() {
        if let Some(list) = parse_json_path_list(&args[1..])? {
            return Ok(j.as_ref().keys(&list)?);
        }
    }
    Ok(None)
}

#[rpn_fn(raw_varg,min_args= 1, max_args = 2, extra_validator = json_with_path_validator)]
#[inline]
fn json_length(args: &[ScalarValueRef]) -> Result<Option<Int>> {
    assert!(!args.is_empty() && args.len() <= 2);
    let j: &Option<Json> = args[0].as_ref();
    let j = match j.as_ref() {
        None => return Ok(None),
        Some(j) => j.to_owned(),
    };
    Ok(match parse_json_path_list(&args[1..])? {
        Some(path_expr_list) => j.as_ref().json_length(&path_expr_list)?,
        None => None,
    })
}

#[rpn_fn(raw_varg, min_args = 2, extra_validator = json_with_paths_validator)]
#[inline]
fn json_remove(args: &[ScalarValueRef]) -> Result<Option<Json>> {
    assert!(args.len() >= 2);
    let j: &Option<Json> = args[0].as_ref();
    let j = match j.as_ref() {
        None => return Ok(None),
        Some(j) => j.to_owned(),
    };

    let path_expr_list = try_opt!(parse_json_path_list(&args[1..]));

    Ok(Some(j.as_ref().remove(&path_expr_list)?))
}

fn parse_json_path_list(args: &[ScalarValueRef]) -> Result<Option<Vec<PathExpression>>> {
    let mut path_expr_list = Vec::with_capacity(args.len());
    for arg in args {
        let json_path: &Option<Bytes> = arg.as_ref();

        path_expr_list.push(try_opt!(parse_json_path(json_path)));
    }
    Ok(Some(path_expr_list))
}

#[inline]
fn parse_json_path(path: &Option<Bytes>) -> Result<Option<PathExpression>> {
    let json_path = match path.as_ref() {
        None => return Ok(None),
        Some(p) => std::str::from_utf8(&p).map_err(crate::codec::Error::from),
    }?;

    Ok(Some(parse_json_path_expr(&json_path)?))
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;

    use tipb::ScalarFuncSig;

    use crate::rpn_expr::types::test_util::RpnFnScalarEvaluator;

    #[test]
    fn test_json_depth() {
        let cases = vec![
            (None, None),
            (Some("null"), Some(1)),
            (Some("[true, 2017]"), Some(2)),
            (
                Some(r#"{"a": {"a1": [3]}, "b": {"b1": {"c": {"d": [5]}}}}"#),
                Some(6),
            ),
            (Some("{}"), Some(1)),
            (Some("[]"), Some(1)),
            (Some("true"), Some(1)),
            (Some("1"), Some(1)),
            (Some("-1"), Some(1)),
            (Some(r#""a""#), Some(1)),
            (Some(r#"[10, 20]"#), Some(2)),
            (Some(r#"[[], {}]"#), Some(2)),
            (Some(r#"[10, {"a": 20}]"#), Some(3)),
            (Some(r#"[[2], 3, [[[4]]]]"#), Some(5)),
            (Some(r#"{"Name": "Homer"}"#), Some(2)),
            (Some(r#"[10, {"a": 20}]"#), Some(3)),
            (
                Some(
                    r#"{"Person": {"Name": "Homer", "Age": 39, "Hobbies": ["Eating", "Sleeping"]} }"#,
                ),
                Some(4),
            ),
            (Some(r#"{"a":1}"#), Some(2)),
            (Some(r#"{"a":[1]}"#), Some(3)),
            (Some(r#"{"b":2, "c":3}"#), Some(2)),
            (Some(r#"[1]"#), Some(2)),
            (Some(r#"[1,2]"#), Some(2)),
            (Some(r#"[1,2,[1,3]]"#), Some(3)),
            (Some(r#"[1,2,[1,[5,[3]]]]"#), Some(5)),
            (Some(r#"[1,2,[1,[5,{"a":[2,3]}]]]"#), Some(6)),
            (Some(r#"[{"a":1}]"#), Some(3)),
            (Some(r#"[{"a":1,"b":2}]"#), Some(3)),
            (Some(r#"[{"a":{"a":1},"b":2}]"#), Some(4)),
        ];
        for (arg, expect_output) in cases {
            let arg = arg.map(|input| Json::from_str(input).unwrap());

            let output = RpnFnScalarEvaluator::new()
                .push_param(arg.clone())
                .evaluate(ScalarFuncSig::JsonDepthSig)
                .unwrap();
            assert_eq!(output, expect_output, "{:?}", arg);
        }
    }

    #[test]
    fn test_json_type() {
        let cases = vec![
            (None, None),
            (Some(r#"true"#), Some("BOOLEAN")),
            (Some(r#"null"#), Some("NULL")),
            (Some(r#"-3"#), Some("INTEGER")),
            (Some(r#"3"#), Some("INTEGER")),
            (Some(r#"9223372036854775808"#), Some("DOUBLE")),
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
                    Some(Json::from_i64(9).unwrap()).into(),
                    Some(b"$[1]".to_vec()).into(),
                    Some(Json::from_u64(3).unwrap()).into(),
                ],
                Some(r#"[9,3]"#.parse().unwrap()),
            ),
            (
                ScalarFuncSig::JsonInsertSig,
                vec![
                    Some(Json::from_i64(9).unwrap()).into(),
                    Some(b"$[1]".to_vec()).into(),
                    Some(Json::from_u64(3).unwrap()).into(),
                ],
                Some(r#"[9,3]"#.parse().unwrap()),
            ),
            (
                ScalarFuncSig::JsonReplaceSig,
                vec![
                    Some(Json::from_i64(9).unwrap()).into(),
                    Some(b"$[1]".to_vec()).into(),
                    Some(Json::from_u64(3).unwrap()).into(),
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
    fn test_json_object() {
        let cases = vec![
            (vec![], r#"{}"#),
            (vec![("1", None)], r#"{"1":null}"#),
            (
                vec![
                    ("1", None),
                    ("2", Some(r#""sdf""#)),
                    ("k1", Some(r#""v1""#)),
                ],
                r#"{"1":null,"2":"sdf","k1":"v1"}"#,
            ),
        ];

        for (vargs, expected) in cases {
            let vargs = vargs
                .into_iter()
                .map(|(key, value)| (Bytes::from(key), value.map(|s| Json::from_str(s).unwrap())))
                .collect::<Vec<_>>();

            let mut new_vargs: Vec<ScalarValue> = vec![];
            for (key, value) in vargs.into_iter() {
                new_vargs.push(ScalarValue::from(key));
                new_vargs.push(ScalarValue::from(value));
            }

            let expected = Json::from_str(expected).unwrap();

            let output: Json = RpnFnScalarEvaluator::new()
                .push_params(new_vargs)
                .evaluate(ScalarFuncSig::JsonObjectSig)
                .unwrap()
                .unwrap();
            assert_eq!(output, expected);
        }

        let err_cases = vec![
            vec![
                ScalarValue::from(Bytes::from("1")),
                ScalarValue::from(None::<Json>),
                ScalarValue::from(Bytes::from("1")),
            ],
            vec![
                ScalarValue::from(None::<Bytes>),
                ScalarValue::from(Json::from_str("1").unwrap()),
            ],
        ];

        for err_args in err_cases {
            let output: Result<Option<Json>> = RpnFnScalarEvaluator::new()
                .push_params(err_args)
                .evaluate(ScalarFuncSig::JsonObjectSig);

            assert!(output.is_err());
        }
    }

    #[test]
    fn test_json_unquote() {
        let cases = vec![
            (None, None),
            (Some(r#"""#), Some(r#"""#)),
            (Some(r"a"), Some("a")),
            (Some(r#""3"#), Some(r#""3"#)),
            (Some(r#"{"a":  "b"}"#), Some(r#"{"a":  "b"}"#)),
            (
                Some(r#""hello,\"quoted string\",world""#),
                Some(r#"hello,"quoted string",world"#),
            ),
            (Some(r#"A中\\\"文B"#), Some(r#"A中\\\"文B"#)),
            (Some(r#""A中\\\"文B""#), Some(r#"A中\"文B"#)),
            (Some(r#""\u00E0A中\\\"文B""#), Some(r#"àA中\"文B"#)),
        ];

        for (arg, expect_output) in cases {
            let arg = arg.map(Bytes::from);
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
        let cases: Vec<(Vec<ScalarValue>, _)> = vec![(
            vec![
                Some(Json::from_str(r#"["a", ["b", "c"], "d"]"#).unwrap()).into(),
                Some(b"$[1]".to_vec()).into(),
            ],
            Some(r#"["a", "d"]"#),
        )];

        for (vargs, expected) in cases {
            let expected = expected.map(|s| Json::from_str(s).unwrap());

            let output = RpnFnScalarEvaluator::new()
                .push_params(vargs.clone())
                .evaluate(ScalarFuncSig::JsonRemoveSig)
                .unwrap();
            assert_eq!(output, expected, "{:?}", vargs);
        }
    }

    #[test]
    fn test_json_length() {
        let cases: Vec<(Vec<ScalarValue>, Option<i64>)> = vec![
            (
                vec![
                    Some(Json::from_str("null").unwrap()).into(),
                    None::<Bytes>.into(),
                ],
                None,
            ),
            (
                vec![
                    Some(Json::from_str("false").unwrap()).into(),
                    None::<Bytes>.into(),
                ],
                None,
            ),
            (vec![Some(Json::from_str("1").unwrap()).into()], Some(1)),
            (
                vec![
                    Some(Json::from_str(r#"{"a": [1, 2, {"aa": "xx"}]}"#).unwrap()).into(),
                    Some(b"$.*".to_vec()).into(),
                ],
                None,
            ),
            (
                vec![
                    Some(Json::from_str(r#"{"a":{"a":1},"b":2}"#).unwrap()).into(),
                    Some(b"$".to_vec()).into(),
                ],
                Some(2),
            ),
        ];

        for (vargs, expected) in cases {
            let output = RpnFnScalarEvaluator::new()
                .push_params(vargs.clone())
                .evaluate(ScalarFuncSig::JsonLengthSig)
                .unwrap();
            assert_eq!(output, expected, "{:?}", vargs);
        }
    }

    #[test]
    fn test_json_keys() {
        let cases: Vec<(Vec<ScalarValue>, Option<Json>, bool)> = vec![
            // Tests nil arguments
            (vec![None::<Json>.into(), None::<Bytes>.into()], None, true),
            (
                vec![None::<Json>.into(), Some(b"$.c".to_vec()).into()],
                None,
                true,
            ),
            (
                vec![
                    Some(Json::from_str(r#"{"a": 1}"#).unwrap()).into(),
                    None::<Bytes>.into(),
                ],
                None,
                true,
            ),
            (vec![None::<Json>.into()], None, true),
            // Tests with other type
            (vec![Some(Json::from_str("1").unwrap()).into()], None, true),
            (
                vec![Some(Json::from_str(r#""str""#).unwrap()).into()],
                None,
                true,
            ),
            (
                vec![Some(Json::from_str(r#"true"#).unwrap()).into()],
                None,
                true,
            ),
            (
                vec![Some(Json::from_str("null").unwrap()).into()],
                None,
                true,
            ),
            (
                vec![Some(Json::from_str(r#"[1, 2]"#).unwrap()).into()],
                None,
                true,
            ),
            (
                vec![Some(Json::from_str(r#"["1", "2"]"#).unwrap()).into()],
                None,
                true,
            ),
            // Tests without path expression
            (
                vec![Some(Json::from_str(r#"{}"#).unwrap()).into()],
                Some(Json::from_str("[]").unwrap()).into(),
                true,
            ),
            (
                vec![Some(Json::from_str(r#"{"a": 1}"#).unwrap()).into()],
                Some(Json::from_str(r#"["a"]"#).unwrap()).into(),
                true,
            ),
            (
                vec![Some(Json::from_str(r#"{"a": 1, "b": 2}"#).unwrap()).into()],
                Some(Json::from_str(r#"["a", "b"]"#).unwrap()).into(),
                true,
            ),
            (
                vec![Some(Json::from_str(r#"{"a": {"c": 3}, "b": 2}"#).unwrap()).into()],
                Some(Json::from_str(r#"["a", "b"]"#).unwrap()).into(),
                true,
            ),
            // Tests with path expression
            (
                vec![
                    Some(Json::from_str(r#"{"a": 1}"#).unwrap()).into(),
                    Some(b"$.a".to_vec()).into(),
                ],
                None,
                true,
            ),
            (
                vec![
                    Some(Json::from_str(r#"{"a": {"c": 3}, "b": 2}"#).unwrap()).into(),
                    Some(b"$.a".to_vec()).into(),
                ],
                Some(Json::from_str(r#"["c"]"#).unwrap()).into(),
                true,
            ),
            (
                vec![
                    Some(Json::from_str(r#"{"a": {"c": 3}, "b": 2}"#).unwrap()).into(),
                    None::<Bytes>.into(),
                ],
                None,
                true,
            ),
            (
                vec![
                    Some(Json::from_str(r#"{"a": {"c": 3}, "b": 2}"#).unwrap()).into(),
                    Some(b"$.a.c".to_vec()).into(),
                ],
                None,
                true,
            ),
            // Tests path expression contains any asterisk
            (
                vec![
                    Some(Json::from_str(r#"{}"#).unwrap()).into(),
                    Some(b"$.*".to_vec()).into(),
                ],
                None,
                false,
            ),
            (
                vec![
                    Some(Json::from_str(r#"{"a": 1}"#).unwrap()).into(),
                    Some(b"$.*".to_vec()).into(),
                ],
                None,
                false,
            ),
            (
                vec![
                    Some(Json::from_str(r#"{"a": {"c": 3}, "b": 2}"#).unwrap()).into(),
                    Some(b"$.*".to_vec()).into(),
                ],
                None,
                false,
            ),
            (
                vec![
                    Some(Json::from_str(r#"{"a": {"c": 3}, "b": 2}"#).unwrap()).into(),
                    Some(b"$.a.*".to_vec()).into(),
                ],
                None,
                false,
            ),
            // Tests path expression does not identify a section of the target document
            (
                vec![
                    Some(Json::from_str(r#"{"a": 1}"#).unwrap()).into(),
                    Some(b"$.b".to_vec()).into(),
                ],
                None,
                true,
            ),
            (
                vec![
                    Some(Json::from_str(r#"{"a": {"c": 3}, "b": 2}"#).unwrap()).into(),
                    Some(b"$.c".to_vec()).into(),
                ],
                None,
                true,
            ),
            (
                vec![
                    Some(Json::from_str(r#"{"a": {"c": 3}, "b": 2}"#).unwrap()).into(),
                    Some(b"$.a.d".to_vec()).into(),
                ],
                None,
                true,
            ),
        ];
        for (vargs, expected, is_success) in cases {
            let output = RpnFnScalarEvaluator::new().push_params(vargs.clone());
            let output = if vargs.len() == 1 {
                output.evaluate(ScalarFuncSig::JsonKeysSig)
            } else {
                output.evaluate(ScalarFuncSig::JsonKeys2ArgsSig)
            };
            if is_success {
                assert_eq!(output.unwrap(), expected, "{:?}", vargs);
            } else {
                assert!(output.is_err());
            }
        }
    }
}
