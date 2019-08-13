// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use tidb_query_codegen::rpn_fn;

use crate::codec::data_type::*;
use crate::codec::mysql::json::*;
use crate::Result;

use std::collections::BTreeMap;

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

fn json_validator(expr: &tipb::Expr) -> Result<()> {
    for chunk in expr.get_children().chunks(2) {
        if chunk.len() == 1 {
            return Err(other_err!(
                "Incorrect parameter count in the call to native function 'JSON_OBJECT'"
            ));
        } else {
            super::function::validate_expr_return_type(&chunk[0], Bytes::EVAL_TYPE)?;
            super::function::validate_expr_return_type(&chunk[1], Json::EVAL_TYPE)?;
        }
    }
    Ok(())
}

/// Required args like `&[(&Option<Byte>, &Option<Json>)]`.
#[rpn_fn(raw_varg, extra_validator = json_validator)]
#[inline]
fn json_object(raw_args: &[ScalarValueRef]) -> Result<Option<Json>> {
    let mut pairs = BTreeMap::new();
    for chunk in raw_args.chunks(2) {
        // chunk.len() must be 1 or 2 here.
        if chunk.len() == 1 {
            return Err(other_err!(
                "Incorrect parameter count in the call to native function 'JSON_OBJECT'"
            ));
        }
        let key: &Option<Bytes> = Evaluable::borrow_scalar_value_ref(&chunk[0]);
        let key = match key {
            // json_object should raise an error if key is None(NULL)
            None => Err(other_err!(
                "Data truncation: JSON documents may not contain NULL member names."
            )),
            Some(v) => {
                String::from_utf8(v.to_owned()).map_err(|e| crate::codec::Error::from(e).into())
            }
        }?;

        let value: &Option<Json> = Evaluable::borrow_scalar_value_ref(&chunk[1]);
        let value = match value {
            None => Json::None,
            Some(v) => v.to_owned(),
        };
        pairs.insert(key, value);
    }
    Ok(Some(Json::Object(pairs)))
}

macro_rules! parse_opt {
    ($expr:expr) => {{
        match $expr {
            Some(ref v) => v.to_owned().clone(),
            None => return Ok(None),
        }
    }};
}

// According to mysql 5.7,
// arguments of json_merge should not be less than 2.
#[rpn_fn(varg, min_args = 2)]
#[inline]
pub fn json_merge(args: &[&Option<Json>]) -> Result<Option<Json>> {
    // min_args = 2, so it's ok to call args[0]
    let mut base_json = parse_opt!(args[0]);
    for json_to_merge in &args[1..] {
        base_json = base_json.merge(parse_opt!(json_to_merge));
    }
    Ok(Some(base_json))
}

#[cfg(test)]
mod tests {
    use super::*;

    use tipb::ScalarFuncSig;

    use crate::rpn_expr::types::test_util::RpnFnScalarEvaluator;

    use std::str::FromStr;

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
                new_vargs.push(ScalarValue::from(key.clone()));
                new_vargs.push(ScalarValue::from(value.clone()));
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
                ScalarValue::from(Bytes::from("1")),
            ],
        ];

        for err_args in err_cases {
            let output: Result<Option<Json>> = RpnFnScalarEvaluator::new()
                .push_params(err_args)
                .evaluate(ScalarFuncSig::JsonObjectSig);

            assert!(output.is_err());
        }
    }
}
