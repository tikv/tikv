// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

use super::{Error, EvalContext, Expression, Result, ScalarFunc};
use crate::codec::mysql::json::{parse_json_path_expr, ModifyType, PathExpression};
use crate::codec::mysql::Json;
use crate::codec::Datum;
use std::borrow::Cow;
use std::collections::BTreeMap;

impl ScalarFunc {
    #[inline]
    pub fn json_depth<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<i64>> {
        let j = try_opt!(self.children[0].eval_json(ctx, row));
        Ok(Some(j.depth()))
    }

    #[inline]
    pub fn json_type<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, [u8]>>> {
        let j = try_opt!(self.children[0].eval_json(ctx, row));
        Ok(Some(Cow::Borrowed(j.json_type())))
    }

    #[inline]
    pub fn json_unquote<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, [u8]>>> {
        let j = try_opt!(self.children[0].eval_json(ctx, row));
        j.unquote()
            .map_err(Error::from)
            .map(|s| Some(Cow::Owned(s.into_bytes())))
    }

    pub fn json_array<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Json>>> {
        let parser = JsonFuncArgsParser::new(row);
        let elems = try_opt!(self
            .children
            .iter()
            .map(|e| parser.get_json(ctx, e))
            .collect());
        Ok(Some(Cow::Owned(Json::Array(elems))))
    }

    pub fn json_object<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Json>>> {
        let mut pairs = BTreeMap::new();
        let parser = JsonFuncArgsParser::new(row);
        for chunk in self.children.chunks(2) {
            let key = try_opt!(chunk[0].eval_string_and_decode(ctx, row)).into_owned();
            let val = try_opt!(parser.get_json(ctx, &chunk[1]));
            pairs.insert(key, val);
        }
        Ok(Some(Cow::Owned(Json::Object(pairs))))
    }

    pub fn json_extract<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Json>>> {
        // TODO: We can cache the PathExpressions if children are Constant.
        let j = try_opt!(self.children[0].eval_json(ctx, row));
        let parser = JsonFuncArgsParser::new(row);
        let path_exprs: Vec<_> = try_opt!(parser.get_path_exprs(ctx, &self.children[1..]));
        Ok(j.extract(&path_exprs).map(Cow::Owned))
    }

    pub fn json_length<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<i64>> {
        let j = try_opt!(self.children[0].eval_json(ctx, row));
        let parser = JsonFuncArgsParser::new(row);
        let path_exprs: Vec<_> = match parser.get_path_exprs(ctx, &self.children[1..])? {
            Some(list) => list,
            None => return Ok(None),
        };
        Ok(j.json_length(&path_exprs))
    }

    #[inline]
    pub fn json_set<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Json>>> {
        self.json_modify(ctx, row, ModifyType::Set)
    }

    #[inline]
    pub fn json_insert<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Json>>> {
        self.json_modify(ctx, row, ModifyType::Insert)
    }

    #[inline]
    pub fn json_replace<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Json>>> {
        self.json_modify(ctx, row, ModifyType::Replace)
    }

    pub fn json_remove<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Json>>> {
        let mut j = try_opt!(self.children[0].eval_json(ctx, row)).into_owned();
        let parser = JsonFuncArgsParser::new(row);
        let path_exprs: Vec<_> = try_opt!(parser.get_path_exprs(ctx, &self.children[1..]));
        j.remove(&path_exprs)
            .map(|_| Some(Cow::Owned(j)))
            .map_err(Error::from)
    }

    pub fn json_merge<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Json>>> {
        let parser = JsonFuncArgsParser::new(row);
        let mut head = try_opt!(self.children[0].eval_json(ctx, row)).into_owned();
        for e in &self.children[1..] {
            let suffix = try_opt!(parser.get_json_not_none(ctx, e));
            head = head.merge(suffix);
        }
        Ok(Some(Cow::Owned(head)))
    }

    fn json_modify<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
        mt: ModifyType,
    ) -> Result<Option<Cow<'a, Json>>> {
        let mut j = try_opt!(self.children[0].eval_json(ctx, row)).into_owned();
        let parser = JsonFuncArgsParser::new(row);
        let mut path_exprs = Vec::with_capacity(self.children.len() / 2);
        let mut values = Vec::with_capacity(self.children.len() / 2);
        for chunk in self.children[1..].chunks(2) {
            path_exprs.push(try_opt!(parser.get_path_expr(ctx, &chunk[0])));
            values.push(try_opt!(parser.get_json(ctx, &chunk[1])));
        }
        j.modify(&path_exprs, values, mt)
            .map(|_| Some(Cow::Owned(j)))
            .map_err(Error::from)
    }
}

struct JsonFuncArgsParser<'a> {
    row: &'a [Datum],
}

impl<'a> JsonFuncArgsParser<'a> {
    #[inline]
    fn new(row: &'a [Datum]) -> Self {
        JsonFuncArgsParser { row }
    }

    fn get_path_expr(
        &self,
        ctx: &mut EvalContext,
        e: &Expression,
    ) -> Result<Option<PathExpression>> {
        let s = try_opt!(e.eval_string_and_decode(ctx, self.row));
        let expr = parse_json_path_expr(&s)?;
        Ok(Some(expr))
    }

    fn get_path_exprs(
        &self,
        ctx: &mut EvalContext,
        es: &[Expression],
    ) -> Result<Option<Vec<PathExpression>>> {
        es.iter().map(|e| self.get_path_expr(ctx, e)).collect()
    }

    fn get_json(&self, ctx: &mut EvalContext, e: &Expression) -> Result<Option<Json>> {
        let j = e
            .eval_json(ctx, self.row)?
            .map_or(Json::None, Cow::into_owned);
        Ok(Some(j))
    }

    fn get_json_not_none(&self, ctx: &mut EvalContext, e: &Expression) -> Result<Option<Json>> {
        let j = try_opt!(e.eval_json(ctx, self.row)).into_owned();
        Ok(Some(j))
    }
}

#[cfg(test)]
mod tests {
    use crate::codec::mysql::Json;
    use crate::codec::Datum;
    use crate::expr::tests::{datum_expr, make_null_datums, scalar_func_expr};
    use crate::expr::{EvalContext, Expression};
    use tipb::ScalarFuncSig;

    #[test]
    fn test_json_length() {
        let cases = vec![
            (None, None, None),
            (None, Some(Datum::Null), None),
            (Some(r#"{}"#), Some(Datum::Null), None),
            (Some("null"), None, Some(1)),
            (
                Some(r#"{"a":{"a":1},"b":2}"#),
                Some(Datum::Bytes(b"$".to_vec())),
                Some(2),
            ),
            (Some("1"), None, Some(1)),
            (
                Some(r#"{"a": [1, 2, {"aa": "xx"}]}"#),
                Some(Datum::Bytes(b"$.*".to_vec())),
                None,
            ),
            (
                Some(r#"{"a":{"a":1},"b":2}"#),
                Some(Datum::Bytes(b"$".to_vec())),
                Some(2),
            ),
            // Tests with path expression
            (
                Some(r#"[1,2,[1,[5,[3]]]]"#),
                Some(Datum::Bytes(b"$[2]".to_vec())),
                Some(2),
            ),
            (
                Some(r#"[{"a":1}]"#),
                Some(Datum::Bytes(b"$".to_vec())),
                Some(1),
            ),
            (
                Some(r#"[{"a":1,"b":2}]"#),
                Some(Datum::Bytes(b"$[0].a".to_vec())),
                Some(1),
            ),
            (
                Some(r#"{"a":{"a":1},"b":2}"#),
                Some(Datum::Bytes(b"$".to_vec())),
                Some(2),
            ),
            (
                Some(r#"{"a":{"a":1},"b":2}"#),
                Some(Datum::Bytes(b"$.a".to_vec())),
                Some(1),
            ),
            (
                Some(r#"{"a":{"a":1},"b":2}"#),
                Some(Datum::Bytes(b"$.a.a".to_vec())),
                Some(1),
            ),
            (
                Some(r#"{"a": [1, 2, {"aa": "xx"}]}"#),
                Some(Datum::Bytes(b"$.a[2].aa".to_vec())),
                Some(1),
            ),
            // Tests without path expression
            (Some(r#"{}"#), None, Some(0)),
            (Some(r#"{"a":1}"#), None, Some(1)),
            (Some(r#"{"a":[1]}"#), None, Some(1)),
            (Some(r#"{"b":2, "c":3}"#), None, Some(2)),
            (Some(r#"[1]"#), None, Some(1)),
            (Some(r#"[1,2]"#), None, Some(2)),
            (Some(r#"[1,2,[1,3]]"#), None, Some(3)),
            (Some(r#"[1,2,[1,[5,[3]]]]"#), None, Some(3)),
            (Some(r#"[1,2,[1,[5,{"a":[2,3]}]]]"#), None, Some(3)),
            (Some(r#"[{"a":1}]"#), None, Some(1)),
            (Some(r#"[{"a":1,"b":2}]"#), None, Some(1)),
            (Some(r#"[{"a":{"a":1},"b":2}]"#), None, Some(1)),
            // Tests path expression contains any asterisk
            (
                Some(r#"{"a": [1, 2, {"aa": "xx"}]}"#),
                Some(Datum::Bytes(b"$.*".to_vec())),
                None,
            ),
            (
                Some(r#"{"a": [1, 2, {"aa": "xx"}]}"#),
                Some(Datum::Bytes(b"$[*]".to_vec())),
                None,
            ),
            (
                Some(r#"{"a": [1, 2, {"aa": "xx"}]}"#),
                Some(Datum::Bytes(b"$**.a".to_vec())),
                None,
            ),
            // Tests path expression does not identify a section of the target document
            (
                Some(r#"{"a": [1, 2, {"aa": "xx"}]}"#),
                Some(Datum::Bytes(b"$.c".to_vec())),
                None,
            ),
            (
                Some(r#"{"a": [1, 2, {"aa": "xx"}]}"#),
                Some(Datum::Bytes(b"$.a[3]".to_vec())),
                None,
            ),
            (
                Some(r#"{"a": [1, 2, {"aa": "xx"}]}"#),
                Some(Datum::Bytes(b"$.a[2].b".to_vec())),
                None,
            ),
        ];
        let mut ctx = EvalContext::default();
        for (input, param, exp) in cases {
            let json = datum_expr(match input {
                None => Datum::Null,
                Some(s) => Datum::Json(s.parse().unwrap()),
            });
            let op = if let Some(b) = param {
                scalar_func_expr(ScalarFuncSig::JsonLengthSig, &[json, datum_expr(b)])
            } else {
                scalar_func_expr(ScalarFuncSig::JsonLengthSig, &[json])
            };
            let op = Expression::build(&mut ctx, op).unwrap();
            let got = op.eval(&mut ctx, &[]).unwrap();
            let exp = match exp {
                None => Datum::Null,
                Some(e) => Datum::I64(e),
            };
            assert_eq!(got, exp);
        }
    }

    #[test]
    fn test_json_depth() {
        let cases = vec![
            (None, None),
            (Some("null"), Some(1)),
            (Some("[true, 2017]"), Some(2)),
            (Some(r#"{"a": {"a1": [3]}, "b": {"b1": {"c": {"d": [5]}}}}"#), Some(6)),
            (Some("{}"), Some(1)),
            (Some("[]"), Some(1)),
            (Some("true"), Some(1)),
            (Some("1"), Some(1)),
            (Some("-1"), Some(1)),
            (Some(r#""a""#), Some(1)),
            (Some(r#"[10, 20]"#), Some(2)),
            (Some(r#"[[], {}]"#),Some(2) ),
            (Some(r#"[10, {"a": 20}]"#), Some(3)),
            (Some(r#"[[2], 3, [[[4]]]]"#), Some(5)),
            (Some(r#"{"Name": "Homer"}"#), Some(2)),
            (Some(r#"[10, {"a": 20}]"#), Some(3)),
            (Some(r#"{"Person": {"Name": "Homer", "Age": 39, "Hobbies": ["Eating", "Sleeping"]} }"#), Some(4)),
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
        let mut ctx = EvalContext::default();
        for (input, exp) in cases {
            let input = match input {
                None => Datum::Null,
                Some(s) => Datum::Json(s.parse().unwrap()),
            };
            let exp = match exp {
                None => Datum::Null,
                Some(s) => Datum::I64(s.to_owned()),
            };
            let arg = datum_expr(input);
            let op = scalar_func_expr(ScalarFuncSig::JsonDepthSig, &[arg]);
            let op = Expression::build(&mut ctx, op).unwrap();
            let got = op.eval(&mut ctx, &[]).unwrap();
            assert_eq!(got, exp);
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
            (Some(r#"3.14"#), Some("DOUBLE")),
            (Some(r#"9223372036854775808"#), Some("DOUBLE")),
            (Some(r#"[1, 2, 3]"#), Some("ARRAY")),
            (Some(r#"{"name": 123}"#), Some("OBJECT")),
        ];
        let mut ctx = EvalContext::default();
        for (input, exp) in cases {
            let input = match input {
                None => Datum::Null,
                Some(s) => Datum::Json(s.parse().unwrap()),
            };
            let exp = match exp {
                None => Datum::Null,
                Some(s) => Datum::Bytes(s.to_owned().into_bytes()),
            };

            let arg = datum_expr(input);
            let op = scalar_func_expr(ScalarFuncSig::JsonTypeSig, &[arg]);
            let op = Expression::build(&mut ctx, op).unwrap();
            let got = op.eval(&mut ctx, &[]).unwrap();
            assert_eq!(got, exp);
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
        let mut ctx = EvalContext::default();
        for (input, parse, exp) in cases {
            let input = match input {
                None => Datum::Null,
                Some(s) => {
                    if parse {
                        Datum::Json(s.parse().unwrap())
                    } else {
                        Datum::Json(Json::String(s.to_owned()))
                    }
                }
            };
            let exp = match exp {
                None => Datum::Null,
                Some(s) => Datum::Bytes(s.to_owned().into_bytes()),
            };

            let arg = datum_expr(input);
            let op = scalar_func_expr(ScalarFuncSig::JsonUnquoteSig, &[arg]);
            let op = Expression::build(&mut ctx, op).unwrap();
            let got = op.eval(&mut ctx, &[]).unwrap();
            assert_eq!(got, exp);
        }
    }

    #[test]
    fn test_json_object() {
        let cases = vec![
            (vec![], Datum::Json(r#"{}"#.parse().unwrap())),
            (
                vec![Datum::Bytes(b"1".to_vec()), Datum::Null],
                Datum::Json(r#"{"1":null}"#.parse().unwrap()),
            ),
            (
                vec![
                    Datum::Bytes(b"1".to_vec()),
                    Datum::Null,
                    Datum::Bytes(b"2".to_vec()),
                    Datum::Json(Json::String("sdf".to_owned())),
                    Datum::Bytes(b"k1".to_vec()),
                    Datum::Json(Json::String("v1".to_owned())),
                ],
                Datum::Json(r#"{"1":null,"2":"sdf","k1":"v1"}"#.parse().unwrap()),
            ),
        ];
        let mut ctx = EvalContext::default();
        for (inputs, exp) in cases {
            let args = inputs.into_iter().map(datum_expr).collect::<Vec<_>>();
            let op = scalar_func_expr(ScalarFuncSig::JsonObjectSig, &args);
            let op = Expression::build(&mut ctx, op).unwrap();
            let got = op.eval(&mut ctx, &[]).unwrap();
            assert_eq!(got, exp);
        }
    }

    #[test]
    fn test_json_array() {
        let cases = vec![
            (vec![], Datum::Json(r#"[]"#.parse().unwrap())),
            (
                vec![Datum::Json("1".parse().unwrap()), Datum::Null],
                Datum::Json(r#"[1, null]"#.parse().unwrap()),
            ),
            (
                vec![
                    Datum::Json("1".parse().unwrap()),
                    Datum::Null,
                    Datum::Json("2".parse().unwrap()),
                    Datum::Json(Json::String("sdf".to_owned())),
                    Datum::Json(Json::String("k1".to_owned())),
                    Datum::Json(Json::String("v1".to_owned())),
                ],
                Datum::Json(r#"[1, null, 2, "sdf", "k1", "v1"]"#.parse().unwrap()),
            ),
        ];
        let mut ctx = EvalContext::default();
        for (inputs, exp) in cases {
            let args = inputs.into_iter().map(datum_expr).collect::<Vec<_>>();
            let op = scalar_func_expr(ScalarFuncSig::JsonArraySig, &args);
            let op = Expression::build(&mut ctx, op).unwrap();
            let got = op.eval(&mut ctx, &[]).unwrap();
            assert_eq!(got, exp);
        }
    }

    #[test]
    fn test_json_modify() {
        let cases = vec![
            (
                ScalarFuncSig::JsonSetSig,
                vec![Datum::Null, Datum::Null, Datum::Null],
                Datum::Null,
            ),
            (
                ScalarFuncSig::JsonSetSig,
                vec![
                    Datum::Json(Json::I64(9)),
                    Datum::Bytes(b"$[1]".to_vec()),
                    Datum::Json(Json::U64(3)),
                ],
                Datum::Json(r#"[9,3]"#.parse().unwrap()),
            ),
            (
                ScalarFuncSig::JsonInsertSig,
                vec![
                    Datum::Json(Json::I64(9)),
                    Datum::Bytes(b"$[1]".to_vec()),
                    Datum::Json(Json::U64(3)),
                ],
                Datum::Json(r#"[9,3]"#.parse().unwrap()),
            ),
            (
                ScalarFuncSig::JsonReplaceSig,
                vec![
                    Datum::Json(Json::I64(9)),
                    Datum::Bytes(b"$[1]".to_vec()),
                    Datum::Json(Json::U64(3)),
                ],
                Datum::Json(r#"9"#.parse().unwrap()),
            ),
            (
                ScalarFuncSig::JsonSetSig,
                vec![
                    Datum::Json(r#"{"a":"x"}"#.parse().unwrap()),
                    Datum::Bytes(b"$.a".to_vec()),
                    Datum::Null,
                ],
                Datum::Json(r#"{"a":null}"#.parse().unwrap()),
            ),
        ];
        let mut ctx = EvalContext::default();
        for (sig, inputs, exp) in cases {
            let args: Vec<_> = inputs.into_iter().map(datum_expr).collect();
            let op = scalar_func_expr(sig, &args);
            let op = Expression::build(&mut ctx, op).unwrap();
            let got = op.eval(&mut ctx, &[]).unwrap();
            assert_eq!(got, exp);
        }
    }

    #[test]
    fn test_json_merge() {
        let cases = vec![
            (vec![Datum::Null, Datum::Null], Datum::Null),
            (
                vec![
                    Datum::Json("{}".parse().unwrap()),
                    Datum::Json("[]".parse().unwrap()),
                ],
                Datum::Json("[{}]".parse().unwrap()),
            ),
            (
                vec![
                    Datum::Json("{}".parse().unwrap()),
                    Datum::Json("[]".parse().unwrap()),
                    Datum::Json("3".parse().unwrap()),
                    Datum::Json(r#""4""#.parse().unwrap()),
                ],
                Datum::Json(r#"[{}, 3, "4"]"#.parse().unwrap()),
            ),
        ];
        let mut ctx = EvalContext::default();
        for (inputs, exp) in cases {
            let args: Vec<_> = inputs.into_iter().map(datum_expr).collect();
            let op = scalar_func_expr(ScalarFuncSig::JsonMergeSig, &args);
            let op = Expression::build(&mut ctx, op).unwrap();
            let got = op.eval(&mut ctx, &[]).unwrap();
            assert_eq!(got, exp);
        }
    }

    #[test]
    fn test_json_invalid_arguments() {
        let cases = vec![
            (ScalarFuncSig::JsonObjectSig, make_null_datums(3)),
            (ScalarFuncSig::JsonSetSig, make_null_datums(4)),
            (ScalarFuncSig::JsonInsertSig, make_null_datums(6)),
            (ScalarFuncSig::JsonReplaceSig, make_null_datums(8)),
        ];
        let mut ctx = EvalContext::default();
        for (sig, args) in cases {
            let args: Vec<_> = args.into_iter().map(datum_expr).collect();
            let op = Expression::build(&mut ctx, scalar_func_expr(sig, &args));
            assert!(op.is_err());
        }
    }
}
