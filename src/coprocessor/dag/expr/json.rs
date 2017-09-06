// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

use std::borrow::Cow;
use std::collections::BTreeMap;
use coprocessor::codec::Datum;
use coprocessor::codec::mysql::Json;
use coprocessor::codec::mysql::json::{parse_json_path_expr, ModifyType, PathExpression};
use super::{Error, Expression, FnCall, Result, StatementContext};

impl FnCall {
    #[inline]
    pub fn json_type<'a, 'b: 'a>(
        &'b self,
        ctx: &StatementContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, [u8]>>> {
        let j = try_opt!(self.children[0].eval_json(ctx, row));
        Ok(Some(Cow::Borrowed(j.json_type())))
    }

    #[inline]
    pub fn json_unquote<'a, 'b: 'a>(
        &'b self,
        ctx: &StatementContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, [u8]>>> {
        let j = try_opt!(self.children[0].eval_json(ctx, row));
        j.unquote()
            .map_err(Error::from)
            .map(|s| Some(Cow::Owned(s.into_bytes())))
    }

    pub fn json_array<'a, 'b: 'a>(
        &'b self,
        ctx: &StatementContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Json>>> {
        let parser = JsonFuncArgsParser::new(ctx, row);
        let elems = try_opt!(parser.get_jsons(self.children.iter(), true));
        Ok(Some(Cow::Owned(Json::Array(elems))))
    }

    pub fn json_object<'a, 'b: 'a>(
        &'b self,
        ctx: &StatementContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Json>>> {
        let mut pairs = BTreeMap::new();
        if !self.children.is_empty() {
            let parser = JsonFuncArgsParser::new(ctx, row);
            let keys = try_opt!(parser.get_strings(self.children.iter().step_by(2)));
            let elems = try_opt!(parser.get_jsons(self.children[1..].iter().step_by(2), true));
            if keys.len() != elems.len() {
                return Err(box_err!("Incorrect parameter count for 'json_object'"));
            }
            pairs.extend(keys.into_iter().zip(elems.into_iter()));
        }
        Ok(Some(Cow::Owned(Json::Object(pairs))))
    }

    pub fn json_extract<'a, 'b: 'a>(
        &'b self,
        ctx: &StatementContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Json>>> {
        // TODO: We can cache the PathExpressions if children are Constant.
        let j = try_opt!(self.children[0].eval_json(ctx, row));
        let parser = JsonFuncArgsParser::new(ctx, row);
        let path_exprs = try_opt!(parser.get_path_exprs(self.children[1..].iter()));
        Ok(j.extract(&path_exprs).map(Cow::Owned))
    }

    #[inline]
    pub fn json_set<'a, 'b: 'a>(
        &'b self,
        ctx: &StatementContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Json>>> {
        self.json_modify(ctx, row, ModifyType::Set)
    }

    #[inline]
    pub fn json_insert<'a, 'b: 'a>(
        &'b self,
        ctx: &StatementContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Json>>> {
        self.json_modify(ctx, row, ModifyType::Insert)
    }

    #[inline]
    pub fn json_replace<'a, 'b: 'a>(
        &'b self,
        ctx: &StatementContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Json>>> {
        self.json_modify(ctx, row, ModifyType::Replace)
    }

    pub fn json_remove<'a, 'b: 'a>(
        &'b self,
        ctx: &StatementContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Json>>> {
        let mut j = try_opt!(self.children[0].eval_json(ctx, row)).into_owned();
        let parser = JsonFuncArgsParser::new(ctx, row);
        let path_exprs = try_opt!(parser.get_path_exprs(self.children[1..].iter()));
        j.remove(&path_exprs)
            .map(|_| Some(Cow::Owned(j)))
            .map_err(Error::from)
    }

    pub fn json_merge<'a, 'b: 'a>(
        &'b self,
        ctx: &StatementContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Json>>> {
        let parser = JsonFuncArgsParser::new(ctx, row);
        let head = try_opt!(self.children[0].eval_json(ctx, row)).into_owned();
        let suffixes = try_opt!(parser.get_jsons(self.children[1..].iter(), false));
        Ok(Some(Cow::Owned(head.merge(suffixes))))
    }

    fn json_modify<'a, 'b: 'a>(
        &'b self,
        ctx: &StatementContext,
        row: &'a [Datum],
        mt: ModifyType,
    ) -> Result<Option<Cow<'a, Json>>> {
        let mut j = try_opt!(self.children[0].eval_json(ctx, row)).into_owned();
        let parser = JsonFuncArgsParser::new(ctx, row);
        let path_exprs = try_opt!(parser.get_path_exprs(self.children[1..].iter().step_by(2)));
        let values = try_opt!(parser.get_jsons(self.children[2..].iter().step_by(2), true));
        j.modify(&path_exprs, values, mt)
            .map(|_| Some(Cow::Owned(j)))
            .map_err(Error::from)
    }
}

struct JsonFuncArgsParser<'a> {
    ctx: &'a StatementContext,
    row: &'a [Datum],
}

impl<'a> JsonFuncArgsParser<'a> {
    #[inline]
    fn new(ctx: &'a StatementContext, row: &'a [Datum]) -> Self {
        JsonFuncArgsParser { ctx: ctx, row: row }
    }

    #[inline]
    fn parse<'b: 'a, It, T, F>(args: It, f: F) -> Result<Option<Vec<T>>>
    where
        It: Iterator<Item = &'b Expression>,
        F: Fn(&'b Expression) -> Result<Option<T>>,
    {
        args.map(f).collect::<Result<Option<Vec<_>>>>()
    }

    fn get_path_exprs<'b: 'a, It>(&'a self, args: It) -> Result<Option<Vec<PathExpression>>>
    where
        It: Iterator<Item = &'b Expression>,
    {
        let func = |e: &'b Expression| {
            let s = try_opt!(e.eval_string_and_decode(self.ctx, self.row));
            parse_json_path_expr(&s).map_err(Error::from).map(Some)
        };
        JsonFuncArgsParser::parse(args, func)
    }

    fn get_jsons<'b: 'a, It>(&'a self, args: It, nullable: bool) -> Result<Option<Vec<Json>>>
    where
        It: Iterator<Item = &'b Expression>,
    {
        if !nullable {
            let func = |e: &'b Expression| {
                let j = try_opt!(e.eval_json(self.ctx, self.row)).into_owned();
                Ok(Some(j))
            };
            JsonFuncArgsParser::parse(args, func)
        } else {
            let func = |e: &'b Expression| {
                let j = try!(e.eval_json(self.ctx, self.row))
                    .map(Cow::into_owned)
                    .unwrap_or(Json::None);
                Ok(Some(j))
            };
            JsonFuncArgsParser::parse(args, func)
        }
    }

    fn get_strings<'b: 'a, It>(&'a self, args: It) -> Result<Option<Vec<String>>>
    where
        It: Iterator<Item = &'b Expression>,
    {
        let func = |e: &'b Expression| {
            let bytes = try_opt!(e.eval_string(self.ctx, self.row)).into_owned();
            String::from_utf8(bytes).map(Some).map_err(Error::from)
        };
        JsonFuncArgsParser::parse(args, func)
    }
}

#[cfg(test)]
mod test {
    use tipb::expression::ScalarFuncSig;
    use coprocessor::codec::Datum;
    use coprocessor::codec::mysql::Json;
    use coprocessor::dag::expr::{Expression, StatementContext};
    use coprocessor::dag::expr::test::fncall_expr;
    use coprocessor::select::xeval::evaluator::test::datum_expr;

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
        let ctx = StatementContext::default();
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
            let op = fncall_expr(ScalarFuncSig::JsonTypeSig, &[arg]);
            let op = Expression::build(op, &ctx).unwrap();
            let got = op.eval(&ctx, &[]).unwrap();
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
        let ctx = StatementContext::default();
        for (input, parse, exp) in cases {
            let input = match input {
                None => Datum::Null,
                Some(s) => if parse {
                    Datum::Json(s.parse().unwrap())
                } else {
                    Datum::Json(Json::String(s.to_owned()))
                },
            };
            let exp = match exp {
                None => Datum::Null,
                Some(s) => Datum::Bytes(s.to_owned().into_bytes()),
            };

            let arg = datum_expr(input);
            let op = fncall_expr(ScalarFuncSig::JsonUnquoteSig, &[arg]);
            let op = Expression::build(op, &ctx).unwrap();
            let got = op.eval(&ctx, &[]).unwrap();
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
        let ctx = StatementContext::default();
        for (inputs, exp) in cases {
            let args = inputs.into_iter().map(datum_expr).collect::<Vec<_>>();
            let op = fncall_expr(ScalarFuncSig::JsonObjectSig, &args);
            let op = Expression::build(op, &ctx).unwrap();
            let got = op.eval(&ctx, &[]).unwrap();
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
        let ctx = StatementContext::default();
        for (inputs, exp) in cases {
            let args = inputs.into_iter().map(datum_expr).collect::<Vec<_>>();
            let op = fncall_expr(ScalarFuncSig::JsonArraySig, &args);
            let op = Expression::build(op, &ctx).unwrap();
            let got = op.eval(&ctx, &[]).unwrap();
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
        let ctx = StatementContext::default();
        for (sig, inputs, exp) in cases {
            let args: Vec<_> = inputs.into_iter().map(datum_expr).collect();
            let op = fncall_expr(sig, &args);
            let op = Expression::build(op, &ctx).unwrap();
            let got = op.eval(&ctx, &[]).unwrap();
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
        let ctx = StatementContext::default();
        for (inputs, exp) in cases {
            let args: Vec<_> = inputs.into_iter().map(datum_expr).collect();
            let op = fncall_expr(ScalarFuncSig::JsonMergeSig, &args);
            let op = Expression::build(op, &ctx).unwrap();
            let got = op.eval(&ctx, &[]).unwrap();
            assert_eq!(got, exp);
        }
    }
}
