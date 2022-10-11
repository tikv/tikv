// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{borrow::Cow, collections::HashSet};

use regex::Regex;
use tidb_query_codegen::rpn_fn;
use tidb_query_common::Result;
use tidb_query_datatype::codec::{collation::Collator, data_type::*, Error};
use tipb::{Expr, ExprType};

const PATTERN_IDX: usize = 1;
const LIKE_MATCH_IDX: usize = 2;
const SUBSTR_MATCH_IDX: usize = 4;
const INSTR_MATCH_IDX: usize = 5;
const REPLACE_MATCH_IDX: usize = 5;

fn invalid_pos_error(pos: i64, count: usize) -> Error {
    Error::regexp_error(format!("Invalid pos: {} in regexp, count: {}", pos, count))
}

fn is_valid_match_type(m: char) -> bool {
    matches!(m, 'i' | 'c' | 'm' | 's')
}

fn get_match_type<C: Collator>(match_type: &[u8]) -> Result<String> {
    let match_type = std::str::from_utf8(match_type)?;
    let mut flag_set = HashSet::<char>::new();

    if C::IS_CASE_INSENSITIVE {
        flag_set.insert('i');
    }

    for m in match_type.chars() {
        if !is_valid_match_type(m) {
            let err = format!("Invalid match type: {} in regexp", m);
            return Err(Error::regexp_error(err).into());
        }
        if m == 'c' {
            // re2 is case-sensitive by default, so we only need to delete 'i' flag
            // to enable the case-sensitive for the regexp.
            flag_set.remove(&'i');
            continue;
        }
        flag_set.insert(m);
    }

    let mut flag = String::new();
    for m in flag_set {
        flag.push(m);
    }

    Ok(flag)
}

fn build_regexp<C: Collator>(pattern: &[u8], match_type: &[u8]) -> Result<Regex> {
    if pattern.is_empty() {
        return Err(Error::regexp_error("Empty pattern is invalid in regexp".into()).into());
    }

    let match_type = get_match_type::<C>(match_type)?;

    let pattern = String::from_utf8(pattern.to_vec())?;
    let pattern_with_tp = if !match_type.is_empty() {
        format!("(?{}){}", match_type, pattern)
    } else {
        pattern
    };

    Regex::new(&pattern_with_tp)
        .map_err(|e| Error::regexp_error(format!("Invalid regexp pattern: {:?}", e)).into())
}

fn build_regexp_from_args<C: Collator>(
    args: &[ScalarValueRef<'_>],
    match_idx: usize,
) -> Result<Option<Regex>> {
    let pattern = match args[PATTERN_IDX].as_bytes() {
        Some(b) => b,
        None => return Ok(None),
    };

    let match_type = if args.len() > match_idx {
        match args[match_idx].as_bytes() {
            Some(b) => b,
            None => return Ok(None),
        }
    } else {
        b""
    };

    build_regexp::<C>(pattern, match_type).map(Some)
}

fn init_regexp_data<C: Collator, const N: usize>(expr: &mut Expr) -> Result<Option<Regex>> {
    let children = expr.mut_children();
    if children.len() <= PATTERN_IDX {
        return Ok(None);
    }

    let pattern = match children[PATTERN_IDX].get_tp() {
        ExprType::Bytes | ExprType::String => children[PATTERN_IDX].get_val(),
        _ => return Ok(None),
    };

    let match_type = if children.len() > N {
        match children[N].get_tp() {
            ExprType::Bytes | ExprType::String => children[N].get_val(),
            _ => return Ok(None),
        }
    } else {
        b""
    };

    build_regexp::<C>(pattern, match_type).map(Some)
}

/// Currently, TiDB only supports regular expressions for utf-8 strings.
/// See https://dev.mysql.com/doc/refman/8.0/en/regexp.html#function_regexp-like.
#[rpn_fn(nullable, raw_varg, min_args = 2, max_args = 3, capture = [metadata], metadata_mapper = init_regexp_data::<C, LIKE_MATCH_IDX>)]
#[inline]
pub fn regexp_like<C: Collator>(
    metadata: &Option<Regex>,
    args: &[ScalarValueRef<'_>],
) -> Result<Option<i64>> {
    let expr = match args[0].as_bytes() {
        Some(e) => std::str::from_utf8(e)?,
        None => return Ok(None),
    };
    let regex = match metadata {
        Some(r) => Cow::Borrowed(r),
        None => match build_regexp_from_args::<C>(args, LIKE_MATCH_IDX)? {
            Some(r) => Cow::Owned(r),
            None => return Ok(None),
        },
    };

    Ok(Some(regex.is_match(expr) as i64))
}

/// Currently, TiDB only supports regular expressions for utf-8 strings.
/// See https://dev.mysql.com/doc/refman/8.0/en/regexp.html#function_regexp-substr.
#[rpn_fn(nullable, raw_varg, min_args = 2, max_args = 5, capture = [metadata], metadata_mapper = init_regexp_data::<C, SUBSTR_MATCH_IDX>)]
#[inline]
pub fn regexp_substr<C: Collator>(
    metadata: &Option<Regex>,
    args: &[ScalarValueRef<'_>],
) -> Result<Option<Bytes>> {
    let mut expr = match args[0].as_bytes() {
        Some(e) => std::str::from_utf8(e)?,
        None => return Ok(None),
    };
    let regex = match metadata {
        Some(r) => Cow::Borrowed(r),
        None => match build_regexp_from_args::<C>(args, SUBSTR_MATCH_IDX)? {
            Some(r) => Cow::Owned(r),
            None => return Ok(None),
        },
    };

    if args.len() >= 3 {
        let pos = match EvaluableRef::borrow_scalar_value_ref(args[2]) {
            Some::<&i64>(p) => *p,
            None => return Ok(None),
        };

        if pos >= 1 {
            if let Some((idx, _)) = expr.char_indices().nth((pos - 1) as usize) {
                expr = &expr[idx..];
            } else if pos != 1 {
                // Char count == 0 && pos == 1 is valid.
                return Err(invalid_pos_error(pos, expr.chars().count()).into());
            }
        } else {
            return Err(invalid_pos_error(pos, expr.chars().count()).into());
        }
    }

    let mut occurrence = 1i64;
    if args.len() >= 4 {
        occurrence = match EvaluableRef::borrow_scalar_value_ref(args[3]) {
            Some::<&i64>(o) => *o,
            None => return Ok(None),
        };

        if occurrence < 1 {
            occurrence = 1;
        }
    };

    if let Some(m) = regex.find_iter(expr).nth((occurrence - 1) as usize) {
        return Ok(Some(m.as_str().as_bytes().to_vec()));
    }

    Ok(None)
}

/// Currently, TiDB only supports regular expressions for utf-8 strings.
/// See https://dev.mysql.com/doc/refman/8.0/en/regexp.html#function_regexp-instr.
#[rpn_fn(nullable, raw_varg, min_args = 2, max_args = 6, capture = [metadata], metadata_mapper = init_regexp_data::<C, INSTR_MATCH_IDX>)]
#[inline]
pub fn regexp_instr<C: Collator>(
    metadata: &Option<Regex>,
    args: &[ScalarValueRef<'_>],
) -> Result<Option<i64>> {
    let mut expr = match args[0].as_bytes() {
        Some(e) => std::str::from_utf8(e)?,
        None => return Ok(None),
    };
    let regex = match metadata {
        Some(r) => Cow::Borrowed(r),
        None => match build_regexp_from_args::<C>(args, INSTR_MATCH_IDX)? {
            Some(r) => Cow::Owned(r),
            None => return Ok(None),
        },
    };

    let mut pos = 1i64;
    if args.len() >= 3 {
        pos = match EvaluableRef::borrow_scalar_value_ref(args[2]) {
            Some::<&i64>(p) => *p,
            None => return Ok(None),
        };

        if pos >= 1 {
            if let Some((idx, _)) = expr.char_indices().nth((pos - 1) as usize) {
                expr = &expr[idx..];
            } else if pos != 1 {
                // Char count == 0 && pos == 1 is valid.
                return Err(invalid_pos_error(pos, expr.chars().count()).into());
            }
        } else {
            return Err(invalid_pos_error(pos, expr.chars().count()).into());
        }
    }

    let mut occurrence = 1i64;
    if args.len() >= 4 {
        occurrence = match EvaluableRef::borrow_scalar_value_ref(args[3]) {
            Some::<&i64>(o) => *o,
            None => return Ok(None),
        };

        if occurrence < 1 {
            occurrence = 1;
        }
    };

    let mut return_option = 0i64;
    if args.len() >= 5 {
        return_option = match EvaluableRef::borrow_scalar_value_ref(args[4]) {
            Some::<&i64>(o) => *o,
            None => return Ok(None),
        };

        if return_option != 0 && return_option != 1 {
            let err = format!("Invalid regexp return option: {}", return_option);
            return Err(Error::regexp_error(err).into());
        }
    };

    if let Some(m) = regex.find_iter(expr).nth((occurrence - 1) as usize) {
        let find_pos = if return_option == 0 {
            m.start()
        } else {
            m.end()
        };

        let count = expr[..find_pos].chars().count() as i64;
        return Ok(Some(count + pos));
    }

    Ok(Some(0))
}

/// Currently, TiDB only supports regular expressions for utf-8 strings.
/// See https://dev.mysql.com/doc/refman/8.0/en/regexp.html#function_regexp-replace.
#[rpn_fn(nullable, raw_varg, min_args = 3, max_args = 6, capture = [metadata], metadata_mapper = init_regexp_data::<C, REPLACE_MATCH_IDX>)]
#[inline]
pub fn regexp_replace<C: Collator>(
    metadata: &Option<Regex>,
    args: &[ScalarValueRef<'_>],
) -> Result<Option<Bytes>> {
    let expr = match args[0].as_bytes() {
        Some(e) => std::str::from_utf8(e)?,
        None => return Ok(None),
    };
    let regex = match metadata {
        Some(r) => Cow::Borrowed(r),
        None => match build_regexp_from_args::<C>(args, REPLACE_MATCH_IDX)? {
            Some(r) => Cow::Owned(r),
            None => return Ok(None),
        },
    };
    let replace_expr = match args[2].as_bytes() {
        Some(e) => std::str::from_utf8(e)?,
        None => return Ok(None),
    };

    let (mut before_trimmed, mut trimmed) = ("", expr);
    if args.len() >= 4 {
        let pos = match EvaluableRef::borrow_scalar_value_ref(args[3]) {
            Some::<&i64>(p) => *p,
            None => return Ok(None),
        };

        if pos >= 1 {
            if let Some((idx, _)) = expr.char_indices().nth((pos - 1) as usize) {
                before_trimmed = &expr[..idx];
                trimmed = &expr[idx..];
            } else if pos != 1 {
                // Char count == 0 && pos == 1 is valid.
                return Err(invalid_pos_error(pos, expr.chars().count()).into());
            }
        } else {
            return Err(invalid_pos_error(pos, expr.chars().count()).into());
        }
    }

    let mut occurrence = 0i64;
    if args.len() >= 5 {
        occurrence = match EvaluableRef::borrow_scalar_value_ref(args[4]) {
            Some::<&i64>(o) => *o,
            None => return Ok(None),
        };

        if occurrence < 0 {
            occurrence = 1;
        }
    };

    if occurrence == 0 {
        let rep = regex.replace_all(trimmed, replace_expr);
        let mut result = String::with_capacity(before_trimmed.len() + rep.len());
        result.push_str(before_trimmed);
        result.push_str(&rep);

        Ok(Some(result.into_bytes()))
    } else {
        if let Some(m) = regex.find_iter(trimmed).nth((occurrence - 1) as usize) {
            let mut result = String::with_capacity(
                before_trimmed.len()
                    + trimmed[..m.start()].len()
                    + replace_expr.len()
                    + trimmed[m.end()..].len(),
            );
            result.push_str(before_trimmed);
            result.push_str(&trimmed[..m.start()]);
            result.push_str(replace_expr);
            result.push_str(&trimmed[m.end()..]);

            return Ok(Some(result.into_bytes()));
        }

        Ok(Some(expr.as_bytes().to_vec()))
    }
}

#[cfg(test)]
mod tests {
    use tidb_query_datatype::{codec::batch::LazyBatchColumnVec, expr::EvalContext, FieldTypeTp};
    use tipb::ScalarFuncSig;
    use tipb_helper::ExprDefBuilder;

    use crate::RpnExpressionBuilder;

    #[test]
    fn test_regexp_like() {
        let cases = vec![
            ("a", "^$", None, Some(0)),
            ("a", "a", None, Some(1)),
            ("b", "a", None, Some(0)),
            ("aA", "Aa", None, Some(0)),
            ("aaa", ".", None, Some(1)),
            ("ab", "^.$", None, Some(0)),
            ("b", "..", None, Some(0)),
            ("aab", ".ab", None, Some(1)),
            ("abcd", ".*", None, Some(1)),
            ("你", "^.$", None, Some(1)),
            ("你好", "你好", None, Some(1)),
            ("你好", "^你好$", None, Some(1)),
            ("你好", "^您好$", None, Some(0)),
            // Test wrong pattern
            ("", "(", None, None),
            ("", "(*", None, None),
            ("", "[a", None, None),
            ("aa", "", None, None),
            ("你好", "", None, None),
            // Test case-insensitive
            ("abc", "AbC", Some(""), Some(0)),
            ("abc", "AbC", Some("i"), Some(1)),
            // Test multiple-line mode
            ("123\n321", "23$", Some(""), Some(0)),
            ("123\n321", "23$", Some("m"), Some(1)),
            ("good\nday", "^day", Some("m"), Some(1)),
            // Test s flag(in mysql it's n flag)
            ("\n", ".", Some(""), Some(0)),
            ("\n", ".", Some("s"), Some(1)),
            // Test rightmost rule
            ("abc", "aBc", Some("ic"), Some(0)),
            ("abc", "aBc", Some("ci"), Some(1)),
            // Test invalid match type
            ("abc", "abc", Some("p"), None),
            ("abc", "abc", Some("cpi"), None),
        ];

        for (expr, pattern, match_type, expected) in cases {
            let mut ctx = EvalContext::default();

            let mut builder =
                ExprDefBuilder::scalar_func(ScalarFuncSig::RegexpLikeSig, FieldTypeTp::LongLong);
            builder = builder
                .push_child(ExprDefBuilder::constant_bytes(expr.as_bytes().to_vec()))
                .push_child(ExprDefBuilder::constant_bytes(pattern.as_bytes().to_vec()));
            if let Some(m) = match_type {
                builder = builder.push_child(ExprDefBuilder::constant_bytes(m.as_bytes().to_vec()));
            }

            let node = builder.build();
            let exp = RpnExpressionBuilder::build_from_expr_tree(node, &mut ctx, 1);
            if expected.is_none() {
                assert!(exp.is_err());
                continue;
            }
            let exp = exp.unwrap();

            let schema = &[];
            let mut columns = LazyBatchColumnVec::empty();

            let val = exp.eval(&mut ctx, schema, &mut columns, &[], 1).unwrap();

            assert!(val.is_vector());
            let v = val.vector_value().unwrap().as_ref().to_int_vec();
            assert_eq!(v.len(), 1);
            assert_eq!(v[0], expected);
        }

        // Test null
        let cases = vec![
            (None, Some("a"), Some("i")),
            (Some("a"), None, Some("i")),
            (Some("a"), Some("a"), None),
        ];
        for (expr, pattern, match_type) in cases {
            let mut ctx = EvalContext::default();

            let mut builder =
                ExprDefBuilder::scalar_func(ScalarFuncSig::RegexpSig, FieldTypeTp::LongLong);
            if let Some(e) = expr {
                builder = builder.push_child(ExprDefBuilder::constant_bytes(e.as_bytes().to_vec()));
            } else {
                builder = builder.push_child(ExprDefBuilder::constant_null(FieldTypeTp::String));
            }
            if let Some(p) = pattern {
                builder = builder.push_child(ExprDefBuilder::constant_bytes(p.as_bytes().to_vec()));
            } else {
                builder = builder.push_child(ExprDefBuilder::constant_null(FieldTypeTp::String));
            }
            if let Some(m) = match_type {
                builder = builder.push_child(ExprDefBuilder::constant_bytes(m.as_bytes().to_vec()));
            } else {
                builder = builder.push_child(ExprDefBuilder::constant_null(FieldTypeTp::String));
            }

            let node = builder.build();
            let exp = RpnExpressionBuilder::build_from_expr_tree(node, &mut ctx, 1).unwrap();

            let schema = &[];
            let mut columns = LazyBatchColumnVec::empty();

            let val = exp.eval(&mut ctx, schema, &mut columns, &[], 1).unwrap();
            assert!(val.is_vector());
            let v = val.vector_value().unwrap().as_ref().to_int_vec();
            assert_eq!(v.len(), 1);
            assert_eq!(v[0], None);
        }
    }

    #[test]
    fn test_regexp_substr() {
        let cases = vec![
            ("abc", "bc", None, None, None, Some("bc"), false),
            ("你好啊", "好", None, None, None, Some("好"), false),
            ("abc", "bc", Some(2), None, None, Some("bc"), false),
            ("你好啊", "好", Some(2), None, None, Some("好"), false),
            ("你好啊", "好", Some(3), None, None, None, false),
            // Test invalid position index
            ("你好啊", "好", Some(4), None, None, None, true),
            ("你好啊", "好", Some(-1), None, None, None, true),
            ("", "a", Some(1), None, None, None, false),
            (
                "abc abd abe",
                "ab.",
                Some(1),
                Some(1),
                None,
                Some("abc"),
                false,
            ),
            (
                "abc abd abe",
                "ab.",
                Some(1),
                Some(0),
                None,
                Some("abc"),
                false,
            ),
            (
                "abc abd abe",
                "ab.",
                Some(1),
                Some(-1),
                None,
                Some("abc"),
                false,
            ),
            (
                "abc abd abe",
                "ab.",
                Some(1),
                Some(2),
                None,
                Some("abd"),
                false,
            ),
            (
                "abc abd abe",
                "ab.",
                Some(3),
                Some(1),
                None,
                Some("abd"),
                false,
            ),
            (
                "abc abd abe",
                "ab.",
                Some(3),
                Some(2),
                None,
                Some("abe"),
                false,
            ),
            (
                "abc abd abe",
                "ab.",
                Some(6),
                Some(1),
                None,
                Some("abe"),
                false,
            ),
            ("abc abd abe", "ab.", Some(6), Some(100), None, None, false),
            ("abc abd abe", "ab.", Some(100), Some(1), None, None, true),
            (
                "嗯嗯 嗯好 嗯呐",
                "嗯.",
                Some(1),
                Some(1),
                None,
                Some("嗯嗯"),
                false,
            ),
            (
                "嗯嗯 嗯好 嗯呐",
                "嗯.",
                Some(1),
                Some(2),
                None,
                Some("嗯好"),
                false,
            ),
            (
                "嗯嗯 嗯好 嗯呐",
                "嗯.",
                Some(5),
                Some(1),
                None,
                Some("嗯呐"),
                false,
            ),
            ("嗯嗯 嗯好 嗯呐", "嗯.", Some(5), Some(2), None, None, false),
            ("abc", "ab.", Some(1), Some(1), Some(""), Some("abc"), false),
            (
                "abc",
                "aB.",
                Some(1),
                Some(1),
                Some("i"),
                Some("abc"),
                false,
            ),
            ("abc", "aB.", Some(100), Some(1), Some("i"), None, true),
            (
                "good\nday",
                "od",
                Some(1),
                Some(1),
                Some("m"),
                Some("od"),
                false,
            ),
            ("\n", ".", Some(1), Some(1), Some("s"), Some("\n"), false),
        ];

        for (expr, pattern, pos, occur, match_type, expected, error) in cases {
            let mut ctx = EvalContext::default();

            let mut builder =
                ExprDefBuilder::scalar_func(ScalarFuncSig::RegexpSubstrSig, FieldTypeTp::String);
            builder = builder
                .push_child(ExprDefBuilder::constant_bytes(expr.as_bytes().to_vec()))
                .push_child(ExprDefBuilder::constant_bytes(pattern.as_bytes().to_vec()));
            if let Some(p) = pos {
                builder = builder.push_child(ExprDefBuilder::constant_int(p));
            }
            if let Some(o) = occur {
                builder = builder.push_child(ExprDefBuilder::constant_int(o));
            }
            if let Some(m) = match_type {
                builder = builder.push_child(ExprDefBuilder::constant_bytes(m.as_bytes().to_vec()));
            }

            let node = builder.build();
            let exp = RpnExpressionBuilder::build_from_expr_tree(node, &mut ctx, 1).unwrap();
            let schema = &[];
            let mut columns = LazyBatchColumnVec::empty();
            let val = exp.eval(&mut ctx, schema, &mut columns, &[], 1);
            match val {
                Ok(val) => {
                    assert!(val.is_vector());
                    let v = val.vector_value().unwrap().as_ref().to_bytes_vec();
                    assert_eq!(v.len(), 1);
                    assert_eq!(
                        v[0],
                        expected.map(|e| e.as_bytes().to_vec()),
                        "{:?} {:?} {:?}",
                        expr,
                        pattern,
                        pos
                    );
                }
                Err(e) => {
                    assert!(error, "val has error {:?}", e);
                }
            }
        }

        // Test null
        let cases = vec![
            (None, Some("a"), Some(1), Some(1), Some("i")),
            (Some("a"), None, Some(1), Some(1), Some("i")),
            (Some("a"), Some("a"), None, Some(1), Some("i")),
            (Some("a"), Some("a"), Some(1), None, Some("i")),
            (Some("a"), Some("a"), Some(1), Some(1), None),
        ];
        for (expr, pattern, pos, occur, match_type) in cases {
            let mut ctx = EvalContext::default();

            let mut builder =
                ExprDefBuilder::scalar_func(ScalarFuncSig::RegexpSubstrSig, FieldTypeTp::String);
            if let Some(e) = expr {
                builder = builder.push_child(ExprDefBuilder::constant_bytes(e.as_bytes().to_vec()));
            } else {
                builder = builder.push_child(ExprDefBuilder::constant_null(FieldTypeTp::String));
            }
            if let Some(p) = pattern {
                builder = builder.push_child(ExprDefBuilder::constant_bytes(p.as_bytes().to_vec()));
            } else {
                builder = builder.push_child(ExprDefBuilder::constant_null(FieldTypeTp::String));
            }
            if let Some(p) = pos {
                builder = builder.push_child(ExprDefBuilder::constant_int(p));
            } else {
                builder = builder.push_child(ExprDefBuilder::constant_null(FieldTypeTp::LongLong));
            }
            if let Some(o) = occur {
                builder = builder.push_child(ExprDefBuilder::constant_int(o));
            } else {
                builder = builder.push_child(ExprDefBuilder::constant_null(FieldTypeTp::LongLong));
            }
            if let Some(m) = match_type {
                builder = builder.push_child(ExprDefBuilder::constant_bytes(m.as_bytes().to_vec()));
            } else {
                builder = builder.push_child(ExprDefBuilder::constant_null(FieldTypeTp::String));
            }

            let node = builder.build();
            let exp = RpnExpressionBuilder::build_from_expr_tree(node, &mut ctx, 1).unwrap();

            let schema = &[];
            let mut columns = LazyBatchColumnVec::empty();

            let val = exp.eval(&mut ctx, schema, &mut columns, &[], 1).unwrap();
            assert!(val.is_vector());
            let v = val.vector_value().unwrap().as_ref().to_bytes_vec();
            assert_eq!(v.len(), 1);
            assert_eq!(v[0], None);
        }
    }

    #[test]
    fn test_regexp_instr() {
        let cases = vec![
            ("abc", "bc", None, None, None, None, Some(2), false),
            ("你好啊", "好", None, None, None, None, Some(2), false),
            ("abc", "bc", Some(2), None, None, None, Some(2), false),
            ("你好啊", "好", Some(2), None, None, None, Some(2), false),
            ("你好啊", "好", Some(3), None, None, None, Some(0), false),
            // Test invalid position index
            ("你好啊", "好", Some(4), None, None, None, Some(0), true),
            ("你好啊", "好", Some(-1), None, None, None, Some(0), true),
            ("", "a", Some(1), None, None, None, Some(0), false),
            (
                "abc abd abe",
                "ab.",
                Some(1),
                Some(1),
                None,
                None,
                Some(1),
                false,
            ),
            (
                "abc abd abe",
                "ab.",
                Some(1),
                Some(0),
                None,
                None,
                Some(1),
                false,
            ),
            (
                "abc abd abe",
                "ab.",
                Some(1),
                Some(-1),
                None,
                None,
                Some(1),
                false,
            ),
            (
                "abc abd abe",
                "ab.",
                Some(1),
                Some(2),
                None,
                None,
                Some(5),
                false,
            ),
            (
                "abc abd abe",
                "ab.",
                Some(3),
                Some(1),
                None,
                None,
                Some(5),
                false,
            ),
            (
                "abc abd abe",
                "ab.",
                Some(3),
                Some(2),
                None,
                None,
                Some(9),
                false,
            ),
            (
                "abc abd abe",
                "ab.",
                Some(6),
                Some(1),
                None,
                None,
                Some(9),
                false,
            ),
            (
                "abc abd abe",
                "ab.",
                Some(6),
                Some(100),
                None,
                None,
                Some(0),
                false,
            ),
            (
                "嗯嗯 嗯好 嗯呐",
                "嗯.",
                Some(1),
                Some(1),
                None,
                None,
                Some(1),
                false,
            ),
            (
                "嗯嗯 嗯好 嗯呐",
                "嗯.",
                Some(1),
                Some(2),
                None,
                None,
                Some(4),
                false,
            ),
            (
                "嗯嗯 嗯好 嗯呐",
                "嗯.",
                Some(5),
                Some(1),
                None,
                None,
                Some(7),
                false,
            ),
            (
                "嗯嗯 嗯好 嗯呐",
                "嗯.",
                Some(5),
                Some(2),
                None,
                None,
                Some(0),
                false,
            ),
            (
                "嗯嗯 嗯好 嗯呐",
                "嗯.",
                Some(1),
                Some(100),
                None,
                None,
                Some(0),
                false,
            ),
            (
                "abc abd abe",
                "ab.",
                Some(1),
                Some(1),
                Some(0),
                None,
                Some(1),
                false,
            ),
            (
                "abc abd abe",
                "ab.",
                Some(1),
                Some(1),
                Some(1),
                None,
                Some(4),
                false,
            ),
            (
                "嗯嗯 嗯好 嗯呐",
                "嗯.",
                Some(1),
                Some(1),
                Some(0),
                None,
                Some(1),
                false,
            ),
            (
                "嗯嗯 嗯好 嗯呐",
                "嗯.",
                Some(1),
                Some(1),
                Some(1),
                None,
                Some(3),
                false,
            ),
            ("", "^$", Some(1), Some(1), Some(0), None, Some(1), false),
            ("", "^$", Some(1), Some(1), Some(1), None, Some(1), false),
            (
                "abc",
                "ab.",
                Some(1),
                Some(1),
                Some(0),
                Some(""),
                Some(1),
                false,
            ),
            (
                "abc",
                "aB.",
                Some(1),
                Some(1),
                Some(0),
                Some("i"),
                Some(1),
                false,
            ),
            (
                "good\nday",
                "od$",
                Some(1),
                Some(1),
                Some(0),
                Some("m"),
                Some(3),
                false,
            ),
            (
                "good\nday",
                "oD$",
                Some(1),
                Some(1),
                Some(0),
                Some("mi"),
                Some(3),
                false,
            ),
            (
                "\n",
                ".",
                Some(1),
                Some(1),
                Some(0),
                Some("s"),
                Some(1),
                false,
            ),
        ];

        for (expr, pattern, pos, occur, ret_opt, match_type, expected, error) in cases {
            let mut ctx = EvalContext::default();

            let mut builder =
                ExprDefBuilder::scalar_func(ScalarFuncSig::RegexpInStrSig, FieldTypeTp::LongLong);
            builder = builder
                .push_child(ExprDefBuilder::constant_bytes(expr.as_bytes().to_vec()))
                .push_child(ExprDefBuilder::constant_bytes(pattern.as_bytes().to_vec()));
            if let Some(p) = pos {
                builder = builder.push_child(ExprDefBuilder::constant_int(p));
            }
            if let Some(o) = occur {
                builder = builder.push_child(ExprDefBuilder::constant_int(o));
            }
            if let Some(r) = ret_opt {
                builder = builder.push_child(ExprDefBuilder::constant_int(r));
            }
            if let Some(m) = match_type {
                builder = builder.push_child(ExprDefBuilder::constant_bytes(m.as_bytes().to_vec()));
            }

            let node = builder.build();
            let exp = RpnExpressionBuilder::build_from_expr_tree(node, &mut ctx, 1).unwrap();

            let schema = &[];
            let mut columns = LazyBatchColumnVec::empty();

            let val = exp.eval(&mut ctx, schema, &mut columns, &[], 1);

            match val {
                Ok(val) => {
                    assert!(val.is_vector());
                    let v = val.vector_value().unwrap().as_ref().to_int_vec();
                    assert_eq!(v.len(), 1);
                    assert_eq!(v[0], expected, "{:?} {:?} {:?}", expr, pattern, pos);
                }
                Err(e) => {
                    assert!(error, "val has error {:?}", e);
                }
            }
        }

        // Test null
        let cases = vec![
            (None, Some("a"), Some(1), Some(1), Some(0), Some("i")),
            (Some("a"), None, Some(1), Some(1), Some(0), Some("i")),
            (Some("a"), Some("a"), None, Some(1), Some(0), Some("i")),
            (Some("a"), Some("a"), Some(1), None, Some(0), Some("i")),
            (Some("a"), Some("a"), Some(1), Some(1), None, Some("i")),
            (Some("a"), Some("a"), Some(1), Some(1), Some(0), None),
        ];
        for (expr, pattern, pos, occur, ret_opt, match_type) in cases {
            let mut ctx = EvalContext::default();

            let mut builder =
                ExprDefBuilder::scalar_func(ScalarFuncSig::RegexpInStrSig, FieldTypeTp::LongLong);
            if let Some(e) = expr {
                builder = builder.push_child(ExprDefBuilder::constant_bytes(e.as_bytes().to_vec()));
            } else {
                builder = builder.push_child(ExprDefBuilder::constant_null(FieldTypeTp::String));
            }
            if let Some(p) = pattern {
                builder = builder.push_child(ExprDefBuilder::constant_bytes(p.as_bytes().to_vec()));
            } else {
                builder = builder.push_child(ExprDefBuilder::constant_null(FieldTypeTp::String));
            }
            if let Some(p) = pos {
                builder = builder.push_child(ExprDefBuilder::constant_int(p));
            } else {
                builder = builder.push_child(ExprDefBuilder::constant_null(FieldTypeTp::LongLong));
            }
            if let Some(o) = occur {
                builder = builder.push_child(ExprDefBuilder::constant_int(o));
            } else {
                builder = builder.push_child(ExprDefBuilder::constant_null(FieldTypeTp::LongLong));
            }
            if let Some(r) = ret_opt {
                builder = builder.push_child(ExprDefBuilder::constant_int(r));
            } else {
                builder = builder.push_child(ExprDefBuilder::constant_null(FieldTypeTp::LongLong));
            }
            if let Some(m) = match_type {
                builder = builder.push_child(ExprDefBuilder::constant_bytes(m.as_bytes().to_vec()));
            } else {
                builder = builder.push_child(ExprDefBuilder::constant_null(FieldTypeTp::String));
            }

            let node = builder.build();
            let exp = RpnExpressionBuilder::build_from_expr_tree(node, &mut ctx, 1).unwrap();

            let schema = &[];
            let mut columns = LazyBatchColumnVec::empty();

            let val = exp.eval(&mut ctx, schema, &mut columns, &[], 1).unwrap();
            assert!(val.is_vector());
            let v = val.vector_value().unwrap().as_ref().to_int_vec();
            assert_eq!(v.len(), 1);
            assert_eq!(v[0], None);
        }
    }

    #[test]
    fn test_regexp_replace() {
        let cases = vec![
            (
                "abc abd abe",
                "ab.",
                "cz",
                None,
                None,
                None,
                Some("cz cz cz"),
                false,
            ),
            (
                "你好 好的",
                "好",
                "逸",
                None,
                None,
                None,
                Some("你逸 逸的"),
                false,
            ),
            ("", "^$", "123", None, None, None, Some("123"), false),
            ("abc", "ab.", "cc", Some(1), None, None, Some("cc"), false),
            ("abc", "bc", "cc", Some(3), None, None, Some("abc"), false),
            ("你好", "好", "的", Some(2), None, None, Some("你的"), false),
            (
                "你好啊",
                "好",
                "的",
                Some(3),
                None,
                None,
                Some("你好啊"),
                false,
            ),
            ("", "^$", "cc", Some(1), None, None, Some("cc"), false),
            // Test invalid position index
            ("", "^$", "a", Some(2), None, None, None, true),
            ("", "^&", "a", Some(0), None, None, None, true),
            ("abc", "bc", "a", Some(-1), None, None, None, true),
            ("abc", "bc", "a", Some(4), None, None, None, true),
            (
                "abc abd",
                "ab.",
                "cc",
                Some(1),
                Some(1),
                None,
                Some("cc abd"),
                false,
            ),
            (
                "abc abd",
                "ab.",
                "cc",
                Some(1),
                Some(-1),
                None,
                Some("cc abd"),
                false,
            ),
            (
                "abc abd",
                "ab.",
                "cc",
                Some(1),
                Some(2),
                None,
                Some("abc cc"),
                false,
            ),
            (
                "abc abd",
                "ab.",
                "cc",
                Some(1),
                Some(0),
                None,
                Some("cc cc"),
                false,
            ),
            (
                "abc abd abe",
                "ab.",
                "cc",
                Some(1),
                Some(3),
                None,
                Some("abc abd cc"),
                false,
            ),
            (
                "abc abd abe",
                "ab.",
                "cc",
                Some(3),
                Some(2),
                None,
                Some("abc abd cc"),
                false,
            ),
            (
                "abc abd abe",
                "ab.",
                "cc",
                Some(3),
                Some(10),
                None,
                Some("abc abd abe"),
                false,
            ),
            (
                "你好 好啊",
                "好",
                "的",
                Some(1),
                Some(1),
                None,
                Some("你的 好啊"),
                false,
            ),
            (
                "你好 好啊",
                "好",
                "的",
                Some(3),
                Some(1),
                None,
                Some("你好 的啊"),
                false,
            ),
            ("", "^$", "cc", Some(1), Some(1), None, Some("cc"), false),
            ("", "^$", "cc", Some(1), Some(2), None, Some(""), false),
            ("", "^$", "cc", Some(1), Some(-1), None, Some("cc"), false),
            (
                "abc",
                "ab.",
                "cc",
                Some(1),
                Some(0),
                Some(""),
                Some("cc"),
                false,
            ),
            (
                "abc",
                "aB.",
                "cc",
                Some(1),
                Some(0),
                Some("i"),
                Some("cc"),
                false,
            ),
            (
                "good\nday",
                "od$",
                "cc",
                Some(1),
                Some(0),
                Some("m"),
                Some("gocc\nday"),
                false,
            ),
            (
                "good\nday",
                "oD$",
                "cc",
                Some(1),
                Some(0),
                Some("mi"),
                Some("gocc\nday"),
                false,
            ),
            (
                "\n",
                ".",
                "cc",
                Some(1),
                Some(0),
                Some("s"),
                Some("cc"),
                false,
            ),
            (
                "好的 好滴 好~",
                ".",
                "的",
                Some(1),
                Some(0),
                Some("msi"),
                Some("的的的的的的的的"),
                false,
            ),
        ];

        for (expr, pattern, replace, pos, occur, match_type, expected, error) in cases {
            let mut ctx = EvalContext::default();

            let mut builder =
                ExprDefBuilder::scalar_func(ScalarFuncSig::RegexpReplaceSig, FieldTypeTp::String);
            builder = builder
                .push_child(ExprDefBuilder::constant_bytes(expr.as_bytes().to_vec()))
                .push_child(ExprDefBuilder::constant_bytes(pattern.as_bytes().to_vec()))
                .push_child(ExprDefBuilder::constant_bytes(replace.as_bytes().to_vec()));
            if let Some(p) = pos {
                builder = builder.push_child(ExprDefBuilder::constant_int(p));
            }
            if let Some(o) = occur {
                builder = builder.push_child(ExprDefBuilder::constant_int(o));
            }
            if let Some(m) = match_type {
                builder = builder.push_child(ExprDefBuilder::constant_bytes(m.as_bytes().to_vec()));
            }

            let node = builder.build();
            let exp = RpnExpressionBuilder::build_from_expr_tree(node, &mut ctx, 1).unwrap();

            let schema = &[];
            let mut columns = LazyBatchColumnVec::empty();

            let val = exp.eval(&mut ctx, schema, &mut columns, &[], 1);

            match val {
                Ok(val) => {
                    assert!(val.is_vector());
                    let v = val.vector_value().unwrap().as_ref().to_bytes_vec();
                    assert_eq!(v.len(), 1);
                    assert_eq!(
                        v[0],
                        expected.map(|e| e.as_bytes().to_vec()),
                        "{:?} {:?} {:?}",
                        expr,
                        pattern,
                        replace,
                    );
                }
                Err(e) => {
                    assert!(error, "val has error {:?}", e);
                }
            }
        }

        // Test null
        let cases = vec![
            (None, Some("a"), Some("a"), Some(1), Some(0), Some("i")),
            (Some("a"), None, Some("a"), Some(1), Some(0), Some("i")),
            (Some("a"), Some("a"), None, Some(1), Some(0), Some("i")),
            (Some("a"), Some("a"), Some("a"), None, Some(0), Some("i")),
            (Some("a"), Some("a"), Some("a"), Some(1), None, Some("i")),
            (Some("a"), Some("a"), Some("a"), Some(1), Some(0), None),
        ];
        for (expr, pattern, replace, pos, occur, match_type) in cases {
            let mut ctx = EvalContext::default();

            let mut builder =
                ExprDefBuilder::scalar_func(ScalarFuncSig::RegexpReplaceSig, FieldTypeTp::String);
            if let Some(e) = expr {
                builder = builder.push_child(ExprDefBuilder::constant_bytes(e.as_bytes().to_vec()));
            } else {
                builder = builder.push_child(ExprDefBuilder::constant_null(FieldTypeTp::String));
            }
            if let Some(p) = pattern {
                builder = builder.push_child(ExprDefBuilder::constant_bytes(p.as_bytes().to_vec()));
            } else {
                builder = builder.push_child(ExprDefBuilder::constant_null(FieldTypeTp::String));
            }
            if let Some(r) = replace {
                builder = builder.push_child(ExprDefBuilder::constant_bytes(r.as_bytes().to_vec()));
            } else {
                builder = builder.push_child(ExprDefBuilder::constant_null(FieldTypeTp::String));
            }
            if let Some(p) = pos {
                builder = builder.push_child(ExprDefBuilder::constant_int(p));
            } else {
                builder = builder.push_child(ExprDefBuilder::constant_null(FieldTypeTp::LongLong));
            }
            if let Some(o) = occur {
                builder = builder.push_child(ExprDefBuilder::constant_int(o));
            } else {
                builder = builder.push_child(ExprDefBuilder::constant_null(FieldTypeTp::LongLong));
            }
            if let Some(m) = match_type {
                builder = builder.push_child(ExprDefBuilder::constant_bytes(m.as_bytes().to_vec()));
            } else {
                builder = builder.push_child(ExprDefBuilder::constant_null(FieldTypeTp::String));
            }

            let node = builder.build();
            let exp = RpnExpressionBuilder::build_from_expr_tree(node, &mut ctx, 1).unwrap();

            let schema = &[];
            let mut columns = LazyBatchColumnVec::empty();

            let val = exp.eval(&mut ctx, schema, &mut columns, &[], 1).unwrap();
            assert!(val.is_vector());
            let v = val.vector_value().unwrap().as_ref().to_bytes_vec();
            assert_eq!(v.len(), 1);
            assert_eq!(v[0], None);
        }
    }
}
