// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::collections::HashSet;

use regex::Regex;
use tidb_query_codegen::rpn_fn;
use tidb_query_common::Result;
use tidb_query_datatype::codec::{collation::Collator, data_type::*};
use tipb::{Expr, ExprType};

const PATTERN_IDX: usize = 1;
const LIKE_MATCH_IDX: usize = 2;
const SUBSTR_MATCH_IDX: usize = 4;
const INSTR_MATCH_IDX: usize = 5;
const REPLACE_MATCH_IDX: usize = 5;

fn is_valid_match_type(m: u8) -> bool {
    match m {
        b'i' | b'c' | b'm' | b's' => true,
        _ => false,
    }
}

fn get_match_type<C: Collator>(match_type: &[u8]) -> Result<String> {
    let mut flag_set = HashSet::<u8>::new();

    if C::IS_CASE_INSENSITIVE {
        flag_set.insert(b'i');
    }

    for m in match_type {
        if !is_valid_match_type(*m) {
            return Err(box_err!("invalid match type: {}", m));
        }
        if *m == b'c' {
            // re2 is case-sensitive by default, so we only need to delete 'i' flag
            // to enable the case-sensitive for the regexp.
            flag_set.remove(&b'i');
            continue;
        }
        flag_set.insert(*m);
    }

    let mut flag = String::new();
    for m in flag_set {
        flag += &m.to_string();
    }

    Ok(flag)
}

fn build_regexp<C: Collator>(pattern: &[u8], match_type: &[u8]) -> Result<Regex> {
    let match_type = get_match_type::<C>(match_type)?;

    let pattern = String::from_utf8(pattern.to_vec())?;
    let pattern_with_tp = if !match_type.is_empty() {
        format!("(?{}){}", match_type, pattern)
    } else {
        pattern
    };

    Regex::new(&pattern_with_tp).map_err(|e| box_err!("invalid regex pattern: {:?}", e))
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

    build_regexp::<C>(pattern, match_type).map(|reg| Some(reg))
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

    build_regexp::<C>(pattern, match_type).map(|reg| Some(reg))
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
        Some(e) => String::from_utf8(e.to_vec())?,
        None => return Ok(None),
    };
    let regex = match metadata {
        Some(r) => r.clone(),
        None => match build_regexp_from_args::<C>(args, LIKE_MATCH_IDX)? {
            Some(r) => r,
            None => return Ok(None),
        },
    };
    Ok(Some(regex.is_match(&expr) as i64))
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
        Some(e) => String::from_utf8(e.to_vec())?,
        None => return Ok(None),
    };
    let regex = match metadata {
        Some(r) => r.clone(),
        None => match build_regexp_from_args::<C>(args, SUBSTR_MATCH_IDX)? {
            Some(r) => r,
            None => return Ok(None),
        },
    };

    if args.len() >= 3 {
        let pos = match EvaluableRef::borrow_scalar_value_ref(args[2]) {
            Some::<&i64>(p) => *p,
            None => return Ok(None),
        };

        let count = expr.chars().count() as i64;
        if (pos < 1 || pos > count) && (count != 0 || pos != 1) {
            return Err(box_err!("invalid regex pos: {}, count: {}", pos, count));
        }
        let mut new_expr = String::new();
        for (i, c) in expr.chars().enumerate() {
            if i as i64 >= pos - 1 {
                new_expr += &c.to_string();
            }
        }
        expr = new_expr;
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

    for (i, m) in regex.find_iter(&expr).enumerate() {
        if i as i64 == occurrence - 1 {
            return Ok(Some(m.as_str().as_bytes().to_vec()));
        }
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
        Some(e) => String::from_utf8(e.to_vec())?,
        None => return Ok(None),
    };
    let regex = match metadata {
        Some(r) => r.clone(),
        None => match build_regexp_from_args::<C>(args, INSTR_MATCH_IDX)? {
            Some(r) => r,
            None => return Ok(None),
        },
    };

    let mut pos = 1i64;
    if args.len() >= 3 {
        pos = match EvaluableRef::borrow_scalar_value_ref(args[2]) {
            Some::<&i64>(p) => *p,
            None => return Ok(None),
        };

        let count = expr.chars().count() as i64;
        if (pos < 1 || pos > count) && (count != 0 || pos != 1) {
            return Err(box_err!("invalid regex pos: {}, count: {}", pos, count));
        }
        let mut new_expr = String::new();
        for (i, c) in expr.chars().enumerate() {
            if i as i64 >= pos - 1 {
                new_expr += &c.to_string();
            }
        }
        expr = new_expr;
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
            return Err(box_err!("invalid regex return option: {}", return_option));
        }
    };

    for (i, m) in regex.find_iter(&expr).enumerate() {
        if i as i64 == occurrence - 1 {
            let find_pos = if return_option == 0 {
                m.start()
            } else {
                m.end()
            };
            let count = expr[..find_pos].to_string().chars().count() as i64;

            return Ok(Some(count + pos - 1));
        }
    }

    Ok(None)
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
        Some(e) => String::from_utf8(e.to_vec())?,
        None => return Ok(None),
    };
    let regex = match metadata {
        Some(r) => r.clone(),
        None => match build_regexp_from_args::<C>(args, REPLACE_MATCH_IDX)? {
            Some(r) => r,
            None => return Ok(None),
        },
    };
    let replace_expr = match args[2].as_bytes() {
        Some(e) => String::from_utf8(e.to_vec())?,
        None => return Ok(None),
    };

    let (before_trimmed, trimmed) = if args.len() >= 4 {
        let pos = match EvaluableRef::borrow_scalar_value_ref(args[3]) {
            Some::<&i64>(p) => *p,
            None => return Ok(None),
        };

        let count = expr.chars().count() as i64;
        if (pos < 1 || pos > count) && (count != 0 || pos != 1) {
            return Err(box_err!("invalid regex pos: {}, count: {}", pos, count));
        }
        let mut trimmed = String::new();
        let mut before_trimmed = String::new();
        for (i, c) in expr.chars().enumerate() {
            if i as i64 >= pos - 1 {
                trimmed += &c.to_string();
            } else {
                before_trimmed += &c.to_string();
            }
        }
        (before_trimmed, trimmed)
    } else {
        (String::new(), expr.clone())
    };

    let mut occurrence = 1i64;
    if args.len() >= 5 {
        occurrence = match EvaluableRef::borrow_scalar_value_ref(args[4]) {
            Some::<&i64>(o) => *o,
            None => return Ok(None),
        };

        if occurrence < 1 {
            occurrence = 1;
        }
    };

    let result = before_trimmed + &regex.replacen(&trimmed, occurrence as usize, replace_expr);

    Ok(Some(result.into_bytes()))
}

#[cfg(test)]
mod tests {
    use tidb_query_datatype::{
        builder::FieldTypeBuilder, codec::batch::LazyBatchColumnVec, expr::EvalContext, Collation,
        FieldTypeTp,
    };
    use tipb::ScalarFuncSig;
    use tipb_helper::ExprDefBuilder;

    use crate::{test_util::RpnFnScalarEvaluator, RpnExpressionBuilder};

    #[test]
    fn test_regexp_utf8() {
        let cases = vec![
            ("a", r"^$", Some(0)),
            ("a", r"a", Some(1)),
            ("b", r"a", Some(0)),
            ("aA", r"Aa", Some(0)),
            ("aaa", r".", Some(1)),
            ("ab", r"^.$", Some(0)),
            ("b", r"..", Some(0)),
            ("aab", r".ab", Some(1)),
            ("abcd", r".*", Some(1)),
            ("你", r"^.$", Some(1)),
            ("你好", r"你好", Some(1)),
            ("你好", r"^你好$", Some(1)),
            ("你好", r"^您好$", Some(0)),
        ];

        for (target, pattern, expected) in cases {
            let mut ctx = EvalContext::default();

            let builder =
                ExprDefBuilder::scalar_func(ScalarFuncSig::RegexpUtf8Sig, FieldTypeTp::LongLong);
            let node = builder
                .push_child(ExprDefBuilder::constant_bytes(target.as_bytes().to_vec()))
                .push_child(ExprDefBuilder::constant_bytes(pattern.as_bytes().to_vec()))
                .build();

            let exp = RpnExpressionBuilder::build_from_expr_tree(node, &mut ctx, 1).unwrap();
            let schema = &[];
            let mut columns = LazyBatchColumnVec::empty();
            let val = exp.eval(&mut ctx, schema, &mut columns, &[], 1).unwrap();

            assert!(val.is_vector());
            let v = val.vector_value().unwrap().as_ref().to_int_vec();
            assert_eq!(v.len(), 1);
            assert_eq!(v[0], expected);
        }
    }

    #[test]
    fn test_regexp() {
        let cases = vec![
            ("a".as_bytes().to_vec(), r"^$", Some(0)),
            ("a".as_bytes().to_vec(), r"a", Some(1)),
            ("b".as_bytes().to_vec(), r"a", Some(0)),
            ("aA".as_bytes().to_vec(), r"Aa", Some(0)),
            ("aaa".as_bytes().to_vec(), r".", Some(1)),
            ("ab".as_bytes().to_vec(), r"^.$", Some(0)),
            ("b".as_bytes().to_vec(), r"..", Some(0)),
            ("aab".as_bytes().to_vec(), r".ab", Some(1)),
            ("abcd".as_bytes().to_vec(), r".*", Some(1)),
            (vec![0x7f], r"^.$", Some(1)), // dot should match one byte which is less than 128
            (vec![0xf0], r"^.$", Some(0)), // dot can't match one byte greater than 128
            // dot should match "你" even if the char has 3 bytes.
            ("你".as_bytes().to_vec(), r"^.$", Some(1)),
            ("你好".as_bytes().to_vec(), r"你好", Some(1)),
            ("你好".as_bytes().to_vec(), r"^你好$", Some(1)),
            ("你好".as_bytes().to_vec(), r"^您好$", Some(0)),
            (
                vec![255, 255, 0xE4, 0xBD, 0xA0, 0xE5, 0xA5, 0xBD],
                r"你好",
                Some(1),
            ),
        ];

        for (target, pattern, expected) in cases {
            let mut ctx = EvalContext::default();

            let builder =
                ExprDefBuilder::scalar_func(ScalarFuncSig::RegexpSig, FieldTypeTp::LongLong);
            let node = builder
                .push_child(ExprDefBuilder::constant_bytes(target))
                .push_child(ExprDefBuilder::constant_bytes(pattern.as_bytes().to_vec()))
                .build();

            let exp = RpnExpressionBuilder::build_from_expr_tree(node, &mut ctx, 1).unwrap();
            let schema = &[];
            let mut columns = LazyBatchColumnVec::empty();
            let val = exp.eval(&mut ctx, schema, &mut columns, &[], 1).unwrap();

            assert!(val.is_vector());
            let v = val.vector_value().unwrap().as_ref().to_int_vec();
            assert_eq!(v.len(), 1);
            assert_eq!(v[0], expected);
        }
    }
}
