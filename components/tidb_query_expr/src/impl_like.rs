// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use regex::{bytes::Regex as BytesRegex, Regex};

use tipb::{Expr, ExprType};

use tidb_query_codegen::rpn_fn;

use tidb_query_common::Result;
use tidb_query_datatype::codec::collation::*;
use tidb_query_datatype::codec::data_type::*;

#[rpn_fn]
#[inline]
pub fn like<C: Collator>(target: BytesRef, pattern: BytesRef, escape: &i64) -> Result<Option<i64>> {
    let escape = *escape as u32;
    // current search positions in pattern and target.
    let (mut px, mut tx) = (0, 0);
    // positions for backtrace.
    let (mut next_px, mut next_tx) = (0, 0);
    while px < pattern.len() || tx < target.len() {
        if let Some((c, mut poff)) = C::Charset::decode_one(&pattern[px..]) {
            let code: u32 = c.into();
            if code == '_' as u32 {
                if let Some((_, toff)) = C::Charset::decode_one(&target[tx..]) {
                    px += poff;
                    tx += toff;
                    continue;
                }
            } else if code == '%' as u32 {
                // update the backtrace point.
                next_px = px;
                px += poff;
                next_tx = tx;
                next_tx += if let Some((_, toff)) = C::Charset::decode_one(&target[tx..]) {
                    toff
                } else {
                    1
                };
                continue;
            } else {
                if code == escape && px + poff < pattern.len() {
                    px += poff;
                    poff = if let Some((_, off)) = C::Charset::decode_one(&pattern[px..]) {
                        off
                    } else {
                        break;
                    }
                }
                if let Some((_, toff)) = C::Charset::decode_one(&target[tx..]) {
                    if let Ok(std::cmp::Ordering::Equal) =
                        C::sort_compare(&target[tx..tx + toff], &pattern[px..px + poff])
                    {
                        tx += toff;
                        px += poff;
                        continue;
                    }
                }
            }
        }
        // mismatch and backtrace to last %.
        if 0 < next_tx && next_tx <= target.len() {
            px = next_px;
            tx = next_tx;
            continue;
        }
        return Ok(Some(false as i64));
    }

    Ok(Some(true as i64))
}

#[rpn_fn(capture = [metadata], metadata_mapper = init_regexp_utf8_data)]
#[inline]
pub fn regexp_utf8(
    metadata: &Option<Regex>,
    target: BytesRef,
    pattern: BytesRef,
) -> Result<Option<i64>> {
    let target = String::from_utf8(target.to_vec())?;

    match metadata {
        Some(regex) => Ok(Some(regex.is_match(target.as_ref()) as i64)),
        None => {
            let pattern = String::from_utf8(pattern.to_vec())?;
            let pattern = format!("(?i){}", pattern);

            match Regex::new(&pattern) {
                Ok(regex) => Ok(Some(regex.is_match(target.as_ref()) as i64)),
                Err(err) => Err(box_err!("invalid regex pattern: {:?}", err)),
            }
        }
    }
}

#[rpn_fn(capture = [metadata], metadata_mapper = init_regexp_data)]
#[inline]
pub fn regexp(
    metadata: &Option<BytesRegex>,
    target: BytesRef,
    pattern: BytesRef,
) -> Result<Option<i64>> {
    match metadata {
        Some(regex) => Ok(Some(regex.is_match(target.as_ref()) as i64)),
        None => {
            let pattern = String::from_utf8(pattern.to_vec())?;
            match BytesRegex::new(&pattern) {
                Ok(bytes_regex) => Ok(Some(bytes_regex.is_match(target.as_ref()) as i64)),
                Err(err) => Err(box_err!("invalid regex pattern: {:?}", err)),
            }
        }
    }
}

fn init_regexp_utf8_data(expr: &mut Expr) -> Result<Option<Regex>> {
    let children = expr.mut_children();
    if children.len() <= 1 {
        return Ok(None);
    }

    let tree_node = &mut children[1];
    match tree_node.get_tp() {
        ExprType::Bytes | ExprType::String => {
            let val = match String::from_utf8(tree_node.take_val()) {
                Ok(val) => val,
                Err(_) => return Ok(None),
            };
            match Regex::new(val.as_ref()) {
                Ok(regex) => Ok(Some(regex)),
                Err(_) => Ok(None),
            }
        }
        _ => Ok(None),
    }
}

fn init_regexp_data(expr: &mut Expr) -> Result<Option<BytesRegex>> {
    let children = expr.mut_children();
    if children.len() <= 1 {
        return Ok(None);
    }

    let tree_node = &mut children[1];
    match tree_node.get_tp() {
        ExprType::Bytes | ExprType::String => {
            let val = match String::from_utf8(tree_node.take_val()) {
                Ok(val) => val,
                Err(_) => return Ok(None),
            };
            match BytesRegex::new(val.as_ref()) {
                Ok(regex) => Ok(Some(regex)),
                Err(_) => Ok(None),
            }
        }
        _ => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use tipb::ScalarFuncSig;

    use crate::{test_util::RpnFnScalarEvaluator, RpnExpressionBuilder};

    use tidb_query_datatype::builder::FieldTypeBuilder;
    use tidb_query_datatype::codec::batch::LazyBatchColumnVec;
    use tidb_query_datatype::expr::EvalContext;
    use tidb_query_datatype::Collation;
    use tidb_query_datatype::FieldTypeTp;
    use tipb_helper::ExprDefBuilder;

    #[test]
    fn test_like() {
        let cases = vec![
            (r#"hello"#, r#"%HELLO%"#, '\\', Collation::Binary, Some(0)),
            (
                r#"Hello, World"#,
                r#"Hello, World"#,
                '\\',
                Collation::Binary,
                Some(1),
            ),
            (
                r#"Hello, World"#,
                r#"Hello, %"#,
                '\\',
                Collation::Binary,
                Some(1),
            ),
            (
                r#"Hello, World"#,
                r#"%, World"#,
                '\\',
                Collation::Binary,
                Some(1),
            ),
            (r#"test"#, r#"te%st"#, '\\', Collation::Binary, Some(1)),
            (r#"test"#, r#"te%%st"#, '\\', Collation::Binary, Some(1)),
            (r#"test"#, r#"test%"#, '\\', Collation::Binary, Some(1)),
            (r#"test"#, r#"%test%"#, '\\', Collation::Binary, Some(1)),
            (r#"test"#, r#"t%e%s%t"#, '\\', Collation::Binary, Some(1)),
            (r#"test"#, r#"_%_%_%_"#, '\\', Collation::Binary, Some(1)),
            (r#"test"#, r#"_%_%st"#, '\\', Collation::Binary, Some(1)),
            (r#"C:"#, r#"%\"#, '\\', Collation::Binary, Some(0)),
            (r#"C:\"#, r#"%\"#, '\\', Collation::Binary, Some(1)),
            (r#"C:\Programs"#, r#"%\"#, '\\', Collation::Binary, Some(0)),
            (r#"C:\Programs\"#, r#"%\"#, '\\', Collation::Binary, Some(1)),
            (r#"C:"#, r#"%\\"#, '\\', Collation::Binary, Some(0)),
            (r#"C:\"#, r#"%\\"#, '\\', Collation::Binary, Some(1)),
            (r#"C:\\"#, r#"C:\\"#, '\\', Collation::Binary, Some(0)),
            (r#"C:\Programs"#, r#"%\\"#, '\\', Collation::Binary, Some(0)),
            (
                r#"C:\Programs\"#,
                r#"%\\"#,
                '\\',
                Collation::Binary,
                Some(1),
            ),
            (
                r#"C:\Programs\"#,
                r#"%Prog%"#,
                '\\',
                Collation::Binary,
                Some(1),
            ),
            (
                r#"C:\Programs\"#,
                r#"%Pr_g%"#,
                '\\',
                Collation::Binary,
                Some(1),
            ),
            (r#"C:\Programs\"#, r#"%%\"#, '%', Collation::Binary, Some(1)),
            (r#"C:\Programs%"#, r#"%%%"#, '%', Collation::Binary, Some(1)),
            (
                r#"C:\Programs%"#,
                r#"%%%%"#,
                '%',
                Collation::Binary,
                Some(1),
            ),
            (r#"hello"#, r#"\%"#, '\\', Collation::Binary, Some(0)),
            (r#"%"#, r#"\%"#, '\\', Collation::Binary, Some(1)),
            (r#"3hello"#, r#"%%hello"#, '%', Collation::Binary, Some(1)),
            (r#"3hello"#, r#"3%hello"#, '3', Collation::Binary, Some(0)),
            (r#"3hello"#, r#"__hello"#, '_', Collation::Binary, Some(0)),
            (r#"3hello"#, r#"%_hello"#, '%', Collation::Binary, Some(1)),
            (
                r#"aaaaaaaaaaaaaaaaaaaaaaaaaaa"#,
                r#"a%a%a%a%a%a%a%a%b"#,
                '\\',
                Collation::Binary,
                Some(0),
            ),
            (
                r#"夏威夷吉他"#,
                r#"_____"#,
                '\\',
                Collation::Binary,
                Some(0),
            ),
            (
                r#"🐶🍐🍳➕🥜🎗🐜"#,
                r#"_______"#,
                '\\',
                Collation::Utf8Mb4Bin,
                Some(1),
            ),
            (
                r#"IpHONE"#,
                r#"iPhone"#,
                '\\',
                Collation::Utf8Mb4Bin,
                Some(0),
            ),
            (
                r#"IpHONE xs mAX"#,
                r#"iPhone XS Max"#,
                '\\',
                Collation::Utf8Mb4GeneralCi,
                Some(1),
            ),
            (r#"🕺_"#, r#"🕺🕺🕺_"#, '🕺', Collation::Binary, Some(0)),
            (
                r#"🕺_"#,
                r#"🕺🕺🕺_"#,
                '🕺',
                Collation::Utf8Mb4GeneralCi,
                Some(1),
            ),
            (r#"baab"#, r#"b_%b"#, '\\', Collation::Utf8Mb4Bin, Some(1)),
            (r#"baab"#, r#"b%_b"#, '\\', Collation::Utf8Mb4Bin, Some(1)),
            (r#"bab"#, r#"b_%b"#, '\\', Collation::Utf8Mb4Bin, Some(1)),
            (r#"bab"#, r#"b%_b"#, '\\', Collation::Utf8Mb4Bin, Some(1)),
            (r#"bb"#, r#"b_%b"#, '\\', Collation::Utf8Mb4Bin, Some(0)),
            (r#"bb"#, r#"b%_b"#, '\\', Collation::Utf8Mb4Bin, Some(0)),
            (
                r#"baabccc"#,
                r#"b_%b%"#,
                '\\',
                Collation::Utf8Mb4Bin,
                Some(1),
            ),
            (
                r#"ßssß"#,
                r#"_sSß"#,
                '\\',
                Collation::Utf8Mb4GeneralCi,
                Some(1),
            ),
            (
                r#"ßssß"#,
                r#"_ßß"#,
                '\\',
                Collation::Utf8Mb4GeneralCi,
                Some(0),
            ),
        ];
        for (target, pattern, escape, collation, expected) in cases {
            let output = RpnFnScalarEvaluator::new()
                .return_field_type(
                    FieldTypeBuilder::new()
                        .tp(FieldTypeTp::LongLong)
                        .collation(collation)
                        .build(),
                )
                .push_param(target.to_owned().into_bytes())
                .push_param(pattern.to_owned().into_bytes())
                .push_param(escape as i64)
                .evaluate(ScalarFuncSig::LikeSig)
                .unwrap();
            assert_eq!(
                output, expected,
                "target={}, pattern={}, escape={}",
                target, pattern, escape
            );
        }
    }

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
