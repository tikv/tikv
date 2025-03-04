// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use tidb_query_codegen::rpn_fn;
use tidb_query_common::Result;
use tidb_query_datatype::codec::{collation::*, data_type::*, Error};
use tipb::{Expr, ExprType};

const PATTERN_IDX: usize = 1;
const ESCAPE_IDX: usize = 2;

enum PatternType {
    PatternOne,
    PatternAny,
    PatternMatch,
}

pub struct LikeMeta {
    pattern_bytes: Vec<u8>,
    pattern_character_len: Vec<usize>,
    pattern_types: Vec<PatternType>,
}

fn init_like_meta<CS: Charset>(expr: &mut Expr) -> Result<Option<LikeMeta>> {
    let children = expr.mut_children();
    if children.len() != 3 {
        return Err(Error::incorrect_parameters(&format!(
            "Number of parameter is not 3, but get {}",
            children.len()
        ))
        .into());
    }

    let pattern = match children[PATTERN_IDX].get_tp() {
        ExprType::Bytes | ExprType::String => children[PATTERN_IDX].get_val(),
        _ => return Ok(None),
    };

    let mut escape: u32 = '\\' as u32;

    match children[ESCAPE_IDX].get_tp() {
        ExprType::Int64 | ExprType::Uint64 => {
            let mut buf: [u8; 8] = [0; 8];
            let buf_vec = children[ESCAPE_IDX].get_val().to_vec();
            if buf_vec.len() == 8 {
                for (i, &item) in buf_vec.iter().enumerate() {
                    buf[i] = item;
                }

                escape = i64::from_be_bytes(buf) as u32; // TODO need tests
            }
        }
        _ => return Ok(None),
    };

    let mut i = 0;
    let pattern_len = pattern.len();
    let mut pattern_bytes = Vec::<u8>::new();
    let mut pattern_single_character_len = Vec::<usize>::new();
    let mut pattern_types = Vec::<PatternType>::new();
    let mut is_last_pattern_any = false;

    pattern_bytes.reserve(pattern_len);
    pattern_single_character_len.reserve(pattern_len);
    pattern_types.reserve(pattern_len);

    while i < pattern_len {
        if let Some((c, mut c_len)) = CS::decode_one(&pattern[i..]) {
            let mut current_c_bytes = &pattern[i..i + c_len];
            let item: u32 = c.into();
            let tp: PatternType;
            if item == escape {
                tp = PatternType::PatternMatch;
                if i < pattern_len - c_len {
                    i += c_len;
                    if let Some((_, next_c_len)) = CS::decode_one(&pattern[i..]) {
                        current_c_bytes = &pattern[i..i + next_c_len];
                        c_len = next_c_len;
                        i += c_len;
                    }
                } else {
                    i += c_len;
                }
                is_last_pattern_any = false;
            } else if item == '_' as u32 {
                // %_ => _%
                if is_last_pattern_any {
                    let last_idx = pattern_single_character_len.len() - 1;
                    let last_len = pattern_single_character_len[last_idx];
                    pattern_bytes.resize(pattern_bytes.len() - last_len, 0);
                    pattern_bytes.extend_from_slice(current_c_bytes);
                    pattern_types[last_idx] = PatternType::PatternOne;

                    tp = PatternType::PatternAny;
                    current_c_bytes = "%".as_bytes();
                    c_len = current_c_bytes.len();
                    is_last_pattern_any = true;
                } else {
                    tp = PatternType::PatternOne;
                    is_last_pattern_any = false;
                }
                i += c_len;
            } else if item == '%' as u32 {
                // %% => %
                i += c_len;
                if is_last_pattern_any {
                    continue;
                }

                tp = PatternType::PatternAny;
                is_last_pattern_any = true;
            } else {
                tp = PatternType::PatternMatch;
                is_last_pattern_any = false;
                i += c_len
            }

            pattern_bytes.extend_from_slice(&current_c_bytes);
            pattern_single_character_len.push(c_len);
            pattern_types.push(tp);
        }
    }

    Ok(Some(LikeMeta {
        pattern_bytes,
        pattern_character_len: pattern_single_character_len,
        pattern_types,
    }))
}

fn like_without_cache<C: Collator, CS: Charset>(
    target: BytesRef,
    pattern: BytesRef,
    escape: &i64,
) -> Result<Option<i64>> {
    let escape = *escape as u32;

    // current search positions in pattern and target.
    let (mut px, mut tx) = (0, 0);

    // positions for backtrace.
    let (mut next_px, mut next_tx) = (0, 0);

    let pattern_len = pattern.len();
    let target_len = target.len();

    while px < pattern_len || tx < target_len {
        if let Some((c, mut poff)) = CS::decode_one(&pattern[px..]) {
            let code: u32 = c.into();
            if code == '_' as u32 && escape != '_' as u32 {
                if let Some((_, toff)) = CS::decode_one(&target[tx..]) {
                    px += poff;
                    tx += toff;
                    continue;
                }
            } else if code == '%' as u32 && escape != '%' as u32 {
                // update the backtrace point.
                next_px = px;
                px += poff;
                next_tx = tx;
                next_tx += if let Some((_, toff)) = CS::decode_one(&target[tx..]) {
                    toff
                } else {
                    1
                };
                continue;
            } else {
                if code == escape && px + poff < pattern.len() {
                    px += poff;
                    poff = if let Some((_, off)) = CS::decode_one(&pattern[px..]) {
                        off
                    } else {
                        break;
                    }
                }
                if let Some((_, toff)) = CS::decode_one(&target[tx..]) {
                    if let Ok(std::cmp::Ordering::Equal) =
                        C::sort_compare(&target[tx..tx + toff], &pattern[px..px + poff], true)
                    {
                        tx += toff;
                        px += poff;
                        continue;
                    }
                }
            }
        }
        // mismatch and backtrace to last %.
        if next_tx > 0 && next_tx <= target_len {
            px = next_px;
            tx = next_tx;
            continue;
        }
        return Ok(Some(false as i64));
    }

    Ok(Some(true as i64))
}

fn like_with_cache<C: Collator, CS: Charset>(
    metadata: &LikeMeta,
    target: BytesRef,
) -> Result<Option<i64>> {
    let (mut px, mut pbx, mut tx, mut next_px, mut next_pbx, mut next_tx) = (0, 0, 0, 0, 0, 0);
    let pattern_len = metadata.pattern_types.len();
    let target_len = target.len();

    while px < pattern_len || tx < target_len {
        if px < pattern_len {
            let pattern_type = &metadata.pattern_types[px];
            match pattern_type {
                PatternType::PatternAny => {
                    // update the backtrace point.
                    next_px = px;
                    next_pbx = pbx;
                    pbx += metadata.pattern_character_len[px];
                    px += 1;
                    next_tx = tx;
                    next_tx += if let Some((_, toff)) = CS::decode_one(&target[tx..]) {
                        toff
                    } else {
                        1
                    };
                    continue;
                }
                PatternType::PatternMatch => {
                    if let Some((_, toff)) = CS::decode_one(&target[tx..]) {
                        if let Ok(std::cmp::Ordering::Equal) = C::sort_compare(
                            &target[tx..tx + toff],
                            &metadata.pattern_bytes[pbx..pbx + metadata.pattern_character_len[px]],
                            true,
                        ) {
                            tx += toff;
                            pbx += metadata.pattern_character_len[px];
                            px += 1;
                            continue;
                        }
                    }
                }
                PatternType::PatternOne => {
                    if let Some((_, toff)) = CS::decode_one(&target[tx..]) {
                        pbx += metadata.pattern_character_len[px];
                        px += 1;
                        tx += toff;
                        continue;
                    }
                }
            }
        }

        // mismatch and backtrace to last %.
        if 0 < next_tx && next_tx <= target.len() {
            px = next_px;
            pbx = next_pbx;
            tx = next_tx;
            continue;
        }
        return Ok(Some(false as i64));
    }

    Ok(Some(true as i64))
}

#[rpn_fn(capture = [metadata], metadata_mapper = init_like_meta::<CS>)]
#[inline]
pub fn like<C: Collator, CS: Charset>(
    metadata: &Option<LikeMeta>,
    target: BytesRef,
    pattern: BytesRef,
    escape: &i64,
) -> Result<Option<i64>> {
    match metadata {
        Some(like_meta) => like_with_cache::<C, CS>(like_meta, target),
        None => like_without_cache::<C, CS>(target, pattern, escape),
    }
}

#[cfg(test)]
mod tests {
    use tidb_query_datatype::{
        codec::batch::{LazyBatchColumn, LazyBatchColumnVec},
        expr::EvalContext,
        Collation, EvalType, FieldTypeTp,
    };
    use tipb::ScalarFuncSig;
    use tipb_helper::ExprDefBuilder;

    use crate::RpnExpressionBuilder;

    fn test_like_impl(
        target: &str,
        pattern: &str,
        escape: char,
        collation: Collation,
        target_collation: Collation,
        pattern_collation: Collation,
        expected: Option<i64>,
    ) {
        for i in 0..2 {
            let use_column_ref = i == 1;

            let mut ctx = EvalContext::default();

            let mut builder = ExprDefBuilder::scalar_func_with_collation(
                ScalarFuncSig::LikeSig,
                FieldTypeTp::LongLong,
                collation,
            );

            let mut schema = Vec::new();
            let mut columns = LazyBatchColumnVec::empty();
            if use_column_ref {
                schema.extend_from_slice(&[
                    FieldTypeTp::String.into(),
                    FieldTypeTp::String.into(),
                    FieldTypeTp::LongLong.into(),
                ]);
                builder = builder
                    .push_child(ExprDefBuilder::column_ref_with_collation(
                        0,
                        FieldTypeTp::String,
                        target_collation,
                    ))
                    .push_child(ExprDefBuilder::column_ref_with_collation(
                        1,
                        FieldTypeTp::String,
                        pattern_collation,
                    ))
                    .push_child(ExprDefBuilder::column_ref(2, FieldTypeTp::LongLong));
                let mut col_vec = Vec::new();
                let mut col = LazyBatchColumn::decoded_with_capacity_and_tp(1, EvalType::Bytes);
                col.mut_decoded()
                    .push_bytes(Some(target.as_bytes().to_vec()));
                col_vec.push(col);

                let mut col = LazyBatchColumn::decoded_with_capacity_and_tp(1, EvalType::Bytes);
                col.mut_decoded()
                    .push_bytes(Some(pattern.as_bytes().to_vec()));
                col_vec.push(col);

                let mut col = LazyBatchColumn::decoded_with_capacity_and_tp(1, EvalType::Int);
                col.mut_decoded().push_int(Some(escape as i64));
                col_vec.push(col);

                columns = LazyBatchColumnVec::from(col_vec);
            } else {
                builder = builder
                    .push_child(ExprDefBuilder::constant_bytes_with_collation(
                        target.as_bytes().to_vec(),
                        target_collation,
                    ))
                    .push_child(ExprDefBuilder::constant_bytes_with_collation(
                        pattern.as_bytes().to_vec(),
                        pattern_collation,
                    ))
                    .push_child(ExprDefBuilder::constant_int(escape as i64));
            }

            let node = builder.build();
            let exp =
                RpnExpressionBuilder::build_from_expr_tree(node, &mut ctx, schema.len()).unwrap();

            let val = exp.eval(&mut ctx, &schema, &mut columns, &[0], 1);

            match val {
                Ok(val) => {
                    assert!(val.is_vector());
                    let v = val.vector_value().unwrap().as_ref().to_int_vec();
                    assert_eq!(v.len(), 1);
                    assert_eq!(
                        v[0].unwrap(),
                        expected.unwrap(),
                        "{:?} {:?}",
                        target,
                        pattern,
                    );
                }
                Err(_) => panic!("Get unexpected error"),
            }
        }
    }

    #[test]
    fn test_like() {
        let cases = vec![
            (r#"123"#, r#"%%"#, '%', Collation::Binary, Some(0)),
            (r#"%"#, r#"%"#, '%', Collation::Binary, Some(1)),
            (r#"%"#, r#"%%"#, '%', Collation::Binary, Some(1)),
            (r#"123"#, r#"%"#, '%', Collation::Binary, Some(0)),
            (r#"_"#, r#"__"#, '_', Collation::Binary, Some(1)),
            (r#"1"#, r#"_"#, '_', Collation::Binary, Some(0)),
            (r#"hello"#, r#"%"#, '\\', Collation::Binary, Some(1)),
            (r#"hello"#, r#"hel%a"#, '\\', Collation::Binary, Some(0)),
            (r#"hello"#, r#"hel"#, '\\', Collation::Binary, Some(0)),
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
            (r#"C:\Programs\"#, r#"%%\"#, '%', Collation::Binary, Some(0)),
            (r#"C:\Programs%"#, r#"%%%"#, '%', Collation::Binary, Some(0)),
            (
                r#"C:\Programs%"#,
                r#"%%%%"#,
                '%',
                Collation::Binary,
                Some(0),
            ),
            (r#"hello"#, r#"\%"#, '\\', Collation::Binary, Some(0)),
            (r#"%"#, r#"\%"#, '\\', Collation::Binary, Some(1)),
            (r#"3hello"#, r#"%%hello"#, '%', Collation::Binary, Some(0)),
            (r#"3hello"#, r#"3%hello"#, '3', Collation::Binary, Some(0)),
            (r#"3hello"#, r#"__hello"#, '_', Collation::Binary, Some(0)),
            (r#"3hello"#, r#"%_hello"#, '%', Collation::Binary, Some(0)),
            (
                r#"aaaaaaaaaaaaaaaaaaaaaaaaaaa"#,
                r#"a%a%a%a%a%a%a%a%b"#,
                '\\',
                Collation::Binary,
                Some(0),
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
            (r#"Ⱕ"#, r#"ⱕ"#, '\\', Collation::Utf8Mb40900AiCi, Some(1)),
            (
                r#"a　a"#,
                r#"a a"#,
                '\\',
                Collation::Utf8Mb4UnicodeCi,
                Some(1),
            ),
        ];

        for (target, pattern, escape, collation, expected) in cases {
            test_like_impl(
                target, pattern, escape, collation, collation, collation, expected,
            );
        }
    }

    #[test]
    fn test_like_wide_character() {
        let cases = vec![
            (
                r#"夏威夷吉他"#,
                r#"_____"#,
                '\\',
                Collation::Binary,
                Collation::Binary,
                Collation::Binary,
                Some(0),
            ),
            (
                r#"🐶🍐🍳➕🥜🎗🐜"#,
                r#"_______"#,
                '\\',
                Collation::Utf8Mb4Bin,
                Collation::Utf8Mb4Bin,
                Collation::Utf8Mb4Bin,
                Some(1),
            ),
            (
                r#"🕺_"#,
                r#"🕺🕺🕺_"#,
                '🕺',
                Collation::Binary,
                Collation::Binary,
                Collation::Binary,
                Some(0),
            ),
            (
                r#"🕺_"#,
                r#"🕺🕺🕺_"#,
                '🕺',
                Collation::Utf8Mb4GeneralCi,
                Collation::Utf8Mb4GeneralCi,
                Collation::Utf8Mb4GeneralCi,
                Some(1),
            ),
            // When the new collation framework is not enabled, the collation
            // will always be binary Some related tests are added here
            (
                r#"夏威夷吉他"#,
                r#"_____"#,
                '\\',
                Collation::Binary,
                Collation::Utf8Mb4Bin,
                Collation::Utf8Mb4Bin,
                Some(1),
            ),
            (
                r#"🐶🍐🍳➕🥜🎗🐜"#,
                r#"_______"#,
                '\\',
                Collation::Binary,
                Collation::Utf8Mb4Bin,
                Collation::Utf8Mb4Bin,
                Some(1),
            ),
            (
                r#"🕺_"#,
                r#"🕺🕺🕺_"#,
                '🕺',
                Collation::Binary,
                Collation::Binary,
                Collation::Binary,
                Some(0),
            ),
            (
                r#"🕺_"#,
                r#"🕺🕺🕺_"#,
                '🕺',
                Collation::Binary,
                Collation::Utf8Mb4Bin,
                Collation::Utf8Mb4Bin,
                Some(1),
            ),
            // Will not match, because '_' matches only one byte.
            (
                r#"测试"#,
                r#"测_"#,
                '\\',
                Collation::Binary,
                Collation::Utf8Mb4Bin,
                Collation::Binary,
                Some(0),
            ),
            // Both of them should be decoded with binary charset, so that we'll
            // compare byte with byte, but not comparing a long character with a
            // byte.
            (
                r#"测试"#,
                r#"测%"#,
                '\\',
                Collation::Binary,
                Collation::Utf8Mb4Bin,
                Collation::Binary,
                Some(1),
            ),
            // This can happen when the new collation is not enabled, and TiDB
            // doesn't push down the collation information. Using binary
            // comparing order is fine, but we'll need to decode strings with
            // their own charset (so '_' could match single character, rather
            // than single byte).
            (
                r#"测试"#,
                r#"测_"#,
                '\\',
                Collation::Binary,
                Collation::Utf8Mb4Bin,
                Collation::Utf8Mb4Bin,
                Some(1),
            ),
            // This can happen when the new collation is not enabled, and TiDB
            // doesn't push down the collation information. Though the two collations
            // are the same, we still use the binary order.
            (
                r#"测试A"#,
                r#"测_a"#,
                '\\',
                Collation::Binary,
                Collation::Utf8Mb4UnicodeCi,
                Collation::Utf8Mb4UnicodeCi,
                Some(0),
            ),
        ];

        for (target, pattern, escape, collation, target_collation, pattern_collation, expected) in
            cases
        {
            test_like_impl(
                target,
                pattern,
                escape,
                collation,
                target_collation,
                pattern_collation,
                expected,
            );
        }
    }
}
