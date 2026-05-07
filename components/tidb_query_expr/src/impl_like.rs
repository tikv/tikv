// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use tidb_query_codegen::rpn_fn;
use tidb_query_common::Result;
use tidb_query_datatype::codec::{collation::*, data_type::*};

#[rpn_fn]
#[inline]
pub fn like<C: Collator, CS: Charset>(
    target: BytesRef,
    pattern: BytesRef,
    escape: &i64,
) -> Result<Option<i64>> {
    let escape = *escape as u32;
    // current search positions in pattern and target.
    let (mut px, mut tx) = (0, 0);
    // positions for backtrace.
    let (mut next_px, mut next_tx) = (0, 0);
    while px < pattern.len() || tx < target.len() {
        if let Some((c, mut poff)) = CS::decode_one(&pattern[px..]) {
            let code: u32 = c.into();
            if code == '_' as u32 {
                if let Some((_, toff)) = CS::decode_one(&target[tx..]) {
                    px += poff;
                    tx += toff;
                    continue;
                }
            } else if code == '%' as u32 {
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
        if 0 < next_tx && next_tx <= target.len() {
            px = next_px;
            tx = next_tx;
            continue;
        }
        return Ok(Some(false as i64));
    }

    Ok(Some(true as i64))
}

#[cfg(test)]
mod tests {
    use tidb_query_datatype::{builder::FieldTypeBuilder, Collation, FieldTypeTp};
    use tipb::ScalarFuncSig;

    use crate::test_util::RpnFnScalarEvaluator;

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
            let ret_ft = FieldTypeBuilder::new()
                .tp(FieldTypeTp::LongLong)
                .collation(collation)
                .build();
            let arg_ft = FieldTypeBuilder::new()
                .tp(FieldTypeTp::String)
                .collation(collation)
                .build();
            let output = RpnFnScalarEvaluator::new()
                .return_field_type(ret_ft.clone())
                .push_param_with_field_type(target.to_owned().into_bytes(), arg_ft.clone())
                .push_param_with_field_type(pattern.to_owned().into_bytes(), arg_ft)
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
            let output = RpnFnScalarEvaluator::new()
                .return_field_type(
                    FieldTypeBuilder::new()
                        .tp(FieldTypeTp::LongLong)
                        .collation(collation)
                        .build(),
                )
                .push_param_with_field_type(
                    target.to_owned().into_bytes(),
                    FieldTypeBuilder::new()
                        .tp(FieldTypeTp::String)
                        .collation(target_collation),
                )
                .push_param_with_field_type(
                    pattern.to_owned().into_bytes(),
                    FieldTypeBuilder::new()
                        .tp(FieldTypeTp::String)
                        .collation(pattern_collation),
                )
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
}
