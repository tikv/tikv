// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use tidb_query_codegen::rpn_fn;
use tidb_query_common::Result;
use tidb_query_datatype::codec::{collation::*, data_type::*};

const UTF8_REPLACEMENT_CHARACTER: &[u8] = b"\xEF\xBF\xBD";

// TiDB decodes malformed UTF-8 as U+FFFD when matching with a character
// collation. Canonicalize only that case; collators using byte-wise LIKE
// literal matching must continue to compare the original bytes.
#[inline]
fn char_bytes_for_compare<C: Collator, CS: Charset>(data: &[u8], ch: CS::Char) -> &[u8] {
    if <C::Charset as Charset>::charset() == tidb_query_datatype::Charset::Utf8Mb4
        && ch.into() == char::REPLACEMENT_CHARACTER as u32
        && data.len() == 1
    {
        UTF8_REPLACEMENT_CHARACTER
    } else {
        data
    }
}

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
        if let Some((mut pattern_char, mut poff)) = CS::decode_one(&pattern[px..]) {
            let code: u32 = pattern_char.into();
            let is_escape = code == escape;
            if is_escape && px + poff < pattern.len() {
                px += poff;
                (pattern_char, poff) = if let Some((ch, off)) = CS::decode_one(&pattern[px..]) {
                    (ch, off)
                } else {
                    break;
                };
            }
            if !is_escape && code == '_' as u32 {
                if let Some((_, toff)) = CS::decode_one(&target[tx..]) {
                    px += poff;
                    tx += toff;
                    continue;
                }
            } else if !is_escape && code == '%' as u32 {
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
                if let Some((target_char, toff)) = CS::decode_one(&target[tx..]) {
                    let target_bytes = &target[tx..tx + toff];
                    let pattern_bytes = &pattern[px..px + poff];
                    let matches = if C::LIKE_PATTERN_MODE == LikePatternMode::Bytes {
                        target_bytes == pattern_bytes
                    } else {
                        let target_char_bytes =
                            char_bytes_for_compare::<C, CS>(target_bytes, target_char);
                        let pattern_char_bytes =
                            char_bytes_for_compare::<C, CS>(pattern_bytes, pattern_char);
                        C::like_pattern_compare(target_char_bytes, pattern_char_bytes)?
                    };
                    if matches {
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

    fn eval_like_with_collation_ids(
        target: &[u8],
        pattern: &[u8],
        ret_collation_id: i32,
        arg_collation_id: i32,
    ) -> Option<i64> {
        eval_like_with_collation_ids_and_escape(
            target,
            pattern,
            '\\',
            ret_collation_id,
            arg_collation_id,
        )
    }

    fn eval_like_with_collation_ids_and_escape(
        target: &[u8],
        pattern: &[u8],
        escape: char,
        ret_collation_id: i32,
        arg_collation_id: i32,
    ) -> Option<i64> {
        let mut ret_ft = FieldTypeBuilder::new()
            .tp(FieldTypeTp::LongLong)
            .collation(Collation::Binary)
            .build();
        ret_ft.set_collate(ret_collation_id);
        let mut arg_ft = FieldTypeBuilder::new()
            .tp(FieldTypeTp::String)
            .collation(Collation::Binary)
            .build();
        arg_ft.set_collate(arg_collation_id);
        RpnFnScalarEvaluator::new()
            .return_field_type(ret_ft)
            .push_param_with_field_type(target.to_vec(), arg_ft.clone())
            .push_param_with_field_type(pattern.to_vec(), arg_ft)
            .push_param(escape as i64)
            .evaluate(ScalarFuncSig::LikeSig)
            .unwrap()
    }

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
            (r#"😀"#, r#"😁"#, '\\', Collation::Utf8Mb4UnicodeCi, Some(0)),
            (r#"😀"#, r#"😀"#, '\\', Collation::Utf8Mb4UnicodeCi, Some(1)),
            (r#"😀"#, r#"�"#, '\\', Collation::Utf8Mb4UnicodeCi, Some(0)),
            (
                r#"x😀y"#,
                r#"%😀%"#,
                '\\',
                Collation::Utf8Mb4UnicodeCi,
                Some(1),
            ),
            (
                r#"x😀y"#,
                r#"%😁%"#,
                '\\',
                Collation::Utf8Mb4UnicodeCi,
                Some(0),
            ),
            (
                "\u{321D}",
                "\u{321D}",
                '\\',
                Collation::Utf8Mb4UnicodeCi,
                Some(1),
            ),
            (
                "\u{321D}",
                "\u{321E}",
                '\\',
                Collation::Utf8Mb4UnicodeCi,
                Some(0),
            ),
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
    fn test_like_invalid_utf8() {
        fn eval_like(
            target: &[u8],
            pattern: &[u8],
            ret_collation: Collation,
            arg_collation: Collation,
        ) -> Option<i64> {
            eval_like_with_collation_ids(
                target,
                pattern,
                ret_collation as i32,
                arg_collation as i32,
            )
        }

        // Regression tests for pingcap/tidb#66597 and pingcap/tidb#67082.
        let issue_values = [
            vec![0xE4],
            vec![0x02, 0xAA, 0xAA, 0xAA, 0xAA, 0xAA, 0xAA, 0xAA],
        ];
        for value in issue_values {
            assert_eq!(
                eval_like(&value, &value, Collation::Utf8Mb4Bin, Collation::Utf8Mb4Bin,),
                Some(1)
            );
        }

        let replacement = char::REPLACEMENT_CHARACTER.to_string().into_bytes();
        let character_cases = [
            (vec![0xE4], vec![0xAA], Some(1)),
            (vec![0xE4], replacement.clone(), Some(1)),
            (replacement.clone(), vec![0xE4], Some(1)),
            (vec![0xE4], b"a".to_vec(), Some(0)),
            (vec![0xAA], vec![b'\\', 0xE4], Some(1)),
        ];
        for collation in [
            Collation::Utf8Mb4Bin,
            Collation::Utf8Mb4GeneralCi,
            Collation::Utf8Mb4UnicodeCi,
            Collation::Utf8Mb40900AiCi,
            Collation::Utf8Mb40900Bin,
            Collation::GbkBin,
            Collation::GbkChineseCi,
            Collation::Gb18030ChineseCi,
        ] {
            for (target, pattern, expected) in &character_cases {
                assert_eq!(
                    eval_like(target, pattern, collation, collation),
                    *expected,
                    "target={target:?}, pattern={pattern:?}, collation={collation:?}"
                );
            }
        }

        for (target, pattern, expected) in [
            (vec![0xE4], vec![0xE4], Some(1)),
            (vec![0xE4], vec![0xAA], Some(0)),
            (vec![0xE4], replacement, Some(0)),
            ("中".as_bytes().to_vec(), "中".as_bytes().to_vec(), Some(1)),
            ("中".as_bytes().to_vec(), "文".as_bytes().to_vec(), Some(0)),
        ] {
            assert_eq!(
                eval_like(
                    &target,
                    &pattern,
                    Collation::Gb18030Bin,
                    Collation::Gb18030Bin,
                ),
                expected,
                "target={target:?}, pattern={pattern:?}, collation=Gb18030Bin"
            );
        }
    }

    #[test]
    fn test_like_pattern_modes() {
        const LEGACY_BINARY: i32 = Collation::Binary as i32;
        const NEW_BINARY: i32 = -(Collation::Binary as i32);
        const LEGACY_UTF8MB4_BIN: i32 = Collation::Utf8Mb4BinNoPadding as i32;
        const NEW_UTF8MB4_BIN: i32 = Collation::Utf8Mb4Bin as i32;
        const NEW_UTF8MB4_GENERAL_CI: i32 = Collation::Utf8Mb4GeneralCi as i32;
        const NEW_LATIN1_BIN: i32 = Collation::Latin1Bin as i32;
        const NEW_GBK_BIN: i32 = Collation::GbkBin as i32;
        const NEW_GB18030_BIN: i32 = Collation::Gb18030Bin as i32;

        let replacement = char::REPLACEMENT_CHARACTER.to_string().into_bytes();

        // Escape takes precedence when the escape character is itself a LIKE
        // wildcard. Cover the legacy, byte, derived-binary rune, and
        // collator-defined pattern paths.
        for collation_id in [
            LEGACY_BINARY,
            NEW_BINARY,
            NEW_UTF8MB4_BIN,
            NEW_UTF8MB4_GENERAL_CI,
        ] {
            for (target, pattern, escape, expected) in [
                (b"".as_slice(), b"%".as_slice(), '%', Some(0)),
                (b"%".as_slice(), b"%".as_slice(), '%', Some(1)),
                (b"a".as_slice(), b"_".as_slice(), '_', Some(0)),
                (b"_".as_slice(), b"_".as_slice(), '_', Some(1)),
            ] {
                assert_eq!(
                    eval_like_with_collation_ids_and_escape(
                        target,
                        pattern,
                        escape,
                        collation_id,
                        collation_id,
                    ),
                    expected,
                    "target={target:?}, pattern={pattern:?}, escape={escape:?}, collation_id={collation_id}"
                );
            }
        }

        // The legacy collation framework uses a derived binary pattern for all
        // collations. It matches decoded runes without applying collation
        // weights.
        assert_eq!(
            eval_like_with_collation_ids(&[0xE4], &[0xAA], LEGACY_BINARY, LEGACY_UTF8MB4_BIN,),
            Some(1)
        );
        assert_eq!(
            eval_like_with_collation_ids(&[0xE4], &replacement, LEGACY_BINARY, LEGACY_UTF8MB4_BIN,),
            Some(1)
        );
        assert_eq!(
            eval_like_with_collation_ids(
                &[0xAA],
                &[b'\\', 0xE4],
                LEGACY_BINARY,
                LEGACY_UTF8MB4_BIN,
            ),
            Some(1)
        );
        assert_eq!(
            eval_like_with_collation_ids("中".as_bytes(), b"_", LEGACY_BINARY, LEGACY_UTF8MB4_BIN,),
            Some(1)
        );
        assert_eq!(
            eval_like_with_collation_ids("中".as_bytes(), b"_", LEGACY_BINARY, LEGACY_BINARY,),
            Some(1)
        );
        assert_eq!(
            eval_like_with_collation_ids(&[0xE4], &[0xAA], LEGACY_BINARY, LEGACY_BINARY,),
            Some(1)
        );

        // New binary and gb18030_bin collations use byte patterns.
        for collation_id in [NEW_BINARY, NEW_GB18030_BIN] {
            assert_eq!(
                eval_like_with_collation_ids(&[0xE4], &[0xAA], collation_id, collation_id),
                Some(0)
            );
            assert_eq!(
                eval_like_with_collation_ids("中".as_bytes(), b"_", collation_id, collation_id,),
                Some(0)
            );
        }

        // Other new collations use rune/weight patterns.
        assert_eq!(
            eval_like_with_collation_ids(&[0xE4], &[0xAA], NEW_UTF8MB4_BIN, NEW_UTF8MB4_BIN,),
            Some(1)
        );
        assert_eq!(
            eval_like_with_collation_ids("中".as_bytes(), b"_", NEW_UTF8MB4_BIN, NEW_UTF8MB4_BIN,),
            Some(1)
        );

        // New derived-binary collations decode UTF-8 runes but compare rune
        // identity instead of collation weights.
        assert_eq!(
            eval_like_with_collation_ids(&[0xC3, 0xA9], b"_", NEW_LATIN1_BIN, NEW_LATIN1_BIN,),
            Some(1)
        );
        assert_eq!(
            eval_like_with_collation_ids(&[0xE4], &[0xAA], NEW_LATIN1_BIN, NEW_LATIN1_BIN,),
            Some(1)
        );
        assert_eq!(
            eval_like_with_collation_ids(
                "😀".as_bytes(),
                "😁".as_bytes(),
                NEW_GBK_BIN,
                NEW_GBK_BIN,
            ),
            Some(0)
        );

        // '%' backtracking advances by bytes for byte patterns and by decoded
        // runes for derived-binary and collation-weight patterns.
        for collation_id in [NEW_BINARY, NEW_GB18030_BIN] {
            assert_eq!(
                eval_like_with_collation_ids("中X".as_bytes(), b"%__X", collation_id, collation_id,),
                Some(1)
            );
        }
        assert_eq!(
            eval_like_with_collation_ids(
                "中X".as_bytes(),
                b"%__X",
                LEGACY_BINARY,
                LEGACY_UTF8MB4_BIN,
            ),
            Some(0)
        );
        for collation_id in [NEW_LATIN1_BIN, NEW_GBK_BIN, NEW_UTF8MB4_GENERAL_CI] {
            assert_eq!(
                eval_like_with_collation_ids("中X".as_bytes(), b"%__X", collation_id, collation_id,),
                Some(0)
            );
        }
        assert_eq!(
            eval_like_with_collation_ids(
                "中X".as_bytes(),
                b"%__X",
                NEW_UTF8MB4_BIN,
                NEW_UTF8MB4_BIN,
            ),
            Some(0)
        );
    }

    #[test]
    fn test_like_wide_character() {
        const LEGACY_BINARY: i32 = Collation::Binary as i32;
        const NEW_BINARY: i32 = -(Collation::Binary as i32);

        let cases = vec![
            (
                r#"夏威夷吉他"#,
                r#"_____"#,
                '\\',
                NEW_BINARY,
                Collation::Binary,
                Collation::Binary,
                Some(0),
            ),
            (
                r#"🐶🍐🍳➕🥜🎗🐜"#,
                r#"_______"#,
                '\\',
                Collation::Utf8Mb4Bin as i32,
                Collation::Utf8Mb4Bin,
                Collation::Utf8Mb4Bin,
                Some(1),
            ),
            (
                r#"🕺_"#,
                r#"🕺🕺🕺_"#,
                '🕺',
                NEW_BINARY,
                Collation::Binary,
                Collation::Binary,
                Some(0),
            ),
            (
                r#"🕺_"#,
                r#"🕺🕺🕺_"#,
                '🕺',
                Collation::Utf8Mb4GeneralCi as i32,
                Collation::Utf8Mb4GeneralCi,
                Collation::Utf8Mb4GeneralCi,
                Some(1),
            ),
            // When the new collation framework is not enabled, the collation
            // will always be binary. Some related tests are added here.
            (
                r#"夏威夷吉他"#,
                r#"_____"#,
                '\\',
                LEGACY_BINARY,
                Collation::Utf8Mb4Bin,
                Collation::Utf8Mb4Bin,
                Some(1),
            ),
            (
                r#"🐶🍐🍳➕🥜🎗🐜"#,
                r#"_______"#,
                '\\',
                LEGACY_BINARY,
                Collation::Utf8Mb4Bin,
                Collation::Utf8Mb4Bin,
                Some(1),
            ),
            (
                r#"🕺_"#,
                r#"🕺🕺🕺_"#,
                '🕺',
                NEW_BINARY,
                Collation::Binary,
                Collation::Binary,
                Some(0),
            ),
            (
                r#"🕺_"#,
                r#"🕺🕺🕺_"#,
                '🕺',
                LEGACY_BINARY,
                Collation::Utf8Mb4Bin,
                Collation::Utf8Mb4Bin,
                Some(1),
            ),
            // Will not match, because '_' matches only one byte.
            (
                r#"测试"#,
                r#"测_"#,
                '\\',
                NEW_BINARY,
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
                NEW_BINARY,
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
                LEGACY_BINARY,
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
                LEGACY_BINARY,
                Collation::Utf8Mb4UnicodeCi,
                Collation::Utf8Mb4UnicodeCi,
                Some(0),
            ),
        ];
        for (
            target,
            pattern,
            escape,
            ret_collation_id,
            target_collation,
            pattern_collation,
            expected,
        ) in cases
        {
            let mut ret_ft = FieldTypeBuilder::new()
                .tp(FieldTypeTp::LongLong)
                .collation(Collation::Binary)
                .build();
            ret_ft.set_collate(ret_collation_id);
            let output = RpnFnScalarEvaluator::new()
                .return_field_type(ret_ft)
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
