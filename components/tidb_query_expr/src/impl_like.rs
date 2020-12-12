// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use regex::{bytes::Regex as BytesRegex, Regex};

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

#[rpn_fn]
#[inline]
pub fn regexp_utf8(target: BytesRef, pattern: BytesRef) -> Result<Option<i64>> {
    let target = match String::from_utf8(target.to_vec()) {
        Ok(target) => target,
        Err(err) => return Err(box_err!("invalid input value: {:?}", err)),
    };
    let pattern = match String::from_utf8(pattern.to_vec()) {
        Ok(pattern) => pattern,
        Err(err) => return Err(box_err!("invalid input value: {:?}", err)),
    };
    let pattern = format!("(?i){}", pattern);

    // TODO: cache compiled result
    match Regex::new(&pattern) {
        Ok(regex) => Ok(Some(regex.is_match(target.as_ref()) as i64)),
        Err(err) => Err(box_err!("invalid regex pattern: {:?}", err)),
    }
}

#[rpn_fn]
#[inline]
pub fn regexp(target: BytesRef, pattern: BytesRef) -> Result<Option<i64>> {
    let pattern = match String::from_utf8(pattern.to_vec()) {
        Ok(pattern) => pattern,
        Err(err) => return Err(box_err!("invalid input value: {:?}", err)),
    };

    // TODO: cache compiled result
    match BytesRegex::new(&pattern) {
        Ok(bytes_regex) => Ok(Some(bytes_regex.is_match(target.as_ref()) as i64)),
        Err(err) => Err(box_err!("invalid regex pattern: {:?}", err)),
    }
}

#[cfg(test)]
mod tests {
    use tidb_query_datatype::builder::FieldTypeBuilder;
    use tidb_query_datatype::{Collation, FieldTypeTp};
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
            ("aA", r"Aa", Some(1)),
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
            let output = RpnFnScalarEvaluator::new()
                .push_param(target.to_owned().into_bytes())
                .push_param(pattern.to_owned().into_bytes())
                .evaluate(ScalarFuncSig::RegexpUtf8Sig)
                .unwrap();
            assert_eq!(output, expected);
        }
    }

    #[test]
    fn test_regexp() {
        let cases = vec![
            ("a".to_owned().into_bytes(), r"^$", Some(0)),
            ("a".to_owned().into_bytes(), r"a", Some(1)),
            ("b".to_owned().into_bytes(), r"a", Some(0)),
            ("aA".to_owned().into_bytes(), r"Aa", Some(0)),
            ("aaa".to_owned().into_bytes(), r".", Some(1)),
            ("ab".to_owned().into_bytes(), r"^.$", Some(0)),
            ("b".to_owned().into_bytes(), r"..", Some(0)),
            ("aab".to_owned().into_bytes(), r".ab", Some(1)),
            ("abcd".to_owned().into_bytes(), r".*", Some(1)),
            (vec![0x7f], r"^.$", Some(1)), // dot should match one byte which is less than 128
            (vec![0xf0], r"^.$", Some(0)), // dot can't match one byte greater than 128
            // dot should match "你" even if the char has 3 bytes.
            ("你".to_owned().into_bytes(), r"^.$", Some(1)),
            ("你好".to_owned().into_bytes(), r"你好", Some(1)),
            ("你好".to_owned().into_bytes(), r"^你好$", Some(1)),
            ("你好".to_owned().into_bytes(), r"^您好$", Some(0)),
            (
                vec![255, 255, 0xE4, 0xBD, 0xA0, 0xE5, 0xA5, 0xBD],
                r"你好",
                Some(1),
            ),
        ];

        for (target, pattern, expected) in cases {
            let output = RpnFnScalarEvaluator::new()
                .push_param(target)
                .push_param(pattern.to_owned().into_bytes())
                .evaluate(ScalarFuncSig::RegexpSig)
                .unwrap();
            assert_eq!(output, expected);
        }
    }
}
