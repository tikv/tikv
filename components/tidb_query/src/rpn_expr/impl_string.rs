// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::str;
use tidb_query_codegen::rpn_fn;

use crate::codec::data_type::*;
use crate::Result;

const SPACE: u8 = 0o40u8;

#[rpn_fn]
#[inline]
pub fn bin(num: &Option<Int>) -> Result<Option<Bytes>> {
    Ok(num.as_ref().map(|i| Bytes::from(format!("{:b}", i))))
}

#[rpn_fn]
#[inline]
pub fn length(arg: &Option<Bytes>) -> Result<Option<i64>> {
    Ok(arg.as_ref().map(|bytes| bytes.len() as i64))
}

#[rpn_fn]
#[inline]
pub fn bit_length(arg: &Option<Bytes>) -> Result<Option<i64>> {
    Ok(arg.as_ref().map(|bytes| bytes.len() as i64 * 8))
}

#[rpn_fn(varg, min_args = 1)]
#[inline]
pub fn concat(args: &[&Option<Bytes>]) -> Result<Option<Bytes>> {
    let mut output = Bytes::new();
    for arg in args {
        if let Some(s) = arg {
            output.extend_from_slice(s);
        } else {
            return Ok(None);
        }
    }
    Ok(Some(output))
}

#[rpn_fn]
#[inline]
pub fn ascii(arg: &Option<Bytes>) -> Result<Option<i64>> {
    Ok(arg.as_ref().map(|bytes| {
        if bytes.is_empty() {
            0
        } else {
            i64::from(bytes[0])
        }
    }))
}

#[rpn_fn]
#[inline]
pub fn reverse(arg: &Option<Bytes>) -> Result<Option<Bytes>> {
    Ok(arg.as_ref().map(|bytes| {
        let s = String::from_utf8_lossy(bytes);
        s.chars().rev().collect::<String>().into_bytes()
    }))
}

#[rpn_fn]
#[inline]
pub fn hex_int_arg(arg: &Option<Int>) -> Result<Option<Bytes>> {
    Ok(arg.as_ref().map(|i| format!("{:X}", i).into_bytes()))
}

#[rpn_fn]
#[inline]
pub fn ltrim(arg: &Option<Bytes>) -> Result<Option<Bytes>> {
    Ok(arg.as_ref().map(|bytes| {
        let pos = bytes.iter().position(|&x| x != SPACE);
        if let Some(i) = pos {
            bytes[i..].to_vec()
        } else {
            b"".to_vec()
        }
    }))
}

#[rpn_fn]
#[inline]
pub fn rtrim(arg: &Option<Bytes>) -> Result<Option<Bytes>> {
    Ok(arg.as_ref().map(|bytes| {
        let pos = bytes.iter().rposition(|&x| x != SPACE);
        if let Some(i) = pos {
            bytes[..=i].to_vec()
        } else {
            Vec::new()
        }
    }))
}

#[rpn_fn]
#[inline]
pub fn replace(
    s: &Option<Bytes>,
    from_str: &Option<Bytes>,
    to_str: &Option<Bytes>,
) -> Result<Option<Bytes>> {
    Ok(match (s, from_str, to_str) {
        (Some(s), Some(from_str), Some(to_str)) => {
            if from_str.is_empty() {
                return Ok(Some(s.clone()));
            }
            let mut dest = Vec::with_capacity(s.len());
            let mut last = 0;
            while let Some(mut start) = twoway::find_bytes(&s[last..], from_str) {
                start += last;
                dest.extend_from_slice(&s[last..start]);
                dest.extend_from_slice(to_str);
                last = start + from_str.len();
            }
            dest.extend_from_slice(&s[last..]);
            Some(dest)
        }
        _ => None,
    })
}

#[rpn_fn]
#[inline]
pub fn left(lhs: &Option<Bytes>, rhs: &Option<Int>) -> Result<Option<Bytes>> {
    match (lhs, rhs) {
        (Some(lhs), Some(rhs)) => {
            if *rhs <= 0 {
                return Ok(Some(Vec::new()));
            }
            match str::from_utf8(&*lhs) {
                Ok(s) => {
                    let l = *rhs as usize;
                    if s.chars().count() > l {
                        Ok(Some(s.chars().take(l).collect::<String>().into_bytes()))
                    } else {
                        Ok(Some(s.to_string().into_bytes()))
                    }
                }
                Err(err) => Err(box_err!("invalid input value: {:?}", err)),
            }
        }
        _ => Ok(None),
    }
}

#[rpn_fn]
#[inline]
pub fn hex_str_arg(arg: &Option<Bytes>) -> Result<Option<Bytes>> {
    Ok(arg.as_ref().map(|b| hex::encode_upper(b).into_bytes()))
}

#[rpn_fn]
#[inline]
pub fn locate_binary_2_args(substr: &Option<Bytes>, s: &Option<Bytes>) -> Result<Option<i64>> {
    let (substr, s) = match (substr, s) {
        (Some(v1), Some(v2)) => (v1, v2),
        _ => return Ok(None),
    };

    Ok(twoway::find_bytes(s.as_slice(), substr.as_slice())
        .map(|i| 1 + i as i64)
        .or(Some(0)))
}

#[rpn_fn]
#[inline]
pub fn locate_binary_3_args(
    substr: &Option<Bytes>,
    s: &Option<Bytes>,
    pos: &Option<Int>,
) -> Result<Option<Int>> {
    if let (Some(substr), Some(s), Some(pos)) = (substr, s, pos) {
        if *pos < 1 || *pos as usize > s.len() + 1 {
            return Ok(Some(0));
        }
        Ok(twoway::find_bytes(&s[*pos as usize - 1..], substr)
            .map(|i| pos + i as i64)
            .or(Some(0)))
    } else {
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use tipb::ScalarFuncSig;

    use crate::rpn_expr::types::test_util::RpnFnScalarEvaluator;

    #[test]
    fn test_bin() {
        let cases = vec![
            (Some(10), Some(b"1010".to_vec())),
            (Some(0), Some(b"0".to_vec())),
            (Some(1), Some(b"1".to_vec())),
            (Some(365), Some(b"101101101".to_vec())),
            (Some(1024), Some(b"10000000000".to_vec())),
            (None, None),
            (
                Some(Int::max_value()),
                Some(b"111111111111111111111111111111111111111111111111111111111111111".to_vec()),
            ),
            (
                Some(Int::min_value()),
                Some(b"1000000000000000000000000000000000000000000000000000000000000000".to_vec()),
            ),
            (
                Some(-1),
                Some(b"1111111111111111111111111111111111111111111111111111111111111111".to_vec()),
            ),
            (
                Some(-365),
                Some(b"1111111111111111111111111111111111111111111111111111111010010011".to_vec()),
            ),
        ];
        for (arg0, expect_output) in cases {
            let output = RpnFnScalarEvaluator::new()
                .push_param(arg0)
                .evaluate(ScalarFuncSig::Bin)
                .unwrap();
            assert_eq!(output, expect_output);
        }
    }

    #[test]
    fn test_length() {
        let test_cases = vec![
            (None, None),
            (Some(""), Some(0i64)),
            (Some("你好"), Some(6i64)),
            (Some("TiKV"), Some(4i64)),
            (Some("あなたのことが好きです"), Some(33i64)),
            (Some("분산 데이터베이스"), Some(25i64)),
            (Some("россия в мире  кубок"), Some(38i64)),
            (Some("قاعدة البيانات"), Some(27i64)),
        ];

        for (arg, expect_output) in test_cases {
            let arg = arg.map(|s| s.as_bytes().to_vec());
            let output = RpnFnScalarEvaluator::new()
                .push_param(arg)
                .evaluate(ScalarFuncSig::Length)
                .unwrap();
            assert_eq!(output, expect_output);
        }
    }

    #[test]
    fn test_concat() {
        let cases = vec![
            (
                vec![Some(b"abc".to_vec()), Some(b"defg".to_vec())],
                Some(b"abcdefg".to_vec()),
            ),
            (
                vec![
                    Some("忠犬ハチ公".as_bytes().to_vec()),
                    Some("CAFÉ".as_bytes().to_vec()),
                    Some("数据库".as_bytes().to_vec()),
                    Some("قاعدة البيانات".as_bytes().to_vec()),
                    Some("НОЧЬ НА ОКРАИНЕ МОСКВЫ".as_bytes().to_vec()),
                ],
                Some(
                    "忠犬ハチ公CAFÉ数据库قاعدة البياناتНОЧЬ НА ОКРАИНЕ МОСКВЫ"
                        .as_bytes()
                        .to_vec(),
                ),
            ),
            (
                vec![
                    Some(b"abc".to_vec()),
                    Some("CAFÉ".as_bytes().to_vec()),
                    Some("数据库".as_bytes().to_vec()),
                ],
                Some("abcCAFÉ数据库".as_bytes().to_vec()),
            ),
            (
                vec![Some(b"abc".to_vec()), None, Some(b"defg".to_vec())],
                None,
            ),
            (vec![None], None),
        ];
        for (row, exp) in cases {
            let output = RpnFnScalarEvaluator::new()
                .push_params(row)
                .evaluate(ScalarFuncSig::Concat)
                .unwrap();
            assert_eq!(output, exp);
        }
    }

    #[test]
    fn test_bit_length() {
        let test_cases = vec![
            (None, None),
            (Some(""), Some(0i64)),
            (Some("你好"), Some(48i64)),
            (Some("TiKV"), Some(32i64)),
            (Some("あなたのことが好きです"), Some(264i64)),
            (Some("분산 데이터베이스"), Some(200i64)),
            (Some("россия в мире  кубок"), Some(304i64)),
            (Some("قاعدة البيانات"), Some(216i64)),
        ];

        for (arg, expect_output) in test_cases {
            let arg = arg.map(|s| s.as_bytes().to_vec());
            let output = RpnFnScalarEvaluator::new()
                .push_param(arg)
                .evaluate(ScalarFuncSig::BitLength)
                .unwrap();
            assert_eq!(output, expect_output);
        }
    }

    #[test]
    fn test_ascii() {
        let test_cases = vec![
            (None, None),
            (Some(b"1010".to_vec()), Some(49i64)),
            (Some(b"-1".to_vec()), Some(45i64)),
            (Some(b"".to_vec()), Some(0i64)),
            (Some(b"999".to_vec()), Some(57i64)),
            (Some(b"hello".to_vec()), Some(104i64)),
            (Some("Grüße".as_bytes().to_vec()), Some(71i64)),
            (Some("München".as_bytes().to_vec()), Some(77i64)),
            (Some("数据库".as_bytes().to_vec()), Some(230i64)),
            (Some("忠犬ハチ公".as_bytes().to_vec()), Some(229i64)),
            (Some("Αθήνα".as_bytes().to_vec()), Some(206i64)),
        ];

        for (arg, expect_output) in test_cases {
            let output = RpnFnScalarEvaluator::new()
                .push_param(arg)
                .evaluate(ScalarFuncSig::Ascii)
                .unwrap();
            assert_eq!(output, expect_output);
        }
    }

    #[test]
    fn test_reverse() {
        let cases = vec![
            (Some(b"hello".to_vec()), Some(b"olleh".to_vec())),
            (Some(b"".to_vec()), Some(b"".to_vec())),
            (
                Some("数据库".as_bytes().to_vec()),
                Some("库据数".as_bytes().to_vec()),
            ),
            (
                Some("忠犬ハチ公".as_bytes().to_vec()),
                Some("公チハ犬忠".as_bytes().to_vec()),
            ),
            (
                Some("あなたのことが好きです".as_bytes().to_vec()),
                Some("すでき好がとこのたなあ".as_bytes().to_vec()),
            ),
            (
                Some("Bayern München".as_bytes().to_vec()),
                Some("nehcnüM nreyaB".as_bytes().to_vec()),
            ),
            (
                Some("Η Αθηνά  ".as_bytes().to_vec()),
                Some("  άνηθΑ Η".as_bytes().to_vec()),
            ),
            (None, None),
        ];

        for (arg, expect_output) in cases {
            let output = RpnFnScalarEvaluator::new()
                .push_param(arg)
                .evaluate(ScalarFuncSig::Reverse)
                .unwrap();
            assert_eq!(output, expect_output);
        }
    }

    #[test]
    fn test_hex_int_arg() {
        let test_cases = vec![
            (Some(12), Some(b"C".to_vec())),
            (Some(0x12), Some(b"12".to_vec())),
            (Some(0b1100), Some(b"C".to_vec())),
            (Some(0), Some(b"0".to_vec())),
            (Some(-1), Some(b"FFFFFFFFFFFFFFFF".to_vec())),
            (None, None),
        ];

        for (arg, expect_output) in test_cases {
            let output = RpnFnScalarEvaluator::new()
                .push_param(arg)
                .evaluate(ScalarFuncSig::HexIntArg)
                .unwrap();
            assert_eq!(output, expect_output);
        }
    }

    #[test]
    fn test_ltrim() {
        let test_cases = vec![
            (None, None),
            (Some("   bar   "), Some("bar   ")),
            (Some("   b   ar   "), Some("b   ar   ")),
            (Some("bar"), Some("bar")),
            (Some("    "), Some("")),
            (Some("\t  bar"), Some("\t  bar")),
            (Some("\r  bar"), Some("\r  bar")),
            (Some("\n  bar"), Some("\n  bar")),
            (Some("  \tbar"), Some("\tbar")),
            (Some(""), Some("")),
            (Some("  你好"), Some("你好")),
            (Some("  你  好"), Some("你  好")),
            (
                Some("  분산 데이터베이스    "),
                Some("분산 데이터베이스    "),
            ),
            (
                Some("   あなたのことが好きです   "),
                Some("あなたのことが好きです   "),
            ),
        ];

        for (arg, expect_output) in test_cases {
            let output = RpnFnScalarEvaluator::new()
                .push_param(arg.map(|s| s.as_bytes().to_vec()))
                .evaluate(ScalarFuncSig::LTrim)
                .unwrap();
            assert_eq!(output, expect_output.map(|s| s.as_bytes().to_vec()));
        }
    }

    #[test]
    fn test_rtrim() {
        let test_cases = vec![
            (None, None),
            (Some("   bar   "), Some("   bar")),
            (Some("bar"), Some("bar")),
            (Some("ba  r"), Some("ba  r")),
            (Some("    "), Some("")),
            (Some("  bar\t  "), Some("  bar\t")),
            (Some(" bar   \t"), Some(" bar   \t")),
            (Some("bar   \r"), Some("bar   \r")),
            (Some("bar   \n"), Some("bar   \n")),
            (Some(""), Some("")),
            (Some("  你好  "), Some("  你好")),
            (Some("  你  好  "), Some("  你  好")),
            (Some("  분산 데이터베이스    "), Some("  분산 데이터베이스")),
            (
                Some("   あなたのことが好きです   "),
                Some("   あなたのことが好きです"),
            ),
        ];

        for (arg, expect_output) in test_cases {
            let output = RpnFnScalarEvaluator::new()
                .push_param(arg.map(|s| s.as_bytes().to_vec()))
                .evaluate(ScalarFuncSig::RTrim)
                .unwrap();
            assert_eq!(output, expect_output.map(|s| s.as_bytes().to_vec()));
        }
    }

    #[test]
    fn test_replace() {
        let cases = vec![
            ((None, None, None), None),
            ((None, Some(b"a".to_vec()), Some(b"b".to_vec())), None),
            ((Some(b"a".to_vec()), None, Some(b"b".to_vec())), None),
            ((Some(b"a".to_vec()), Some(b"b".to_vec()), None), None),
            (
                (
                    Some(b"www.mysql.com".to_vec()),
                    Some(b"mysql".to_vec()),
                    Some(b"pingcap".to_vec()),
                ),
                Some(b"www.pingcap.com".to_vec()),
            ),
            (
                (
                    Some(b"www.mysql.com".to_vec()),
                    Some(b"w".to_vec()),
                    Some(b"1".to_vec()),
                ),
                Some(b"111.mysql.com".to_vec()),
            ),
            (
                (
                    Some(b"1234".to_vec()),
                    Some(b"2".to_vec()),
                    Some(b"55".to_vec()),
                ),
                Some(b"15534".to_vec()),
            ),
            (
                (Some(b"".to_vec()), Some(b"a".to_vec()), Some(b"b".to_vec())),
                Some(b"".to_vec()),
            ),
            (
                (
                    Some(b"abc".to_vec()),
                    Some(b"".to_vec()),
                    Some(b"d".to_vec()),
                ),
                Some(b"abc".to_vec()),
            ),
            (
                (
                    Some(b"aaa".to_vec()),
                    Some(b"a".to_vec()),
                    Some(b"".to_vec()),
                ),
                Some(b"".to_vec()),
            ),
            (
                (
                    Some(b"aaa".to_vec()),
                    Some(b"A".to_vec()),
                    Some(b"".to_vec()),
                ),
                Some(b"aaa".to_vec()),
            ),
            (
                (
                    Some("新年快乐".as_bytes().to_vec()),
                    Some("年".as_bytes().to_vec()),
                    Some("春".as_bytes().to_vec()),
                ),
                Some("新春快乐".as_bytes().to_vec()),
            ),
            (
                (
                    Some("心心相印".as_bytes().to_vec()),
                    Some("心".as_bytes().to_vec()),
                    Some("❤️".as_bytes().to_vec()),
                ),
                Some("❤️❤️相印".as_bytes().to_vec()),
            ),
            // some invalid bytes
            (
                (
                    Some(b"Hello \xF0\x90\x80World".to_vec()),
                    Some(b"World".to_vec()),
                    Some(b"123".to_vec()),
                ),
                Some(b"Hello \xF0\x90\x80123".to_vec()),
            ),
        ];

        for ((s, from_str, to_str), expect_output) in cases {
            let output = RpnFnScalarEvaluator::new()
                .push_param(s)
                .push_param(from_str)
                .push_param(to_str)
                .evaluate(ScalarFuncSig::Replace)
                .unwrap();
            assert_eq!(output, expect_output);
        }
    }

    #[test]
    fn test_left() {
        let cases = vec![
            (Some(b"hello".to_vec()), Some(0i64), Some(b"".to_vec())),
            (Some(b"hello".to_vec()), Some(1i64), Some(b"h".to_vec())),
            (
                Some("数据库".as_bytes().to_vec()),
                Some(2i64),
                Some("数据".as_bytes().to_vec()),
            ),
            (
                Some("忠犬ハチ公".as_bytes().to_vec()),
                Some(3i64),
                Some("忠犬ハ".as_bytes().to_vec()),
            ),
            (
                Some("数据库".as_bytes().to_vec()),
                Some(100i64),
                Some("数据库".as_bytes().to_vec()),
            ),
            (
                Some("数据库".as_bytes().to_vec()),
                Some(-1i64),
                Some(b"".to_vec()),
            ),
            (
                Some("数据库".as_bytes().to_vec()),
                Some(i64::max_value()),
                Some("数据库".as_bytes().to_vec()),
            ),
            (None, Some(-1), None),
            (Some(b"hello".to_vec()), None, None),
            (None, None, None),
        ];

        for (lhs, rhs, expect_output) in cases {
            let output = RpnFnScalarEvaluator::new()
                .push_param(lhs)
                .push_param(rhs)
                .evaluate(ScalarFuncSig::Left)
                .unwrap();
            assert_eq!(output, expect_output);
        }
    }

    #[test]
    fn test_hex_str_arg() {
        let test_cases = vec![
            (Some(b"abc".to_vec()), Some(b"616263".to_vec())),
            (
                Some("你好".as_bytes().to_vec()),
                Some(b"E4BDA0E5A5BD".to_vec()),
            ),
            (None, None),
        ];

        for (arg, expect_output) in test_cases {
            let output = RpnFnScalarEvaluator::new()
                .push_param(arg)
                .evaluate(ScalarFuncSig::HexStrArg)
                .unwrap();
            assert_eq!(output, expect_output);
        }
    }

    #[test]
    fn test_locate_binary_2_args() {
        let test_cases = vec![
            (None, None, None),
            (None, Some("abc"), None),
            (Some("abc"), None, None),
            (Some(""), Some("foobArbar"), Some(1)),
            (Some(""), Some(""), Some(1)),
            (Some("xxx"), Some(""), Some(0)),
            (Some("BaR"), Some("foobArbar"), Some(0)),
            (Some("bar"), Some("foobArbar"), Some(7)),
            (
                Some("好世"),
                Some("你好世界"),
                Some(1 + "你好世界".find("好世").unwrap() as i64),
            ),
        ];

        for (substr, s, expect_output) in test_cases {
            let substr = substr.map(|v| v.as_bytes().to_vec());
            let s = s.map(|v| v.as_bytes().to_vec());
            let output = RpnFnScalarEvaluator::new()
                .push_param(substr)
                .push_param(s)
                .evaluate(ScalarFuncSig::LocateBinary2Args)
                .unwrap();
            assert_eq!(output, expect_output);
        }
    }

    #[test]
    fn test_locate_binary_3_args() {
        let cases = vec![
            ("", "foobArbar", 0, 0),
            ("", "foobArbar", 1, 1),
            ("", "foobArbar", 2, 2),
            ("", "foobArbar", 9, 9),
            ("", "foobArbar", 10, 10),
            ("", "foobArbar", 11, 0),
            ("", "", 1, 1),
            ("BaR", "foobArbar", 3, 0),
            ("bar", "foobArbar", 1, 7),
            (
                "好世",
                "你好世界",
                1,
                1 + "你好世界".find("好世").unwrap() as i64,
            ),
        ];

        for (substr, s, pos, exp) in cases {
            let substr = Some(substr.as_bytes().to_vec());
            let s = Some(s.as_bytes().to_vec());
            let pos = Some(pos);
            let output = RpnFnScalarEvaluator::new()
                .push_param(substr)
                .push_param(s)
                .push_param(pos)
                .evaluate(ScalarFuncSig::LocateBinary3Args)
                .unwrap();
            assert_eq!(output, Some(exp))
        }

        let null_cases = vec![
            (None, Some(b"".to_vec()), Some(1), None),
            (Some(b"".to_vec()), None, None, None),
            (None, None, None, None),
        ];

        for (substr, s, pos, exp) in null_cases {
            let output: Option<i64> = RpnFnScalarEvaluator::new()
                .push_param(substr)
                .push_param(s)
                .push_param(pos)
                .evaluate(ScalarFuncSig::LocateBinary3Args)
                .unwrap();
            assert_eq!(output, exp)
        }
    }
}
