// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::str;
use tidb_query_codegen::rpn_fn;

use crate::codec::data_type::*;
use crate::Result;
use tidb_query_datatype::EvalType;

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

#[rpn_fn(varg, min_args = 2)]
#[inline]
pub fn concat_ws(args: &[&Option<Bytes>]) -> Result<Option<Bytes>> {
    if let Some(sep) = args[0] {
        let rest = &args[1..];
        Ok(Some(
            rest.iter()
                .filter_map(|x| x.as_ref().map(|inner| inner.as_slice()))
                .collect::<Vec<&[u8]>>()
                .join::<&[u8]>(sep.as_ref()),
        ))
    } else {
        Ok(None)
    }
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
pub fn right(lhs: &Option<Bytes>, rhs: &Option<Int>) -> Result<Option<Bytes>> {
    match (lhs, rhs) {
        (Some(lhs), Some(rhs)) => {
            if *rhs <= 0 {
                return Ok(Some(Vec::new()));
            }
            match str::from_utf8(&*lhs) {
                Ok(s) => {
                    let rhs = *rhs as usize;
                    let len = s.chars().count();
                    if len > rhs {
                        let idx = s
                            .char_indices()
                            .nth(len - rhs)
                            .map(|(idx, _)| idx)
                            .unwrap_or_else(|| s.len());
                        Ok(Some(s[idx..].to_string().into_bytes()))
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
pub fn reverse_binary(arg: &Option<Bytes>) -> Result<Option<Bytes>> {
    Ok(arg.as_ref().map(|bytes| {
        let mut s = bytes.to_vec();
        s.reverse();
        s
    }))
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

#[rpn_fn(varg, min_args = 1)]
#[inline]
fn field<T: Evaluable + PartialEq>(args: &[&Option<T>]) -> Result<Option<Int>> {
    Ok(Some(match args[0] {
        // As per the MySQL doc, if the first argument is NULL, this function always returns 0.
        None => 0,
        Some(val) => args
            .iter()
            .skip(1)
            .position(|&i| i.as_ref() == Some(val))
            .map_or(0, |pos| (pos + 1) as i64),
    }))
}

#[rpn_fn(raw_varg, min_args = 2, extra_validator = elt_validator)]
#[inline]
pub fn elt(raw_args: &[ScalarValueRef]) -> Result<Option<Bytes>> {
    assert!(raw_args.len() >= 2);
    let index = raw_args[0].as_int();
    Ok(match *index {
        None => None,
        Some(i) => {
            if i <= 0 || i + 1 > raw_args.len() as i64 {
                return Ok(None);
            }
            raw_args[i as usize].as_bytes().to_owned()
        }
    })
}

/// validate the arguments are `(&Option<Int>, &[&Option<Bytes>)])`
fn elt_validator(expr: &tipb::Expr) -> Result<()> {
    let children = expr.get_children();
    assert!(children.len() >= 2);
    super::function::validate_expr_return_type(&children[0], EvalType::Int)?;
    for i in 1..children.len() {
        super::function::validate_expr_return_type(&children[i], EvalType::Bytes)?;
    }
    Ok(())
}

#[rpn_fn]
#[inline]
pub fn space(len: &Option<Int>) -> Result<Option<Bytes>> {
    Ok(match *len {
        Some(len) => {
            if len > i64::from(tidb_query_datatype::MAX_BLOB_WIDTH) {
                None
            } else if len <= 0 {
                Some(Vec::new())
            } else {
                Some(vec![SPACE; len as usize])
            }
        }
        None => None,
    })
}

#[rpn_fn]
#[inline]
pub fn strcmp(left: &Option<Bytes>, right: &Option<Bytes>) -> Result<Option<i64>> {
    use std::cmp::Ordering::*;
    Ok(match (left, right) {
        (Some(left), Some(right)) => Some(match left.cmp(right) {
            Less => -1,
            Equal => 0,
            Greater => 1,
        }),
        _ => None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::{f64, i64};
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
            let output = RpnFnScalarEvaluator::new()
                .push_param(arg.map(|s| s.as_bytes().to_vec()))
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
    fn test_concat_ws() {
        let cases = vec![
            (
                vec![
                    Some(b",".to_vec()),
                    Some(b"abc".to_vec()),
                    Some(b"defg".to_vec()),
                ],
                Some(b"abc,defg".to_vec()),
            ),
            (
                vec![
                    Some(b",".to_vec()),
                    Some("忠犬ハチ公".as_bytes().to_vec()),
                    Some("CAFÉ".as_bytes().to_vec()),
                    Some("数据库".as_bytes().to_vec()),
                    Some("قاعدة البيانات".as_bytes().to_vec()),
                    Some("НОЧЬ НА ОКРАИНЕ МОСКВЫ".as_bytes().to_vec()),
                ],
                Some(
                    "忠犬ハチ公,CAFÉ,数据库,قاعدة البيانات,НОЧЬ НА ОКРАИНЕ МОСКВЫ"
                        .as_bytes()
                        .to_vec(),
                ),
            ),
            (
                vec![
                    Some(b",".to_vec()),
                    Some(b"abc".to_vec()),
                    Some("CAFÉ".as_bytes().to_vec()),
                    Some("数据库".as_bytes().to_vec()),
                ],
                Some("abc,CAFÉ,数据库".as_bytes().to_vec()),
            ),
            (
                vec![
                    Some(b",".to_vec()),
                    Some(b"abc".to_vec()),
                    None,
                    Some(b"defg".to_vec()),
                ],
                Some(b"abc,defg".to_vec()),
            ),
            (
                vec![Some(b",".to_vec()), Some(b"abc".to_vec())],
                Some(b"abc".to_vec()),
            ),
            (
                vec![Some(b",".to_vec()), None, Some(b"abc".to_vec())],
                Some(b"abc".to_vec()),
            ),
            (
                vec![
                    Some(b",".to_vec()),
                    Some(b"".to_vec()),
                    Some(b"abc".to_vec()),
                ],
                Some(b",abc".to_vec()),
            ),
            (
                vec![
                    Some("忠犬ハチ公".as_bytes().to_vec()),
                    Some("CAFÉ".as_bytes().to_vec()),
                    Some("数据库".as_bytes().to_vec()),
                    Some("قاعدة البيانات".as_bytes().to_vec()),
                ],
                Some(
                    "CAFÉ忠犬ハチ公数据库忠犬ハチ公قاعدة البيانات"
                        .as_bytes()
                        .to_vec(),
                ),
            ),
            (vec![None, Some(b"abc".to_vec())], None),
            (
                vec![Some(b",".to_vec()), None, Some(b"abc".to_vec())],
                Some(b"abc".to_vec()),
            ),
            (
                vec![Some(b",".to_vec()), Some(b"abc".to_vec()), None],
                Some(b"abc".to_vec()),
            ),
            (
                vec![
                    Some(b",".to_vec()),
                    Some(b"".to_vec()),
                    Some(b"abc".to_vec()),
                ],
                Some(b",abc".to_vec()),
            ),
            (
                vec![
                    Some("忠犬ハチ公".as_bytes().to_vec()),
                    Some("CAFÉ".as_bytes().to_vec()),
                    Some("数据库".as_bytes().to_vec()),
                    Some("قاعدة البيانات".as_bytes().to_vec()),
                ],
                Some(
                    "CAFÉ忠犬ハチ公数据库忠犬ハチ公قاعدة البيانات"
                        .as_bytes()
                        .to_vec(),
                ),
            ),
            (
                vec![
                    Some(b",".to_vec()),
                    None,
                    Some(b"abc".to_vec()),
                    None,
                    None,
                    Some(b"defg".to_vec()),
                    None,
                ],
                Some(b"abc,defg".to_vec()),
            ),
        ];
        for (row, exp) in cases {
            let output = RpnFnScalarEvaluator::new()
                .push_params(row)
                .evaluate(ScalarFuncSig::ConcatWs)
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
            let output = RpnFnScalarEvaluator::new()
                .push_param(arg.map(|s| s.as_bytes().to_vec()))
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
            (None, None, None, None),
            (None, Some(b"a".to_vec()), Some(b"b".to_vec()), None),
            (Some(b"a".to_vec()), None, Some(b"b".to_vec()), None),
            (Some(b"a".to_vec()), Some(b"b".to_vec()), None, None),
            (
                Some(b"www.mysql.com".to_vec()),
                Some(b"mysql".to_vec()),
                Some(b"pingcap".to_vec()),
                Some(b"www.pingcap.com".to_vec()),
            ),
            (
                Some(b"www.mysql.com".to_vec()),
                Some(b"w".to_vec()),
                Some(b"1".to_vec()),
                Some(b"111.mysql.com".to_vec()),
            ),
            (
                Some(b"1234".to_vec()),
                Some(b"2".to_vec()),
                Some(b"55".to_vec()),
                Some(b"15534".to_vec()),
            ),
            (
                Some(b"".to_vec()),
                Some(b"a".to_vec()),
                Some(b"b".to_vec()),
                Some(b"".to_vec()),
            ),
            (
                Some(b"abc".to_vec()),
                Some(b"".to_vec()),
                Some(b"d".to_vec()),
                Some(b"abc".to_vec()),
            ),
            (
                Some(b"aaa".to_vec()),
                Some(b"a".to_vec()),
                Some(b"".to_vec()),
                Some(b"".to_vec()),
            ),
            (
                Some(b"aaa".to_vec()),
                Some(b"A".to_vec()),
                Some(b"".to_vec()),
                Some(b"aaa".to_vec()),
            ),
            (
                Some("新年快乐".as_bytes().to_vec()),
                Some("年".as_bytes().to_vec()),
                Some("春".as_bytes().to_vec()),
                Some("新春快乐".as_bytes().to_vec()),
            ),
            (
                Some("心心相印".as_bytes().to_vec()),
                Some("心".as_bytes().to_vec()),
                Some("❤️".as_bytes().to_vec()),
                Some("❤️❤️相印".as_bytes().to_vec()),
            ),
            (
                Some(b"Hello \xF0\x90\x80World".to_vec()),
                Some(b"World".to_vec()),
                Some(b"123".to_vec()),
                Some(b"Hello \xF0\x90\x80123".to_vec()),
            ),
        ];

        for (s, from_str, to_str, expect_output) in cases {
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
    fn test_right() {
        let cases = vec![
            (Some(b"hello".to_vec()), Some(0), Some(b"".to_vec())),
            (Some(b"hello".to_vec()), Some(1), Some(b"o".to_vec())),
            (
                Some("数据库".as_bytes().to_vec()),
                Some(2),
                Some("据库".as_bytes().to_vec()),
            ),
            (
                Some("忠犬ハチ公".as_bytes().to_vec()),
                Some(3),
                Some("ハチ公".as_bytes().to_vec()),
            ),
            (
                Some("数据库".as_bytes().to_vec()),
                Some(100),
                Some("数据库".as_bytes().to_vec()),
            ),
            (
                Some("数据库".as_bytes().to_vec()),
                Some(-1),
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
                .evaluate(ScalarFuncSig::Right)
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
            let output = RpnFnScalarEvaluator::new()
                .push_param(substr.map(|v| v.as_bytes().to_vec()))
                .push_param(s.map(|v| v.as_bytes().to_vec()))
                .evaluate(ScalarFuncSig::LocateBinary2Args)
                .unwrap();
            assert_eq!(output, expect_output);
        }
    }

    #[test]
    fn test_reverse_binary() {
        let cases = vec![
            (Some(b"hello".to_vec()), Some(b"olleh".to_vec())),
            (Some(b"".to_vec()), Some(b"".to_vec())),
            (
                Some("中国".as_bytes().to_vec()),
                Some(vec![0o275u8, 0o233u8, 0o345u8, 0o255u8, 0o270u8, 0o344u8]),
            ),
            (None, None),
        ];

        for (arg, expect_output) in cases {
            let output = RpnFnScalarEvaluator::new()
                .push_param(arg)
                .evaluate(ScalarFuncSig::ReverseBinary)
                .unwrap();
            assert_eq!(output, expect_output);
        }
    }

    #[test]
    fn test_locate_binary_3_args() {
        let cases = vec![
            (None, None, None, None),
            (None, Some(""), Some(1), None),
            (Some(""), None, None, None),
            (Some(""), Some("foobArbar"), Some(1), Some(1)),
            (Some(""), Some("foobArbar"), Some(0), Some(0)),
            (Some(""), Some("foobArbar"), Some(2), Some(2)),
            (Some(""), Some("foobArbar"), Some(9), Some(9)),
            (Some(""), Some("foobArbar"), Some(10), Some(10)),
            (Some(""), Some("foobArbar"), Some(11), Some(0)),
            (Some(""), Some(""), Some(1), Some(1)),
            (Some("BaR"), Some("foobArbar"), Some(3), Some(0)),
            (Some("bar"), Some("foobArbar"), Some(1), Some(7)),
            (
                Some("好世"),
                Some("你好世界"),
                Some(1),
                Some(1 + "你好世界".find("好世").unwrap() as i64),
            ),
        ];

        for (substr, s, pos, exp) in cases {
            let output = RpnFnScalarEvaluator::new()
                .push_param(substr.map(|v| v.as_bytes().to_vec()))
                .push_param(s.map(|v| v.as_bytes().to_vec()))
                .push_param(pos)
                .evaluate(ScalarFuncSig::LocateBinary3Args)
                .unwrap();
            assert_eq!(output, exp)
        }
    }

    #[test]
    fn test_field_int() {
        let test_cases = vec![
            (vec![Some(1), Some(-2), Some(3)], Some(0)),
            (vec![Some(-1), Some(2), Some(-1), Some(2)], Some(2)),
            (
                vec![Some(i64::MAX), Some(0), Some(i64::MIN), Some(i64::MAX)],
                Some(3),
            ),
            (vec![None, Some(0), Some(0)], Some(0)),
            (vec![None, None, Some(0)], Some(0)),
            (vec![Some(100)], Some(0)),
        ];

        for (args, expect_output) in test_cases {
            let output = RpnFnScalarEvaluator::new()
                .push_params(args)
                .evaluate(ScalarFuncSig::FieldInt)
                .unwrap();
            assert_eq!(output, expect_output);
        }
    }

    #[test]
    fn test_field_real() {
        let test_cases = vec![
            (vec![Some(1.0), Some(-2.0), Some(9.0)], Some(0)),
            (vec![Some(-1.0), Some(2.0), Some(-1.0), Some(2.0)], Some(2)),
            (
                vec![Some(f64::MAX), Some(0.0), Some(f64::MIN), Some(f64::MAX)],
                Some(3),
            ),
            (vec![None, Some(1.0), Some(1.0)], Some(0)),
            (vec![None, None, Some(0.0)], Some(0)),
            (vec![Some(10.0)], Some(0)),
        ];

        for (args, expect_output) in test_cases {
            let output = RpnFnScalarEvaluator::new()
                .push_params(args)
                .evaluate(ScalarFuncSig::FieldReal)
                .unwrap();
            assert_eq!(output, expect_output);
        }
    }

    #[test]
    fn test_field_string() {
        let test_cases = vec![
            (
                vec![
                    Some(b"foo".to_vec()),
                    Some(b"foo".to_vec()),
                    Some(b"bar".to_vec()),
                    Some(b"baz".to_vec()),
                ],
                Some(1),
            ),
            (
                vec![
                    Some(b"foo".to_vec()),
                    Some(b"bar".to_vec()),
                    Some(b"baz".to_vec()),
                    Some(b"hello".to_vec()),
                ],
                Some(0),
            ),
            (
                vec![
                    Some(b"hello".to_vec()),
                    Some(b"world".to_vec()),
                    Some(b"world".to_vec()),
                    Some(b"hello".to_vec()),
                ],
                Some(3),
            ),
            (
                vec![
                    Some(b"Hello".to_vec()),
                    Some(b"Hola".to_vec()),
                    Some("Cześć".as_bytes().to_vec()),
                    Some("你好".as_bytes().to_vec()),
                    Some("Здравствуйте".as_bytes().to_vec()),
                    Some(b"Hello World!".to_vec()),
                    Some(b"Hello".to_vec()),
                ],
                Some(6),
            ),
            (
                vec![
                    None,
                    Some(b"DataBase".to_vec()),
                    Some(b"Hello World!".to_vec()),
                ],
                Some(0),
            ),
            (vec![None, None, Some(b"Hello World!".to_vec())], Some(0)),
            (vec![Some(b"Hello World!".to_vec())], Some(0)),
        ];

        for (args, expect_output) in test_cases {
            let output = RpnFnScalarEvaluator::new()
                .push_params(args)
                .evaluate(ScalarFuncSig::FieldString)
                .unwrap();
            assert_eq!(output, expect_output);
        }
    }

    #[test]
    fn test_space() {
        let test_cases = vec![
            (None, None),
            (Some(0), Some(b"".to_vec())),
            (Some(0), Some(b"".to_vec())),
            (Some(3), Some(b"   ".to_vec())),
            (Some(-1), Some(b"".to_vec())),
            (Some(i64::max_value()), None),
            (
                Some(i64::from(tidb_query_datatype::MAX_BLOB_WIDTH) + 1),
                None,
            ),
            (
                Some(i64::from(tidb_query_datatype::MAX_BLOB_WIDTH)),
                Some(vec![
                    super::SPACE;
                    tidb_query_datatype::MAX_BLOB_WIDTH as usize
                ]),
            ),
        ];

        for (len, exp) in test_cases {
            let output = RpnFnScalarEvaluator::new()
                .push_param(len)
                .evaluate(ScalarFuncSig::Space)
                .unwrap();
            assert_eq!(output, exp);
        }
    }

    #[test]
    fn test_elt() {
        let test_cases: Vec<(Vec<ScalarValue>, _)> = vec![
            (
                vec![
                    Some(1).into(),
                    Some(b"DataBase".to_vec()).into(),
                    Some(b"Hello World!".to_vec()).into(),
                ],
                Some(b"DataBase".to_vec()),
            ),
            (
                vec![
                    Some(2).into(),
                    Some(b"DataBase".to_vec()).into(),
                    Some(b"Hello World!".to_vec()).into(),
                ],
                Some(b"Hello World!".to_vec()),
            ),
            (
                vec![
                    None::<Int>.into(),
                    Some(b"DataBase".to_vec()).into(),
                    Some(b"Hello World!".to_vec()).into(),
                ],
                None,
            ),
            (vec![None::<Int>.into(), None::<Bytes>.into()], None),
            (
                vec![
                    Some(1).into(),
                    None::<Bytes>.into(),
                    Some(b"Hello World!".to_vec()).into(),
                ],
                None,
            ),
            (
                vec![
                    Some(3).into(),
                    None::<Bytes>.into(),
                    Some(b"Hello World!".to_vec()).into(),
                ],
                None,
            ),
            (
                vec![
                    Some(0).into(),
                    None::<Bytes>.into(),
                    Some(b"Hello World!".to_vec()).into(),
                ],
                None,
            ),
            (
                vec![
                    Some(-1).into(),
                    None::<Bytes>.into(),
                    Some(b"Hello World!".to_vec()).into(),
                ],
                None,
            ),
            (
                vec![
                    Some(4).into(),
                    None::<Bytes>.into(),
                    Some(b"Hello".to_vec()).into(),
                    Some(b"Hola".to_vec()).into(),
                    Some("Cześć".as_bytes().to_vec()).into(),
                    Some("你好".as_bytes().to_vec()).into(),
                    Some("Здравствуйте".as_bytes().to_vec()).into(),
                    Some(b"Hello World!".to_vec()).into(),
                ],
                Some("Cześć".as_bytes().to_vec()),
            ),
        ];
        for (args, expect_output) in test_cases {
            let output = RpnFnScalarEvaluator::new()
                .push_params(args)
                .evaluate(ScalarFuncSig::Elt)
                .unwrap();
            assert_eq!(output, expect_output);
        }
    }

    #[test]
    fn test_strcmp() {
        let test_cases = vec![
            (Some(b"123".to_vec()), Some(b"123".to_vec()), Some(0)),
            (Some(b"123".to_vec()), Some(b"1".to_vec()), Some(1)),
            (Some(b"1".to_vec()), Some(b"123".to_vec()), Some(-1)),
            (Some(b"123".to_vec()), Some(b"45".to_vec()), Some(-1)),
            (
                Some("你好".as_bytes().to_vec()),
                Some(b"hello".to_vec()),
                Some(1),
            ),
            (Some(b"".to_vec()), Some(b"123".to_vec()), Some(-1)),
            (Some(b"123".to_vec()), Some(b"".to_vec()), Some(1)),
            (Some(b"".to_vec()), Some(b"".to_vec()), Some(0)),
            (None, Some(b"123".to_vec()), None),
            (Some(b"123".to_vec()), None, None),
            (Some(b"".to_vec()), None, None),
            (None, Some(b"".to_vec()), None),
        ];

        for (left, right, expect_output) in test_cases {
            let output = RpnFnScalarEvaluator::new()
                .push_param(left)
                .push_param(right)
                .evaluate(ScalarFuncSig::Strcmp)
                .unwrap();
            assert_eq!(output, expect_output);
        }
    }
}
