// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::str;
use tidb_query_codegen::rpn_fn;

use tidb_query_common::Result;
use tidb_query_datatype::codec::data_type::*;
use tidb_query_datatype::*;
use tidb_query_shared_expr::string::{
    encoded_size, line_wrap, strip_whitespace, trim, validate_target_len_for_pad, TrimDirection,
    BASE64_ENCODED_CHUNK_LENGTH, BASE64_INPUT_CHUNK_LENGTH,
};

const SPACE: u8 = 0o40u8;

#[rpn_fn(writer)]
#[inline]
pub fn bin(num: &Int, writer: BytesWriter) -> Result<BytesGuard> {
    Ok(writer.write(Some(Bytes::from(format!("{:b}", num)))))
}

#[rpn_fn(writer)]
#[inline]
pub fn oct_int(num: &Int, writer: BytesWriter) -> Result<BytesGuard> {
    Ok(writer.write(Some(Bytes::from(format!("{:o}", num)))))
}

#[rpn_fn]
#[inline]
pub fn length(arg: BytesRef) -> Result<Option<i64>> {
    Ok(Some(arg.len() as i64))
}

#[rpn_fn(writer)]
#[inline]
pub fn unhex(arg: BytesRef, writer: BytesWriter) -> Result<BytesGuard> {
    // hex::decode will fail on odd-length content
    // but mysql won't
    // so do some padding
    let mut padded_content = Vec::with_capacity(arg.len() + arg.len() % 2);
    if arg.len() % 2 == 1 {
        padded_content.push(b'0')
    }
    padded_content.extend_from_slice(arg);
    Ok(writer.write(hex::decode(padded_content).ok()))
}

#[rpn_fn]
#[inline]
pub fn bit_length(arg: BytesRef) -> Result<Option<i64>> {
    Ok(Some(arg.len() as i64 * 8))
}

#[rpn_fn(nullable)]
#[inline]
pub fn ord(arg: Option<BytesRef>) -> Result<Option<i64>> {
    let mut result = 0;
    if let Some(content) = arg {
        let size = bstr::decode_utf8(content).1;
        let bytes = &content[..size];
        let mut factor = 1;

        for b in bytes.iter().rev() {
            result += i64::from(*b) * factor;
            factor *= 256;
        }
    }
    Ok(Some(result))
}

#[rpn_fn(nullable, varg, min_args = 1)]
#[inline]
pub fn concat(args: &[Option<BytesRef>]) -> Result<Option<Bytes>> {
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

#[rpn_fn(nullable, varg, min_args = 2)]
#[inline]
pub fn concat_ws(args: &[Option<BytesRef>]) -> Result<Option<Bytes>> {
    if let Some(sep) = args[0] {
        let rest = &args[1..];
        Ok(Some(
            rest.iter()
                .filter_map(|x| *x)
                .collect::<Vec<&[u8]>>()
                .join::<&[u8]>(sep),
        ))
    } else {
        Ok(None)
    }
}

#[rpn_fn]
#[inline]
pub fn ascii(arg: BytesRef) -> Result<Option<i64>> {
    let result = match arg.is_empty() {
        true => 0,
        false => i64::from(arg[0]),
    };

    Ok(Some(result))
}

#[rpn_fn(writer)]
#[inline]
pub fn reverse_utf8(arg: BytesRef, writer: BytesWriter) -> Result<BytesGuard> {
    match str::from_utf8(arg) {
        Ok(s) => Ok(writer.write(Some(s.chars().rev().collect::<String>().into_bytes()))),
        Err(err) => Err(box_err!("invalid input value: {:?}", err)),
    }
}

#[rpn_fn(writer)]
#[inline]
pub fn hex_int_arg(arg: &Int, writer: BytesWriter) -> Result<BytesGuard> {
    Ok(writer.write(Some(format!("{:X}", arg).into_bytes())))
}

#[rpn_fn(writer)]
#[inline]
pub fn ltrim(arg: BytesRef, writer: BytesWriter) -> Result<BytesGuard> {
    let pos = arg.iter().position(|&x| x != SPACE);
    let result = if let Some(i) = pos { &arg[i..] } else { b"" };

    Ok(writer.write_ref(Some(result)))
}

#[rpn_fn(writer)]
#[inline]
pub fn rtrim(arg: BytesRef, writer: BytesWriter) -> Result<BytesGuard> {
    let pos = arg.iter().rposition(|&x| x != SPACE);
    let result = if let Some(i) = pos { &arg[..=i] } else { b"" };

    Ok(writer.write_ref(Some(result)))
}

#[rpn_fn(writer)]
#[inline]
pub fn lpad(arg: BytesRef, len: &Int, pad: BytesRef, writer: BytesWriter) -> Result<BytesGuard> {
    match validate_target_len_for_pad(*len < 0, *len, arg.len(), 1, pad.is_empty()) {
        None => Ok(writer.write(None)),
        Some(0) => Ok(writer.write_ref(Some(b""))),
        Some(target_len) => {
            let r = if let Some(remain) = target_len.checked_sub(arg.len()) {
                pad.iter()
                    .cycle()
                    .take(remain)
                    .chain(arg.iter())
                    .copied()
                    .collect::<Bytes>()
            } else {
                arg[..target_len].to_vec()
            };
            Ok(writer.write(Some(r)))
        }
    }
}

#[rpn_fn(writer)]
#[inline]
pub fn lpad_utf8(
    arg: BytesRef,
    len: &Int,
    pad: BytesRef,
    writer: BytesWriter,
) -> Result<BytesGuard> {
    let input = match str::from_utf8(&*arg) {
        Ok(arg) => arg,
        Err(err) => return Err(box_err!("invalid input value: {:?}", err)),
    };
    let pad = match str::from_utf8(&*pad) {
        Ok(pad) => pad,
        Err(err) => return Err(box_err!("invalid input value: {:?}", err)),
    };
    let input_len = input.chars().count();
    match validate_target_len_for_pad(*len < 0, *len, input_len, 4, pad.is_empty()) {
        None => Ok(writer.write(None)),
        Some(0) => Ok(writer.write_ref(Some(b""))),
        Some(target_len) => {
            let r = if let Some(remain) = target_len.checked_sub(input_len) {
                pad.chars()
                    .cycle()
                    .take(remain)
                    .chain(input.chars())
                    .collect::<String>()
            } else {
                input.chars().take(target_len).collect::<String>()
            };
            Ok(writer.write(Some(r.into_bytes())))
        }
    }
}

#[rpn_fn(writer)]
#[inline]
pub fn rpad(arg: BytesRef, len: &Int, pad: BytesRef, writer: BytesWriter) -> Result<BytesGuard> {
    match validate_target_len_for_pad(*len < 0, *len, arg.len(), 1, pad.is_empty()) {
        None => Ok(writer.write(None)),
        Some(0) => Ok(writer.write_ref(Some(b""))),
        Some(target_len) => {
            let r = arg
                .iter()
                .chain(pad.iter().cycle())
                .copied()
                .take(target_len)
                .collect::<Bytes>();
            Ok(writer.write(Some(r)))
        }
    }
}

#[rpn_fn(writer)]
#[inline]
pub fn replace(
    s: BytesRef,
    from_str: BytesRef,
    to_str: BytesRef,
    writer: BytesWriter,
) -> Result<BytesGuard> {
    if from_str.is_empty() {
        return Ok(writer.write_ref(Some(s)));
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
    Ok(writer.write(Some(dest)))
}

#[rpn_fn(writer)]
#[inline]
pub fn left(lhs: BytesRef, rhs: &Int, writer: BytesWriter) -> Result<BytesGuard> {
    if *rhs <= 0 {
        return Ok(writer.write_ref(Some(b"")));
    }
    let rhs = *rhs as usize;
    let result = if lhs.len() < rhs { &lhs } else { &lhs[..rhs] };

    Ok(writer.write_ref(Some(result)))
}

#[rpn_fn(writer)]
#[inline]
pub fn left_utf8(lhs: BytesRef, rhs: &Int, writer: BytesWriter) -> Result<BytesGuard> {
    if *rhs <= 0 {
        return Ok(writer.write_ref(Some(b"")));
    }
    match str::from_utf8(&*lhs) {
        Ok(s) => {
            let rhs = *rhs as usize;
            let len = s.chars().count();
            let result = if len > rhs {
                let idx = s
                    .char_indices()
                    .nth(rhs)
                    .map(|(idx, _)| idx)
                    .unwrap_or_else(|| s.len());
                s[..idx].as_bytes()
            } else {
                s.as_bytes()
            };

            Ok(writer.write_ref(Some(result)))
        }
        Err(err) => Err(box_err!("invalid input value: {:?}", err)),
    }
}

#[rpn_fn(writer)]
#[inline]
pub fn right(lhs: BytesRef, rhs: &Int, writer: BytesWriter) -> Result<BytesGuard> {
    if *rhs <= 0 {
        return Ok(writer.write_ref(Some(b"")));
    }
    let rhs = *rhs as usize;
    let result = if lhs.len() < rhs {
        lhs
    } else {
        &lhs[(lhs.len() - rhs)..]
    };

    Ok(writer.write_ref(Some(result)))
}

#[rpn_fn(writer)]
#[inline]
pub fn insert(
    s: BytesRef,
    pos: &Int,
    len: &Int,
    newstr: BytesRef,
    writer: BytesWriter,
) -> Result<BytesGuard> {
    let pos = *pos;
    let len = *len;
    let upos: usize = pos as usize;
    let mut ulen: usize = len as usize;
    if pos < 1 || upos > s.len() {
        return Ok(writer.write_ref(Some(s)));
    }
    if ulen > s.len() - upos + 1 || len < 0 {
        ulen = s.len() - upos + 1;
    }
    let mut ret = Vec::with_capacity(newstr.len() + s.len());
    ret.extend_from_slice(&s[0..upos - 1]);
    ret.extend_from_slice(&newstr);
    ret.extend_from_slice(&s[upos + ulen - 1..]);
    Ok(writer.write(Some(ret)))
}

#[rpn_fn(writer)]
#[inline]
pub fn right_utf8(lhs: BytesRef, rhs: &Int, writer: BytesWriter) -> Result<BytesGuard> {
    if *rhs <= 0 {
        return Ok(writer.write_ref(Some(b"")));
    }
    match str::from_utf8(&*lhs) {
        Ok(s) => {
            let rhs = *rhs as usize;
            let len = s.chars().count();
            let result = if len > rhs {
                let idx = s
                    .char_indices()
                    .nth(len - rhs)
                    .map(|(idx, _)| idx)
                    .unwrap_or_else(|| s.len());
                s[idx..].as_bytes()
            } else {
                s.as_bytes()
            };

            Ok(writer.write_ref(Some(result)))
        }
        Err(err) => Err(box_err!("invalid input value: {:?}", err)),
    }
}

#[rpn_fn(writer)]
#[inline]
pub fn upper_utf8(arg: BytesRef, writer: BytesWriter) -> Result<BytesGuard> {
    match str::from_utf8(arg) {
        Ok(s) => Ok(writer.write_ref(Some(s.to_uppercase().as_bytes()))),
        Err(err) => Err(box_err!("invalid input value: {:?}", err)),
    }
}

#[rpn_fn(writer)]
#[inline]
// upper is a noop in TiDB side, keep the same logic here.
// ref: https://github.com/pingcap/tidb/blob/master/expression/builtin_string_vec.go#L152-L158
pub fn upper(arg: BytesRef, writer: BytesWriter) -> Result<BytesGuard> {
    Ok(writer.write_ref(Some(arg)))
}

#[rpn_fn(writer)]
#[inline]
pub fn hex_str_arg(arg: BytesRef, writer: BytesWriter) -> Result<BytesGuard> {
    Ok(writer.write(Some(hex::encode_upper(arg).into_bytes())))
}

#[rpn_fn]
#[inline]
pub fn locate_2_args(substr: BytesRef, s: BytesRef) -> Result<Option<i64>> {
    Ok(twoway::find_bytes(s, substr)
        .map(|i| 1 + i as i64)
        .or(Some(0)))
}

#[rpn_fn(writer)]
#[inline]
pub fn reverse(arg: BytesRef, writer: BytesWriter) -> Result<BytesGuard> {
    let mut result = arg.to_vec();
    result.reverse();
    Ok(writer.write(Some(result)))
}

#[rpn_fn]
#[inline]
pub fn locate_3_args(substr: BytesRef, s: BytesRef, pos: &Int) -> Result<Option<Int>> {
    if *pos < 1 || *pos as usize > s.len() + 1 {
        return Ok(Some(0));
    }
    Ok(twoway::find_bytes(&s[*pos as usize - 1..], substr)
        .map(|i| pos + i as i64)
        .or(Some(0)))
}

#[rpn_fn(nullable, varg, min_args = 1)]
#[inline]
fn field<T: Evaluable + EvaluableRet + PartialEq>(args: &[Option<&T>]) -> Result<Option<Int>> {
    Ok(Some(match args[0] {
        // As per the MySQL doc, if the first argument is NULL, this function always returns 0.
        None => 0,
        Some(val) => args
            .iter()
            .skip(1)
            .position(|&i| i == Some(val))
            .map_or(0, |pos| (pos + 1) as i64),
    }))
}

#[rpn_fn(nullable, varg, min_args = 1)]
#[inline]
fn field_bytes(args: &[Option<BytesRef>]) -> Result<Option<Int>> {
    Ok(Some(match args[0] {
        // As per the MySQL doc, if the first argument is NULL, this function always returns 0.
        None => 0,
        Some(val) => args
            .iter()
            .skip(1)
            .position(|&i| i == Some(val))
            .map_or(0, |pos| (pos + 1) as i64),
    }))
}

#[rpn_fn(nullable, raw_varg, min_args = 2, extra_validator = elt_validator)]
#[inline]
pub fn make_set(raw_args: &[ScalarValueRef]) -> Result<Option<Bytes>> {
    assert!(raw_args.len() >= 2);
    let mask = raw_args[0].as_int();
    let mut output = Vec::new();
    let mut pow2 = 1;
    let s = b",";
    let mut q = false;
    match mask {
        None => {
            return Ok(None);
        }
        Some(mask2) => {
            for i in 1..raw_args.len() {
                if pow2 & mask2 != 0 {
                    let input = raw_args[i].as_bytes();
                    match input {
                        None => {}
                        Some(s2) => {
                            if q {
                                output.extend_from_slice(s);
                            }
                            output.extend_from_slice(s2);
                            q = true;
                        }
                    };
                }
                pow2 <<= 1;
            }
        }
    };
    Ok(Some(output))
}

#[rpn_fn(nullable, raw_varg, min_args = 2, extra_validator = elt_validator)]
#[inline]
pub fn elt(raw_args: &[ScalarValueRef]) -> Result<Option<Bytes>> {
    assert!(raw_args.len() >= 2);
    let index = raw_args[0].as_int();
    Ok(match index {
        None => None,
        Some(i) => {
            let i = *i;
            if i <= 0 || i + 1 > raw_args.len() as i64 {
                return Ok(None);
            }
            raw_args[i as usize].as_bytes().map(|x| x.to_vec())
        }
    })
}

/// validate the arguments are `(Option<&Int>, &[Option<BytesRef>)])`
fn elt_validator(expr: &tipb::Expr) -> Result<()> {
    let children = expr.get_children();
    assert!(children.len() >= 2);
    super::function::validate_expr_return_type(&children[0], EvalType::Int)?;
    for i in 1..children.len() {
        super::function::validate_expr_return_type(&children[i], EvalType::Bytes)?;
    }
    Ok(())
}

#[rpn_fn(writer)]
#[inline]
pub fn space(len: &Int, writer: BytesWriter) -> Result<BytesGuard> {
    let guard = if *len > i64::from(tidb_query_datatype::MAX_BLOB_WIDTH) {
        writer.write(None)
    } else if *len <= 0 {
        writer.write_ref(Some(b""))
    } else {
        writer.write(Some(vec![SPACE; *len as usize]))
    };

    Ok(guard)
}

#[rpn_fn(writer)]
#[inline]
pub fn substring_index(
    s: BytesRef,
    delim: BytesRef,
    count: &Int,
    writer: BytesWriter,
) -> Result<BytesGuard> {
    let count = *count;
    if count == 0 || s.is_empty() || delim.is_empty() {
        return Ok(writer.write_ref(Some(b"")));
    }
    let finder = if count > 0 {
        twoway::find_bytes
    } else {
        twoway::rfind_bytes
    };
    let mut remaining = &s[..];
    let mut remaining_pattern_count = count.abs();
    let mut bound = 0;
    while remaining_pattern_count > 0 {
        if let Some(offset) = finder(&remaining, delim) {
            if count > 0 {
                bound += offset + delim.len();
                remaining = &s[bound..];
            } else {
                bound = offset;
                remaining = &s[..bound];
            }
        } else {
            break;
        }
        remaining_pattern_count -= 1;
    }

    let result = if remaining_pattern_count > 0 {
        &s[..]
    } else if count > 0 {
        &s[..bound - delim.len()]
    } else {
        &s[bound + delim.len()..]
    };

    Ok(writer.write_ref(Some(result)))
}

#[rpn_fn]
#[inline]
pub fn strcmp(left: BytesRef, right: BytesRef) -> Result<Option<i64>> {
    use std::cmp::Ordering::*;
    Ok(Some(match left.cmp(right) {
        Less => -1,
        Equal => 0,
        Greater => 1,
    }))
}

#[rpn_fn]
#[inline]
pub fn instr_utf8(s: BytesRef, substr: BytesRef) -> Result<Option<Int>> {
    let s = String::from_utf8_lossy(s);
    let substr = String::from_utf8_lossy(substr);
    let index = twoway::find_str(&s.to_lowercase(), &substr.to_lowercase())
        .map(|i| s[..i].chars().count())
        .map(|i| 1 + i as i64)
        .or(Some(0));
    Ok(index)
}

#[rpn_fn]
#[inline]
pub fn find_in_set(s: BytesRef, str_list: BytesRef) -> Result<Option<Int>> {
    if str_list.is_empty() {
        return Ok(Some(0));
    }

    let s = String::from_utf8_lossy(s);
    let result = String::from_utf8_lossy(str_list)
        .split(',')
        .position(|str_in_set| str_in_set == s)
        .map(|p| p as i64 + 1)
        .or(Some(0));

    Ok(result)
}

#[rpn_fn(writer)]
#[inline]
pub fn trim_1_arg(arg: BytesRef, writer: BytesWriter) -> Result<BytesGuard> {
    let l_pos = arg.iter().position(|&x| x != SPACE);

    let result = if let Some(i) = l_pos {
        let r_pos = arg.iter().rposition(|&x| x != SPACE);
        &arg[i..=r_pos.unwrap()]
    } else {
        b""
    };

    Ok(writer.write_ref(Some(result)))
}

#[rpn_fn(writer)]
#[inline]
pub fn trim_3_args(
    arg: BytesRef,
    pat: BytesRef,
    direction: &i64,
    writer: BytesWriter,
) -> Result<BytesGuard> {
    match TrimDirection::from_i64(*direction) {
        Some(d) => {
            let arg = String::from_utf8_lossy(arg);
            let pat = String::from_utf8_lossy(pat);
            Ok(writer.write(Some(trim(&arg, &pat, d))))
        }
        _ => Err(box_err!("invalid direction value: {}", direction)),
    }
}

#[rpn_fn]
#[inline]
pub fn char_length(bs: BytesRef) -> Result<Option<Int>> {
    Ok(Some(bs.len() as i64))
}

#[rpn_fn]
#[inline]
pub fn char_length_utf8(bs: BytesRef) -> Result<Option<Int>> {
    match str::from_utf8(bs) {
        Ok(s) => Ok(Some(s.chars().count() as i64)),
        Err(err) => Err(box_err!("invalid input value: {:?}", err)),
    }
}

#[rpn_fn(writer)]
#[inline]
pub fn to_base64(bs: BytesRef, writer: BytesWriter) -> Result<BytesGuard> {
    if bs.len() > tidb_query_datatype::MAX_BLOB_WIDTH as usize {
        return Ok(writer.write_ref(Some(b"")));
    }

    if let Some(size) = encoded_size(bs.len()) {
        let mut buf = vec![0; size];
        let len_without_wrap = base64::encode_config_slice(bs, base64::STANDARD, &mut buf);
        line_wrap(&mut buf, len_without_wrap);
        Ok(writer.write(Some(buf)))
    } else {
        Ok(writer.write_ref(Some(b"")))
    }
}

#[rpn_fn(writer)]
#[inline]
pub fn from_base64(bs: BytesRef, writer: BytesWriter) -> Result<BytesGuard> {
    let input_copy = strip_whitespace(bs);
    let will_overflow = input_copy
        .len()
        .checked_mul(BASE64_INPUT_CHUNK_LENGTH)
        .is_none();
    // mysql will return "" when the input is incorrectly padded
    let invalid_padding = input_copy.len() % BASE64_ENCODED_CHUNK_LENGTH != 0;
    if will_overflow || invalid_padding {
        Ok(writer.write_ref(Some(b"")))
    } else {
        Ok(writer.write(base64::decode_config(&input_copy, base64::STANDARD).ok()))
    }
}

#[rpn_fn(nullable)]
#[inline]
pub fn quote(input: Option<BytesRef>) -> Result<Option<Bytes>> {
    match input {
        Some(bytes) => {
            let mut result = Vec::with_capacity(bytes.len() * 2 + 2);
            result.push(b'\'');
            for byte in bytes.iter() {
                if *byte == b'\'' || *byte == b'\\' {
                    result.push(b'\\');
                    result.push(*byte)
                } else if *byte == b'\0' {
                    result.push(b'\\');
                    result.push(b'0')
                } else if *byte == 26u8 {
                    result.push(b'\\');
                    result.push(b'Z');
                } else {
                    result.push(*byte)
                }
            }
            result.push(b'\'');
            Ok(Some(result))
        }
        _ => Ok(Some(Vec::from("NULL"))),
    }
}

#[rpn_fn(writer)]
#[inline]
pub fn repeat(input: BytesRef, cnt: &Int, writer: BytesWriter) -> Result<BytesGuard> {
    let cnt = if *cnt > std::i32::MAX.into() {
        std::i32::MAX.into()
    } else {
        *cnt
    };
    let mut writer = writer.begin();
    for _i in 0..cnt {
        writer.partial_write(input);
    }
    Ok(writer.finish())
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::{f64, i64};
    use tipb::ScalarFuncSig;

    use crate::types::test_util::RpnFnScalarEvaluator;

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
    fn test_unhex() {
        let cases = vec![
            (Some(b"4D7953514C".to_vec()), Some(b"MySQL".to_vec())),
            (Some(b"GG".to_vec()), None),
            (Some(b"41\0".to_vec()), None),
            (Some(b"".to_vec()), Some(b"".to_vec())),
            (Some(b"b".to_vec()), Some(vec![0xb])),
            (Some(b"a1b".to_vec()), Some(vec![0xa, 0x1b])),
            (None, None),
        ];
        for (arg, expect_output) in cases {
            let output: Option<Bytes> = RpnFnScalarEvaluator::new()
                .push_param(arg)
                .evaluate(ScalarFuncSig::UnHex)
                .unwrap();
            assert_eq!(output, expect_output);
        }
    }

    #[test]
    fn test_oct_int() {
        let cases = vec![
            (Some(-1), Some(b"1777777777777777777777".to_vec())),
            (Some(0), Some(b"0".to_vec())),
            (Some(1), Some(b"1".to_vec())),
            (Some(8), Some(b"10".to_vec())),
            (Some(12), Some(b"14".to_vec())),
            (Some(20), Some(b"24".to_vec())),
            (Some(100), Some(b"144".to_vec())),
            (Some(1024), Some(b"2000".to_vec())),
            (Some(2048), Some(b"4000".to_vec())),
            (Some(i64::MAX), Some(b"777777777777777777777".to_vec())),
            (Some(i64::MIN), Some(b"1000000000000000000000".to_vec())),
            (None, None),
        ];
        for (arg0, expect_output) in cases {
            let output = RpnFnScalarEvaluator::new()
                .push_param(arg0)
                .evaluate(ScalarFuncSig::OctInt)
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
    fn test_ord() {
        let cases = vec![
            (Some("2"), Some(50i64)),
            (Some("23"), Some(50i64)),
            (Some("2.3"), Some(50i64)),
            (Some(""), Some(0i64)),
            (Some("你好"), Some(14990752i64)),
            (Some("にほん"), Some(14909867i64)),
            (Some("한국"), Some(15570332i64)),
            (Some("👍"), Some(4036989325i64)),
            (Some("א"), Some(55184i64)),
            (None, Some(0)),
        ];

        for (arg, expect_output) in cases {
            let output = RpnFnScalarEvaluator::new()
                .push_param(arg.map(|s| s.as_bytes().to_vec()))
                .evaluate(ScalarFuncSig::Ord)
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
    fn test_reverse_utf8() {
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
                .evaluate(ScalarFuncSig::ReverseUtf8)
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

    #[allow(clippy::type_complexity)]
    fn common_lpad_cases() -> Vec<(Option<Bytes>, Option<Int>, Option<Bytes>, Option<Bytes>)> {
        vec![
            (
                Some(b"hi".to_vec()),
                Some(5),
                Some(b"?".to_vec()),
                Some(b"???hi".to_vec()),
            ),
            (
                Some(b"hi".to_vec()),
                Some(1),
                Some(b"?".to_vec()),
                Some(b"h".to_vec()),
            ),
            (
                Some(b"hi".to_vec()),
                Some(0),
                Some(b"?".to_vec()),
                Some(b"".to_vec()),
            ),
            (Some(b"hi".to_vec()), Some(-1), Some(b"?".to_vec()), None),
            (
                Some(b"hi".to_vec()),
                Some(1),
                Some(b"".to_vec()),
                Some(b"h".to_vec()),
            ),
            (Some(b"hi".to_vec()), Some(5), Some(b"".to_vec()), None),
            (
                Some(b"hi".to_vec()),
                Some(5),
                Some(b"ab".to_vec()),
                Some(b"abahi".to_vec()),
            ),
            (
                Some(b"hi".to_vec()),
                Some(6),
                Some(b"ab".to_vec()),
                Some(b"ababhi".to_vec()),
            ),
        ]
    }

    #[test]
    fn test_lpad() {
        let mut cases = vec![
            (
                Some(b"hello".to_vec()),
                Some(0),
                Some(b"h".to_vec()),
                Some(b"".to_vec()),
            ),
            (
                Some(b"hello".to_vec()),
                Some(1),
                Some(b"h".to_vec()),
                Some(b"h".to_vec()),
            ),
            (Some(b"hello".to_vec()), Some(-1), Some(b"h".to_vec()), None),
            (
                Some(b"hello".to_vec()),
                Some(3),
                Some(b"".to_vec()),
                Some(b"hel".to_vec()),
            ),
            (Some(b"hello".to_vec()), Some(8), Some(b"".to_vec()), None),
            (
                Some(b"hello".to_vec()),
                Some(8),
                Some(b"he".to_vec()),
                Some(b"hehhello".to_vec()),
            ),
            (
                Some(b"hello".to_vec()),
                Some(9),
                Some(b"he".to_vec()),
                Some(b"hehehello".to_vec()),
            ),
            (
                Some(b"hello".to_vec()),
                Some(5),
                Some("您好".as_bytes().to_vec()),
                Some(b"hello".to_vec()),
            ),
            (Some(b"hello".to_vec()), Some(6), Some(b"".to_vec()), None),
            (
                Some(b"\x61\x76\x5e".to_vec()),
                Some(2),
                Some(b"\x35".to_vec()),
                Some(b"\x61\x76".to_vec()),
            ),
            (
                Some(b"\x61\x76\x5e".to_vec()),
                Some(5),
                Some(b"\x35".to_vec()),
                Some(b"\x35\x35\x61\x76\x5e".to_vec()),
            ),
            (
                Some(b"hello".to_vec()),
                Some(i64::from(MAX_BLOB_WIDTH) + 1),
                Some(b"he".to_vec()),
                None,
            ),
            (None, Some(-1), Some(b"h".to_vec()), None),
            (None, None, None, None),
        ];
        cases.append(&mut common_lpad_cases());

        for (arg, len, pad, expect_output) in cases {
            let output = RpnFnScalarEvaluator::new()
                .push_param(arg)
                .push_param(len)
                .push_param(pad)
                .evaluate(ScalarFuncSig::Lpad)
                .unwrap();
            assert_eq!(output, expect_output);
        }
    }

    #[allow(clippy::type_complexity)]
    fn common_rpad_cases() -> Vec<(Option<Bytes>, Option<Int>, Option<Bytes>, Option<Bytes>)> {
        vec![
            (
                Some(b"hi".to_vec()),
                Some(5),
                Some(b"?".to_vec()),
                Some(b"hi???".to_vec()),
            ),
            (
                Some(b"hi".to_vec()),
                Some(1),
                Some(b"?".to_vec()),
                Some(b"h".to_vec()),
            ),
            (
                Some(b"hi".to_vec()),
                Some(0),
                Some(b"?".to_vec()),
                Some(b"".to_vec()),
            ),
            (
                Some(b"hi".to_vec()),
                Some(1),
                Some(b"".to_vec()),
                Some(b"h".to_vec()),
            ),
            (
                Some(b"hi".to_vec()),
                Some(5),
                Some(b"ab".to_vec()),
                Some(b"hiaba".to_vec()),
            ),
            (
                Some(b"hi".to_vec()),
                Some(6),
                Some(b"ab".to_vec()),
                Some(b"hiabab".to_vec()),
            ),
            (Some(b"hi".to_vec()), Some(-1), Some(b"?".to_vec()), None),
            (Some(b"hi".to_vec()), Some(5), Some(b"".to_vec()), None),
            (
                Some(b"hi".to_vec()),
                Some(0),
                Some(b"".to_vec()),
                Some(b"".to_vec()),
            ),
        ]
    }

    #[test]
    fn test_rpad() {
        let mut cases = vec![
            (
                Some(b"\x61\x76\x5e".to_vec()),
                Some(5),
                Some(b"\x35".to_vec()),
                Some(b"\x61\x76\x5e\x35\x35".to_vec()),
            ),
            (
                Some(b"\x61\x76\x5e".to_vec()),
                Some(2),
                Some(b"\x35".to_vec()),
                Some(b"\x61\x76".to_vec()),
            ),
            (
                Some("a多字节".as_bytes().to_vec()),
                Some(13),
                Some("测试".as_bytes().to_vec()),
                Some("a多字节测".as_bytes().to_vec()),
            ),
            (
                Some(b"abc".to_vec()),
                Some(i64::from(MAX_BLOB_WIDTH) + 1),
                Some(b"aa".to_vec()),
                None,
            ),
        ];
        cases.append(&mut common_rpad_cases());

        for (arg, len, pad, expect_output) in cases {
            let output = RpnFnScalarEvaluator::new()
                .push_param(arg)
                .push_param(len)
                .push_param(pad)
                .evaluate(ScalarFuncSig::Rpad)
                .unwrap();
            assert_eq!(output, expect_output);
        }
    }

    #[test]
    fn test_lpad_utf8() {
        let mut cases = vec![
            (
                Some("a多字节".as_bytes().to_vec()),
                Some(3),
                Some("测试".as_bytes().to_vec()),
                Some("a多字".as_bytes().to_vec()),
            ),
            (
                Some("a多字节".as_bytes().to_vec()),
                Some(4),
                Some("测试".as_bytes().to_vec()),
                Some("a多字节".as_bytes().to_vec()),
            ),
            (
                Some("a多字节".as_bytes().to_vec()),
                Some(5),
                Some("测试".as_bytes().to_vec()),
                Some("测a多字节".as_bytes().to_vec()),
            ),
            (
                Some("a多字节".as_bytes().to_vec()),
                Some(6),
                Some("测试".as_bytes().to_vec()),
                Some("测试a多字节".as_bytes().to_vec()),
            ),
            (
                Some("a多字节".as_bytes().to_vec()),
                Some(7),
                Some("测试".as_bytes().to_vec()),
                Some("测试测a多字节".as_bytes().to_vec()),
            ),
            (
                Some("a多字节".as_bytes().to_vec()),
                Some(i64::from(MAX_BLOB_WIDTH) / 4 + 1),
                Some("测试".as_bytes().to_vec()),
                None,
            ),
        ];
        cases.append(&mut common_lpad_cases());

        for (arg, len, pad, expect_output) in cases {
            let output = RpnFnScalarEvaluator::new()
                .push_param(arg)
                .push_param(len)
                .push_param(pad)
                .evaluate(ScalarFuncSig::LpadUtf8)
                .unwrap();
            assert_eq!(output, expect_output);
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
            (Some(b"hello".to_vec()), Some(0), Some(b"".to_vec())),
            (Some(b"hello".to_vec()), Some(1), Some(b"h".to_vec())),
            (
                Some("数据库".as_bytes().to_vec()),
                Some(2),
                Some(vec![230u8, 149u8]),
            ),
            (
                Some("忠犬ハチ公".as_bytes().to_vec()),
                Some(3),
                Some(vec![229u8, 191u8, 160u8]),
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
                .evaluate(ScalarFuncSig::Left)
                .unwrap();
            assert_eq!(output, expect_output);
        }
    }

    #[test]
    fn test_left_utf8() {
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
                .evaluate(ScalarFuncSig::LeftUtf8)
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
                Some(vec![186u8, 147u8]),
            ),
            (
                Some("忠犬ハチ公".as_bytes().to_vec()),
                Some(3),
                Some(vec![229u8, 133u8, 172u8]),
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
    fn test_insert() {
        let cases = vec![
            ("hello, world!", 1, 0, "asd", "asdhello, world!"),
            ("hello, world!", 0, -1, "asd", "hello, world!"),
            ("hello, world!", 0, 0, "asd", "hello, world!"),
            ("hello, world!", -1, 0, "asd", "hello, world!"),
            ("hello, world!", 1, -1, "asd", "asd"),
            ("hello, world!", 1, 1, "asd", "asdello, world!"),
            ("hello, world!", 1, 3, "asd", "asdlo, world!"),
            ("hello, world!", 2, 2, "asd", "hasdlo, world!"),
            ("hello", 5, 2, "asd", "hellasd"),
            ("hello", 5, 200, "asd", "hellasd"),
            ("hello", 2, 200, "asd", "hasd"),
            ("hello", -1, 200, "asd", "hello"),
            ("hello", 0, 200, "asd", "hello"),
        ];
        for (s1, i1, i2, s2, exp) in cases {
            let s1 = Some(s1.as_bytes().to_vec());
            let i1 = Some(i1);
            let i2 = Some(i2);
            let s2 = Some(s2.as_bytes().to_vec());
            let exp = Some(exp.as_bytes().to_vec());
            let got = RpnFnScalarEvaluator::new()
                .push_param(s1)
                .push_param(i1)
                .push_param(i2)
                .push_param(s2)
                .evaluate(ScalarFuncSig::Insert)
                .unwrap();
            assert_eq!(got, exp);
        }

        let null_cases = vec![
            (None, Some(-1), Some(200), Some(b"asd".to_vec())),
            (
                Some(b"hello".to_vec()),
                None,
                Some(200),
                Some(b"asd".to_vec()),
            ),
            (
                Some(b"hello".to_vec()),
                Some(-1),
                None,
                Some(b"asd".to_vec()),
            ),
            (Some(b"hello".to_vec()), Some(-1), Some(200), None),
        ];
        for (s1, i1, i2, s2) in null_cases {
            let got = RpnFnScalarEvaluator::new()
                .push_param(s1)
                .push_param(i1)
                .push_param(i2)
                .push_param(s2)
                .evaluate::<Bytes>(ScalarFuncSig::Insert)
                .unwrap();
            assert_eq!(got, None);
        }
    }

    #[test]
    fn test_right_utf8() {
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
                .evaluate(ScalarFuncSig::RightUtf8)
                .unwrap();
            assert_eq!(output, expect_output);
        }
    }

    #[test]
    fn test_upper_utf8() {
        let cases = vec![
            (Some(b"hello".to_vec()), Some(b"HELLO".to_vec())),
            (Some(b"123".to_vec()), Some(b"123".to_vec())),
            (
                Some("café".as_bytes().to_vec()),
                Some("CAFÉ".as_bytes().to_vec()),
            ),
            (
                Some("数据库".as_bytes().to_vec()),
                Some("数据库".as_bytes().to_vec()),
            ),
            (
                Some("ночь на окраине москвы".as_bytes().to_vec()),
                Some("НОЧЬ НА ОКРАИНЕ МОСКВЫ".as_bytes().to_vec()),
            ),
            (
                Some("قاعدة البيانات".as_bytes().to_vec()),
                Some("قاعدة البيانات".as_bytes().to_vec()),
            ),
            (None, None),
        ];

        for (arg, exp) in cases {
            let output = RpnFnScalarEvaluator::new()
                .push_param(arg.clone())
                .evaluate(ScalarFuncSig::UpperUtf8)
                .unwrap();
            assert_eq!(output, exp);
        }
    }

    #[test]
    fn test_upper() {
        let cases = vec![
            (Some(b"hello".to_vec()), Some(b"hello".to_vec())),
            (Some(b"123".to_vec()), Some(b"123".to_vec())),
            (
                Some("café".as_bytes().to_vec()),
                Some("café".as_bytes().to_vec()),
            ),
            (
                Some("数据库".as_bytes().to_vec()),
                Some("数据库".as_bytes().to_vec()),
            ),
            (
                Some("ночь на окраине москвы".as_bytes().to_vec()),
                Some("ночь на окраине москвы".as_bytes().to_vec()),
            ),
            (
                Some("قاعدة البيانات".as_bytes().to_vec()),
                Some("قاعدة البيانات".as_bytes().to_vec()),
            ),
            (None, None),
        ];

        for (arg, exp) in cases {
            let output = RpnFnScalarEvaluator::new()
                .push_param(arg.clone())
                .evaluate(ScalarFuncSig::Upper)
                .unwrap();
            assert_eq!(output, exp);
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
    fn test_locate_2_args() {
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
                .evaluate(ScalarFuncSig::Locate2Args)
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
                Some("中国".as_bytes().to_vec()),
                Some(vec![0o275u8, 0o233u8, 0o345u8, 0o255u8, 0o270u8, 0o344u8]),
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
    fn test_locate_3_args() {
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
                .evaluate(ScalarFuncSig::Locate3Args)
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
    fn test_make_set() {
        let test_cases: Vec<(Vec<ScalarValue>, _)> = vec![
            (
                vec![
                    Some(0b110).into(),
                    Some(b"DataBase".to_vec()).into(),
                    Some(b"Hello World!".to_vec()).into(),
                ],
                Some(b"Hello World!".to_vec()),
            ),
            (
                vec![
                    Some(0b100).into(),
                    Some(b"DataBase".to_vec()).into(),
                    Some(b"Hello World!".to_vec()).into(),
                ],
                Some(b"".to_vec()),
            ),
            (
                vec![
                    Some(0b0).into(),
                    Some(b"DataBase".to_vec()).into(),
                    Some(b"Hello World!".to_vec()).into(),
                ],
                Some(b"".to_vec()),
            ),
            (
                vec![
                    Some(0b1).into(),
                    Some(b"DataBase".to_vec()).into(),
                    Some(b"Hello World!".to_vec()).into(),
                ],
                Some(b"DataBase".to_vec()),
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
                    Some(0b1).into(),
                    None::<Bytes>.into(),
                    Some(b"Hello World!".to_vec()).into(),
                ],
                Some(b"".to_vec()),
            ),
            (
                vec![
                    Some(0b11).into(),
                    None::<Bytes>.into(),
                    Some(b"Hello World!".to_vec()).into(),
                ],
                Some(b"Hello World!".to_vec()),
            ),
            (
                vec![
                    Some(0b0).into(),
                    None::<Bytes>.into(),
                    Some(b"Hello World!".to_vec()).into(),
                ],
                Some(b"".to_vec()),
            ),
            (
                vec![
                    Some(0xffffffff).into(),
                    None::<Bytes>.into(),
                    Some(b"Hello World!".to_vec()).into(),
                    None::<Bytes>.into(),
                ],
                Some(b"Hello World!".to_vec()),
            ),
            (
                vec![
                    Some(0b10).into(),
                    Some(b"DataBase".to_vec()).into(),
                    Some(b"Hello World!".to_vec()).into(),
                ],
                Some(b"Hello World!".to_vec()),
            ),
            (
                vec![
                    Some(0xffffffff).into(),
                    Some(b"a".to_vec()).into(),
                    Some(b"b".to_vec()).into(),
                    Some(b"c".to_vec()).into(),
                ],
                Some(b"a,b,c".to_vec()),
            ),
            (
                vec![
                    Some(0xfffffffe).into(),
                    Some(b"a".to_vec()).into(),
                    Some(b"b".to_vec()).into(),
                    Some(b"c".to_vec()).into(),
                ],
                Some(b"b,c".to_vec()),
            ),
            (
                vec![
                    Some(0xfffffffd).into(),
                    Some(b"a".to_vec()).into(),
                    Some(b"b".to_vec()).into(),
                    Some(b"c".to_vec()).into(),
                ],
                Some(b"a,c".to_vec()),
            ),
        ];
        for (args, expect_output) in test_cases {
            let output = RpnFnScalarEvaluator::new()
                .push_params(args)
                .evaluate(ScalarFuncSig::MakeSet)
                .unwrap();
            assert_eq!(output, expect_output);
        }
    }

    #[test]
    fn test_substring_index() {
        let test_cases = vec![
            (None, None, None, None),
            (Some(vec![]), None, None, None),
            (Some(vec![]), Some(vec![]), Some(1i64), Some(vec![])),
            (Some(vec![0x1]), Some(vec![]), Some(1), Some(vec![])),
            (Some(vec![0x1]), Some(vec![]), Some(-1), Some(vec![])),
            (Some(vec![]), Some(vec![0x1]), Some(1), Some(vec![])),
            (Some(vec![]), Some(vec![0x1]), Some(-1), Some(vec![])),
            (
                Some(b"abc".to_vec()),
                Some(b"ab".to_vec()),
                Some(0),
                Some(vec![]),
            ),
            (
                Some(b"aaaaaaaa".to_vec()),
                Some(b"aa".to_vec()),
                Some(1),
                Some(vec![]),
            ),
            (
                Some(b"bbbbbbbb".to_vec()),
                Some(b"bb".to_vec()),
                Some(-1),
                Some(vec![]),
            ),
            (
                Some(b"cccccccc".to_vec()),
                Some(b"cc".to_vec()),
                Some(2),
                Some(b"cc".to_vec()),
            ),
            (
                Some(b"dddddddd".to_vec()),
                Some(b"dd".to_vec()),
                Some(-2),
                Some(b"dd".to_vec()),
            ),
            (
                Some(b"eeeeeeee".to_vec()),
                Some(b"ee".to_vec()),
                Some(5),
                Some(b"eeeeeeee".to_vec()),
            ),
            (
                Some(b"ffffffff".to_vec()),
                Some(b"ff".to_vec()),
                Some(-5),
                Some(b"ffffffff".to_vec()),
            ),
            (
                Some(b"gggggggg".to_vec()),
                Some(b"gg".to_vec()),
                Some(6),
                Some(b"gggggggg".to_vec()),
            ),
            (
                Some(b"hhhhhhhh".to_vec()),
                Some(b"hh".to_vec()),
                Some(-6),
                Some(b"hhhhhhhh".to_vec()),
            ),
            (
                Some(b"iiiii".to_vec()),
                Some(b"ii".to_vec()),
                Some(1),
                Some(vec![]),
            ),
            (
                Some(b"jjjjj".to_vec()),
                Some(b"jj".to_vec()),
                Some(-1),
                Some(vec![]),
            ),
            (
                Some(b"kkkkk".to_vec()),
                Some(b"kk".to_vec()),
                Some(3),
                Some(b"kkkkk".to_vec()),
            ),
            (
                Some(b"lllll".to_vec()),
                Some(b"ll".to_vec()),
                Some(-3),
                Some(b"lllll".to_vec()),
            ),
            (
                Some(b"www.mysql.com".to_vec()),
                Some(b".".to_vec()),
                Some(2),
                Some(b"www.mysql".to_vec()),
            ),
            (
                Some(b"www.mysql.com".to_vec()),
                Some(b".".to_vec()),
                Some(-2),
                Some(b"mysql.com".to_vec()),
            ),
            (
                Some(b"abcabcabc".to_vec()),
                Some(b"ab".to_vec()),
                Some(1),
                Some(vec![]),
            ),
            (
                Some(b"abcabcabc".to_vec()),
                Some(b"ab".to_vec()),
                Some(-1),
                Some(b"c".to_vec()),
            ),
            (
                Some(b"abcabcabc".to_vec()),
                Some(b"ab".to_vec()),
                Some(2),
                Some(b"abc".to_vec()),
            ),
            (
                Some(b"abcabcabc".to_vec()),
                Some(b"ab".to_vec()),
                Some(-2),
                Some(b"cabc".to_vec()),
            ),
            (
                Some(b"abcabcabc".to_vec()),
                Some(b"ab".to_vec()),
                Some(5),
                Some(b"abcabcabc".to_vec()),
            ),
            (
                Some(b"abcabcabc".to_vec()),
                Some(b"ab".to_vec()),
                Some(-5),
                Some(b"abcabcabc".to_vec()),
            ),
            (
                Some(b"abcabcabc".to_vec()),
                Some(b"d".to_vec()),
                Some(1),
                Some(b"abcabcabc".to_vec()),
            ),
            (
                Some(b"abcabcabc".to_vec()),
                Some(b"d".to_vec()),
                Some(-1),
                Some(b"abcabcabc".to_vec()),
            ),
        ];
        for (s, delim, count, exp) in test_cases {
            let output = RpnFnScalarEvaluator::new()
                .push_param(s)
                .push_param(delim)
                .push_param(count)
                .evaluate(ScalarFuncSig::SubstringIndex)
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

    #[test]
    fn test_instr_utf8() {
        let cases: Vec<(&str, &str, i64)> = vec![
            ("a", "abcdefg", 1),
            ("0", "abcdefg", 0),
            ("c", "abcdefg", 3),
            ("F", "abcdefg", 6),
            ("cd", "abcdefg", 3),
            (" ", "abcdefg", 0),
            ("", "", 1),
            (" ", " ", 1),
            (" ", "", 0),
            ("", " ", 1),
            ("eFg", "abcdefg", 5),
            ("def", "abcdefg", 4),
            ("字节", "a多字节", 3),
            ("a", "a多字节", 1),
            ("bar", "foobarbar", 4),
            ("xbar", "foobarbar", 0),
            ("好世", "你好世界", 2),
        ];

        for (substr, s, exp) in cases {
            let substr = Some(substr.as_bytes().to_vec());
            let s = Some(s.as_bytes().to_vec());
            let got = RpnFnScalarEvaluator::new()
                .push_param(s)
                .push_param(substr)
                .evaluate::<Int>(ScalarFuncSig::InstrUtf8)
                .unwrap();
            assert_eq!(got, Some(exp))
        }

        let null_cases = vec![
            (None, Some(b"".to_vec()), None),
            (None, Some(b"foobar".to_vec()), None),
            (Some(b"".to_vec()), None, None),
            (Some(b"bar".to_vec()), None, None),
            (None, None, None),
        ];
        for (substr, s, exp) in null_cases {
            let got = RpnFnScalarEvaluator::new()
                .push_param(s)
                .push_param(substr)
                .evaluate::<Int>(ScalarFuncSig::InstrUtf8)
                .unwrap();
            assert_eq!(got, exp);
        }
    }

    #[test]
    fn test_find_in_set() {
        let cases = vec![
            ("foo", "foo,bar", 1),
            ("foo", "foobar,bar", 0),
            (" foo ", "foo, foo ", 2),
            ("", "foo,bar,", 3),
            ("", "", 0),
            ("a,b", "a,b,c", 0),
        ];

        for (s, str_list, exp) in cases {
            let s = Some(s.as_bytes().to_vec());
            let str_list = Some(str_list.as_bytes().to_vec());
            let got = RpnFnScalarEvaluator::new()
                .push_param(s)
                .push_param(str_list)
                .evaluate::<Int>(ScalarFuncSig::FindInSet)
                .unwrap();
            assert_eq!(got, Some(exp))
        }

        let null_cases = vec![
            (Some(b"foo".to_vec()), None, None),
            (None, Some(b"bar".to_vec()), None),
            (None, None, None),
        ];
        for (s, str_list, exp) in null_cases {
            let got = RpnFnScalarEvaluator::new()
                .push_param(s)
                .push_param(str_list)
                .evaluate::<Int>(ScalarFuncSig::FindInSet)
                .unwrap();
            assert_eq!(got, exp);
        }
    }

    #[test]
    fn test_trim_1_arg() {
        let test_cases = vec![
            (None, None),
            (Some("   bar   "), Some("bar")),
            (Some("   b   "), Some("b")),
            (Some("   b   ar   "), Some("b   ar")),
            (Some("bar"), Some("bar")),
            (Some("    "), Some("")),
            (Some("  \tbar\t   "), Some("\tbar\t")),
            (Some("  \rbar\r   "), Some("\rbar\r")),
            (Some("  \nbar\n   "), Some("\nbar\n")),
            (Some(""), Some("")),
            (Some("  你好"), Some("你好")),
            (Some("  你  好  "), Some("你  好")),
            (Some("  분산 데이터베이스    "), Some("분산 데이터베이스")),
            (
                Some("   あなたのことが好きです   "),
                Some("あなたのことが好きです"),
            ),
        ];

        for (arg, expect_output) in test_cases {
            let output = RpnFnScalarEvaluator::new()
                .push_param(arg.map(|s| s.as_bytes().to_vec()))
                .evaluate(ScalarFuncSig::Trim1Arg)
                .unwrap();
            assert_eq!(output, expect_output.map(|s| s.as_bytes().to_vec()));
        }

        let invalid_utf8_output = RpnFnScalarEvaluator::new()
            .push_param(Some(b"  \xF0 Hello \x90 World \x80 ".to_vec()))
            .evaluate(ScalarFuncSig::Trim1Arg)
            .unwrap();
        assert_eq!(
            invalid_utf8_output,
            Some(b"\xF0 Hello \x90 World \x80".to_vec())
        );
    }

    #[test]
    fn test_trim_3_args() {
        let tests = vec![
            (
                Some("xxxbarxxx"),
                Some("x"),
                Some(TrimDirection::Leading as i64),
                Some("barxxx"),
            ),
            (
                Some("barxxyz"),
                Some("xyz"),
                Some(TrimDirection::Trailing as i64),
                Some("barx"),
            ),
            (
                Some("xxxbarxxx"),
                Some("x"),
                Some(TrimDirection::Both as i64),
                Some("bar"),
            ),
        ];
        for (arg, pat, direction, exp) in tests {
            let arg = arg.map(|s| s.as_bytes().to_vec());
            let pat = pat.map(|s| s.as_bytes().to_vec());
            let exp = exp.map(|s| s.as_bytes().to_vec());

            let got = RpnFnScalarEvaluator::new()
                .push_param(arg)
                .push_param(pat)
                .push_param(direction)
                .evaluate(ScalarFuncSig::Trim3Args)
                .unwrap();
            assert_eq!(got, exp);
        }

        let invalid_tests = vec![
            (
                None,
                Some(b"x".to_vec()),
                Some(TrimDirection::Leading as i64),
                None as Option<Bytes>,
            ),
            (
                Some(b"bar".to_vec()),
                None,
                Some(TrimDirection::Leading as i64),
                None as Option<Bytes>,
            ),
        ];
        for (arg, pat, direction, exp) in invalid_tests {
            let got = RpnFnScalarEvaluator::new()
                .push_param(arg)
                .push_param(pat)
                .push_param(direction)
                .evaluate(ScalarFuncSig::Trim3Args)
                .unwrap();
            assert_eq!(got, exp);
        }

        // test invalid direction value
        let args = (Some(b"bar".to_vec()), Some(b"b".to_vec()), Some(0 as i64));
        let got: Result<Option<Bytes>> = RpnFnScalarEvaluator::new()
            .push_param(args.0)
            .push_param(args.1)
            .push_param(args.2)
            .evaluate(ScalarFuncSig::Trim3Args);
        assert!(got.is_err());
    }

    #[test]
    fn test_char_length() {
        let cases = vec![
            (Some(b"HELLO".to_vec()), Some(5)),
            (Some(b"123".to_vec()), Some(3)),
            (Some(b"".to_vec()), Some(0)),
            (Some("CAFÉ".as_bytes().to_vec()), Some(5)),
            (Some("数据库".as_bytes().to_vec()), Some(9)),
            (Some("НОЧЬ НА ОКРАИНЕ МОСКВЫ".as_bytes().to_vec()), Some(41)),
            (Some("قاعدة البيانات".as_bytes().to_vec()), Some(27)),
            (Some(vec![0x00, 0x9f, 0x92, 0x96]), Some(4)), // invalid utf8
            (Some(b"Hello\xF0\x90\x80World".to_vec()), Some(13)), // invalid utf8
            (None, None),
        ];

        for (arg, expected_output) in cases {
            let output = RpnFnScalarEvaluator::new()
                .push_param(arg)
                .evaluate(ScalarFuncSig::CharLength)
                .unwrap();
            assert_eq!(output, expected_output);
        }
    }

    #[test]
    fn test_char_length_utf8() {
        let cases = vec![
            (Some(b"HELLO".to_vec()), Some(5)),
            (Some(b"123".to_vec()), Some(3)),
            (Some(b"".to_vec()), Some(0)),
            (Some("CAFÉ".as_bytes().to_vec()), Some(4)),
            (Some("数据库".as_bytes().to_vec()), Some(3)),
            (Some("НОЧЬ НА ОКРАИНЕ МОСКВЫ".as_bytes().to_vec()), Some(22)),
            (Some("قاعدة البيانات".as_bytes().to_vec()), Some(14)),
            (None, None),
        ];

        for (arg, expected_output) in cases {
            let output = RpnFnScalarEvaluator::new()
                .push_param(arg)
                .evaluate(ScalarFuncSig::CharLengthUtf8)
                .unwrap();
            assert_eq!(output, expected_output);
        }

        let invalid_utf8_cases: Vec<Vec<u8>> = vec![
            vec![0xc0],
            vec![0xf6],
            vec![0x00, 0x9f],
            vec![0xc3, 0x28],
            vec![0xe2, 0x28, 0xa1],
            vec![0xe2, 0x82, 0x28],
            vec![0xf0, 0x28, 0x8c, 0xbc],
            vec![0xf0, 0x90, 0x28, 0xbc],
            vec![0xf0, 0x28, 0x8c, 0x28],
            vec![0xf8, 0xa1, 0xa1, 0xa1, 0xa0],
            vec![0xfc, 0xa1, 0xa1, 0xa1, 0xa1, 0xa0],
        ];

        for arg in invalid_utf8_cases {
            let output = RpnFnScalarEvaluator::new()
                .push_param(arg)
                .evaluate::<i64>(ScalarFuncSig::CharLengthUtf8);
            assert!(output.is_err());
        }
    }

    #[test]
    fn test_to_base64() {
        let cases = vec![
            ("", ""),
            ("abc", "YWJj"),
            ("ab c", "YWIgYw=="),
            ("1", "MQ=="),
            ("1.1", "MS4x"),
            ("ab\nc", "YWIKYw=="),
            ("ab\tc", "YWIJYw=="),
            ("qwerty123456", "cXdlcnR5MTIzNDU2"),
            (
                "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/",
                "QUJDREVGR0hJSktMTU5PUFFSU1RVVldYWVphYmNkZWZnaGlqa2xtbm9wcXJzdHV2d3h5ejAxMjM0\nNTY3ODkrLw==",
            ),
            (
                "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/",
                "QUJDREVGR0hJSktMTU5PUFFSU1RVVldYWVphYmNkZWZnaGlqa2xtbm9wcXJzdHV2d3h5ejAxMjM0\nNTY3ODkrL0FCQ0RFRkdISUpLTE1OT1BRUlNUVVZXWFlaYWJjZGVmZ2hpamtsbW5vcHFyc3R1dnd4\neXowMTIzNDU2Nzg5Ky9BQkNERUZHSElKS0xNTk9QUVJTVFVWV1hZWmFiY2RlZmdoaWprbG1ub3Bx\ncnN0dXZ3eHl6MDEyMzQ1Njc4OSsv",
            ),
            (
                "ABCD  EFGHI\nJKLMNOPQRSTUVWXY\tZabcdefghijklmnopqrstuv  wxyz012\r3456789+/",
                "QUJDRCAgRUZHSEkKSktMTU5PUFFSU1RVVldYWQlaYWJjZGVmZ2hpamtsbW5vcHFyc3R1diAgd3h5\nejAxMg0zNDU2Nzg5Ky8=",
            ),
            (
                "000000000000000000000000000000000000000000000000000000000",
                "MDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAw",
            ),
            (
                "0000000000000000000000000000000000000000000000000000000000",
                "MDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAw\nMA==",
            ),
            (
                "000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000",
                "MDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAw\nMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAw",
            )
        ];

        for (arg, expected) in cases {
            let param = Some(arg.to_string().into_bytes());
            let expected_output = Some(expected.to_string().into_bytes());
            let output = RpnFnScalarEvaluator::new()
                .push_param(param)
                .evaluate::<Bytes>(ScalarFuncSig::ToBase64)
                .unwrap();
            assert_eq!(output, expected_output);
        }
    }

    #[test]
    fn test_from_base64() {
        let tests = vec![
            ("", ""),
            ("YWJj", "abc"),
            ("YWIgYw==", "ab c"),
            ("YWIKYw==", "ab\nc"),
            ("YWIJYw==", "ab\tc"),
            ("cXdlcnR5MTIzNDU2", "qwerty123456"),
            (
                "QUJDREVGR0hJSktMTU5PUFFSU1RVVldYWVphYmNkZWZnaGlqa2xtbm9wcXJzdHV2d3h5ejAxMjM0\nNTY3ODkrL0FCQ0RFRkdISUpLTE1OT1BRUlNUVVZXWFlaYWJjZGVmZ2hpamtsbW5vcHFyc3R1dnd4\neXowMTIzNDU2Nzg5Ky9BQkNERUZHSElKS0xNTk9QUVJTVFVWV1hZWmFiY2RlZmdoaWprbG1ub3Bx\ncnN0dXZ3eHl6MDEyMzQ1Njc4OSsv",
                "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/",
            ),
            (
                "QUJDREVGR0hJSktMTU5PUFFSU1RVVldYWVphYmNkZWZnaGlqa2xtbm9wcXJzdHV2d3h5ejAxMjM0NTY3ODkrLw==",
                "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/",
            ),
            (
                "QUJDREVGR0hJSktMTU5PUFFSU1RVVldYWVphYmNkZWZnaGlqa2xtbm9wcXJzdHV2d3h5ejAxMjM0NTY3ODkrLw==",
                "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/",
            ),
            (
                "QUJDREVGR0hJSkt\tMTU5PUFFSU1RVVld\nYWVphYmNkZ\rWZnaGlqa2xt   bm9wcXJzdHV2d3h5ejAxMjM0NTY3ODkrLw==",
                "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/",
            ),
        ];
        for (arg, expected) in tests {
            let param = Some(arg.to_string().into_bytes());
            let expected_output = Some(expected.to_string().into_bytes());
            let output = RpnFnScalarEvaluator::new()
                .push_param(param)
                .evaluate::<Bytes>(ScalarFuncSig::FromBase64)
                .unwrap();
            assert_eq!(output, expected_output);
        }

        let invalid_base64_output = RpnFnScalarEvaluator::new()
            .push_param(Some(b"src".to_vec()))
            .evaluate(ScalarFuncSig::FromBase64)
            .unwrap();
        assert_eq!(invalid_base64_output, Some(b"".to_vec()));
    }

    #[test]
    fn test_quote() {
        let cases: Vec<(&str, &str)> = vec![
            (r"Don\'t!", r"'Don\\\'t!'"),
            (r"Don't", r"'Don\'t'"),
            (r"\'", r"'\\\''"),
            (r#"\""#, r#"'\\"'"#),
            (r"萌萌哒(๑•ᴗ•๑)😊", r"'萌萌哒(๑•ᴗ•๑)😊'"),
            (r"㍿㌍㍑㌫", r"'㍿㌍㍑㌫'"),
            (str::from_utf8(&[26, 0]).unwrap(), r"'\Z\0'"),
        ];

        for (input, expect) in cases {
            let input = Bytes::from(input);
            let expect_vec = Bytes::from(expect);
            let got = quote(Some(&input)).unwrap();
            assert_eq!(got, Some(expect_vec))
        }

        // check for null
        let got = quote(None).unwrap();
        assert_eq!(got, Some(Bytes::from("NULL")))
    }

    #[test]
    fn test_repeat() {
        let cases = vec![
            ("hello, world!", -1, ""),
            ("hello, world!", 0, ""),
            ("hello, world!", 1, "hello, world!"),
            (
                "hello, world!",
                3,
                "hello, world!hello, world!hello, world!",
            ),
            ("你好世界", 3, "你好世界你好世界你好世界"),
            ("こんにちは", 2, "こんにちはこんにちは"),
            ("\x2f\x35", 5, "\x2f\x35\x2f\x35\x2f\x35\x2f\x35\x2f\x35"),
        ];

        for (input, cnt, expect) in cases {
            let input = Bytes::from(input);
            let expected_output = Bytes::from(expect);
            let output = RpnFnScalarEvaluator::new()
                .push_param(Some(input))
                .push_param(Some(cnt))
                .evaluate::<Bytes>(ScalarFuncSig::Repeat)
                .unwrap();
            assert_eq!(output, Some(expected_output));
        }

        let null_string: Option<Bytes> = None;
        let null_cnt: Option<Int> = None;

        // test NULL case
        let output = RpnFnScalarEvaluator::new()
            .push_param(null_string.clone())
            .push_param(Some(42))
            .evaluate::<Bytes>(ScalarFuncSig::Repeat)
            .unwrap();
        assert_eq!(output, None);

        let output = RpnFnScalarEvaluator::new()
            .push_param(Some(b"hi".to_vec()))
            .push_param(null_cnt)
            .evaluate::<Bytes>(ScalarFuncSig::Repeat)
            .unwrap();
        assert_eq!(output, None);

        let output = RpnFnScalarEvaluator::new()
            .push_param(null_string)
            .push_param(null_cnt)
            .evaluate::<Bytes>(ScalarFuncSig::Repeat)
            .unwrap();
        assert_eq!(output, None);
    }
}
