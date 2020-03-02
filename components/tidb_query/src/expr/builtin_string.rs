// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use base64;
use std::borrow::Cow;
use std::cmp::Ordering;
use std::collections::VecDeque;
use std::i64;
use std::iter;

use hex::{self, FromHex};

use tidb_query_datatype;
use tidb_query_datatype::prelude::*;
use tikv_util::try_opt_or;

use super::{EvalContext, Result, ScalarFunc};
use crate::codec::{datum, Datum};
use safemem;

const SPACE: u8 = 0o40u8;

// see https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_to-base64
// mysql base64 doc: A newline is added after each 76 characters of encoded output
const BASE64_LINE_WRAP_LENGTH: usize = 76;

// mysql base64 doc: Each 3 bytes of the input data are encoded using 4 characters.
const BASE64_INPUT_CHUNK_LENGTH: usize = 3;
const BASE64_ENCODED_CHUNK_LENGTH: usize = 4;
const BASE64_LINE_WRAP: u8 = b'\n';

enum TrimDirection {
    Both = 1,
    Leading,
    Trailing,
}

impl TrimDirection {
    fn from_i64(i: i64) -> Option<Self> {
        match i {
            1 => Some(TrimDirection::Both),
            2 => Some(TrimDirection::Leading),
            3 => Some(TrimDirection::Trailing),
            _ => None,
        }
    }
}

impl ScalarFunc {
    #[inline]
    pub fn length(&self, ctx: &mut EvalContext, row: &[Datum]) -> Result<Option<i64>> {
        let input = try_opt!(self.children[0].eval_string(ctx, row));
        Ok(Some(input.len() as i64))
    }

    #[inline]
    fn find_str(text: &str, pattern: &str) -> Option<usize> {
        twoway::find_str(text, pattern).map(|i| text[..i].chars().count())
    }

    #[inline]
    pub fn locate_2_args_utf8(&self, ctx: &mut EvalContext, row: &[Datum]) -> Result<Option<i64>> {
        let substr = try_opt!(self.children[0].eval_string_and_decode(ctx, row));
        let s = try_opt!(self.children[1].eval_string_and_decode(ctx, row));
        Ok(Self::find_str(&s.to_lowercase(), &substr.to_lowercase())
            .map(|i| 1 + i as i64)
            .or(Some(0)))
    }

    #[inline]
    pub fn locate_3_args_utf8(&self, ctx: &mut EvalContext, row: &[Datum]) -> Result<Option<i64>> {
        let substr = try_opt!(self.children[0].eval_string_and_decode(ctx, row));
        let s = try_opt!(self.children[1].eval_string_and_decode(ctx, row));
        let pos = try_opt!(self.children[2].eval_int(ctx, row));
        if pos < 1 {
            return Ok(Some(0));
        }
        Ok(s.char_indices()
            .map(|(i, _)| i)
            .chain(iter::once(s.len()))
            .nth(pos as usize - 1)
            .map(|offset| {
                Self::find_str(&s[offset..].to_lowercase(), &substr.to_lowercase())
                    .map(|i| i as i64 + pos)
                    .unwrap_or(0)
            })
            .or(Some(0)))
    }

    #[inline]
    pub fn locate_2_args(&self, ctx: &mut EvalContext, row: &[Datum]) -> Result<Option<i64>> {
        let substr = try_opt!(self.children[0].eval_string(ctx, row));
        let s = try_opt!(self.children[1].eval_string(ctx, row));
        Ok(twoway::find_bytes(&s, &substr)
            .map(|i| 1 + i as i64)
            .or(Some(0)))
    }

    #[inline]
    pub fn locate_3_args(&self, ctx: &mut EvalContext, row: &[Datum]) -> Result<Option<i64>> {
        let substr = try_opt!(self.children[0].eval_string(ctx, row));
        let s = try_opt!(self.children[1].eval_string(ctx, row));
        let pos = try_opt!(self.children[2].eval_int(ctx, row));

        if pos < 1 || pos as usize > s.len() + 1 {
            return Ok(Some(0));
        }
        Ok(twoway::find_bytes(&s[pos as usize - 1..], &substr)
            .map(|i| pos + i as i64)
            .or(Some(0)))
    }

    #[inline]
    pub fn bit_length(&self, ctx: &mut EvalContext, row: &[Datum]) -> Result<Option<i64>> {
        let input = try_opt!(self.children[0].eval_string(ctx, row));
        Ok(Some(input.len() as i64 * 8))
    }

    #[inline]
    pub fn ascii(&self, ctx: &mut EvalContext, row: &[Datum]) -> Result<Option<i64>> {
        let input = try_opt!(self.children[0].eval_string(ctx, row));
        if input.len() == 0 {
            Ok(Some(0))
        } else {
            Ok(Some(i64::from(input[0])))
        }
    }

    #[inline]
    pub fn char_length_utf8(&self, ctx: &mut EvalContext, row: &[Datum]) -> Result<Option<i64>> {
        let input = try_opt!(self.children[0].eval_string_and_decode(ctx, row));
        Ok(Some(input.chars().count() as i64))
    }

    #[inline]
    pub fn char_length(&self, ctx: &mut EvalContext, row: &[Datum]) -> Result<Option<i64>> {
        let input = try_opt!(self.children[0].eval_string(ctx, row));
        Ok(Some(input.len() as i64))
    }

    #[inline]
    pub fn bin<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, [u8]>>> {
        let i = try_opt!(self.children[0].eval_int(ctx, row));
        Ok(Some(Cow::Owned(format!("{:b}", i).into_bytes())))
    }

    #[inline]
    pub fn oct_int<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, [u8]>>> {
        let i = try_opt!(self.children[0].eval_int(ctx, row));
        Ok(Some(Cow::Owned(format!("{:o}", i).into_bytes())))
    }

    #[inline]
    pub fn concat<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, [u8]>>> {
        let mut output: Vec<u8> = Vec::new();
        for expr in &self.children {
            let input = try_opt!(expr.eval_string(ctx, row));
            output.extend_from_slice(&input);
        }
        Ok(Some(Cow::Owned(output)))
    }

    #[inline]
    pub fn concat_ws<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, [u8]>>> {
        use crate::expr::Expression;
        fn collect_valid_strs<'a, 'b: 'a>(
            exps: &'b [Expression],
            ctx: &mut EvalContext,
            row: &'a [Datum],
        ) -> Result<Vec<Cow<'a, [u8]>>> {
            let mut result = Vec::new();
            for exp in exps {
                let x = exp.eval_string(ctx, row)?;
                if let Some(s) = x {
                    result.push(s);
                }
            }
            Ok(result)
        }

        let sep = try_opt!(self.children[0].eval_string(ctx, row));
        let strs = collect_valid_strs(&self.children[1..], ctx, row)?;
        let output = strs.as_slice().join(sep.as_ref());
        Ok(Some(Cow::Owned(output)))
    }

    #[inline]
    pub fn ltrim<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, [u8]>>> {
        let val = try_opt!(self.children[0].eval_string(ctx, row));
        let pos = val.iter().position(|&x| x != SPACE);
        if let Some(i) = pos {
            match val {
                Cow::Borrowed(val) => Ok(Some(Cow::Borrowed(&val[i..]))),
                Cow::Owned(val) => Ok(Some(Cow::Owned(val[i..].to_owned()))),
            }
        } else {
            Ok(Some(Cow::Borrowed(b"")))
        }
    }

    #[inline]
    pub fn rtrim<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, [u8]>>> {
        let val = try_opt!(self.children[0].eval_string(ctx, row));
        let pos = val.iter().rev().position(|&x| x != SPACE);
        if let Some(i) = pos {
            match val {
                Cow::Borrowed(val) => Ok(Some(Cow::Borrowed(&val[..val.len() - i]))),
                Cow::Owned(val) => Ok(Some(Cow::Owned(val[..val.len() - i].to_owned()))),
            }
        } else {
            Ok(Some(Cow::Borrowed(b"")))
        }
    }

    pub fn replace<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        raw: &'a [Datum],
    ) -> Result<Option<Cow<'a, [u8]>>> {
        let s = try_opt!(self.children[0].eval_string_and_decode(ctx, raw));
        let from_str = try_opt!(self.children[1].eval_string_and_decode(ctx, raw));
        let to_str = try_opt!(self.children[2].eval_string_and_decode(ctx, raw));
        if from_str.is_empty() {
            return match s {
                Cow::Borrowed(v) => Ok(Some(Cow::Borrowed(v.as_bytes()))),
                Cow::Owned(v) => Ok(Some(Cow::Owned(v.into_bytes()))),
            };
        }
        Ok(Some(Cow::Owned(
            s.replace(from_str.as_ref(), to_str.as_ref()).into_bytes(),
        )))
    }

    pub fn left_utf8<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, [u8]>>> {
        let s = try_opt!(self.children[0].eval_string_and_decode(ctx, row));
        let i = try_opt!(self.children[1].eval_int(ctx, row));
        let (i, length_positive) = i64_to_usize(i, self.children[1].is_unsigned());
        if !length_positive || i == 0 {
            return Ok(Some(Cow::Borrowed(b"")));
        }
        if s.chars().count() > i {
            let t = s.chars();
            return Ok(Some(Cow::Owned(t.take(i).collect::<String>().into_bytes())));
        }
        Ok(Some(Cow::Owned(s.to_string().into_bytes())))
    }

    #[inline]
    pub fn reverse_utf8<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, [u8]>>> {
        let s = try_opt!(self.children[0].eval_string_and_decode(ctx, row));
        Ok(Some(Cow::Owned(
            s.chars().rev().collect::<String>().into_bytes(),
        )))
    }

    #[inline]
    pub fn reverse<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, [u8]>>> {
        let mut s = try_opt!(self.children[0].eval_string(ctx, row));
        s.to_mut().reverse();
        Ok(Some(s))
    }

    pub fn right_utf8<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, [u8]>>> {
        let s = try_opt!(self.children[0].eval_string_and_decode(ctx, row));
        let i = try_opt!(self.children[1].eval_int(ctx, row));
        let (i, length_positive) = i64_to_usize(i, self.children[1].is_unsigned());
        if !length_positive || i == 0 {
            return Ok(Some(Cow::Borrowed(b"")));
        }
        let len = s.chars().count();
        if len > i {
            let idx = s
                .char_indices()
                .nth(len - i)
                .map(|(idx, _)| idx)
                .unwrap_or_else(|| s.len());
            return Ok(Some(Cow::Owned(s[idx..].to_string().into_bytes())));
        }
        Ok(Some(Cow::Owned(s.to_string().into_bytes())))
    }

    #[inline]
    pub fn upper_utf8<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, [u8]>>> {
        let s = try_opt!(self.children[0].eval_string_and_decode(ctx, row));
        Ok(Some(Cow::Owned(s.to_uppercase().into_bytes())))
    }

    #[inline]
    pub fn upper<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, [u8]>>> {
        let s = try_opt!(self.children[0].eval_string(ctx, row));
        Ok(Some(s))
    }

    #[inline]
    pub fn lower<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, [u8]>>> {
        if self.children[0].field_type().is_binary_string_like() {
            let s = try_opt!(self.children[0].eval_string(ctx, row));
            return Ok(Some(s));
        }
        let s = try_opt!(self.children[0].eval_string_and_decode(ctx, row));
        Ok(Some(Cow::Owned(s.to_lowercase().into_bytes())))
    }

    #[inline]
    pub fn hex_int_arg<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, [u8]>>> {
        let i = try_opt!(self.children[0].eval_int(ctx, row));
        Ok(Some(Cow::Owned(format!("{:X}", i).into_bytes())))
    }

    #[inline]
    pub fn hex_str_arg<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, [u8]>>> {
        let s = try_opt!(self.children[0].eval_string(ctx, row));
        Ok(Some(Cow::Owned(hex::encode_upper(s.to_vec()).into_bytes())))
    }

    #[inline]
    pub fn un_hex<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, [u8]>>> {
        let s = try_opt!(self.children[0].eval_string(ctx, row));
        let hex_string = if s.len() % 2 == 1 {
            // Add a '0' to the front, if the length is not the multiple of 2
            let mut vec = vec![b'0'];
            vec.extend_from_slice(&s);
            vec
        } else {
            s.to_vec()
        };
        let result = Vec::from_hex(hex_string);
        result.map(|t| Some(Cow::Owned(t))).or(Ok(None))
    }

    #[inline]
    pub fn elt<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, [u8]>>> {
        let i = try_opt!(self.children[0].eval_int(ctx, row));
        if i <= 0 || i + 1 > self.children.len() as i64 {
            return Ok(None);
        }
        self.children[i as usize].eval_string(ctx, row)
    }

    #[inline]
    pub fn field_int(&self, ctx: &mut EvalContext, row: &[Datum]) -> Result<Option<i64>> {
        // As per the MySQL doc, if the first argument is NULL, this function always returns 0.
        let val = try_opt_or!(self.children[0].eval_int(ctx, row), Some(0));

        for (i, exp) in self.children.iter().skip(1).enumerate() {
            match exp.eval_int(ctx, row) {
                Err(e) => return Err(e),
                Ok(Some(v)) if v == val => return Ok(Some(i as i64 + 1)),
                _ => (),
            }
        }
        Ok(Some(0))
    }

    #[inline]
    pub fn field_real(&self, ctx: &mut EvalContext, row: &[Datum]) -> Result<Option<i64>> {
        let val = try_opt_or!(self.children[0].eval_real(ctx, row), Some(0));

        for (i, exp) in self.children.iter().skip(1).enumerate() {
            match exp.eval_real(ctx, row) {
                Err(e) => return Err(e),
                Ok(Some(v)) => match datum::cmp_f64(v, val) {
                    Ok(Ordering::Equal) => return Ok(Some(i as i64 + 1)),
                    Err(e) => return Err(e),
                    _ => (),
                },
                _ => (),
            }
        }
        Ok(Some(0))
    }

    #[inline]
    pub fn field_string(&self, ctx: &mut EvalContext, row: &[Datum]) -> Result<Option<i64>> {
        let val = try_opt_or!(self.children[0].eval_string(ctx, row), Some(0));

        for (i, exp) in self.children.iter().skip(1).enumerate() {
            match exp.eval_string(ctx, row) {
                Err(e) => return Err(e),
                Ok(Some(ref v)) if *v == val => return Ok(Some(i as i64 + 1)),
                _ => (),
            }
        }
        Ok(Some(0))
    }

    #[inline]
    pub fn trim_1_arg<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, [u8]>>> {
        let s = try_opt!(self.children[0].eval_string_and_decode(ctx, row));
        trim(&s, " ", TrimDirection::Both)
    }

    #[inline]
    pub fn trim_2_args<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, [u8]>>> {
        let s = try_opt!(self.children[0].eval_string_and_decode(ctx, row));
        let pat = try_opt!(self.children[1].eval_string_and_decode(ctx, row));
        trim(&s, &pat, TrimDirection::Both)
    }

    #[inline]
    pub fn trim_3_args<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, [u8]>>> {
        let s = try_opt!(self.children[0].eval_string_and_decode(ctx, row));
        let pat = try_opt!(self.children[1].eval_string_and_decode(ctx, row));
        let direction = try_opt!(self.children[2].eval_int(ctx, row));
        match TrimDirection::from_i64(direction) {
            Some(d) => trim(&s, &pat, d),
            _ => Err(box_err!("invalid direction value: {}", direction)),
        }
    }

    #[inline]
    pub fn to_base64<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, [u8]>>> {
        let s = try_opt!(self.children[0].eval_string(ctx, row));

        if self.field_type.get_flen() == -1
            || self.field_type.get_flen() > tidb_query_datatype::MAX_BLOB_WIDTH
        {
            return Ok(Some(Cow::Borrowed(b"")));
        }

        if let Some(size) = encoded_size(s.len()) {
            let mut buf = vec![0; size];
            let len_without_wrap =
                base64::encode_config_slice(s.as_ref(), base64::STANDARD, &mut buf);
            line_wrap(&mut buf, len_without_wrap);
            Ok(Some(Cow::Owned(buf)))
        } else {
            Ok(Some(Cow::Borrowed(b"")))
        }
    }

    #[allow(clippy::wrong_self_convention)]
    #[inline]
    pub fn from_base64<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, [u8]>>> {
        let input = try_opt!(self.children[0].eval_string(ctx, row));

        let input_copy = strip_whitespace(&input);
        let will_overflow = input_copy
            .len()
            .checked_mul(BASE64_INPUT_CHUNK_LENGTH)
            .is_none();
        // mysql will return "" when the input is incorrectly padded
        let invalid_padding = input_copy.len() % BASE64_ENCODED_CHUNK_LENGTH != 0;
        if will_overflow || invalid_padding {
            return Ok(Some(Cow::Borrowed(b"")));
        }

        match base64::decode_config(&input_copy, base64::STANDARD) {
            Ok(r) => Ok(Some(Cow::Owned(r))),
            _ => Ok(None),
        }
    }

    #[inline]
    pub fn substring_index<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, [u8]>>> {
        let s = try_opt!(self.children[0].eval_string_and_decode(ctx, row));
        let delim = try_opt!(self.children[1].eval_string_and_decode(ctx, row));
        let count = try_opt!(self.children[2].eval_int(ctx, row));
        if delim.is_empty() || count == 0 {
            return Ok(Some(Cow::Borrowed(b"")));
        }

        let (count, is_positive) = i64_to_usize(count, self.children[2].is_unsigned());

        let r = if is_positive {
            substring_index_positive(&s, delim.as_ref(), count)
        } else {
            substring_index_negative(&s, delim.as_ref(), count)
        };
        Ok(Some(Cow::Owned(r.into_bytes())))
    }

    #[inline]
    pub fn substring_2_args_utf8<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, [u8]>>> {
        let s = try_opt!(self.children[0].eval_string_and_decode(ctx, row));
        let pos = try_opt!(self.children[1].eval_int(ctx, row));
        if pos == 0 {
            return Ok(Some(Cow::Borrowed(b"")));
        }

        // we need to check the unsigned_flag , otherwise a input larger than
        // i64::max_value() will overflow to a negative number
        let (pos, positive_search) = i64_to_usize(pos, self.children[1].is_unsigned());

        let start = if positive_search {
            s.char_indices()
                .enumerate()
                .find(|(cnt, _)| cnt + 1 == pos)
                .map(|(_, (i, _))| i)
        } else {
            s.char_indices()
                .rev()
                .enumerate()
                .find(|(cnt, _)| cnt + 1 == pos)
                .map(|(_, (i, _))| i)
        };

        if let Some(start) = start {
            Ok(Some(Cow::Owned(s[start..].as_bytes().to_vec())))
        } else {
            Ok(Some(Cow::Borrowed(b"")))
        }
    }

    #[inline]
    pub fn substring_3_args_utf8<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, [u8]>>> {
        let s = try_opt!(self.children[0].eval_string_and_decode(ctx, row));
        let pos = try_opt!(self.children[1].eval_int(ctx, row));
        let len = try_opt!(self.children[2].eval_int(ctx, row));

        let (pos, positive_search) = i64_to_usize(pos, self.children[1].is_unsigned());
        let (len, len_positive) = i64_to_usize(len, self.children[2].is_unsigned());

        if pos == 0 || len == 0 || !len_positive {
            return Ok(Some(Cow::Borrowed(b"")));
        }

        let mut start = None;
        let end = if positive_search {
            s.char_indices()
                .enumerate()
                .find(|(cnt, (i, _))| {
                    if cnt + 1 == pos {
                        start = Some(*i);
                    }
                    cnt + 1 > len && (cnt + 1 - len) >= pos
                })
                .map(|(_, (i, _))| i)
                .unwrap_or_else(|| s.len())
        } else {
            let mut positions = VecDeque::with_capacity(len.min(s.len()));
            positions.push_back(s.len());
            start = s
                .char_indices()
                .rev()
                .enumerate()
                .find(|(cnt, (i, _))| {
                    if cnt + 1 != pos {
                        if positions.len() == len {
                            positions.pop_front();
                        }
                        positions.push_back(*i);
                        false
                    } else {
                        true
                    }
                })
                .map(|(_, (i, _))| i);
            positions[0]
        };
        if let Some(start) = start {
            Ok(Some(Cow::Owned(s[start..end].as_bytes().to_vec())))
        } else {
            Ok(Some(Cow::Borrowed(b"")))
        }
    }

    #[inline]
    pub fn substring_2_args<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, [u8]>>> {
        self.substring(ctx, row, false)
    }

    #[inline]
    pub fn substring_3_args<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, [u8]>>> {
        self.substring(ctx, row, true)
    }

    #[inline]
    fn substring<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
        with_len: bool,
    ) -> Result<Option<Cow<'a, [u8]>>> {
        let s = try_opt!(self.children[0].eval_string(ctx, row));
        let pos = try_opt!(self.children[1].eval_int(ctx, row));
        let (len, len_positive) = if with_len {
            let len = try_opt!(self.children[2].eval_int(ctx, row));
            i64_to_usize(len, self.children[2].is_unsigned())
        } else {
            (s.len(), true)
        };

        if pos == 0 || len == 0 || !len_positive {
            return Ok(Some(Cow::Borrowed(b"")));
        }

        let (pos, positive_search) = i64_to_usize(pos, self.children[1].is_unsigned());

        let start = if positive_search {
            (pos - 1).min(s.len())
        } else {
            s.len().checked_sub(pos).unwrap_or_else(|| s.len())
        };

        let end = start.saturating_add(len).min(s.len());
        Ok(Some(Cow::Owned(s[start..end].to_vec())))
    }

    #[inline]
    pub fn space<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, [u8]>>> {
        let len = try_opt!(self.children[0].eval_int(ctx, row));
        let unsigned = self.children[0].is_unsigned();
        let len = if unsigned {
            len as u64 as usize
        } else if len <= 0 {
            return Ok(Some(Cow::Borrowed(b"")));
        } else {
            len as usize
        };

        if len > tidb_query_datatype::MAX_BLOB_WIDTH as usize {
            return Ok(None);
        }

        Ok(Some(Cow::Owned(vec![SPACE; len])))
    }

    #[inline]
    pub fn strcmp(&self, ctx: &mut EvalContext, row: &[Datum]) -> Result<Option<i64>> {
        use std::cmp::Ordering::*;
        let left = try_opt!(self.children[0].eval_string(ctx, row));
        let right = try_opt!(self.children[1].eval_string(ctx, row));
        match left.cmp(&right) {
            Less => Ok(Some(-1)),
            Equal => Ok(Some(0)),
            Greater => Ok(Some(1)),
        }
    }

    #[inline]
    pub fn rpad_utf8<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, [u8]>>> {
        let input = try_opt!(self.children[0].eval_string_and_decode(ctx, row));
        let target_len = try_opt!(self.children[1].eval_int(ctx, row));
        let pad = try_opt!(self.children[2].eval_string_and_decode(ctx, row));
        let input_len = input.chars().count();

        match validate_target_len_for_pad(
            self.children[1].is_unsigned(),
            target_len,
            input_len,
            4,
            pad.is_empty(),
        ) {
            None => Ok(None),
            Some(0) => Ok(Some(Cow::Borrowed(b""))),
            Some(target_len) => {
                let r = input
                    .chars()
                    .chain(pad.chars().cycle())
                    .take(target_len)
                    .collect::<String>();
                Ok(Some(Cow::Owned(r.into_bytes())))
            }
        }
    }

    #[inline]
    pub fn rpad<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, [u8]>>> {
        let input = try_opt!(self.children[0].eval_string(ctx, row));
        let target_len = try_opt!(self.children[1].eval_int(ctx, row));
        let pad = try_opt!(self.children[2].eval_string(ctx, row));

        match validate_target_len_for_pad(
            self.children[1].is_unsigned(),
            target_len,
            input.len(),
            1,
            pad.is_empty(),
        ) {
            None => Ok(None),
            Some(0) => Ok(Some(Cow::Borrowed(b""))),
            Some(target_len) => {
                let r = input
                    .iter()
                    .chain(pad.iter().cycle())
                    .cloned()
                    .take(target_len)
                    .collect::<Vec<_>>();
                Ok(Some(Cow::Owned(r)))
            }
        }
    }

    #[inline]
    pub fn lpad_utf8<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, [u8]>>> {
        let input = try_opt!(self.children[0].eval_string_and_decode(ctx, row));
        let target_len = try_opt!(self.children[1].eval_int(ctx, row));
        let pad = try_opt!(self.children[2].eval_string_and_decode(ctx, row));
        let input_len = input.chars().count();

        match validate_target_len_for_pad(
            self.children[1].is_unsigned(),
            target_len,
            input_len,
            4,
            pad.is_empty(),
        ) {
            None => Ok(None),
            Some(0) => Ok(Some(Cow::Borrowed(b""))),
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
                Ok(Some(Cow::Owned(r.into_bytes())))
            }
        }
    }

    #[inline]
    pub fn lpad<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, [u8]>>> {
        let input = try_opt!(self.children[0].eval_string(ctx, row));
        let target_len = try_opt!(self.children[1].eval_int(ctx, row));
        let pad = try_opt!(self.children[2].eval_string(ctx, row));

        match validate_target_len_for_pad(
            self.children[1].is_unsigned(),
            target_len,
            input.len(),
            1,
            pad.is_empty(),
        ) {
            None => Ok(None),
            Some(0) => Ok(Some(Cow::Borrowed(b""))),
            Some(target_len) => {
                let r = if let Some(remain) = target_len.checked_sub(input.len()) {
                    pad.iter()
                        .cycle()
                        .take(remain)
                        .chain(input.iter())
                        .cloned()
                        .collect::<Vec<_>>()
                } else {
                    input[..target_len].to_vec()
                };
                Ok(Some(Cow::Owned(r)))
            }
        }
    }

    #[inline]
    pub fn left<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, [u8]>>> {
        let input = try_opt!(self.children[0].eval_string(ctx, row));
        let length = try_opt!(self.children[1].eval_int(ctx, row));
        let (length, length_positive) = i64_to_usize(length, self.children[1].is_unsigned());
        if length_positive {
            let end = length.min(input.len());
            Ok(Some(Cow::Owned(input[..end].to_vec())))
        } else {
            Ok(Some(Cow::Borrowed(b"")))
        }
    }

    #[inline]
    pub fn right<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, [u8]>>> {
        let input = try_opt!(self.children[0].eval_string(ctx, row));
        let length = try_opt!(self.children[1].eval_int(ctx, row));
        let (length, length_positive) = i64_to_usize(length, self.children[1].is_unsigned());
        if length_positive {
            let start = input.len().saturating_sub(length);
            Ok(Some(Cow::Owned(input[start..].to_vec())))
        } else {
            Ok(Some(Cow::Borrowed(b"")))
        }
    }

    #[inline]
    pub fn instr_utf8<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<i64>> {
        let s = try_opt!(self.children[0].eval_string_and_decode(ctx, row));
        let substr = try_opt!(self.children[1].eval_string_and_decode(ctx, row));
        Ok(Self::find_str(&s.to_lowercase(), &substr.to_lowercase())
            .map(|i| 1 + i as i64)
            .or(Some(0)))
    }

    #[inline]
    pub fn instr<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<i64>> {
        let s = try_opt!(self.children[0].eval_string(ctx, row));
        let substr = try_opt!(self.children[1].eval_string(ctx, row));
        Ok(twoway::find_bytes(&s, &substr)
            .map(|i| 1 + i as i64)
            .or(Some(0)))
    }

    // See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_quote
    pub fn quote<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, [u8]>>> {
        let s = try_opt_or!(
            self.children[0].eval_string(ctx, row),
            Some(Cow::Borrowed(b"NULL"))
        );
        let mut result = Vec::<u8>::with_capacity(s.len() * 2 + 2);
        result.push(b'\'');
        for byte in s.iter() {
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
        Ok(Some(Cow::Owned(result)))
    }

    // see https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_ord
    #[inline]
    pub fn ord<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<i64>> {
        let s = try_opt!(self.children[0].eval_string(ctx, row));
        let size = bstr::decode_utf8(&s).1;
        let bytes = &s[..size];

        let mut result = 0;
        let mut factor = 1;
        for b in bytes.iter().rev() {
            result += i64::from(*b) * factor;
            factor *= 256;
        }
        Ok(Some(result))
    }

    #[inline]
    pub fn find_in_set<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<i64>> {
        let str_list = try_opt!(self.children[1].eval_string_and_decode(ctx, row));
        if str_list.is_empty() {
            return Ok(Some(0));
        }
        let s = try_opt!(self.children[0].eval_string_and_decode(ctx, row));
        Ok(str_list
            .split(',')
            .position(|str_in_set| str_in_set == s)
            .map(|p| p as i64 + 1)
            .or(Some(0)))
    }
}

// when target_len is 0, return Some(0), means the pad function should return empty string
// currently there are three conditions it return None, which means pad function should return Null
//   1. target_len is negative
//   2. target_len of type in byte is larger then MAX_BLOB_WIDTH
//   3. target_len is greater than length of input string, *and* pad string is empty
// otherwise return Some(target_len)
#[inline]
fn validate_target_len_for_pad(
    len_unsigned: bool,
    target_len: i64,
    input_len: usize,
    size_of_type: usize,
    pad_empty: bool,
) -> Option<usize> {
    if target_len == 0 {
        return Some(0);
    }
    let (target_len, target_len_positive) = i64_to_usize(target_len, len_unsigned);
    if !target_len_positive
        || target_len.saturating_mul(size_of_type) > tidb_query_datatype::MAX_BLOB_WIDTH as usize
        || (pad_empty && input_len < target_len)
    {
        return None;
    }
    Some(target_len)
}

// Returns (isize, is_positive): convert an i64 to usize, and whether the input is positive
//
// # Examples
// ```
// assert_eq!(i64_to_usize(1_i64, false), (1_usize, true));
// assert_eq!(i64_to_usize(1_i64, false), (1_usize, true));
// assert_eq!(i64_to_usize(-1_i64, false), (1_usize, false));
// assert_eq!(i64_to_usize(u64::max_value() as i64, true), (u64::max_value() as usize, true));
// assert_eq!(i64_to_usize(u64::max_value() as i64, false), (1_usize, false));
// ```
#[inline]
fn i64_to_usize(i: i64, is_unsigned: bool) -> (usize, bool) {
    if is_unsigned {
        (i as u64 as usize, true)
    } else if i >= 0 {
        (i as usize, true)
    } else {
        let i = if i == i64::min_value() {
            i64::max_value() as usize + 1
        } else {
            -i as usize
        };
        (i, false)
    }
}

#[inline]
fn strip_whitespace(input: &[u8]) -> Vec<u8> {
    let mut input_copy = Vec::<u8>::with_capacity(input.len());
    input_copy.extend(input.iter().filter(|b| !b" \n\t\r\x0b\x0c".contains(b)));
    input_copy
}

#[inline]
fn encoded_size(len: usize) -> Option<usize> {
    if len == 0 {
        return Some(0);
    }
    // size_without_wrap = (len + (3 - 1)) / 3 * 4
    // size = size_without_wrap + (size_withou_wrap - 1) / 76
    len.checked_add(BASE64_INPUT_CHUNK_LENGTH - 1)
        .and_then(|r| r.checked_div(BASE64_INPUT_CHUNK_LENGTH))
        .and_then(|r| r.checked_mul(BASE64_ENCODED_CHUNK_LENGTH))
        .and_then(|r| r.checked_add((r - 1) / BASE64_LINE_WRAP_LENGTH))
}

// similar logic to crate `line-wrap`, since we had call `encoded_size` before,
// there is no need to use checked_xxx math operation like `line-wrap` does.
#[inline]
fn line_wrap(buf: &mut [u8], input_len: usize) {
    let line_len = BASE64_LINE_WRAP_LENGTH;
    if input_len <= line_len {
        return;
    }
    let last_line_len = if input_len % line_len == 0 {
        line_len
    } else {
        input_len % line_len
    };
    let lines_with_ending = (input_len - 1) / line_len;
    let line_with_ending_len = line_len + 1;
    let mut old_start = input_len - last_line_len;
    let mut new_start = buf.len() - last_line_len;
    safemem::copy_over(buf, old_start, new_start, last_line_len);
    for _ in 0..lines_with_ending {
        old_start -= line_len;
        new_start -= line_with_ending_len;
        safemem::copy_over(buf, old_start, new_start, line_len);
        buf[new_start + line_len] = BASE64_LINE_WRAP;
    }
}

#[inline]
fn substring_index_positive(s: &str, delim: &str, count: usize) -> String {
    let mut bg = 0;
    let mut cnt = 0;
    let mut last = 0;
    while cnt < count {
        if let Some(idx) = s[bg..].find(delim) {
            last = bg + idx;
            bg = last + delim.len();
            cnt += 1;
        } else {
            last = s.len();
            break;
        }
    }
    s[..last].to_string()
}

#[inline]
fn substring_index_negative(s: &str, delim: &str, count: usize) -> String {
    let mut bg = 0;
    let mut positions = VecDeque::with_capacity(count.min(128));
    positions.push_back(0);
    while let Some(idx) = s[bg..].find(delim) {
        bg = bg + idx + delim.len();
        if positions.len() == count {
            positions.pop_front();
        }
        positions.push_back(bg);
    }
    s[positions[0]..].to_string()
}

#[inline]
fn trim<'a>(s: &str, pat: &str, direction: TrimDirection) -> Result<Option<Cow<'a, [u8]>>> {
    let r = match direction {
        TrimDirection::Leading => s.trim_start_matches(pat),
        TrimDirection::Trailing => s.trim_end_matches(pat),
        _ => s.trim_start_matches(pat).trim_end_matches(pat),
    };
    Ok(Some(Cow::Owned(r.to_string().into_bytes())))
}

#[cfg(test)]
mod tests {
    use super::{encoded_size, TrimDirection};
    use crate::codec::mysql::charset::CHARSET_BIN;
    use std::{f64, i64, str};
    use tidb_query_datatype::{Collation, FieldTypeFlag, FieldTypeTp, MAX_BLOB_WIDTH};
    use tipb::{Expr, ScalarFuncSig};

    use crate::codec::Datum;
    use crate::expr::tests::{
        col_expr, datum_expr, eval_func, scalar_func_expr, string_datum_expr_with_tp,
    };
    use crate::expr::{EvalContext, Expression};

    #[test]
    fn test_length() {
        let cases = vec![
            ("", 0i64),
            ("你好", 6i64),
            ("TiKV", 4i64),
            ("あなたのことが好きです", 33i64),
            ("분산 데이터베이스", 25i64),
            ("россия в мире  кубок", 38i64),
            ("قاعدة البيانات", 27i64),
        ];

        let mut ctx = EvalContext::default();
        for (input_str, exp) in cases {
            let input = datum_expr(Datum::Bytes(input_str.as_bytes().to_vec()));
            let op = scalar_func_expr(ScalarFuncSig::Length, &[input]);
            let op = Expression::build(&mut ctx, op).unwrap();
            let got = op.eval(&mut ctx, &[]).unwrap();
            let exp = Datum::from(exp);
            assert_eq!(got, exp, "length('{:?}')", input_str);
        }

        // test NULL case
        let input = datum_expr(Datum::Null);
        let op = scalar_func_expr(ScalarFuncSig::Length, &[input]);
        let op = Expression::build(&mut ctx, op).unwrap();
        let got = op.eval(&mut ctx, &[]).unwrap();
        let exp = Datum::Null;
        assert_eq!(got, exp, "length(NULL)");
    }

    #[test]
    fn test_locate_2_args_utf8() {
        let cases = vec![
            ("bar", "foobarbar", 4i64),
            ("xbar", "foobar", 0i64),
            ("", "foobar", 1i64),
            ("foobar", "", 0i64),
            ("", "", 1i64),
            ("好世", "你好世界", 2i64),
            ("界面", "你好世界", 0i64),
            ("b", "中a英b文", 4i64),
            ("BaR", "foobArbar", 4i64),
        ];

        for (substr, s, exp) in cases {
            let substr = Datum::Bytes(substr.as_bytes().to_vec());
            let s = Datum::Bytes(s.as_bytes().to_vec());
            let got = eval_func(ScalarFuncSig::Locate2ArgsUtf8, &[substr, s]).unwrap();
            assert_eq!(got, Datum::I64(exp));
        }

        let null_cases = vec![
            (Datum::Null, Datum::Bytes(b"".to_vec()), Datum::Null),
            (Datum::Null, Datum::Bytes(b"foobar".to_vec()), Datum::Null),
            (Datum::Bytes(b"".to_vec()), Datum::Null, Datum::Null),
            (Datum::Bytes(b"bar".to_vec()), Datum::Null, Datum::Null),
            (Datum::Null, Datum::Null, Datum::Null),
        ];
        for (substr, s, exp) in null_cases {
            let got = eval_func(ScalarFuncSig::Locate2ArgsUtf8, &[substr, s]).unwrap();
            assert_eq!(got, exp);
        }
    }

    #[test]
    fn test_locate_2_args() {
        let cases = vec![
            ("", "foobArbar", 1),
            ("", "", 1),
            ("xxx", "", 0),
            ("BaR", "foobArbar", 0),
            ("bar", "foobArbar", 7),
            (
                "好世",
                "你好世界",
                1 + "你好世界".find("好世").unwrap() as i64,
            ),
        ];

        for (substr, s, exp) in cases {
            let substr = Datum::Bytes(substr.as_bytes().to_vec());
            let s = Datum::Bytes(s.as_bytes().to_vec());
            let got = eval_func(ScalarFuncSig::Locate2Args, &[substr, s]).unwrap();
            assert_eq!(got, Datum::I64(exp))
        }

        let null_cases = vec![
            (Datum::Null, Datum::Bytes(b"".to_vec()), Datum::Null),
            (Datum::Bytes(b"".to_vec()), Datum::Null, Datum::Null),
            (Datum::Null, Datum::Null, Datum::Null),
        ];

        for (substr, s, exp) in null_cases {
            let got = eval_func(ScalarFuncSig::Locate2Args, &[substr, s]).unwrap();
            assert_eq!(got, exp);
        }
    }

    #[test]
    fn test_locate_3_args_utf8() {
        let cases = vec![
            ("bar", "foobarbar", 5, 7),
            ("xbar", "foobar", 1, 0),
            ("", "foobar", 2, 2),
            ("foobar", "", 1, 0),
            ("", "", 2, 0),
            ("A", "大A写的A", 0, 0),
            ("A", "大A写的A", -1, 0),
            ("A", "大A写的A", 1, 2),
            ("A", "大A写的A", 2, 2),
            ("A", "大A写的A", 3, 5),
            ("bAr", "foobarBaR", 5, 7),
            ("", "aa", 2, 2),
            ("", "aa", 3, 3),
            ("", "aa", 4, 0),
        ];

        for (substr, s, pos, exp) in cases {
            let substr = Datum::Bytes(substr.as_bytes().to_vec());
            let s = Datum::Bytes(s.as_bytes().to_vec());
            let pos = Datum::I64(pos);
            let got = eval_func(ScalarFuncSig::Locate3ArgsUtf8, &[substr, s, pos]).unwrap();
            assert_eq!(got, Datum::I64(exp));
        }

        let null_cases = vec![
            (Datum::Null, Datum::Null, Datum::I64(1), Datum::Null),
            (
                Datum::Bytes(b"".to_vec()),
                Datum::Null,
                Datum::I64(1),
                Datum::Null,
            ),
            (
                Datum::Null,
                Datum::Bytes(b"".to_vec()),
                Datum::I64(1),
                Datum::Null,
            ),
            (
                Datum::Bytes(b"foo".to_vec()),
                Datum::Null,
                Datum::I64(-1),
                Datum::Null,
            ),
            (
                Datum::Null,
                Datum::Bytes(b"bar".to_vec()),
                Datum::I64(0),
                Datum::Null,
            ),
        ];

        for (substr, s, pos, exp) in null_cases {
            let got = eval_func(ScalarFuncSig::Locate3ArgsUtf8, &[substr, s, pos]).unwrap();
            assert_eq!(got, exp);
        }
    }

    #[test]
    fn test_locate_3_args() {
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
            let substr = Datum::Bytes(substr.as_bytes().to_vec());
            let s = Datum::Bytes(s.as_bytes().to_vec());
            let pos = Datum::I64(pos);
            let got = eval_func(ScalarFuncSig::Locate3Args, &[substr, s, pos]).unwrap();
            assert_eq!(got, Datum::I64(exp))
        }

        let null_cases = vec![
            (
                Datum::Null,
                Datum::Bytes(b"".to_vec()),
                Datum::I64(1),
                Datum::Null,
            ),
            (
                Datum::Bytes(b"".to_vec()),
                Datum::Null,
                Datum::Null,
                Datum::Null,
            ),
            (Datum::Null, Datum::Null, Datum::Null, Datum::Null),
        ];

        for (substr, s, pos, exp) in null_cases {
            let got = eval_func(ScalarFuncSig::Locate3Args, &[substr, s, pos]).unwrap();
            assert_eq!(got, exp);
        }
    }

    #[test]
    fn test_bit_length() {
        let cases = vec![
            ("", 0i64),
            ("你好", 48i64),
            ("TiKV", 32i64),
            ("あなたのことが好きです", 264i64),
            ("분산 데이터베이스", 200i64),
            ("россия в мире  кубок", 304i64),
            ("قاعدة البيانات", 216i64),
        ];

        let mut ctx = EvalContext::default();
        for (input_str, exp) in cases {
            let input = datum_expr(Datum::Bytes(input_str.as_bytes().to_vec()));
            let op = scalar_func_expr(ScalarFuncSig::BitLength, &[input]);
            let op = Expression::build(&mut ctx, op).unwrap();
            let got = op.eval(&mut ctx, &[]).unwrap();
            let exp = Datum::from(exp);
            assert_eq!(got, exp, "bit_length('{:?}')", input_str);
        }

        // test NULL case
        let input = datum_expr(Datum::Null);
        let op = scalar_func_expr(ScalarFuncSig::BitLength, &[input]);
        let op = Expression::build(&mut ctx, op).unwrap();
        let got = op.eval(&mut ctx, &[]).unwrap();
        let exp = Datum::Null;
        assert_eq!(got, exp, "bit_length(NULL)");
    }

    #[test]
    fn test_bin() {
        let cases = vec![
            (Datum::I64(10), Datum::Bytes(b"1010".to_vec())),
            (Datum::I64(0), Datum::Bytes(b"0".to_vec())),
            (Datum::I64(1), Datum::Bytes(b"1".to_vec())),
            (Datum::I64(365), Datum::Bytes(b"101101101".to_vec())),
            (Datum::I64(1024), Datum::Bytes(b"10000000000".to_vec())),
            (Datum::Null, Datum::Null),
            (
                Datum::I64(i64::max_value()),
                Datum::Bytes(
                    b"111111111111111111111111111111111111111111111111111111111111111".to_vec(),
                ),
            ),
            (
                Datum::I64(i64::min_value()),
                Datum::Bytes(
                    b"1000000000000000000000000000000000000000000000000000000000000000".to_vec(),
                ),
            ),
            (
                Datum::I64(-1),
                Datum::Bytes(
                    b"1111111111111111111111111111111111111111111111111111111111111111".to_vec(),
                ),
            ),
            (
                Datum::I64(-365),
                Datum::Bytes(
                    b"1111111111111111111111111111111111111111111111111111111010010011".to_vec(),
                ),
            ),
        ];

        let mut ctx = EvalContext::default();
        for (input, exp) in cases {
            let input = datum_expr(input);
            let op = scalar_func_expr(ScalarFuncSig::Bin, &[input]);
            let op = Expression::build(&mut ctx, op).unwrap();
            let got = op.eval(&mut ctx, &[]).unwrap();
            assert_eq!(got, exp);
        }
    }

    #[test]
    fn test_oct_int() {
        let cases = vec![
            (
                Datum::I64(-1),
                Datum::Bytes(b"1777777777777777777777".to_vec()),
            ),
            (Datum::I64(0), Datum::Bytes(b"0".to_vec())),
            (Datum::I64(1), Datum::Bytes(b"1".to_vec())),
            (Datum::I64(8), Datum::Bytes(b"10".to_vec())),
            (Datum::I64(12), Datum::Bytes(b"14".to_vec())),
            (Datum::I64(20), Datum::Bytes(b"24".to_vec())),
            (Datum::I64(100), Datum::Bytes(b"144".to_vec())),
            (Datum::I64(1024), Datum::Bytes(b"2000".to_vec())),
            (Datum::I64(2048), Datum::Bytes(b"4000".to_vec())),
            (
                Datum::I64(i64::MAX),
                Datum::Bytes(b"777777777777777777777".to_vec()),
            ),
            (
                Datum::I64(i64::MIN),
                Datum::Bytes(b"1000000000000000000000".to_vec()),
            ),
            (Datum::Null, Datum::Null),
        ];

        let mut ctx = EvalContext::default();
        for (input, exp) in cases {
            let input = datum_expr(input);
            let op = scalar_func_expr(ScalarFuncSig::OctInt, &[input]);
            let op = Expression::build(&mut ctx, op).unwrap();
            let got = op.eval(&mut ctx, &[]).unwrap();
            assert_eq!(got, exp);
        }
    }

    #[test]
    fn test_ltrim() {
        let cases = vec![
            ("   bar   ", "bar   "),
            ("   b   ar   ", "b   ar   "),
            ("bar", "bar"),
            ("    ", ""),
            ("\t  bar", "\t  bar"),
            ("\r  bar", "\r  bar"),
            ("\n  bar", "\n  bar"),
            ("  \tbar", "\tbar"),
            ("", ""),
            ("  你好", "你好"),
            ("  你  好", "你  好"),
            ("  분산 데이터베이스    ", "분산 데이터베이스    "),
            ("   あなたのことが好きです   ", "あなたのことが好きです   "),
        ];

        let mut ctx = EvalContext::default();
        for (input_str, exp) in cases {
            let input = datum_expr(Datum::Bytes(input_str.as_bytes().to_vec()));
            let op = scalar_func_expr(ScalarFuncSig::LTrim, &[input]);
            let op = Expression::build(&mut ctx, op).unwrap();
            let got = op.eval(&mut ctx, &[]).unwrap();
            let exp = Datum::Bytes(exp.as_bytes().to_vec());
            assert_eq!(got, exp, "ltrim('{:?}')", input_str);
        }

        // test NULL case
        let input = datum_expr(Datum::Null);
        let op = scalar_func_expr(ScalarFuncSig::LTrim, &[input]);
        let op = Expression::build(&mut ctx, op).unwrap();
        let got = op.eval(&mut ctx, &[]).unwrap();
        let exp = Datum::Null;
        assert_eq!(got, exp, "ltrim(NULL)");
    }

    #[test]
    fn test_rtrim() {
        let cases = vec![
            ("   bar   ", "   bar"),
            ("bar", "bar"),
            ("ba  r", "ba  r"),
            ("    ", ""),
            ("  bar\t  ", "  bar\t"),
            (" bar   \t", " bar   \t"),
            ("bar   \r", "bar   \r"),
            ("bar   \n", "bar   \n"),
            ("", ""),
            ("  你好  ", "  你好"),
            ("  你  好  ", "  你  好"),
            ("  분산 데이터베이스    ", "  분산 데이터베이스"),
            ("   あなたのことが好きです   ", "   あなたのことが好きです"),
        ];

        let mut ctx = EvalContext::default();
        for (input_str, exp) in cases {
            let input = datum_expr(Datum::Bytes(input_str.as_bytes().to_vec()));
            let op = scalar_func_expr(ScalarFuncSig::RTrim, &[input]);
            let op = Expression::build(&mut ctx, op).unwrap();
            let got = op.eval(&mut ctx, &[]).unwrap();
            let exp = Datum::Bytes(exp.as_bytes().to_vec());
            assert_eq!(got, exp, "rtrim('{:?}')", input_str);
        }

        // test NULL case
        let input = datum_expr(Datum::Null);
        let op = scalar_func_expr(ScalarFuncSig::RTrim, &[input]);
        let op = Expression::build(&mut ctx, op).unwrap();
        let got = op.eval(&mut ctx, &[]).unwrap();
        let exp = Datum::Null;
        assert_eq!(got, exp, "rtrim(NULL)");
    }

    #[test]
    fn test_reverse_utf8() {
        let cases = vec![
            (
                Datum::Bytes(b"hello".to_vec()),
                Datum::Bytes(b"olleh".to_vec()),
            ),
            (Datum::Bytes(b"".to_vec()), Datum::Bytes(b"".to_vec())),
            (
                Datum::Bytes("数据库".as_bytes().to_vec()),
                Datum::Bytes("库据数".as_bytes().to_vec()),
            ),
            (
                Datum::Bytes("忠犬ハチ公".as_bytes().to_vec()),
                Datum::Bytes("公チハ犬忠".as_bytes().to_vec()),
            ),
            (
                Datum::Bytes("あなたのことが好きです".as_bytes().to_vec()),
                Datum::Bytes("すでき好がとこのたなあ".as_bytes().to_vec()),
            ),
            (
                Datum::Bytes("Bayern München".as_bytes().to_vec()),
                Datum::Bytes("nehcnüM nreyaB".as_bytes().to_vec()),
            ),
            (
                Datum::Bytes("Η Αθηνά  ".as_bytes().to_vec()),
                Datum::Bytes("  άνηθΑ Η".as_bytes().to_vec()),
            ),
            (Datum::Null, Datum::Null),
        ];
        let mut ctx = EvalContext::default();
        for (arg, exp) in cases {
            let op = scalar_func_expr(ScalarFuncSig::ReverseUtf8, &[datum_expr(arg)]);
            let op = Expression::build(&mut ctx, op).unwrap();
            let got = op.eval(&mut ctx, &[]).unwrap();
            assert_eq!(got, exp);
        }
    }

    #[test]
    fn test_reverse() {
        let cases = vec![
            (
                Datum::Bytes(b"hello".to_vec()),
                Datum::Bytes(b"olleh".to_vec()),
            ),
            (Datum::Bytes(b"".to_vec()), Datum::Bytes(b"".to_vec())),
            (
                Datum::Bytes("中国".as_bytes().to_vec()),
                Datum::Bytes(vec![0o275u8, 0o233u8, 0o345u8, 0o255u8, 0o270u8, 0o344u8]),
            ),
            (Datum::Null, Datum::Null),
        ];
        let mut ctx = EvalContext::default();
        for (arg, exp) in cases {
            let input = string_datum_expr_with_tp(
                arg,
                FieldTypeTp::VarString,
                FieldTypeFlag::BINARY,
                -1,
                CHARSET_BIN.to_owned(),
                Collation::Binary,
            );
            let op = scalar_func_expr(ScalarFuncSig::Reverse, &[input]);
            let op = Expression::build(&mut ctx, op).unwrap();
            let got = op.eval(&mut ctx, &[]).unwrap();
            assert_eq!(got, exp);
        }
    }

    #[test]
    fn test_left_utf8() {
        let cases = vec![
            (
                Datum::Bytes(b"hello".to_vec()),
                Datum::I64(0),
                Datum::Bytes(b"".to_vec()),
            ),
            (
                Datum::Bytes(b"hello".to_vec()),
                Datum::I64(1),
                Datum::Bytes(b"h".to_vec()),
            ),
            (
                Datum::Bytes("数据库".as_bytes().to_vec()),
                Datum::I64(2),
                Datum::Bytes("数据".as_bytes().to_vec()),
            ),
            (
                Datum::Bytes("忠犬ハチ公".as_bytes().to_vec()),
                Datum::I64(3),
                Datum::Bytes("忠犬ハ".as_bytes().to_vec()),
            ),
            (
                Datum::Bytes("数据库".as_bytes().to_vec()),
                Datum::I64(100),
                Datum::Bytes("数据库".as_bytes().to_vec()),
            ),
            (
                Datum::Bytes("数据库".as_bytes().to_vec()),
                Datum::I64(-1),
                Datum::Bytes(b"".to_vec()),
            ),
            (
                Datum::Bytes("数据库".as_bytes().to_vec()),
                Datum::U64(u64::max_value()),
                Datum::Bytes("数据库".as_bytes().to_vec()),
            ),
            (Datum::Null, Datum::I64(-1), Datum::Null),
            (Datum::Bytes(b"hello".to_vec()), Datum::Null, Datum::Null),
        ];

        let mut ctx = EvalContext::default();
        for (arg1, arg2, exp) in cases {
            let arg1 = datum_expr(arg1);
            let arg2 = datum_expr(arg2);
            let op = scalar_func_expr(ScalarFuncSig::LeftUtf8, &[arg1, arg2]);
            let op = Expression::build(&mut ctx, op).unwrap();
            let got = op.eval(&mut ctx, &[]).unwrap();
            assert_eq!(got, exp);
        }
    }

    #[test]
    fn test_right_utf8() {
        let cases = vec![
            (
                Datum::Bytes(b"hello".to_vec()),
                Datum::I64(0),
                Datum::Bytes(b"".to_vec()),
            ),
            (
                Datum::Bytes(b"hello".to_vec()),
                Datum::I64(1),
                Datum::Bytes(b"o".to_vec()),
            ),
            (
                Datum::Bytes("数据库".as_bytes().to_vec()),
                Datum::I64(2),
                Datum::Bytes("据库".as_bytes().to_vec()),
            ),
            (
                Datum::Bytes("忠犬ハチ公".as_bytes().to_vec()),
                Datum::I64(3),
                Datum::Bytes("ハチ公".as_bytes().to_vec()),
            ),
            (
                Datum::Bytes("数据库".as_bytes().to_vec()),
                Datum::I64(100),
                Datum::Bytes("数据库".as_bytes().to_vec()),
            ),
            (
                Datum::Bytes("数据库".as_bytes().to_vec()),
                Datum::I64(-1),
                Datum::Bytes(b"".to_vec()),
            ),
            (
                Datum::Bytes("数据库".as_bytes().to_vec()),
                Datum::U64(u64::max_value()),
                Datum::Bytes("数据库".as_bytes().to_vec()),
            ),
            (Datum::Null, Datum::I64(-1), Datum::Null),
            (Datum::Bytes(b"hello".to_vec()), Datum::Null, Datum::Null),
        ];
        let mut ctx = EvalContext::default();
        for (arg1, arg2, exp) in cases {
            let arg1 = datum_expr(arg1);
            let arg2 = datum_expr(arg2);
            let op = scalar_func_expr(ScalarFuncSig::RightUtf8, &[arg1, arg2]);
            let op = Expression::build(&mut ctx, op).unwrap();
            let got = op.eval(&mut ctx, &[]).unwrap();
            assert_eq!(got, exp);
        }
    }

    #[test]
    fn test_ascii() {
        let cases = vec![
            (Datum::Bytes(b"1010".to_vec()), Datum::I64(49)),
            (Datum::Bytes(b"-1".to_vec()), Datum::I64(45)),
            (Datum::Bytes(b"".to_vec()), Datum::I64(0)),
            (Datum::Bytes(b"999".to_vec()), Datum::I64(57)),
            (Datum::Bytes(b"hello".to_vec()), Datum::I64(104)),
            (Datum::Bytes("Grüße".as_bytes().to_vec()), Datum::I64(71)),
            (Datum::Bytes("München".as_bytes().to_vec()), Datum::I64(77)),
            (Datum::Null, Datum::Null),
            (Datum::Bytes("数据库".as_bytes().to_vec()), Datum::I64(230)),
            (
                Datum::Bytes("忠犬ハチ公".as_bytes().to_vec()),
                Datum::I64(229),
            ),
            (Datum::Bytes("Αθήνα".as_bytes().to_vec()), Datum::I64(206)),
        ];

        let mut ctx = EvalContext::default();
        for (input, exp) in cases {
            let input = datum_expr(input);
            let op = scalar_func_expr(ScalarFuncSig::Ascii, &[input]);
            let op = Expression::build(&mut ctx, op).unwrap();
            let got = op.eval(&mut ctx, &[]).unwrap();
            assert_eq!(got, exp);
        }
    }

    #[test]
    fn test_upper_utf8() {
        let cases = vec![
            (
                Datum::Bytes(b"hello".to_vec()),
                Datum::Bytes(b"HELLO".to_vec()),
            ),
            (Datum::Bytes(b"123".to_vec()), Datum::Bytes(b"123".to_vec())),
            (
                Datum::Bytes("café".as_bytes().to_vec()),
                Datum::Bytes("CAFÉ".as_bytes().to_vec()),
            ),
            (
                Datum::Bytes("数据库".as_bytes().to_vec()),
                Datum::Bytes("数据库".as_bytes().to_vec()),
            ),
            (
                Datum::Bytes("ночь на окраине москвы".as_bytes().to_vec()),
                Datum::Bytes("НОЧЬ НА ОКРАИНЕ МОСКВЫ".as_bytes().to_vec()),
            ),
            (
                Datum::Bytes("قاعدة البيانات".as_bytes().to_vec()),
                Datum::Bytes("قاعدة البيانات".as_bytes().to_vec()),
            ),
            (Datum::Null, Datum::Null),
        ];

        let mut ctx = EvalContext::default();
        for (input, exp) in cases {
            let input = datum_expr(input);
            let op = scalar_func_expr(ScalarFuncSig::UpperUtf8, &[input]);
            let op = Expression::build(&mut ctx, op).unwrap();
            let got = op.eval(&mut ctx, &[]).unwrap();
            assert_eq!(got, exp);
        }
    }

    #[test]
    fn test_upper() {
        let cases = vec![
            (
                Datum::Bytes(b"hello".to_vec()),
                Datum::Bytes(b"hello".to_vec()),
            ),
            (Datum::Bytes(b"123".to_vec()), Datum::Bytes(b"123".to_vec())),
            (
                Datum::Bytes("café".as_bytes().to_vec()),
                Datum::Bytes("café".as_bytes().to_vec()),
            ),
            (
                Datum::Bytes("数据库".as_bytes().to_vec()),
                Datum::Bytes("数据库".as_bytes().to_vec()),
            ),
            (
                Datum::Bytes("ночь на окраине москвы".as_bytes().to_vec()),
                Datum::Bytes("ночь на окраине москвы".as_bytes().to_vec()),
            ),
            (
                Datum::Bytes("قاعدة البيانات".as_bytes().to_vec()),
                Datum::Bytes("قاعدة البيانات".as_bytes().to_vec()),
            ),
            (Datum::Null, Datum::Null),
        ];

        let mut ctx = EvalContext::default();
        for (input, exp) in cases {
            let input = string_datum_expr_with_tp(
                input,
                FieldTypeTp::VarString,
                FieldTypeFlag::BINARY,
                -1,
                CHARSET_BIN.to_owned(),
                Collation::Binary,
            );
            let op = scalar_func_expr(ScalarFuncSig::Upper, &[input]);
            let op = Expression::build(&mut ctx, op).unwrap();
            let got = op.eval(&mut ctx, &[]).unwrap();
            assert_eq!(got, exp);
        }
    }

    #[test]
    fn test_lower() {
        // Test non-binary string case
        let cases = vec![
            (
                Datum::Bytes(b"HELLO".to_vec()),
                Datum::Bytes(b"hello".to_vec()),
            ),
            (Datum::Bytes(b"123".to_vec()), Datum::Bytes(b"123".to_vec())),
            (
                Datum::Bytes("CAFÉ".as_bytes().to_vec()),
                Datum::Bytes("café".as_bytes().to_vec()),
            ),
            (
                Datum::Bytes("数据库".as_bytes().to_vec()),
                Datum::Bytes("数据库".as_bytes().to_vec()),
            ),
            (
                Datum::Bytes("НОЧЬ НА ОКРАИНЕ МОСКВЫ".as_bytes().to_vec()),
                Datum::Bytes("ночь на окраине москвы".as_bytes().to_vec()),
            ),
            (
                Datum::Bytes("قاعدة البيانات".as_bytes().to_vec()),
                Datum::Bytes("قاعدة البيانات".as_bytes().to_vec()),
            ),
            (Datum::Null, Datum::Null),
        ];

        let mut ctx = EvalContext::default();
        for (input, exp) in cases {
            let input = datum_expr(input);
            let op = scalar_func_expr(ScalarFuncSig::Lower, &[input]);
            let op = Expression::build(&mut ctx, op).unwrap();
            let got = op.eval(&mut ctx, &[]).unwrap();
            assert_eq!(got, exp);
        }

        // Test binary string case
        let cases = vec![
            (
                Datum::Bytes(b"hello".to_vec()),
                Datum::Bytes(b"hello".to_vec()),
            ),
            (
                Datum::Bytes("CAFÉ".as_bytes().to_vec()),
                Datum::Bytes("CAFÉ".as_bytes().to_vec()),
            ),
            (
                Datum::Bytes("数据库".as_bytes().to_vec()),
                Datum::Bytes("数据库".as_bytes().to_vec()),
            ),
            (
                Datum::Bytes("НОЧЬ НА ОКРАИНЕ МОСКВЫ".as_bytes().to_vec()),
                Datum::Bytes("НОЧЬ НА ОКРАИНЕ МОСКВЫ".as_bytes().to_vec()),
            ),
            (
                Datum::Bytes("قاعدة البيانات".as_bytes().to_vec()),
                Datum::Bytes("قاعدة البيانات".as_bytes().to_vec()),
            ),
            (Datum::Null, Datum::Null),
        ];

        let mut ctx = EvalContext::default();
        for (input, exp) in cases {
            let input = string_datum_expr_with_tp(
                input,
                FieldTypeTp::VarString,
                FieldTypeFlag::BINARY,
                -1,
                CHARSET_BIN.to_owned(),
                Collation::Binary,
            );
            let op = scalar_func_expr(ScalarFuncSig::Lower, &[input]);
            let op = Expression::build(&mut ctx, op).unwrap();
            let got = op.eval(&mut ctx, &[]).unwrap();
            assert_eq!(got, exp);
        }
    }

    #[test]
    fn test_concat() {
        let cases = vec![
            (
                vec![
                    Datum::Bytes(b"abc".to_vec()),
                    Datum::Bytes(b"defg".to_vec()),
                ],
                Datum::Bytes(b"abcdefg".to_vec()),
            ),
            (
                vec![
                    Datum::Bytes("忠犬ハチ公".as_bytes().to_vec()),
                    Datum::Bytes("CAFÉ".as_bytes().to_vec()),
                    Datum::Bytes("数据库".as_bytes().to_vec()),
                    Datum::Bytes("قاعدة البيانات".as_bytes().to_vec()),
                    Datum::Bytes("НОЧЬ НА ОКРАИНЕ МОСКВЫ".as_bytes().to_vec()),
                ],
                Datum::Bytes(
                    "忠犬ハチ公CAFÉ数据库قاعدة البياناتНОЧЬ НА ОКРАИНЕ МОСКВЫ"
                        .as_bytes()
                        .to_vec(),
                ),
            ),
            (
                vec![
                    Datum::Bytes(b"abc".to_vec()),
                    Datum::Bytes("CAFÉ".as_bytes().to_vec()),
                    Datum::Bytes("数据库".as_bytes().to_vec()),
                ],
                Datum::Bytes("abcCAFÉ数据库".as_bytes().to_vec()),
            ),
            (
                vec![
                    Datum::Bytes(b"abc".to_vec()),
                    Datum::Null,
                    Datum::Bytes(b"defg".to_vec()),
                ],
                Datum::Null,
            ),
            (vec![Datum::Null], Datum::Null),
        ];
        let mut ctx = EvalContext::default();
        for (row, exp) in cases {
            let children: Vec<Expr> = row.iter().map(|d| datum_expr(d.clone())).collect();
            let expr = scalar_func_expr(ScalarFuncSig::Concat, &children);
            let e = Expression::build(&mut ctx, expr).unwrap();
            let res = e.eval(&mut ctx, &[]).unwrap();
            assert_eq!(res, exp);
        }
    }

    #[test]
    fn test_concat_ws() {
        let cases = vec![
            (
                vec![
                    Datum::Bytes(b",".to_vec()),
                    Datum::Bytes(b"abc".to_vec()),
                    Datum::Bytes(b"defg".to_vec()),
                ],
                Datum::Bytes(b"abc,defg".to_vec()),
            ),
            (
                vec![
                    Datum::Bytes(b",".to_vec()),
                    Datum::Bytes("忠犬ハチ公".as_bytes().to_vec()),
                    Datum::Bytes("CAFÉ".as_bytes().to_vec()),
                    Datum::Bytes("数据库".as_bytes().to_vec()),
                    Datum::Bytes("قاعدة البيانات".as_bytes().to_vec()),
                    Datum::Bytes("НОЧЬ НА ОКРАИНЕ МОСКВЫ".as_bytes().to_vec()),
                ],
                Datum::Bytes(
                    "忠犬ハチ公,CAFÉ,数据库,قاعدة البيانات,НОЧЬ НА ОКРАИНЕ МОСКВЫ"
                        .as_bytes()
                        .to_vec(),
                ),
            ),
            (
                vec![
                    Datum::Bytes(b",".to_vec()),
                    Datum::Bytes(b"abc".to_vec()),
                    Datum::Bytes("CAFÉ".as_bytes().to_vec()),
                    Datum::Bytes("数据库".as_bytes().to_vec()),
                ],
                Datum::Bytes("abc,CAFÉ,数据库".as_bytes().to_vec()),
            ),
            (
                vec![
                    Datum::Bytes(b",".to_vec()),
                    Datum::Bytes(b"abc".to_vec()),
                    Datum::Null,
                    Datum::Bytes(b"defg".to_vec()),
                ],
                Datum::Bytes(b"abc,defg".to_vec()),
            ),
            (
                vec![Datum::Null, Datum::Bytes(b"abc".to_vec())],
                Datum::Null,
            ),
            (
                vec![
                    Datum::Bytes(b",".to_vec()),
                    Datum::Null,
                    Datum::Bytes(b"abc".to_vec()),
                ],
                Datum::Bytes(b"abc".to_vec()),
            ),
            (
                vec![
                    Datum::Bytes(b",".to_vec()),
                    Datum::Bytes(b"abc".to_vec()),
                    Datum::Null,
                ],
                Datum::Bytes(b"abc".to_vec()),
            ),
            (
                vec![
                    Datum::Bytes(b",".to_vec()),
                    Datum::Bytes(b"".to_vec()),
                    Datum::Bytes(b"abc".to_vec()),
                ],
                Datum::Bytes(b",abc".to_vec()),
            ),
            (
                vec![
                    Datum::Bytes(b",".to_vec()),
                    Datum::Null,
                    Datum::Bytes(b"abc".to_vec()),
                    Datum::Null,
                    Datum::Null,
                    Datum::Bytes(b"defg".to_vec()),
                    Datum::Null,
                ],
                Datum::Bytes(b"abc,defg".to_vec()),
            ),
            (
                vec![
                    Datum::Bytes("忠犬ハチ公".as_bytes().to_vec()),
                    Datum::Bytes("CAFÉ".as_bytes().to_vec()),
                    Datum::Bytes("数据库".as_bytes().to_vec()),
                    Datum::Bytes("قاعدة البيانات".as_bytes().to_vec()),
                ],
                Datum::Bytes(
                    "CAFÉ忠犬ハチ公数据库忠犬ハチ公قاعدة البيانات"
                        .as_bytes()
                        .to_vec(),
                ),
            ),
        ];
        let mut ctx = EvalContext::default();
        for (row, exp) in cases {
            let children: Vec<Expr> = row.iter().map(|d| datum_expr(d.clone())).collect();
            let expr = scalar_func_expr(ScalarFuncSig::ConcatWs, &children);
            let e = Expression::build(&mut ctx, expr).unwrap();
            let res = e.eval(&mut ctx, &[]).unwrap();
            assert_eq!(res, exp);
        }
    }

    #[test]
    fn test_replace() {
        let cases = vec![
            (
                vec![
                    Datum::Bytes(b"www.mysql.com".to_vec()),
                    Datum::Bytes(b"mysql".to_vec()),
                    Datum::Bytes(b"pingcap".to_vec()),
                ],
                Datum::Bytes(b"www.pingcap.com".to_vec()),
            ),
            (
                vec![
                    Datum::Bytes(b"www.mysql.com".to_vec()),
                    Datum::Bytes(b"w".to_vec()),
                    Datum::Bytes(b"1".to_vec()),
                ],
                Datum::Bytes(b"111.mysql.com".to_vec()),
            ),
            (
                vec![
                    Datum::Bytes(b"1234".to_vec()),
                    Datum::Bytes(b"2".to_vec()),
                    Datum::Bytes(b"55".to_vec()),
                ],
                Datum::Bytes(b"15534".to_vec()),
            ),
            (
                vec![
                    Datum::Bytes(b"".to_vec()),
                    Datum::Bytes(b"a".to_vec()),
                    Datum::Bytes(b"b".to_vec()),
                ],
                Datum::Bytes(b"".to_vec()),
            ),
            (
                vec![
                    Datum::Bytes(b"abc".to_vec()),
                    Datum::Bytes(b"".to_vec()),
                    Datum::Bytes(b"d".to_vec()),
                ],
                Datum::Bytes(b"abc".to_vec()),
            ),
            (
                vec![
                    Datum::Bytes(b"aaa".to_vec()),
                    Datum::Bytes(b"a".to_vec()),
                    Datum::Bytes(b"".to_vec()),
                ],
                Datum::Bytes(b"".to_vec()),
            ),
            (
                vec![
                    Datum::Null,
                    Datum::Bytes(b"a".to_vec()),
                    Datum::Bytes(b"b".to_vec()),
                ],
                Datum::Null,
            ),
            (
                vec![
                    Datum::Bytes(b"a".to_vec()),
                    Datum::Null,
                    Datum::Bytes(b"b".to_vec()),
                ],
                Datum::Null,
            ),
            (
                vec![
                    Datum::Bytes(b"a".to_vec()),
                    Datum::Bytes(b"b".to_vec()),
                    Datum::Null,
                ],
                Datum::Null,
            ),
        ];
        let mut ctx = EvalContext::default();
        for (row, exp) in cases {
            let children: Vec<Expr> = row.iter().map(|d| datum_expr(d.clone())).collect();
            let expr = scalar_func_expr(ScalarFuncSig::Replace, &children);
            let e = Expression::build(&mut ctx, expr).unwrap();
            let res = e.eval(&mut ctx, &[]).unwrap();
            assert_eq!(res, exp);
        }
    }

    #[test]
    fn test_char_length_utf8() {
        let cases = vec![
            (Datum::Bytes(b"HELLO".to_vec()), Datum::I64(5)),
            (Datum::Bytes(b"123".to_vec()), Datum::I64(3)),
            (Datum::Bytes(b"".to_vec()), Datum::I64(0)),
            (Datum::Bytes("CAFÉ".as_bytes().to_vec()), Datum::I64(4)),
            (Datum::Bytes("数据库".as_bytes().to_vec()), Datum::I64(3)),
            (
                Datum::Bytes("НОЧЬ НА ОКРАИНЕ МОСКВЫ".as_bytes().to_vec()),
                Datum::I64(22),
            ),
            (
                Datum::Bytes("قاعدة البيانات".as_bytes().to_vec()),
                Datum::I64(14),
            ),
            (Datum::Null, Datum::Null),
        ];

        let mut ctx = EvalContext::default();
        for (input, exp) in cases {
            let input = datum_expr(input);
            let op = scalar_func_expr(ScalarFuncSig::CharLengthUtf8, &[input]);
            let op = Expression::build(&mut ctx, op).unwrap();
            let got = op.eval(&mut ctx, &[]).unwrap();
            assert_eq!(got, exp);
        }
    }

    #[test]
    fn test_char_length() {
        let cases = vec![
            (Datum::Bytes(b"HELLO".to_vec()), Datum::I64(5)),
            (Datum::Bytes(b"123".to_vec()), Datum::I64(3)),
            (Datum::Bytes(b"".to_vec()), Datum::I64(0)),
            (Datum::Bytes("CAFÉ".as_bytes().to_vec()), Datum::I64(5)),
            (Datum::Bytes("数据库".as_bytes().to_vec()), Datum::I64(9)),
            (
                Datum::Bytes("НОЧЬ НА ОКРАИНЕ МОСКВЫ".as_bytes().to_vec()),
                Datum::I64(41),
            ),
            (
                Datum::Bytes("قاعدة البيانات".as_bytes().to_vec()),
                Datum::I64(27),
            ),
            (Datum::Null, Datum::Null),
        ];
        let mut ctx = EvalContext::default();
        for (input, exp) in cases {
            let input = string_datum_expr_with_tp(
                input,
                FieldTypeTp::VarString,
                FieldTypeFlag::BINARY,
                -1,
                CHARSET_BIN.to_owned(),
                Collation::Binary,
            );
            let op = scalar_func_expr(ScalarFuncSig::CharLength, &[input]);
            let op = Expression::build(&mut ctx, op).unwrap();
            let got = op.eval(&mut ctx, &[]).unwrap();
            assert_eq!(got, exp);
        }
    }

    #[test]
    fn test_hex_int_arg() {
        let cases = vec![
            (Datum::I64(12), Datum::Bytes(b"C".to_vec())),
            (Datum::I64(0x12), Datum::Bytes(b"12".to_vec())),
            (Datum::I64(0b1100), Datum::Bytes(b"C".to_vec())),
            (Datum::I64(0), Datum::Bytes(b"0".to_vec())),
            (Datum::I64(-1), Datum::Bytes(b"FFFFFFFFFFFFFFFF".to_vec())),
            (Datum::Null, Datum::Null),
        ];

        let mut ctx = EvalContext::default();
        for (input, exp) in cases {
            let input = datum_expr(input);
            let op = scalar_func_expr(ScalarFuncSig::HexIntArg, &[input]);
            let op = Expression::build(&mut ctx, op).unwrap();
            let got = op.eval(&mut ctx, &[]).unwrap();
            assert_eq!(got, exp);
        }
    }

    #[test]
    fn test_hex_str_arg() {
        let cases = vec![
            (
                Datum::Bytes(b"abc".to_vec()),
                Datum::Bytes(b"616263".to_vec()),
            ),
            (
                Datum::Bytes("你好".as_bytes().to_vec()),
                Datum::Bytes(b"E4BDA0E5A5BD".to_vec()),
            ),
            (Datum::Null, Datum::Null),
        ];

        let mut ctx = EvalContext::default();
        for (input, exp) in cases {
            let input = datum_expr(input);
            let op = scalar_func_expr(ScalarFuncSig::HexStrArg, &[input]);
            let op = Expression::build(&mut ctx, op).unwrap();
            let got = op.eval(&mut ctx, &[]).unwrap();
            assert_eq!(got, exp);
        }
    }

    #[test]
    fn test_un_hex() {
        let cases = vec![
            (
                Datum::Bytes(b"4D7953514C".to_vec()),
                Datum::Bytes(b"MySQL".to_vec()),
            ),
            (
                Datum::Bytes(b"1267".to_vec()),
                Datum::Bytes(vec![0x12, 0x67]),
            ),
            (
                Datum::Bytes(b"126".to_vec()),
                Datum::Bytes(vec![0x01, 0x26]),
            ),
            (Datum::Bytes(b"".to_vec()), Datum::Bytes(b"".to_vec())),
            (Datum::Bytes(b"string".to_vec()), Datum::Null),
            (Datum::Bytes("你好".as_bytes().to_vec()), Datum::Null),
            (Datum::Null, Datum::Null),
        ];

        let mut ctx = EvalContext::default();
        for (input, exp) in cases {
            let input = datum_expr(input);
            let op = scalar_func_expr(ScalarFuncSig::UnHex, &[input]);
            let op = Expression::build(&mut ctx, op).unwrap();
            let got = op.eval(&mut ctx, &[]).unwrap();
            assert_eq!(got, exp);
        }
    }

    #[test]
    fn test_elt() {
        let cases = vec![
            (
                vec![
                    Datum::I64(1),
                    Datum::Bytes(b"DataBase".to_vec()),
                    Datum::Bytes(b"Hello World!".to_vec()),
                ],
                Datum::Bytes(b"DataBase".to_vec()),
            ),
            (
                vec![
                    Datum::I64(2),
                    Datum::Bytes(b"DataBase".to_vec()),
                    Datum::Bytes(b"Hello World!".to_vec()),
                ],
                Datum::Bytes(b"Hello World!".to_vec()),
            ),
            (
                vec![
                    Datum::Null,
                    Datum::Bytes(b"DataBase".to_vec()),
                    Datum::Bytes(b"Hello World!".to_vec()),
                ],
                Datum::Null,
            ),
            (
                vec![
                    Datum::I64(1),
                    Datum::Null,
                    Datum::Bytes(b"Hello World!".to_vec()),
                ],
                Datum::Null,
            ),
            (
                vec![
                    Datum::I64(3),
                    Datum::Null,
                    Datum::Bytes(b"Hello World!".to_vec()),
                ],
                Datum::Null,
            ),
            (
                vec![
                    Datum::I64(0),
                    Datum::Null,
                    Datum::Bytes(b"Hello World!".to_vec()),
                ],
                Datum::Null,
            ),
            (
                vec![
                    Datum::I64(-1),
                    Datum::Null,
                    Datum::Bytes(b"Hello World!".to_vec()),
                ],
                Datum::Null,
            ),
            (
                vec![
                    Datum::I64(4),
                    Datum::Null,
                    Datum::Bytes(b"Hello".to_vec()),
                    Datum::Bytes(b"Hola".to_vec()),
                    Datum::Bytes("Cześć".as_bytes().to_vec()),
                    Datum::Bytes("你好".as_bytes().to_vec()),
                    Datum::Bytes("Здравствуйте".as_bytes().to_vec()),
                    Datum::Bytes(b"Hello World!".to_vec()),
                ],
                Datum::Bytes("Cześć".as_bytes().to_vec()),
            ),
        ];

        let mut ctx = EvalContext::default();
        for (args, exp) in cases {
            let children: Vec<Expr> = (0..args.len()).map(|id| col_expr(id as i64)).collect();
            let op = scalar_func_expr(ScalarFuncSig::Elt, &children);
            let e = Expression::build(&mut ctx, op).unwrap();
            let res = e.eval(&mut ctx, &args).unwrap();
            assert_eq!(res, exp);
        }
    }

    #[test]
    fn test_field_int() {
        let cases = vec![
            (
                vec![Datum::I64(1), Datum::I64(-2), Datum::I64(3)],
                Datum::I64(0),
            ),
            (
                vec![Datum::I64(-1), Datum::I64(2), Datum::I64(-1), Datum::I64(2)],
                Datum::I64(2),
            ),
            (
                vec![
                    Datum::I64(i64::MAX),
                    Datum::I64(0),
                    Datum::I64(i64::MIN),
                    Datum::I64(i64::MAX),
                ],
                Datum::I64(3),
            ),
            (
                vec![Datum::Null, Datum::I64(0), Datum::I64(0)],
                Datum::I64(0),
            ),
            (vec![Datum::Null, Datum::Null, Datum::I64(0)], Datum::I64(0)),
            (vec![Datum::I64(100)], Datum::I64(0)),
        ];

        let mut ctx = EvalContext::default();
        for (args, exp) in cases {
            let children: Vec<Expr> = (0..args.len()).map(|id| col_expr(id as i64)).collect();
            let op = scalar_func_expr(ScalarFuncSig::FieldInt, &children);
            let e = Expression::build(&mut ctx, op).unwrap();
            let res = e.eval(&mut ctx, &args).unwrap();
            assert_eq!(res, exp);
        }
    }

    #[test]
    fn test_field_real() {
        let cases = vec![
            (
                vec![Datum::F64(1.0), Datum::F64(-2.0), Datum::F64(9.0)],
                Datum::I64(0),
            ),
            (
                vec![
                    Datum::F64(-1.0),
                    Datum::F64(2.0),
                    Datum::F64(-1.0),
                    Datum::F64(2.0),
                ],
                Datum::I64(2),
            ),
            (
                vec![
                    Datum::F64(f64::MAX),
                    Datum::F64(0.0),
                    Datum::F64(f64::MIN),
                    Datum::F64(f64::MAX),
                ],
                Datum::I64(3),
            ),
            (
                vec![Datum::Null, Datum::F64(1.0), Datum::F64(1.0)],
                Datum::I64(0),
            ),
            (
                vec![Datum::Null, Datum::Null, Datum::F64(0.0)],
                Datum::I64(0),
            ),
            (vec![Datum::F64(10.0)], Datum::I64(0)),
        ];

        let mut ctx = EvalContext::default();
        for (args, exp) in cases {
            let children: Vec<Expr> = (0..args.len()).map(|id| col_expr(id as i64)).collect();
            let op = scalar_func_expr(ScalarFuncSig::FieldReal, &children);
            let e = Expression::build(&mut ctx, op).unwrap();
            let res = e.eval(&mut ctx, &args).unwrap();
            assert_eq!(res, exp);
        }
    }

    #[test]
    fn test_field_string() {
        let cases = vec![
            (
                vec![
                    Datum::Bytes(b"foo".to_vec()),
                    Datum::Bytes(b"foo".to_vec()),
                    Datum::Bytes(b"bar".to_vec()),
                    Datum::Bytes(b"baz".to_vec()),
                ],
                Datum::I64(1),
            ),
            (
                vec![
                    Datum::Bytes(b"foo".to_vec()),
                    Datum::Bytes(b"bar".to_vec()),
                    Datum::Bytes(b"baz".to_vec()),
                    Datum::Bytes(b"hello".to_vec()),
                ],
                Datum::I64(0),
            ),
            (
                vec![
                    Datum::Bytes(b"hello".to_vec()),
                    Datum::Bytes(b"world".to_vec()),
                    Datum::Bytes(b"world".to_vec()),
                    Datum::Bytes(b"hello".to_vec()),
                ],
                Datum::I64(3),
            ),
            (
                vec![
                    Datum::Bytes(b"Hello".to_vec()),
                    Datum::Bytes(b"Hola".to_vec()),
                    Datum::Bytes("Cześć".as_bytes().to_vec()),
                    Datum::Bytes("你好".as_bytes().to_vec()),
                    Datum::Bytes("Здравствуйте".as_bytes().to_vec()),
                    Datum::Bytes(b"Hello World!".to_vec()),
                    Datum::Bytes(b"Hello".to_vec()),
                ],
                Datum::I64(6),
            ),
            (
                vec![
                    Datum::Null,
                    Datum::Bytes(b"DataBase".to_vec()),
                    Datum::Bytes(b"Hello World!".to_vec()),
                ],
                Datum::I64(0),
            ),
            (
                vec![
                    Datum::Null,
                    Datum::Null,
                    Datum::Bytes(b"Hello World!".to_vec()),
                ],
                Datum::I64(0),
            ),
            (vec![Datum::Bytes(b"Hello World!".to_vec())], Datum::I64(0)),
        ];

        let mut ctx = EvalContext::default();
        for (args, exp) in cases {
            let children: Vec<Expr> = (0..args.len()).map(|id| col_expr(id as i64)).collect();
            let op = scalar_func_expr(ScalarFuncSig::FieldString, &children);
            let e = Expression::build(&mut ctx, op).unwrap();
            let res = e.eval(&mut ctx, &args).unwrap();
            assert_eq!(res, exp);
        }
    }

    #[test]
    fn test_trim_1_arg() {
        let tests = vec![
            ("   bar   ", "bar"),
            ("\t   bar   \n", "\t   bar   \n"),
            ("\r   bar   \t", "\r   bar   \t"),
            ("   \tbar\n     ", "\tbar\n"),
            ("", ""),
        ];
        for (s, exp) in tests {
            let s = Datum::Bytes(s.as_bytes().to_vec());
            let exp = Datum::Bytes(exp.as_bytes().to_vec());
            let got = eval_func(ScalarFuncSig::Trim1Arg, &[s]).unwrap();
            assert_eq!(got, exp);
        }

        let got = eval_func(ScalarFuncSig::Trim1Arg, &[Datum::Null]).unwrap();
        assert_eq!(got, Datum::Null);
    }

    #[test]
    fn test_trim_2_args() {
        let tests = vec![
            ("xxxbarxxx", "x", "bar"),
            ("bar", "x", "bar"),
            ("   bar   ", "", "   bar   "),
            ("", "x", ""),
            ("张三和张三", "张三", "和"),
        ];
        for (s, pat, exp) in tests {
            let s = Datum::Bytes(s.as_bytes().to_vec());
            let pat = Datum::Bytes(pat.as_bytes().to_vec());
            let exp = Datum::Bytes(exp.as_bytes().to_vec());
            let got = eval_func(ScalarFuncSig::Trim2Args, &[s, pat]).unwrap();
            assert_eq!(got, exp);
        }

        let invalid_tests = vec![
            (Datum::Null, Datum::Bytes(b"x".to_vec()), Datum::Null),
            (Datum::Bytes(b"bar".to_vec()), Datum::Null, Datum::Null),
        ];
        for (s, pat, exp) in invalid_tests {
            let got = eval_func(ScalarFuncSig::Trim2Args, &[s, pat]).unwrap();
            assert_eq!(got, exp);
        }
    }

    #[test]
    fn test_trim_3_args() {
        let tests = vec![
            ("xxxbarxxx", "x", TrimDirection::Leading as i64, "barxxx"),
            ("barxxyz", "xyz", TrimDirection::Trailing as i64, "barx"),
            ("xxxbarxxx", "x", TrimDirection::Both as i64, "bar"),
        ];
        for (s, pat, direction, exp) in tests {
            let s = Datum::Bytes(s.as_bytes().to_vec());
            let pat = Datum::Bytes(pat.as_bytes().to_vec());
            let direction = Datum::I64(direction);
            let exp = Datum::Bytes(exp.as_bytes().to_vec());

            let got = eval_func(ScalarFuncSig::Trim3Args, &[s, pat, direction]).unwrap();
            assert_eq!(got, exp);
        }

        let invalid_tests = vec![
            (
                Datum::Null,
                Datum::Bytes(b"x".to_vec()),
                Datum::I64(TrimDirection::Leading as i64),
                Datum::Null,
            ),
            (
                Datum::Bytes(b"bar".to_vec()),
                Datum::Null,
                Datum::I64(TrimDirection::Leading as i64),
                Datum::Null,
            ),
        ];
        for (s, pat, direction, exp) in invalid_tests {
            let got = eval_func(ScalarFuncSig::Trim3Args, &[s, pat, direction]).unwrap();
            assert_eq!(got, exp);
        }

        // test invalid direction value
        let args = [
            Datum::Bytes(b"bar".to_vec()),
            Datum::Bytes(b"b".to_vec()),
            Datum::I64(0),
        ];
        let got = eval_func(ScalarFuncSig::Trim3Args, &args);
        assert!(got.is_err());
    }

    #[test]
    fn test_encoded_size() {
        assert_eq!(encoded_size(0).unwrap(), 0);
        assert_eq!(encoded_size(54).unwrap(), 72);
        assert_eq!(encoded_size(58).unwrap(), 81);
        assert!(encoded_size(usize::max_value()).is_none());
    }

    #[test]
    fn test_to_base64() {
        let tests = vec![
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
        for (s, exp) in tests {
            let s = Datum::Bytes(s.to_string().into_bytes());
            let exp = Datum::Bytes(exp.to_string().into_bytes());
            let got = eval_func(ScalarFuncSig::ToBase64, &[s]).unwrap();
            assert_eq!(got, exp);
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
        for (s, exp) in tests {
            let s = Datum::Bytes(s.to_string().into_bytes());
            let exp = Datum::Bytes(exp.to_string().into_bytes());
            let got = eval_func(ScalarFuncSig::FromBase64, &[s]).unwrap();
            assert_eq!(got, exp);
        }

        let s = Datum::Bytes(b"src".to_vec());
        let exp = Datum::Bytes(b"".to_vec());
        let got = eval_func(ScalarFuncSig::FromBase64, &[s]).unwrap();
        assert_eq!(got, exp);
    }

    #[test]
    fn test_substring_index() {
        let tests = vec![
            ("www.pingcap.com", ".", 2, "www.pingcap"),
            ("www.pingcap.com", ".", -2, "pingcap.com"),
            ("www.pingcap.com", ".", -3, "www.pingcap.com"),
            ("www.pingcap.com", ".", 0, ""),
            ("www.pingcap.com", ".", 100, "www.pingcap.com"),
            ("www.pingcap.com", ".", -100, "www.pingcap.com"),
            ("www.pingcap.com", "d", 0, ""),
            ("www.pingcap.com", "d", 1, "www.pingcap.com"),
            ("www.pingcap.com", "d", -1, "www.pingcap.com"),
            ("www.pingcap.com", "", 0, ""),
            ("www.pingcap.com", "", 1, ""),
            ("www.pingcap.com", "", -1, ""),
            ("1aaa1", "aa", 1, "1"),
            ("1aaa1", "aa", 2, "1aaa1"),
            ("1aaaaaa1", "aa", 2, "1aa"),
            ("1aaa1", "aa", -1, "a1"),
            ("1aaaaaa1", "aa", -1, "1"),
            ("1aaa1", "aa", -2, "1aaa1"),
            ("1aaaaaa1", "aa", -2, "aa1"),
            ("aaa1aa1aa", "aa", -3, "a1aa1aa"),
            ("aaa1aa1aa", "aa", i64::max_value(), "aaa1aa1aa"),
            ("aaa1aa1aa", "aa", i64::min_value() + 1, "aaa1aa1aa"),
            ("aaa1aa1aa", "aa", i64::min_value(), "aaa1aa1aa"),
            // empty parts after split
            ("...", ".", 1, ""),
            ("...", ".", 2, "."),
            ("...", ".", 3, ".."),
            ("...", ".", 4, "..."),
            ("...", ".", -1, ""),
            ("...", ".", -2, "."),
            ("...", ".", -3, ".."),
            ("...", ".", -4, "..."),
            // weird boundary conditions
            ("...www...pingcap...com...", ".", 3, ".."),
            ("...www...pingcap...com...", ".", 4, "...www"),
            ("...www...pingcap...com...", ".", 5, "...www."),
            ("...www...pingcap...com...", ".", -3, ".."),
            ("...www...pingcap...com...", ".", -4, "com..."),
            ("...www...pingcap...com...", ".", -5, ".com..."),
            ("", ".", 0, ""),
            ("", ".", 1, ""),
            ("", ".", -1, ""),
        ];
        for (s, delim, count, exp) in tests {
            let s = Datum::Bytes(s.as_bytes().to_vec());
            let delim = Datum::Bytes(delim.as_bytes().to_vec());
            let count = Datum::I64(count);
            let got = eval_func(ScalarFuncSig::SubstringIndex, &[s, delim, count]).unwrap();
            assert_eq!(got, Datum::Bytes(exp.as_bytes().to_vec()));
        }

        // u64 count
        let args = [
            Datum::Bytes(b"www.pingcap.com".to_vec()),
            Datum::Bytes(b".".to_vec()),
            Datum::U64(u64::max_value()),
        ];
        let got = eval_func(ScalarFuncSig::SubstringIndex, &args).unwrap();
        assert_eq!(got, Datum::Bytes(b"www.pingcap.com".to_vec()));

        let invalid_tests = vec![
            (
                Datum::Null,
                Datum::Bytes(b"".to_vec()),
                Datum::I64(1),
                Datum::Null,
            ),
            (
                Datum::Bytes(b"www.pingcap.com".to_vec()),
                Datum::Null,
                Datum::I64(1),
                Datum::Null,
            ),
            (
                Datum::Bytes(b"www.pingcap.com".to_vec()),
                Datum::Bytes(b"".to_vec()),
                Datum::Null,
                Datum::Null,
            ),
        ];
        for (s, delim, count, exp) in invalid_tests {
            let got = eval_func(ScalarFuncSig::SubstringIndex, &[s, delim, count]).unwrap();
            assert_eq!(got, exp);
        }
    }

    #[test]
    fn test_substring_2_args_utf8() {
        let tests = vec![
            ("中文a测试bb", 1, "中文a测试bb"),
            ("中文a测试bb", 2, "文a测试bb"),
            ("中文a测试bb", 7, "b"),
            ("中文a测试bb", 8, ""),
            ("中文a测试bb", -6, "文a测试bb"),
            ("中文a测试bb", -7, "中文a测试bb"),
            ("中文a测试bb", -8, ""),
            ("中文a测a试", -1, "试"),
            ("中文a测a试", -2, "a试"),
            ("Quadratically", 5, "ratically"),
            ("Sakila", 1, "Sakila"),
            ("Sakila", -3, "ila"),
            ("Sakila", 0, ""),
            ("Sakila", 100, ""),
            ("Sakila", -100, ""),
            ("Sakila", i64::max_value(), ""),
            ("Sakila", i64::min_value(), ""),
            ("", 1, ""),
            ("", -1, ""),
        ];
        for (s, pos, exp) in tests {
            let s = Datum::Bytes(s.as_bytes().to_vec());
            let pos = Datum::I64(pos);
            let got = eval_func(ScalarFuncSig::Substring2ArgsUtf8, &[s, pos]).unwrap();
            assert_eq!(got, Datum::Bytes(exp.as_bytes().to_vec()));
        }

        let s = Datum::Bytes(b"Sakila".to_vec());
        let pos = Datum::U64(u64::max_value());
        let got = eval_func(ScalarFuncSig::Substring2ArgsUtf8, &[s, pos]).unwrap();
        assert_eq!(got, Datum::Bytes(b"".to_vec()));
    }

    #[test]
    fn test_substring_3_args_utf8() {
        let tests = vec![
            ("Quadratically", 5, 6, "ratica"),
            ("Sakila", -5, 3, "aki"),
            ("Sakila", 2, 0, ""),
            ("Sakila", 2, -1, ""),
            ("Sakila", 2, 100, "akila"),
            ("中文a测试bb", -3, 1, "试"),
            ("中文a测试bb", -3, 2, "试b"),
            ("中文a测a试", 2, 1, "文"),
            ("中文a测a试", 2, 3, "文a测"),
            ("中文a测a试", -1, 1, "试"),
            ("中文a测a试", -1, 5, "试"),
            ("中文a测a试", -6, 20, "中文a测a试"),
            ("中文a测a试", -7, 5, ""),
            ("", 1, 1, ""),
            ("", -1, 1, ""),
        ];
        for (s, pos, len, exp) in tests {
            let s = Datum::Bytes(s.as_bytes().to_vec());
            let pos = Datum::I64(pos);
            let len = Datum::I64(len);
            let got = eval_func(ScalarFuncSig::Substring3ArgsUtf8, &[s, pos, len]).unwrap();
            assert_eq!(got, Datum::Bytes(exp.as_bytes().to_vec()));
        }

        let tests = vec![
            (
                "中文a测a试",
                Datum::U64(u64::max_value()),
                Datum::I64(5),
                "",
            ),
            (
                "中文a测a试",
                Datum::I64(2),
                Datum::U64(u64::max_value()),
                "文a测a试",
            ),
            (
                "中文a测a试",
                Datum::I64(-2),
                Datum::U64(u64::max_value()),
                "a试",
            ),
        ];
        for (s, pos, len, exp) in tests {
            let s = Datum::Bytes(s.as_bytes().to_vec());
            let got = eval_func(ScalarFuncSig::Substring3ArgsUtf8, &[s, pos, len]).unwrap();
            assert_eq!(got, Datum::Bytes(exp.as_bytes().to_vec()));
        }
    }

    #[test]
    fn test_substring_2_args() {
        let tests = vec![
            ("中文a测试bb", 1, "中文a测试bb"),
            ("中文a测试", -3, "试"),
            ("\x61\x76\x5e\x38\x2f\x35", -1, "\x35"),
            ("\x61\x76\x5e\x38\x2f\x35", 2, "\x76\x5e\x38\x2f\x35"),
            ("Quadratically", 5, "ratically"),
            ("Sakila", 1, "Sakila"),
            ("Sakila", -3, "ila"),
            ("Sakila", 0, ""),
            ("Sakila", 100, ""),
            ("Sakila", -100, ""),
            ("Sakila", i64::max_value(), ""),
            ("Sakila", i64::min_value(), ""),
            ("", 1, ""),
            ("", -1, ""),
        ];
        for (s, pos, exp) in tests {
            let s = Datum::Bytes(s.as_bytes().to_vec());
            let pos = Datum::I64(pos);
            let got = eval_func(ScalarFuncSig::Substring2Args, &[s, pos]).unwrap();
            assert_eq!(got, Datum::Bytes(exp.as_bytes().to_vec()));
        }

        // multibytes & unsigned position test
        let corner_case_tests = vec![
            ("中文a测试", Datum::I64(-1), vec![149]),
            ("Sakila", Datum::U64(u64::max_value()), b"".to_vec()),
        ];
        for (s, pos, exp) in corner_case_tests {
            let s = Datum::Bytes(s.as_bytes().to_vec());
            let got = eval_func(ScalarFuncSig::Substring2Args, &[s, pos]).unwrap();
            assert_eq!(got, Datum::Bytes(exp));
        }
    }

    #[test]
    fn test_substring_3_args() {
        let tests = vec![
            ("Quadratically", 5, 6, "ratica"),
            ("Sakila", -5, 3, "aki"),
            ("Sakila", 2, 0, ""),
            ("Sakila", 2, -1, ""),
            ("Sakila", 2, 100, "akila"),
            ("Sakila", 100, 5, ""),
            ("Sakila", -100, 5, ""),
            ("中文a测a试", 4, 3, "文"),
            ("中文a测a试", 4, 4, "文a"),
            ("中文a测a试", -3, 3, "试"),
            ("\x61\x76\x5e\x38\x2f\x35", 2, 2, "\x76\x5e"),
            ("\x61\x76\x5e\x38\x2f\x35", 4, 100, "\x38\x2f\x35"),
            ("\x61\x76\x5e\x38\x2f\x35", -1, 2, "\x35"),
            ("\x61\x76\x5e\x38\x2f\x35", -2, 2, "\x2f\x35"),
            ("", 1, 1, ""),
            ("", -1, 1, ""),
        ];
        for (s, pos, len, exp) in tests {
            let s = Datum::Bytes(s.as_bytes().to_vec());
            let pos = Datum::I64(pos);
            let len = Datum::I64(len);
            let got = eval_func(ScalarFuncSig::Substring3Args, &[s, pos, len]).unwrap();
            assert_eq!(got, Datum::Bytes(exp.as_bytes().to_vec()));
        }

        // multibytes & unsigned position test
        let corner_case_tests = vec![
            ("中文a测试", Datum::I64(-2), Datum::I64(2), vec![175, 149]),
            (
                "Sakila",
                Datum::U64(u64::max_value()),
                Datum::I64(2),
                b"".to_vec(),
            ),
            (
                "Sakila",
                Datum::I64(2),
                Datum::U64(u64::max_value()),
                b"akila".to_vec(),
            ),
        ];
        for (s, pos, len, exp) in corner_case_tests {
            let s = Datum::Bytes(s.as_bytes().to_vec());
            let got = eval_func(ScalarFuncSig::Substring3Args, &[s, pos, len]).unwrap();
            assert_eq!(got, Datum::Bytes(exp));
        }
    }

    #[test]
    fn test_space() {
        let tests = vec![
            (Datum::I64(0), Datum::Bytes(b"".to_vec())),
            (Datum::U64(0), Datum::Bytes(b"".to_vec())),
            (Datum::I64(3), Datum::Bytes(b"   ".to_vec())),
            (Datum::I64(-1), Datum::Bytes(b"".to_vec())),
            (Datum::U64(u64::max_value()), Datum::Null),
            (Datum::I64(i64::from(MAX_BLOB_WIDTH) + 1), Datum::Null),
            (
                Datum::I64(i64::from(MAX_BLOB_WIDTH)),
                Datum::Bytes(vec![super::SPACE; MAX_BLOB_WIDTH as usize]),
            ),
        ];

        for (len, exp) in tests {
            let got = eval_func(ScalarFuncSig::Space, &[len]).unwrap();
            assert_eq!(got, exp);
        }
    }

    #[test]
    fn test_strcmp() {
        let tests = vec![
            (
                Datum::Bytes(b"123".to_vec()),
                Datum::Bytes(b"123".to_vec()),
                Datum::I64(0),
            ),
            (
                Datum::Bytes(b"123".to_vec()),
                Datum::Bytes(b"1".to_vec()),
                Datum::I64(1),
            ),
            (
                Datum::Bytes(b"1".to_vec()),
                Datum::Bytes(b"123".to_vec()),
                Datum::I64(-1),
            ),
            (
                Datum::Bytes(b"123".to_vec()),
                Datum::Bytes(b"45".to_vec()),
                Datum::I64(-1),
            ),
            (
                Datum::Bytes("你好".as_bytes().to_vec()),
                Datum::Bytes(b"hello".to_vec()),
                Datum::I64(1),
            ),
            (
                Datum::Bytes(b"".to_vec()),
                Datum::Bytes(b"123".to_vec()),
                Datum::I64(-1),
            ),
            (
                Datum::Bytes(b"123".to_vec()),
                Datum::Bytes(b"".to_vec()),
                Datum::I64(1),
            ),
            (
                Datum::Bytes(b"".to_vec()),
                Datum::Bytes(b"".to_vec()),
                Datum::I64(0),
            ),
            (Datum::Null, Datum::Bytes(b"123".to_vec()), Datum::Null),
            (Datum::Bytes(b"123".to_vec()), Datum::Null, Datum::Null),
            (Datum::Bytes(b"".to_vec()), Datum::Null, Datum::Null),
            (Datum::Null, Datum::Bytes(b"".to_vec()), Datum::Null),
        ];

        for (left, right, exp) in tests {
            let got = eval_func(ScalarFuncSig::Strcmp, &[left, right]).unwrap();
            assert_eq!(got, exp);
        }
    }

    #[test]
    fn test_validate_target_len_for_pad() {
        let cases = vec![
            // target_len, input_len, size_of_type, pad_empty, result
            (0, 10, 1, false, Some(0)),
            (-1, 10, 1, false, None),
            (12, 10, 1, true, None),
            (i64::from(MAX_BLOB_WIDTH) + 1, 10, 1, false, None),
            (i64::from(MAX_BLOB_WIDTH) / 4 + 1, 10, 4, false, None),
            (12, 10, 1, false, Some(12)),
        ];
        for case in cases {
            let got = super::validate_target_len_for_pad(false, case.0, case.1, case.2, case.3);
            assert_eq!(got, case.4);
        }

        let unsigned_cases = vec![
            (u64::max_value(), 10, 1, false, None),
            (u64::max_value(), 10, 4, false, None),
            (u64::max_value(), 10, 1, true, None),
            (u64::max_value(), 10, 4, true, None),
            (12u64, 10, 4, false, Some(12)),
        ];
        for case in unsigned_cases {
            let got =
                super::validate_target_len_for_pad(true, case.0 as i64, case.1, case.2, case.3);
            assert_eq!(got, case.4);
        }
    }

    fn common_rpad_cases() -> Vec<(Datum, Datum, Datum, Datum)> {
        vec![
            (
                Datum::Bytes(b"hi".to_vec()),
                Datum::I64(5),
                Datum::Bytes(b"?".to_vec()),
                Datum::Bytes(b"hi???".to_vec()),
            ),
            (
                Datum::Bytes(b"hi".to_vec()),
                Datum::I64(1),
                Datum::Bytes(b"?".to_vec()),
                Datum::Bytes(b"h".to_vec()),
            ),
            (
                Datum::Bytes(b"hi".to_vec()),
                Datum::I64(0),
                Datum::Bytes(b"?".to_vec()),
                Datum::Bytes(b"".to_vec()),
            ),
            (
                Datum::Bytes(b"hi".to_vec()),
                Datum::I64(1),
                Datum::Bytes(b"".to_vec()),
                Datum::Bytes(b"h".to_vec()),
            ),
            (
                Datum::Bytes(b"hi".to_vec()),
                Datum::I64(5),
                Datum::Bytes(b"ab".to_vec()),
                Datum::Bytes(b"hiaba".to_vec()),
            ),
            (
                Datum::Bytes(b"hi".to_vec()),
                Datum::I64(6),
                Datum::Bytes(b"ab".to_vec()),
                Datum::Bytes(b"hiabab".to_vec()),
            ),
            (
                Datum::Bytes(b"hi".to_vec()),
                Datum::I64(-1),
                Datum::Bytes(b"?".to_vec()),
                Datum::Null,
            ),
            (
                Datum::Bytes(b"hi".to_vec()),
                Datum::I64(5),
                Datum::Bytes(b"".to_vec()),
                Datum::Null,
            ),
            (
                Datum::Bytes(b"hi".to_vec()),
                Datum::I64(0),
                Datum::Bytes(b"".to_vec()),
                Datum::Bytes(b"".to_vec()),
            ),
        ]
    }

    #[test]
    fn test_rpad_utf8() {
        let mut cases = vec![
            (
                Datum::Bytes("a多字节".as_bytes().to_vec()),
                Datum::I64(3),
                Datum::Bytes("测试".as_bytes().to_vec()),
                Datum::Bytes("a多字".as_bytes().to_vec()),
            ),
            (
                Datum::Bytes("a多字节".as_bytes().to_vec()),
                Datum::I64(4),
                Datum::Bytes("测试".as_bytes().to_vec()),
                Datum::Bytes("a多字节".as_bytes().to_vec()),
            ),
            (
                Datum::Bytes("a多字节".as_bytes().to_vec()),
                Datum::I64(5),
                Datum::Bytes("测试".as_bytes().to_vec()),
                Datum::Bytes("a多字节测".as_bytes().to_vec()),
            ),
            (
                Datum::Bytes("a多字节".as_bytes().to_vec()),
                Datum::I64(6),
                Datum::Bytes("测试".as_bytes().to_vec()),
                Datum::Bytes("a多字节测试".as_bytes().to_vec()),
            ),
            (
                Datum::Bytes("a多字节".as_bytes().to_vec()),
                Datum::I64(7),
                Datum::Bytes("测试".as_bytes().to_vec()),
                Datum::Bytes("a多字节测试测".as_bytes().to_vec()),
            ),
            (
                Datum::Bytes("a多字节".as_bytes().to_vec()),
                Datum::I64(i64::from(MAX_BLOB_WIDTH) / 4 + 1),
                Datum::Bytes("测试".as_bytes().to_vec()),
                Datum::Null,
            ),
        ];
        cases.append(&mut common_rpad_cases());

        for (s, len, pad, exp) in cases {
            let got = eval_func(ScalarFuncSig::RpadUtf8, &[s, len, pad]).unwrap();
            assert_eq!(got, exp);
        }
    }

    #[test]
    fn test_rpad() {
        let mut cases = vec![
            (
                Datum::Bytes(b"\x61\x76\x5e".to_vec()),
                Datum::I64(5),
                Datum::Bytes(b"\x35".to_vec()),
                Datum::Bytes(b"\x61\x76\x5e\x35\x35".to_vec()),
            ),
            (
                Datum::Bytes(b"\x61\x76\x5e".to_vec()),
                Datum::I64(2),
                Datum::Bytes(b"\x35".to_vec()),
                Datum::Bytes(b"\x61\x76".to_vec()),
            ),
            (
                Datum::Bytes("a多字节".as_bytes().to_vec()),
                Datum::I64(13),
                Datum::Bytes("测试".as_bytes().to_vec()),
                Datum::Bytes("a多字节测".as_bytes().to_vec()),
            ),
            (
                Datum::Bytes(b"abc".to_vec()),
                Datum::I64(i64::from(MAX_BLOB_WIDTH) + 1),
                Datum::Bytes(b"aa".to_vec()),
                Datum::Null,
            ),
        ];
        cases.append(&mut common_rpad_cases());

        for (s, len, pad, exp) in cases {
            let got = eval_func(ScalarFuncSig::Rpad, &[s, len, pad]).unwrap();
            assert_eq!(got, exp);
        }
    }

    fn common_lpad_cases() -> Vec<(Datum, Datum, Datum, Datum)> {
        vec![
            (
                Datum::Bytes(b"hi".to_vec()),
                Datum::I64(5),
                Datum::Bytes(b"?".to_vec()),
                Datum::Bytes(b"???hi".to_vec()),
            ),
            (
                Datum::Bytes(b"hi".to_vec()),
                Datum::I64(1),
                Datum::Bytes(b"?".to_vec()),
                Datum::Bytes(b"h".to_vec()),
            ),
            (
                Datum::Bytes(b"hi".to_vec()),
                Datum::I64(0),
                Datum::Bytes(b"?".to_vec()),
                Datum::Bytes(b"".to_vec()),
            ),
            (
                Datum::Bytes(b"hi".to_vec()),
                Datum::I64(-1),
                Datum::Bytes(b"?".to_vec()),
                Datum::Null,
            ),
            (
                Datum::Bytes(b"hi".to_vec()),
                Datum::I64(1),
                Datum::Bytes(b"".to_vec()),
                Datum::Bytes(b"h".to_vec()),
            ),
            (
                Datum::Bytes(b"hi".to_vec()),
                Datum::I64(5),
                Datum::Bytes(b"".to_vec()),
                Datum::Null,
            ),
            (
                Datum::Bytes(b"hi".to_vec()),
                Datum::I64(5),
                Datum::Bytes(b"ab".to_vec()),
                Datum::Bytes(b"abahi".to_vec()),
            ),
            (
                Datum::Bytes(b"hi".to_vec()),
                Datum::I64(6),
                Datum::Bytes(b"ab".to_vec()),
                Datum::Bytes(b"ababhi".to_vec()),
            ),
        ]
    }

    #[test]
    fn test_lpad_utf8() {
        let mut cases = vec![
            (
                Datum::Bytes("a多字节".as_bytes().to_vec()),
                Datum::I64(3),
                Datum::Bytes("测试".as_bytes().to_vec()),
                Datum::Bytes("a多字".as_bytes().to_vec()),
            ),
            (
                Datum::Bytes("a多字节".as_bytes().to_vec()),
                Datum::I64(4),
                Datum::Bytes("测试".as_bytes().to_vec()),
                Datum::Bytes("a多字节".as_bytes().to_vec()),
            ),
            (
                Datum::Bytes("a多字节".as_bytes().to_vec()),
                Datum::I64(5),
                Datum::Bytes("测试".as_bytes().to_vec()),
                Datum::Bytes("测a多字节".as_bytes().to_vec()),
            ),
            (
                Datum::Bytes("a多字节".as_bytes().to_vec()),
                Datum::I64(6),
                Datum::Bytes("测试".as_bytes().to_vec()),
                Datum::Bytes("测试a多字节".as_bytes().to_vec()),
            ),
            (
                Datum::Bytes("a多字节".as_bytes().to_vec()),
                Datum::I64(7),
                Datum::Bytes("测试".as_bytes().to_vec()),
                Datum::Bytes("测试测a多字节".as_bytes().to_vec()),
            ),
            (
                Datum::Bytes("a多字节".as_bytes().to_vec()),
                Datum::I64(i64::from(MAX_BLOB_WIDTH) / 4 + 1),
                Datum::Bytes("测试".as_bytes().to_vec()),
                Datum::Null,
            ),
        ];
        cases.append(&mut common_lpad_cases());

        for (s, len, pad, exp) in cases {
            let got = eval_func(ScalarFuncSig::LpadUtf8, &[s, len, pad]).unwrap();
            assert_eq!(got, exp);
        }
    }

    #[test]
    fn test_lpad() {
        let mut cases = vec![
            (
                Datum::Bytes(b"\x61\x76\x5e".to_vec()),
                Datum::I64(5),
                Datum::Bytes(b"\x35".to_vec()),
                Datum::Bytes(b"\x35\x35\x61\x76\x5e".to_vec()),
            ),
            (
                Datum::Bytes(b"\x61\x76\x5e".to_vec()),
                Datum::I64(2),
                Datum::Bytes(b"\x35".to_vec()),
                Datum::Bytes(b"\x61\x76".to_vec()),
            ),
            (
                Datum::Bytes("a多字节".as_bytes().to_vec()),
                Datum::I64(13),
                Datum::Bytes("测试".as_bytes().to_vec()),
                Datum::Bytes("测a多字节".as_bytes().to_vec()),
            ),
            (
                Datum::Bytes(b"abc".to_vec()),
                Datum::I64(i64::from(MAX_BLOB_WIDTH) + 1),
                Datum::Bytes(b"aa".to_vec()),
                Datum::Null,
            ),
        ];
        cases.append(&mut common_lpad_cases());

        for (s, len, pad, exp) in cases {
            let got = eval_func(ScalarFuncSig::Lpad, &[s, len, pad]).unwrap();
            assert_eq!(got, exp);
        }
    }

    #[test]
    fn test_left() {
        let cases = vec![
            (
                Datum::Bytes(b"hello".to_vec()),
                Datum::I64(-1),
                Datum::Bytes(b"".to_vec()),
            ),
            (
                Datum::Bytes(b"hello".to_vec()),
                Datum::I64(0),
                Datum::Bytes(b"".to_vec()),
            ),
            (
                Datum::Bytes(b"hello".to_vec()),
                Datum::I64(1),
                Datum::Bytes(b"h".to_vec()),
            ),
            (
                Datum::Bytes(b"hello".to_vec()),
                Datum::I64(5),
                Datum::Bytes(b"hello".to_vec()),
            ),
            (
                Datum::Bytes(b"hello".to_vec()),
                Datum::I64(6),
                Datum::Bytes(b"hello".to_vec()),
            ),
            (
                Datum::Bytes(b"hello".to_vec()),
                Datum::U64(u64::max_value()),
                Datum::Bytes(b"hello".to_vec()),
            ),
            (
                Datum::Bytes(b"\x61\x76\x5e".to_vec()),
                Datum::I64(1),
                Datum::Bytes(b"\x61".to_vec()),
            ),
        ];

        for (input, length, exp) in cases {
            let got = eval_func(ScalarFuncSig::Left, &[input, length]).unwrap();
            assert_eq!(got, exp);
        }
    }

    #[test]
    fn test_right() {
        let cases = vec![
            (
                Datum::Bytes(b"hello".to_vec()),
                Datum::I64(-1),
                Datum::Bytes(b"".to_vec()),
            ),
            (
                Datum::Bytes(b"hello".to_vec()),
                Datum::I64(0),
                Datum::Bytes(b"".to_vec()),
            ),
            (
                Datum::Bytes(b"hello".to_vec()),
                Datum::I64(1),
                Datum::Bytes(b"o".to_vec()),
            ),
            (
                Datum::Bytes(b"hello".to_vec()),
                Datum::I64(5),
                Datum::Bytes(b"hello".to_vec()),
            ),
            (
                Datum::Bytes(b"hello".to_vec()),
                Datum::I64(6),
                Datum::Bytes(b"hello".to_vec()),
            ),
            (
                Datum::Bytes(b"hello".to_vec()),
                Datum::U64(u64::max_value()),
                Datum::Bytes(b"hello".to_vec()),
            ),
            (
                Datum::Bytes(b"\x61\x76\x5e".to_vec()),
                Datum::I64(1),
                Datum::Bytes(b"\x5e".to_vec()),
            ),
        ];

        for (input, length, exp) in cases {
            let got = eval_func(ScalarFuncSig::Right, &[input, length]).unwrap();
            assert_eq!(got, exp);
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
            let substr = Datum::Bytes(substr.as_bytes().to_vec());
            let s = Datum::Bytes(s.as_bytes().to_vec());
            let got = eval_func(ScalarFuncSig::InstrUtf8, &[s, substr]).unwrap();
            assert_eq!(got, Datum::I64(exp))
        }

        let null_cases = vec![
            (Datum::Null, Datum::Bytes(b"".to_vec()), Datum::Null),
            (Datum::Null, Datum::Bytes(b"foobar".to_vec()), Datum::Null),
            (Datum::Bytes(b"".to_vec()), Datum::Null, Datum::Null),
            (Datum::Bytes(b"bar".to_vec()), Datum::Null, Datum::Null),
            (Datum::Null, Datum::Null, Datum::Null),
        ];
        for (substr, s, exp) in null_cases {
            let got = eval_func(ScalarFuncSig::InstrUtf8, &[substr, s]).unwrap();
            assert_eq!(got, exp);
        }
    }

    #[test]
    fn test_instr() {
        let cases: Vec<(&str, &str, i64)> = vec![
            ("a", "abcdefg", 1),
            ("0", "abcdefg", 0),
            ("c", "abcdefg", 3),
            ("F", "abcdefg", 0),
            ("cd", "abcdefg", 3),
            (" ", "abcdefg", 0),
            ("", "", 1),
            (" ", "", 0),
            ("", " ", 1),
            ("eFg", "abcdefg", 0),
            ("deF", "abcdefg", 0),
            (
                "字节",
                "a多字节",
                1 + "a多字节".find("字节").unwrap() as i64,
            ),
            ("a", "a多字节", 1),
            ("bar", "foobarbar", 4),
            ("bAr", "foobarbar", 0),
            (
                "好世",
                "你好世界",
                1 + "你好世界".find("好世").unwrap() as i64,
            ),
        ];

        for (substr, s, exp) in cases {
            let substr = Datum::Bytes(substr.as_bytes().to_vec());
            let s = Datum::Bytes(s.as_bytes().to_vec());
            let got = eval_func(ScalarFuncSig::Instr, &[s, substr]).unwrap();
            assert_eq!(got, Datum::I64(exp))
        }

        let null_cases = vec![
            (Datum::Null, Datum::Bytes(b"".to_vec()), Datum::Null),
            (Datum::Null, Datum::Bytes(b"foobar".to_vec()), Datum::Null),
            (Datum::Bytes(b"".to_vec()), Datum::Null, Datum::Null),
            (Datum::Bytes(b"bar".to_vec()), Datum::Null, Datum::Null),
            (Datum::Null, Datum::Null, Datum::Null),
        ];
        for (substr, s, exp) in null_cases {
            let got = eval_func(ScalarFuncSig::Instr, &[substr, s]).unwrap();
            assert_eq!(got, exp);
        }
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
            let input = Datum::Bytes(input.as_bytes().to_vec());
            let expect_vec = Datum::Bytes(expect.as_bytes().to_vec());
            let got = eval_func(ScalarFuncSig::Quote, &[input]).unwrap();
            assert_eq!(got, expect_vec)
        }

        //check for null
        let got = eval_func(ScalarFuncSig::Quote, &[Datum::Null]).unwrap();
        assert_eq!(got, Datum::Bytes(b"NULL".to_vec()))
    }

    #[test]
    fn test_ord() {
        let cases = vec![
            ("2", 50),
            ("23", 50),
            ("2.3", 50),
            ("", 0),
            ("你好", 14990752),
            ("にほん", 14909867),
            ("한국", 15570332),
            ("👍", 4036989325),
            ("א", 55184),
        ];

        for (input, expect) in cases {
            let input = Datum::Bytes(input.as_bytes().to_vec());
            let expect = Datum::I64(expect);
            let got = eval_func(ScalarFuncSig::Ord, &[input]).unwrap();
            assert_eq!(got, expect);
        }

        let got = eval_func(ScalarFuncSig::Ord, &[Datum::Null]).unwrap();
        assert_eq!(got, Datum::Null);
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

        for (s, sl, expect) in cases {
            let sl = Datum::Bytes(sl.as_bytes().to_vec());
            let s = Datum::Bytes(s.as_bytes().to_vec());
            let got = eval_func(ScalarFuncSig::FindInSet, &[s, sl]).unwrap();
            assert_eq!(got, Datum::I64(expect))
        }

        let null_cases = vec![
            (Datum::Bytes(b"foo".to_vec()), Datum::Null, Datum::Null),
            (Datum::Null, Datum::Bytes(b"bar".to_vec()), Datum::Null),
            (Datum::Null, Datum::Null, Datum::Null),
        ];

        for (s, sl, exp) in null_cases {
            let got = eval_func(ScalarFuncSig::FindInSet, &[s, sl]).unwrap();
            assert_eq!(got, exp);
        }
    }
}
