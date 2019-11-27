// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use openssl::hash::{self, MessageDigest};
use tidb_query_codegen::rpn_fn;

use super::super::expr::{Error, EvalContext};
use crate::codec::data_type::*;
use crate::Result;

use flate2::read::ZlibDecoder;
use std::io::Read;

#[rpn_fn]
#[inline]
pub fn sha1(arg: &Option<Bytes>) -> Result<Option<Bytes>> {
    Ok(match arg {
        Some(arg) => {
            match hex_digest(MessageDigest::sha1(), arg) {
                Ok(s) => return Ok(Some(s)),
                Err(err) => return Err(err),
            };
        }
        _ => None,
    })
}

#[rpn_fn(capture = [ctx])]
#[inline]
pub fn uncompress(ctx: &mut EvalContext, arg: &Option<Bytes>) -> Result<Option<Bytes>> {
    use byteorder::{ByteOrder, LittleEndian};
    if let Some(s) = arg {
        if s.is_empty() {
            Ok(Some(b"".to_vec()))
        } else if s.len() <= 4 {
            ctx.warnings.append_warning(Error::zlib_data_corrupted());
            Ok(None)
        } else {
            let len = LittleEndian::read_u32(&s[0..4]) as usize;
            let mut decoder = ZlibDecoder::new(&s[4..]);
            let mut vec = Vec::with_capacity(len);
            match decoder.read_to_end(&mut vec) {
                Ok(decoded_len) if len >= decoded_len && decoded_len != 0 => Ok(Some(vec)),
                Ok(decoded_len) if len < decoded_len => {
                    ctx.warnings.append_warning(Error::zlib_length_corrupted());
                    Ok(None)
                }
                _ => {
                    ctx.warnings.append_warning(Error::zlib_data_corrupted());
                    Ok(None)
                }
            }
        }
    } else {
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rpn_expr::types::test_util::RpnFnScalarEvaluator;
    use tipb::ScalarFuncSig;

    #[test]
    fn test_sha1() {
        let cases = vec![
            (Some(""), Some("da39a3ee5e6b4b0d3255bfef95601890afd80709")),
            (Some("a"), Some("86f7e437faa5a7fce15d1ddcb9eaeaea377667b8")),
            (Some("ab"), Some("da23614e02469a0d7c7bd1bdab5c9c474b1904dc")),
            (
                Some("abc"),
                Some("a9993e364706816aba3e25717850c26c9cd0d89d"),
            ),
            (
                Some("123"),
                Some("40bd001563085fc35165329ea1ff5c5ecbdbbeef"),
            ),
            (None, None),
        ];

        for (arg, expect_output) in cases {
            let arg = arg.map(|s| s.as_bytes().to_vec());
            let expect_output = expect_output.map(|s| Bytes::from(s));

            let output = RpnFnScalarEvaluator::new()
                .push_param(arg)
                .evaluate(ScalarFuncSig::Sha1)
                .unwrap();
            assert_eq!(output, expect_output);
        }
    }

    #[test]
    fn test_uncompress() {
        use hex;
        let cases = vec![
            ("", Some(b"".to_vec())),
            (
                "0B000000789CCB48CDC9C95728CF2FCA4901001A0B045D",
                Some(b"hello world".to_vec()),
            ),
            (
                "0C000000789CCB48CDC9C95728CF2F32303402001D8004202E",
                Some(b"hello wor012".to_vec()),
            ),
            // length is greater than the string
            (
                "12000000789CCB48CDC9C95728CF2FCA4901001A0B045D",
                Some(b"hello world".to_vec()),
            ),
            ("010203", None),
            ("01020304", None),
            ("020000000000", None),
            // ZlibDecoder#read_to_end return 0
            ("0000000001", None),
            // length is less than the string
            ("02000000789CCB48CDC9C95728CF2FCA4901001A0B045D", None),
        ];
        for (arg, exp) in cases {
            let output = RpnFnScalarEvaluator::new()
                .push_param(Some(hex::decode(arg.as_bytes().to_vec()).unwrap()))
                .evaluate(ScalarFuncSig::Uncompress)
                .unwrap();
            assert_eq!(output, exp)
        }
    }
}
