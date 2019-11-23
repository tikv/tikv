// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use tidb_query_codegen::rpn_fn;

use super::super::expr::{Error, EvalContext};
use crate::codec::data_type::*;
use crate::Result;

use flate2::read::ZlibDecoder;
use std::io::Read;

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
    use tipb::ScalarFuncSig;

    use crate::rpn_expr::test_util::RpnFnScalarEvaluator;

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
