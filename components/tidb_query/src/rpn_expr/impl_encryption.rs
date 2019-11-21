// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use tidb_query_codegen::rpn_fn;

use openssl::hash::{self, MessageDigest};

use crate::codec::data_type::*;
use crate::codec::{Error, Result};

#[rpn_fn]
#[inline]
pub fn sha1(arg: &Option<Bytes>) -> Result<Option<Bytes>> {
    Ok(match arg {
        Some(arg) => Some(hex_digest(MessageDigest::sha1(), &arg).unwrap()),
        _ => None,
    })
}

#[inline]
fn hex_digest(hashtype: MessageDigest, input: &[u8]) -> Result<Bytes> {
    hash::hash(hashtype, input)
        .map(|digest| hex::encode(digest).into_bytes())
        .map_err(|e| Error::Other(box_err!("OpenSSL error: {:?}", e)))
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
}
