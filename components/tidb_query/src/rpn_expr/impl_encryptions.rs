use tidb_query_codegen::rpn_fn;

use crate::codec::{data_type::*, Result};
use crate::expr_util;
use openssl::hash::MessageDigest;

#[rpn_fn]
#[inline]
pub fn md5(arg: &Option<Bytes>) -> Result<Option<Bytes>> {
    if arg.is_none() {
        return Ok(None);
    }
    expr_util::hex_digest::hex_digest(MessageDigest::md5(), &arg.as_ref().unwrap())
        .map(|digest| Some(digest))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rpn_expr::types::test_util::RpnFnScalarEvaluator;
    use tipb::ScalarFuncSig;

    #[test]
    fn test_md5() {
        let cases = vec![
            (None, None),
            (Some(""), Some("d41d8cd98f00b204e9800998ecf8427e")),
            (Some("a"), Some("0cc175b9c0f1b6a831c399e269772661")),
            (Some("ab"), Some("187ef4436122d1cc2f40dc2b92f0eba0")),
            (Some("abc"), Some("900150983cd24fb0d6963f7d28e17f72")),
            (Some("123"), Some("202cb962ac59075b964b07152d234b70")),
        ];
        for (arg, expect_output) in cases {
            let arg = arg.map(|s| s.as_bytes().to_vec());
            let expect_output = expect_output.map(|s| Bytes::from(s));

            let output = RpnFnScalarEvaluator::new()
                .push_param(arg.clone())
                .evaluate(ScalarFuncSig::Md5)
                .unwrap();
            assert_eq!(output, expect_output, "{:?}", arg);
        }
    }
}
