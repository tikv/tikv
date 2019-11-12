use tidb_query_codegen::rpn_fn;

use crate::codec::data_type::Bytes;
use crate::error::Result;

#[rpn_fn]
#[inline]
pub fn length(arg: &Option<Bytes>) -> Result<Option<i64>> {
    Ok(match arg {
        Some(s) => Some(s.len() as i64),
        _ => None,
    })
}

#[cfg(test)]
mod tests {
    use tipb::ScalarFuncSig;

    use crate::rpn_expr::types::test_util::RpnFnScalarEvaluator;

    #[test]
    fn test_length() {
        let test_cases = vec![
            (ScalarFuncSig::Length, None, None),
            (ScalarFuncSig::Length, Some(""), Some(0i64)),
            (ScalarFuncSig::Length, Some("你好"), Some(6i64)),
            (ScalarFuncSig::Length, Some("TiKV"), Some(4i64)),
            (
                ScalarFuncSig::Length,
                Some("あなたのことが好きです"),
                Some(33i64),
            ),
            (
                ScalarFuncSig::Length,
                Some("분산 데이터베이스"),
                Some(25i64),
            ),
            (
                ScalarFuncSig::Length,
                Some("россия в мире  кубок"),
                Some(38i64),
            ),
            (ScalarFuncSig::Length, Some("قاعدة البيانات"), Some(27i64)),
        ];

        for (sig, arg, expect_output) in test_cases {
            let arg = arg.map(|s| s.as_bytes().to_vec());
            let output = RpnFnScalarEvaluator::new()
                .push_param(arg)
                .evaluate(sig)
                .unwrap();
            assert_eq!(output, expect_output);
        }
    }
}
