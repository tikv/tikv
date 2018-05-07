// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

use super::{EvalContext, FnCall, Result};
use coprocessor::codec::Datum;
use std::i64;

impl FnCall {
    // Evaluate length of the string str in bits
    // See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_bit-length
    #[inline]
    pub fn bit_length(&self, ctx: &mut EvalContext, row: &[Datum]) -> Result<Option<i64>> {
        let lhs = try_opt!(self.children[0].eval_string(ctx, row));
        Ok(Some((lhs.len() * 8) as i64))
    }
}

#[cfg(test)]
mod test {
    use coprocessor::codec::Datum;
    use coprocessor::dag::expr::test::{datum_expr, fncall_expr};
    use coprocessor::dag::expr::{EvalContext, Expression};
    use tipb::expression::ScalarFuncSig;

    #[test]
    fn test_bit_length() {
        let cases = vec![
            (Datum::Bytes(b"hi".to_owned().to_vec()), Datum::I64(16)),
            (Datum::Bytes("你好".as_bytes().to_owned()), Datum::I64(48)),
            (Datum::Bytes(b"".to_owned().to_vec()), Datum::I64(0)),
            (Datum::Null, Datum::Null),
        ];
        let mut ctx = EvalContext::default();
        for (arg, exp) in cases {
            let args = &[datum_expr(arg)];
            let op =
                Expression::build(&mut ctx, fncall_expr(ScalarFuncSig::BitLength, args)).unwrap();
            let res = op.eval(&mut ctx, &[]).unwrap();
            assert_eq!(res, exp);
        }
    }
}
