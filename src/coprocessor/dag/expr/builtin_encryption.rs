// Copyright 2018 PingCAP, Inc.
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

use std::borrow::Cow;

use super::{EvalContext, Result, ScalarFunc};
use coprocessor::codec::Datum;
use crypto::{digest::Digest, md5::Md5, sha1::Sha1};

impl ScalarFunc {
    pub fn md5<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &[Datum],
    ) -> Result<Option<Cow<'a, [u8]>>> {
        let input = try_opt!(self.children[0].eval_string(ctx, row));
        let mut hasher = Md5::new();
        hasher.input(input.as_ref());
        let md5 = hasher.result_str().into_bytes();
        Ok(Some(Cow::Owned(md5)))
    }

    pub fn sha1<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &[Datum],
    ) -> Result<Option<Cow<'a, [u8]>>> {
        let input = try_opt!(self.children[0].eval_string(ctx, row));
        let mut hasher = Sha1::new();
        hasher.input(input.as_ref());
        let sha1 = hasher.result_str().into_bytes();
        Ok(Some(Cow::Owned(sha1)))
    }
}

#[cfg(test)]
mod test {
    use coprocessor::codec::Datum;
    use coprocessor::dag::expr::test::{datum_expr, scalar_func_expr};
    use coprocessor::dag::expr::{EvalContext, Expression};
    use tipb::expression::ScalarFuncSig;

    #[test]
    fn test_md5() {
        let cases = vec![
            ("", "d41d8cd98f00b204e9800998ecf8427e"),
            ("a", "0cc175b9c0f1b6a831c399e269772661"),
            ("ab", "187ef4436122d1cc2f40dc2b92f0eba0"),
            ("abc", "900150983cd24fb0d6963f7d28e17f72"),
            ("123", "202cb962ac59075b964b07152d234b70"),
        ];
        let mut ctx = EvalContext::default();

        for (input_str, exp_str) in cases {
            let input = datum_expr(Datum::Bytes(input_str.as_bytes().to_vec()));
            let op = scalar_func_expr(ScalarFuncSig::MD5, &[input]);
            let op = Expression::build(&mut ctx, op).unwrap();
            let got = op.eval(&mut ctx, &[]).unwrap();
            let exp = Datum::Bytes(exp_str.as_bytes().to_vec());
            assert_eq!(got, exp, "md5('{:?}')", input_str);
        }

        // test NULL case
        let input = datum_expr(Datum::Null);
        let op = scalar_func_expr(ScalarFuncSig::MD5, &[input]);
        let op = Expression::build(&mut ctx, op).unwrap();
        let got = op.eval(&mut ctx, &[]).unwrap();
        let exp = Datum::Null;
        assert_eq!(got, exp, "md5(NULL)");
    }

    #[test]
    fn test_sha1() {
        let cases = vec![
            ("", "da39a3ee5e6b4b0d3255bfef95601890afd80709"),
            ("a", "86f7e437faa5a7fce15d1ddcb9eaeaea377667b8"),
            ("ab", "da23614e02469a0d7c7bd1bdab5c9c474b1904dc"),
            ("abc", "a9993e364706816aba3e25717850c26c9cd0d89d"),
            ("123", "40bd001563085fc35165329ea1ff5c5ecbdbbeef"),
        ];
        let mut ctx = EvalContext::default();

        for (input_str, exp_str) in cases {
            let input = datum_expr(Datum::Bytes(input_str.as_bytes().to_vec()));
            let op = scalar_func_expr(ScalarFuncSig::SHA1, &[input]);
            let op = Expression::build(&mut ctx, op).unwrap();
            let got = op.eval(&mut ctx, &[]).unwrap();
            let exp = Datum::Bytes(exp_str.as_bytes().to_vec());
            assert_eq!(got, exp, "sha1('{:?}')", input_str);
        }

        // test NULL case
        let input = datum_expr(Datum::Null);
        let op = scalar_func_expr(ScalarFuncSig::SHA1, &[input]);
        let op = Expression::build(&mut ctx, op).unwrap();
        let got = op.eval(&mut ctx, &[]).unwrap();
        let exp = Datum::Null;
        assert_eq!(got, exp, "sha1(NULL)");
    }
}
