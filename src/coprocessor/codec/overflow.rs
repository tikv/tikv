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

use coprocessor::codec::{Error, Result};

// div_i64 divides i64 a with b, returns:
// - an Error indicating overflow occurred or the divisor is 0
// - i64 otherwise
#[inline]
pub fn div_i64(a: i64, b: i64) -> Result<i64> {
    if b == 0 {
        return Err(Error::division_by_zero());
    }
    match a.overflowing_div(b) {
        (_res, true) => Err(Error::overflow(
            "UNSIGNED BIGINT",
            &format!("({} / {})", a, b),
        )),
        (res, false) => Ok(res),
    }
}

// div_u64_with_i64 divides u64 a with i64 b, returns:
// - an Error indicating overflow occurred or the divisor is 0
// - u64 otherwise
#[inline]
pub fn div_u64_with_i64(a: u64, b: i64) -> Result<u64> {
    if b == 0 {
        return Err(Error::division_by_zero());
    }
    if b < 0 {
        if a != 0 && (b.overflowing_neg().0 as u64) <= a {
            Err(Error::overflow(
                "UNSIGNED BIGINT",
                &format!("({} / {})", a, b),
            ))
        } else {
            Ok(0)
        }
    } else {
        Ok(a / b as u64)
    }
}

// div_i64_with_u64 divides i64 a with u64 b, returns:
// - an Error indicating overflow occurred or the divisor is 0
// - u64 otherwise
#[inline]
pub fn div_i64_with_u64(a: i64, b: u64) -> Result<u64> {
    if b == 0 {
        return Err(Error::division_by_zero());
    }
    if a < 0 {
        if a.overflowing_neg().0 as u64 >= b {
            Err(Error::overflow(
                "UNSIGNED BIGINT",
                &format!("({} / {})", a, b),
            ))
        } else {
            Ok(0)
        }
    } else {
        Ok(a as u64 / b)
    }
}

#[cfg(test)]
mod test {
    use coprocessor::codec::error::{ERR_DATA_OUT_OF_RANGE, ERR_DIVISION_BY_ZERO};
    use std::{i64, u64};

    macro_rules! do_test {
        ($cases:ident, $func:ident) => {
            for (lsh, rsh, exp, is_overflow) in $cases {
                let desc = format!("Error testing {}({}, {})", stringify!($func), lsh, rsh);
                match super::$func(lsh, rsh) {
                    Ok(res) => {
                        assert!(!is_overflow, "{}: overflowed unexpectedly", desc);
                        assert_eq!(res, exp, "{}: expect {} but got {}", desc, exp, res);
                    }
                    Err(e) => {
                        assert!(is_overflow, "{}: expect overflow", desc);
                        assert_eq!(e.code(), ERR_DATA_OUT_OF_RANGE);
                    }
                }
            }
        };
    }

    #[test]
    fn test_div() {
        let div_i64_cases: Vec<(i64, i64, i64, bool)> = vec![
            (i64::MAX, 1, i64::MAX, false),
            (i64::MIN, 1, i64::MIN, false),
            (i64::MIN, -1, 0, true),
            (i64::MAX, -1, -i64::MAX, false),
            (1, -1, -1, false),
            (-1, 1, -1, false),
            (-1, 2, 0, false),
            (i64::MIN, 2, i64::MIN / 2, false),
        ];
        do_test!(div_i64_cases, div_i64);

        let div_u64_with_i64_cases: Vec<(u64, i64, u64, bool)> = vec![
            (0, -1, 0, false),
            (1, -1, 0, true),
            (i64::MAX as u64, i64::MIN, 0, false),
            (i64::MAX as u64, -1, 0, true),
        ];
        do_test!(div_u64_with_i64_cases, div_u64_with_i64);

        let div_i64_with_u64_cases: Vec<(i64, u64, u64, bool)> = vec![
            (i64::MIN, i64::MAX as u64, 0, true),
            (0, 1, 0, false),
            (-1, i64::MAX as u64, 0, false),
        ];
        do_test!(div_i64_with_u64_cases, div_i64_with_u64);

        assert_eq!(
            super::div_i64(0, 0).unwrap_err().code(),
            ERR_DIVISION_BY_ZERO
        );
        assert_eq!(
            super::div_u64_with_i64(0, 0).unwrap_err().code(),
            ERR_DIVISION_BY_ZERO
        );
        assert_eq!(
            super::div_i64_with_u64(0, 0).unwrap_err().code(),
            ERR_DIVISION_BY_ZERO
        );
    }
}
