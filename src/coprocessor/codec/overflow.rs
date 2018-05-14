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

use coprocessor::dag::expr::{Error, Result};

// add_u64 adds u64 a and b if no overflow, else returns error.
pub fn add_u64(a: u64, b: u64) -> Result<u64> {
    a.checked_add(b)
        .ok_or_else(|| Error::overflow("BIGINT UNSIGNED", &format!("({} + {})", a, b)))
}

// add_i64 adds i64 a and b if no overflow, otherwise returns error.
pub fn add_i64(a: i64, b: i64) -> Result<i64> {
    a.checked_add(b)
        .ok_or_else(|| Error::overflow("BIGINT", &format!("({} + {})", a, b)))
}

// add_integer adds i64 a and i64 b and returns u64 if no overflow error.
pub fn add_integer(a: u64, b: i64) -> Result<u64> {
    if b >= 0 {
        add_u64(a, b as u64)
    } else {
        sub_u64(a, b.overflowing_neg().0 as u64)
    }
}

// sub_u64 subtracts u64 a with b and returns u64 if no overflow error.
pub fn sub_u64(a: u64, b: u64) -> Result<u64> {
    a.checked_sub(b)
        .ok_or_else(|| Error::overflow("BIGINT UNSIGNED", &format!("({} - {})", a, b)))
}

// sub_i64 subtracts i64 a with b and returns i64 if no overflow error.
pub fn sub_i64(a: i64, b: i64) -> Result<i64> {
    a.checked_sub(b)
        .ok_or_else(|| Error::overflow("BIGINT", &format!("({} - {})", a, b)))
}

// sub_u64_with_i64 subtracts u64 a with i64 b and returns u64 if no overflow error.
pub fn sub_u64_with_i64(a: u64, b: i64) -> Result<u64> {
    if b < 0 {
        add_u64(a, b.overflowing_neg().0 as u64)
    } else {
        sub_u64(a, b as u64)
    }
}

// sub_i64_with_u64 subtracts i64 a with u64 b and returns u64 if no overflow error.
pub fn sub_i64_with_u64(a: i64, b: u64) -> Result<u64> {
    if (a < 0) || ((a as u64) < b) {
        Err(Error::overflow(
            "BIGINT UNSIGNED",
            &format!("({} - {})", a, b),
        ))
    } else {
        Ok(a as u64 - b)
    }
}

// mul_u64 multiplies u64 a and b and returns u64 if no overflow error.
pub fn mul_u64(a: u64, b: u64) -> Result<u64> {
    a.checked_mul(b)
        .ok_or_else(|| Error::overflow("BIGINT UNSIGNED", &format!("({} * {})", a, b)))
}

// mul_i64 multiplies i64 a and b and returns i64 if no overflow error.
pub fn mul_i64(a: i64, b: i64) -> Result<i64> {
    a.checked_mul(b)
        .ok_or_else(|| Error::overflow("BIGINT", &format!("({} * {})", a, b)))
}

// mul_integer multiplies u64 a and i64 b and returns u64 if no overflow error.
pub fn mul_integer(a: u64, b: i64) -> Result<u64> {
    if a == 0 || b == 0 {
        Ok(0)
    } else if b < 0 {
        Err(Error::overflow(
            "UNSIGNED BIGINT",
            &format!("({} * {})", a, b),
        ))
    } else {
        mul_u64(a, b as u64)
    }
}

// div_i64 divides i64 a with b, returns:
// - an Error indicating overflow occurred or the divisor is 0
// - i64 otherwise
pub fn div_i64(a: i64, b: i64) -> Result<i64> {
    if b == 0 {
        Err(Error::division_by_zero())
    } else {
        match a.overflowing_div(b) {
            (_res, true) => Err(Error::overflow(
                "UNSIGNED BIGINT",
                &format!("({} / {})", a, b),
            )),
            (res, false) => Ok(res),
        }
    }
}

// div_u64_with_i64 divides u64 a with i64 b, returns:
// - an Error indicating overflow occurred or the divisor is 0
// - u64 otherwise
pub fn div_u64_with_i64(a: u64, b: i64) -> Result<u64> {
    if b == 0 {
        Err(Error::division_by_zero())
    } else if b < 0 {
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
pub fn div_i64_with_u64(a: i64, b: u64) -> Result<u64> {
    if b == 0 {
        Err(Error::division_by_zero())
    } else if a < 0 {
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
    use coprocessor::dag::expr::{ERR_DATA_OUT_OF_RANGE, ERR_DIVISION_BY_ZERO};
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
    fn test_add() {
        let add_u64_cases: Vec<(u64, u64, u64, bool)> = vec![
            (u64::MAX, 1, 0, true),
            (u64::MAX, 0, u64::MAX, false),
            (1, 1, 2, false),
        ];
        do_test!(add_u64_cases, add_u64);

        let add_i64_cases: Vec<(i64, i64, i64, bool)> = vec![
            (i64::MAX, 1, 0, true),
            (i64::MAX, 0, i64::MAX, false),
            (0, i64::MIN, i64::MIN, false),
            (-1, i64::MIN, 0, true),
            (i64::MAX, i64::MIN, -1, false),
            (1, 1, 2, false),
            (1, -1, 0, false),
        ];
        do_test!(add_i64_cases, add_i64);

        let add_integer_cases: Vec<(u64, i64, u64, bool)> = vec![
            (u64::MAX, i64::MIN, 9223372036854775807, false),
            (i64::MAX as u64, i64::MIN, 0, true),
            (0, -1, 0, true),
            (1, -1, 0, false),
            (0, 1, 1, false),
            (1, 1, 2, false),
        ];
        do_test!(add_integer_cases, add_integer);
    }

    #[test]
    fn test_sub() {
        let sub_u64_cases: Vec<(u64, u64, u64, bool)> = vec![
            (u64::MAX, 1, u64::MAX - 1, false),
            (u64::MAX, 0, u64::MAX, false),
            (0, u64::MAX, 0, true),
            (0, 1, 0, true),
            (1, u64::MAX, 0, true),
            (1, 1, 0, false),
        ];
        do_test!(sub_u64_cases, sub_u64);

        let sub_i64_cases: Vec<(i64, i64, i64, bool)> = vec![
            (i64::MIN, 0, i64::MIN, false),
            (i64::MIN, 1, 0, true),
            (i64::MAX, -1, 0, true),
            (0, i64::MIN, 0, true),
            (-1, i64::MIN, i64::MAX, false),
            (i64::MIN, i64::MAX, 0, true),
            (i64::MIN, i64::MIN, 0, false),
            (i64::MIN, -i64::MAX, -1, false),
            (1, 1, 0, false),
        ];
        do_test!(sub_i64_cases, sub_i64);

        let sub_u64_with_i64_cases: Vec<(u64, i64, u64, bool)> = vec![
            (0, i64::MIN, i64::MIN.overflowing_neg().0 as u64, false),
            (0, 1, 0, true),
            (u64::MAX, i64::MIN, 0, true),
            (i64::MAX as u64, i64::MIN, 2 * i64::MAX as u64 + 1, false),
            (u64::MAX, -1, 0, true),
            (0, -1, 1, false),
            (1, 1, 0, false),
        ];
        do_test!(sub_u64_with_i64_cases, sub_u64_with_i64);

        let sub_i64_with_u64_cases: Vec<(i64, u64, u64, bool)> = vec![
            (i64::MIN, 0, 0, true),
            (i64::MAX, 0, i64::MAX as u64, false),
            (i64::MAX, u64::MAX, 0, true),
            (i64::MAX, i64::MIN.overflowing_neg().0 as u64, 0, true),
            (-1, 0, 0, true),
            (1, 1, 0, false),
        ];
        do_test!(sub_i64_with_u64_cases, sub_i64_with_u64);
    }

    #[test]
    fn test_mul() {
        let mul_u64_cases: Vec<(u64, u64, u64, bool)> = vec![
            (u64::MAX, 1, u64::MAX, false),
            (u64::MAX, 0, 0, false),
            (u64::MAX, 2, u64::MAX, true),
            (1, 1, 1, false),
        ];
        do_test!(mul_u64_cases, mul_u64);

        let mul_i64_cases: Vec<(i64, i64, i64, bool)> = vec![
            (i64::MAX, 1, i64::MAX, false),
            (i64::MIN, 1, i64::MIN, false),
            (i64::MAX, -1, -i64::MAX, false),
            (i64::MIN, -1, 0, true),
            (i64::MIN, 0, 0, false),
            (i64::MAX, 0, 0, false),
            (i64::MAX, i64::MAX, 0, true),
            (i64::MAX, i64::MIN, 0, true),
            (i64::MIN / 10, 11, 0, true),
            (1, 1, 1, false),
        ];
        do_test!(mul_i64_cases, mul_i64);

        let mul_integer_cases: Vec<(u64, i64, u64, bool)> = vec![
            (u64::MAX, 0, 0, false),
            (0, -1, 0, false),
            (1, -1, 0, true),
            (u64::MAX, -1, 0, true),
            (u64::MAX, 10, 0, true),
            (1, 1, 1, false),
        ];
        do_test!(mul_integer_cases, mul_integer);
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
