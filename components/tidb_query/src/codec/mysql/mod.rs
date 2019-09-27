// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use super::Result;
use tikv_util::escape;

/// `UNSPECIFIED_FSP` is the unspecified fractional seconds part.
pub const UNSPECIFIED_FSP: i8 = -1;
/// `MAX_FSP` is the maximum digit of fractional seconds part.
pub const MAX_FSP: i8 = 6;
/// `MIN_FSP` is the minimum digit of fractional seconds part.
pub const MIN_FSP: i8 = 0;
/// `DEFAULT_FSP` is the default digit of fractional seconds part.
/// `MySQL` use 0 as the default Fsp.
pub const DEFAULT_FSP: i8 = 0;

fn check_fsp(fsp: i8) -> Result<u8> {
    if fsp == UNSPECIFIED_FSP {
        return Ok(DEFAULT_FSP as u8);
    }
    if fsp > MAX_FSP || fsp < MIN_FSP {
        return Err(invalid_type!("Invalid fsp {}", fsp));
    }
    Ok(fsp as u8)
}

/// Parse string as if it's a fraction part of a number and keep
/// only `fsp` precision.
fn parse_frac(s: &[u8], fsp: u8) -> Result<u32> {
    if s.is_empty() {
        return Ok(0);
    }

    if s.iter().any(|&c| c < b'0' || c > b'9') {
        return Err(invalid_type!("{} contains invalid char", escape(s)));
    }
    let res = s
        .iter()
        .take(fsp as usize + 1)
        .fold(0, |l, r| l * 10 + u32::from(r - b'0'));
    if s.len() > fsp as usize {
        if res % 10 >= 5 {
            Ok(res / 10 + 1)
        } else {
            Ok(res / 10)
        }
    } else {
        Ok(res * 10u32.pow((fsp as usize - s.len()) as u32))
    }
}

pub mod binary_literal;
pub mod charset;
pub mod decimal;
pub mod duration;
pub mod json;
pub mod time;

pub use self::decimal::{dec_encoded_len, Decimal, DecimalDecoder, DecimalEncoder, Res, RoundMode};
pub use self::duration::{Duration, DurationDecoder, DurationEncoder};
pub use self::json::{
    parse_json_path_expr, Json, JsonDecoder, JsonEncoder, ModifyType, PathExpression,
};
pub use self::time::{Time, TimeDecoder, TimeEncoder, TimeType, Tz};

#[cfg(test)]
mod tests {
    #[test]
    fn test_parse_frac() {
        let cases: Vec<(&'static [u8], u8, u32)> = vec![
            (b"", 0, 0),
            (b"", 10, 0),
            (b"1234567", 0, 0),
            (b"1234567", 1, 1),
            (b"0000567", 5, 6),
            (b"1234567", 5, 12346),
            (b"1234567", 6, 123457),
            (b"9999999", 6, 1000000),
            (b"12", 5, 12000),
        ];

        for (s, fsp, exp) in cases {
            let res = super::parse_frac(s, fsp).unwrap();
            assert_eq!(res, exp);
        }

        assert!(
            super::parse_frac(b"00x", 6).is_err(),
            "00x should be invalid for `parse_frac`"
        );
    }
}
