// Copyright 2016 PingCAP, Inc.
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

use num::{self, One, Signed, Zero, ToPrimitive};
use num::bigint::{Sign, BigUint, BigInt};
use num::integer::Integer;
use std::cmp::{self, Ordering};
use std::io::Write;
use std::ops::Add;
use std::fmt::{self, Display, Formatter};
use std::str::{self, FromStr};
use std::{i32, u64};
use byteorder::{ReadBytesExt, WriteBytesExt};

use util::codec::{Error, Result};
use util::codec::number;
use util::codec::bytes::{self, BytesEncoder, BytesDecoder};

const NEGATIVE_SIGN: u8 = 8;
const ZERO_SIGN: u8 = 16;
const POSITIVE_SIGN: u8 = 24;

/// Decimal represents a fixed-point decimal. It is immutable.
///
/// The value should be `coeff` * 10<sup>`exp`</sup>.
/// Also refer [Decimal Arithmetic Specification](http://speleotrove.com/decimal/decarith.html)
#[derive(Clone, Debug)]
pub struct Decimal {
    coeff: BigInt,
    exp: i32,
    // fsp means how many fraction digit should be preseved.
    fsp: u8,
}

pub const MAX_FSP: u8 = 30;

macro_rules! enable_conv_for_int {
    ($t:ty) => {
        impl From<$t> for Decimal {
            fn from(t: $t) -> Decimal {
                Decimal {
                    coeff: t.into(),
                    exp: 0,
                    fsp: 0,
                }
            }
        }
    };
}

enable_conv_for_int!(u32);
enable_conv_for_int!(u64);
enable_conv_for_int!(u16);
enable_conv_for_int!(u8);
enable_conv_for_int!(i32);
enable_conv_for_int!(i64);
enable_conv_for_int!(i16);
enable_conv_for_int!(i8);
enable_conv_for_int!(usize);
enable_conv_for_int!(isize);

impl Decimal {
    /// Convert a float number to decimal.
    ///
    /// This function will use float's canonical string representation
    /// rather than the accurate value the float represent.
    pub fn from_f64(f: f64) -> Result<Decimal> {
        if !f.is_finite() {
            return Err(invalid_type!("{} can't be convert to decimal'", f));
        }

        let s = format!("{}", f);
        Decimal::from_str(&s)
    }

    /// Construct a decimal using the specified coefficient, exponent and fsp.
    ///
    /// If constucted decimal contains more fraction digit than fsp, they will be
    /// rounded.
    pub fn new(coeff: BigInt, mut exp: i32, mut fsp: u8) -> Decimal {
        fsp = cmp::min(fsp, MAX_FSP);

        if coeff.is_zero() {
            exp = 0;
        }

        let d = Decimal {
            coeff: coeff,
            exp: exp,
            fsp: fsp,
        };

        if exp >= 0 {
            return d;
        }

        if fsp as i32 + exp < 0 {
            return d.rescale(-(fsp as i32)).unwrap();
        }
        d
    }

    /// Remove extra zero in `coeff`.
    ///
    /// For example, 1000 * 10<sup>3</sup> will be compact to 1 * 10<sup>6</sup>,
    /// 1000 * 10<sup>-3</sup> will be compact to 1 * 10<sup>0</sup>.
    pub fn compact(mut self) -> Decimal {
        if self.is_zero() {
            return self;
        }
        let ten = 10.into();
        loop {
            let res = self.coeff.div_rem(&ten);
            if !res.1.is_zero() {
                break;
            }
            self.coeff = res.0;
            self.exp += 1;
        }
        self
    }

    /// Adjust decimal's exponent to specified value.
    ///
    /// This may cause the coefficient grows or decreases.
    /// For example, 12345 * 10<sup>3</sup> rescale with 4 will be represented
    /// as 1235 * 10<sup>4</sup>; 12345 * 10<sup>-3</sup> rescale with -5 will
    /// be represented as 1234500 * 10<sup>-5</sup>
    pub fn rescale(&self, mut exp: i32) -> Option<Decimal> {
        if exp < 0 {
            exp = cmp::max(exp, -(MAX_FSP as i32));
        }
        if self.exp == exp {
            return None;
        }

        let to_add = exp - self.exp;
        let coeff = if to_add > 0 {
            let divider = BigInt::from(num::pow(BigUint::from(10u8), to_add as usize));
            let (mut coeff, rem) = self.coeff.div_rem(&divider);
            if rem >= divider / BigInt::from(2) {
                coeff = coeff + BigInt::one();
            }
            coeff
        } else {
            self.coeff.clone() * BigInt::from(num::pow(BigUint::from(10u8), -to_add as usize))
        };

        if coeff.is_zero() && exp > 0 {
            exp = 0;
        }

        Some(Decimal {
            coeff: coeff,
            exp: exp,
            fsp: if exp < 0 { -exp as u8 } else { 0 },
        })
    }

    /// Convert the decimal to float value.
    ///
    /// Please note that this convertion may lose precision.
    pub fn to_f64(&self) -> Result<f64> {
        let s = format!("{}", self);
        // Can this line really return error?
        let f = box_try!(s.parse::<f64>());
        Ok(f)
    }

    /// Test if the decimal is negative.
    pub fn is_negative(&self) -> bool {
        self.coeff.is_negative()
    }

    /// Test if the decimal is zero.
    pub fn is_zero(&self) -> bool {
        self.coeff.is_zero()
    }

    /// Get the max needed capacity to encode this decimal.
    ///
    /// see also `encode_decimal`.
    pub fn max_encode_bytes(&self) -> usize {
        let bits_cnt = self.coeff.bits();

        // 2 * 2 * 2 ≈ 10
        let number_cnt = (bits_cnt + 2) / 3;

        let basic_size = number::I64_SIZE + bytes::max_encoded_bytes_size(number_cnt);
        if self.is_negative() {
            basic_size + 1
        } else {
            basic_size
        }
    }

    /// Get the int part of this decimal.
    ///
    /// Return None if overflow.
    pub fn i64(&self) -> Option<i64> {
        match self.rescale(0) {
            Some(d) => d.i64(),
            None => self.coeff.to_i64(),
        }
    }

    /// Truncate truncates off digits from the number, without rounding.
    /// Note: this function will round the last digit, which is not compatible with
    /// TiDB. But since we are about to use `MyDecimal` very soon, so let's keep it now.
    pub fn truncate(&self, precision: u8) -> Decimal {
        let mut d = if -(precision as i32) > self.exp {
            self.rescale(-(precision as i32)).unwrap()
        } else {
            self.clone()
        };
        d.fsp = precision as u8;
        d
    }
}

impl FromStr for Decimal {
    type Err = Error;

    /// Parse the decimal value from string.
    fn from_str(s: &str) -> Result<Decimal> {
        let bytes = s.as_bytes();
        let (mut dot_pos, mut e_pos) = (bytes.len(), bytes.len());
        let mut filtered_bytes = Vec::with_capacity(bytes.len());
        for (i, &c) in bytes.iter().enumerate() {
            if (c >= b'0' && c <= b'9') || c == b'-' || c == b'+' {
                filtered_bytes.push(c);
                continue;
            }
            if c == b'.' {
                if dot_pos < bytes.len() {
                    return Err(invalid_type!("{} contains at least 2 dot", s));
                }
                dot_pos = i;
                continue;
            } else if c == b'e' || c == b'E' {
                e_pos = i;
                if e_pos == bytes.len() - 1 {
                    return Err(invalid_type!("{} is invalid decimal", s));
                }
                break;
            }
            return Err(invalid_type!("{} is invalid decimal", s));
        }

        if filtered_bytes.is_empty() {
            return Err(invalid_type!("{} is invalid decimal", s));
        }

        if dot_pos < bytes.len() && dot_pos > e_pos {
            return Err(invalid_type!("{} is invalid decimal", s));
        }

        let mut exp = if e_pos < bytes.len() {
            let s = unsafe { str::from_utf8_unchecked(&bytes[e_pos + 1..]) };
            box_try!(i32::from_str(s))
        } else {
            0
        };
        if dot_pos < bytes.len() {
            exp += dot_pos as i32 + 1 - e_pos as i32;
        }

        let fsp = if exp < 0 {
            cmp::min(-exp as u8, MAX_FSP)
        } else {
            0
        };

        let mut last_pos = filtered_bytes.len() - 1;
        while last_pos > 0 && filtered_bytes[last_pos] == b'0' {
            last_pos -= 1;
            exp += 1;
        }
        if filtered_bytes[last_pos] == b'-' || filtered_bytes[last_pos] == b'+' {
            last_pos = cmp::min(last_pos + 1, filtered_bytes.len() - 1);
        }

        let coeff = box_try!(BigInt::from_str(unsafe {
            str::from_utf8_unchecked(&filtered_bytes[..last_pos + 1])
        }));

        Ok(Decimal::new(coeff, exp, fsp))
    }
}

impl Display for Decimal {
    /// Format the decimal with fixed point.
    fn fmt(&self, formatter: &mut Formatter) -> fmt::Result {
        if self.is_zero() && self.fsp == 0 {
            return write!(formatter, "0");
        }
        if self.exp == 0 && self.fsp == 0 {
            return write!(formatter, "{}", self.coeff);
        }

        if self.exp >= 0 {
            let mut v = format!("{}", self.coeff).into_bytes();
            if self.fsp == 0 {
                let exp_len = v.len() + self.exp as usize;
                v.resize(exp_len, b'0');
            } else {
                let point_pos = v.len() + self.exp as usize;
                let exp_len = point_pos + self.fsp as usize + 1;
                v.resize(exp_len, b'0');
                v[point_pos] = b'.';
            }
            return write!(formatter, "{}", unsafe { str::from_utf8_unchecked(&v) });
        }

        let s = match self.rescale(-(self.fsp as i32)) {
            Some(s) => format!("{}", s.coeff).into_bytes(),
            None => format!("{}", self.coeff).into_bytes(),
        };

        let vlen = if self.is_negative() {
            s.len() - 1
        } else {
            s.len()
        };

        let mut v;
        if self.fsp as usize >= vlen {
            v = Vec::with_capacity(self.fsp as usize + 3);
            if self.is_negative() {
                v.push(b'-');
            }
            v.extend_from_slice(b"0.");
            for _ in 0..self.fsp as usize - vlen {
                v.push(b'0');
            }
            v.extend_from_slice(&s[s.len() - vlen..]);
        } else {
            v = Vec::with_capacity(s.len() + 1);
            let insert_pos = s.len() - self.fsp as usize;
            v.extend_from_slice(&s[..insert_pos]);
            v.push(b'.');
            v.extend_from_slice(&s[insert_pos..]);
        }

        write!(formatter, "{}", unsafe { str::from_utf8_unchecked(&v) })
    }
}

impl PartialEq for Decimal {
    fn eq(&self, right: &Decimal) -> bool {
        self.cmp(right) == Ordering::Equal
    }
}

impl PartialOrd for Decimal {
    fn partial_cmp(&self, right: &Decimal) -> Option<Ordering> {
        Some(self.cmp(right))
    }
}

impl Eq for Decimal {}

impl Ord for Decimal {
    fn cmp(&self, right: &Decimal) -> Ordering {
        if self.coeff.sign() < right.coeff.sign() {
            return Ordering::Less;
        } else if self.coeff.sign() > right.coeff.sign() {
            return Ordering::Greater;
        }

        let exp = cmp::min(self.exp, right.exp);

        if let Some(l) = self.rescale(exp) {
            l.coeff.cmp(&right.coeff)
        } else if let Some(r) = right.rescale(exp) {
            self.coeff.cmp(&r.coeff)
        } else {
            self.coeff.cmp(&right.coeff)
        }
    }
}

impl Add<Decimal> for Decimal {
    type Output = Decimal;

    fn add(self, rhs: Decimal) -> Decimal {
        let fsp = cmp::max(self.fsp, rhs.fsp);
        let exp = cmp::min(self.exp, rhs.exp);

        // TODO: check overflow
        let res = if let Some(l) = self.rescale(exp) {
            l.coeff.add(&rhs.coeff)
        } else if let Some(r) = rhs.rescale(exp) {
            self.coeff.add(&r.coeff)
        } else {
            self.coeff.add(&rhs.coeff)
        };

        let d = Decimal::new(res, exp, fsp);
        d.compact()
    }
}

pub trait DecimalEncoder: BytesEncoder {
    /// Encode decimal to compareable bytes.
    ///
    /// Currently, the decimal is encoded as
    ///    sign + exponent + fraction_part
    fn encode_decimal(&mut self, d: &Decimal) -> Result<()> {
        if d.is_zero() {
            return self.write_u8(ZERO_SIGN).map_err(From::from);
        } else if d.is_negative() {
            try!(self.write_u8(NEGATIVE_SIGN));
        } else {
            try!(self.write_u8(POSITIVE_SIGN));
        }

        let s = format!("{}", d.coeff).into_bytes();
        let mut truncated = if s[0] == b'-' || s[0] == b'+' {
            &s[1..]
        } else {
            s.as_slice()
        };

        // To make the decimal encoded bytes compareable, we
        // need to encode decimal to only contains fraction part.
        // For example, origin form is 1234 * (10 ** 3), will be encoded
        // as 0.1234 * (10 ** 7); 1234 * (10 ** -2) will be encoded
        // as 0.1234 * (10 ** 2).
        let digit_cnt = truncated.len();
        let exp = d.exp + digit_cnt as i32;

        let mut pos = truncated.len();
        while pos > 0 && truncated[pos - 1] == b'0' {
            pos -= 1;
        }
        truncated = &truncated[..pos];

        if d.is_negative() {
            try!(self.encode_i64(-exp as i64));
            self.encode_bytes(truncated, true)
        } else {
            try!(self.encode_i64(exp as i64));
            self.encode_bytes(truncated, false)
        }
    }
}

impl<T: Write> DecimalEncoder for T {}

/// Return the first encoded decimal's length.
///
/// Please note that this function won't check if the decimal is
/// encoded correctly.
pub fn encoded_len(encoded: &[u8]) -> usize {
    if encoded.is_empty() {
        return 0;
    }
    let (is_neg, prefix_len) = match encoded[0] {
        ZERO_SIGN => return 1,
        NEGATIVE_SIGN => (true, 1 + number::I64_SIZE),
        POSITIVE_SIGN => (false, 1 + number::I64_SIZE),
        _ => return 0,
    };

    if encoded.len() < prefix_len {
        return encoded.len();
    }

    bytes::encoded_bytes_len(&encoded[prefix_len..], is_neg) + prefix_len
}

pub trait DecimalDecoder: BytesDecoder {
    fn decode_decimal(&mut self) -> Result<Decimal> {
        let (sign, mut exp) = match try!(self.read_u8()) {
            ZERO_SIGN => return Ok(0.into()),
            NEGATIVE_SIGN => (Sign::Minus, -try!(self.decode_i64())),
            POSITIVE_SIGN => (Sign::Plus, try!(self.decode_i64())),
            s => return Err(invalid_type!("unrecognize sign: {}", s)),
        };

        // TODO: add precision limit.
        if exp < i32::MIN as i64 || exp > i32::MAX as i64 {
            return Err(invalid_type!("unable to decode decimal: {} is not i32", exp));
        }

        let bs = if sign == Sign::Plus {
            try!(self.decode_bytes(false))
        } else {
            try!(self.decode_bytes(true))
        };
        let s = box_try!(str::from_utf8(&bs));
        let v = box_try!(BigUint::from_str(s));
        let coeff = BigInt::from_biguint(sign, v);

        exp -= bs.len() as i64;

        let fsp = if exp < 0 {
            cmp::min(-exp as u8, MAX_FSP)
        } else {
            0
        };

        Ok(Decimal::new(coeff, exp as i32, fsp))
    }
}

impl<T: BytesDecoder> DecimalDecoder for T {}

#[allow(approx_constant)]
#[cfg(test)]
mod test {
    use std::f64;
    use std::f32;
    use std::str::FromStr;

    use super::*;

    macro_rules! assert_f64_eq {
        ($l:expr, $r:expr) => (assert!(($l - $r).abs() < f64::EPSILON));
        ($tag:expr, $l:expr, $r:expr) => (assert!(($l - $r).abs() < f64::EPSILON, $tag));
    }

    #[test]
    fn test_decimal_parse() {
        let cases = vec![
            ("1.21", 1.21, "1.21"),
            (".21", 0.21, "0.21"),
            ("1.00", 1.0, "1.00"),
            ("100", 100f64, "100"),
            ("-100", -100f64, "-100"),
            ("100.00", 100.00, "100.00"),
            ("00100.00", 100.00, "100.00"),
            ("-100.00", -100.00, "-100.00"),
            ("-0.00", 0f64, "0.00"),
            ("00.00", 0f64, "0.00"),
            ("0.00", 0f64, "0.00"),
            ("-2.010", -2.01, "-2.010"),
            ("12345", 12345f64, "12345"),
            ("-12345", -12345f64, "-12345"),
            ("-3.", -3f64, "-3"),
            ("1.456e3", 1456f64, "1456"),
            ("3.", 3f64, "3"),
        ];

        for (s, f, t) in cases {
            let d = Decimal::from_str(s).unwrap();
            assert_eq!(format!("{}", d), t);
            assert_f64_eq!(s, d.to_f64().unwrap(), f);
        }
    }

    #[test]
    fn test_decimal_codec() {
        let cases = vec![
            123400f64, 1234f64, 12.34, 0.1234, 0.01234, -0.1234,
            -0.01234, 12.3400, -12.34, 0.00000, 0f64, -0.0, -0.000,
        ];

        for c in cases {
            let tag = format!("checking {}", c);
            let d = Decimal::from_f64(c).expect(&tag);
            d.max_encode_bytes();
            let mut v = vec![];
            v.encode_decimal(&d).expect(&tag);
            let dd = v.as_slice().decode_decimal().expect(&tag);
            assert_eq!(d, dd);
            let f = dd.to_f64().expect(&tag);
            assert_f64_eq!(f, c);
        }
    }

    #[test]
    fn test_parse_decimal() {
        let cases = vec![
            (3.141592653589793, "3.141592653589793"),
            (3f64, "3"),
            (1234567890123456f64, "1234567890123456"),
            (1234567890123456000f64, "1234567890123456000"),
            (1234.567890123456, "1234.567890123456"),
            (0.1234567890123456, "0.1234567890123456"),
            (0f64, "0"),
            (0.1111111111111110, "0.111111111111111"),
            (0.1111111111111111, "0.1111111111111111"),
            (0.1111111111111119, "0.1111111111111119"),
            (0.000000000000000001, "0.000000000000000001"),
            (0.000000000000000002, "0.000000000000000002"),
            (0.000000000000000003, "0.000000000000000003"),
            (0.000000000000000005, "0.000000000000000005"),
            (0.000000000000000008, "0.000000000000000008"),
            (0.1000000000000001, "0.1000000000000001"),
            (0.1000000000000002, "0.1000000000000002"),
            (0.1000000000000003, "0.1000000000000003"),
            (0.1000000000000005, "0.1000000000000005"),
            (0.1000000000000008, "0.1000000000000008"),
        ];

        for (f, s) in cases {
            let tag = format!("decode {}", f);
            let d = Decimal::from_f64(f).expect(&tag);
            assert_eq!(s, &format!("{}", d));
            assert_f64_eq!(d.to_f64().expect(&tag), f);

            let d = Decimal::from_str(s).expect(&tag);
            assert_eq!(s, &format!("{}", d));
            assert_f64_eq!(d.to_f64().expect(&tag), f);
        }
    }

    #[test]
    fn test_parse_decimal_fail() {
        let bad_f64_cases = vec![f64::NAN, f64::INFINITY, f64::NEG_INFINITY];

        for f in bad_f64_cases {
            assert!(Decimal::from_f64(f).is_err());
        }

        let bad_str_cases = vec![
            "", "qwert", "-", ".", "-.", ".-", "234-.56", "234-56",
            "2-", "..", "2..", "..2", ".5.2", "8..2", "8.1.", "--2.34",
            "12ee", "ae10", "12e1a", "12e1.2", "e1",
        ];

        for s in bad_str_cases {
            assert!(Decimal::from_str(s).is_err(), s);
        }
    }

    #[test]
    fn test_parse_scientific() {
        let cases = vec![
            ("314e-2", 3.14),
            ("1e2", 100f64),
            ("2E-1", 0.2),
            ("2E0", 2f64),
            ("2.2E-1", 0.22),
            ("2.23E2", 223f64),
        ];

        for (s, f) in cases {
            let d = Decimal::from_str(s).expect(s);
            assert_f64_eq!(s, d.to_f64().expect(s), f);
        }
    }

    #[test]
    fn test_order() {
        let cases = vec![-1.0,
                         0.0,
                         1.0,
                         f64::MAX,
                         f64::MIN,
                         f32::MAX as f64,
                         f32::MIN as f64,
                         -2.0,
                         2.0,
                         -2.1,
                         2.1,
                         0.0,
                         -1.0,
                         -2.0,
                         0.003,
                         0.04,
                         -0.003,
                         -0.04,
                         -123.45,
                         -123.40,
                         -23.45,
                         -1.43,
                         -0.93,
                         -0.4333,
                         -0.068,
                         -0.0099,
                         0f64,
                         0.001,
                         0.0012,
                         0.12,
                         1.2,
                         1.23,
                         123.3,
                         2424.242424];

        let mut expect = cases.clone();
        expect.sort_by(|a, b| a.partial_cmp(b).unwrap());

        let decimals: Vec<_> = cases.iter()
            .map(|&f| Decimal::from_f64(f).expect(&format!("parsing {} failed", f)))
            .collect();
        let mut sorted_dec = decimals.clone();
        sorted_dec.sort();
        let sorted: Vec<_> = sorted_dec.iter().map(|d| d.to_f64().unwrap()).collect();

        assert_eq!(expect, sorted);

        let mut encoded: Vec<_> = decimals.iter()
            .map(|d| {
                let mut v = vec![];
                v.encode_decimal(d).unwrap();
                v
            })
            .collect();
        encoded.sort();
        let res: Vec<_> = encoded.iter().map(|e| e.as_slice().decode_decimal().unwrap()).collect();
        assert_eq!(sorted_dec, res);
    }

    #[test]
    fn test_rescale() {
        let cases = vec![
            ("1.454", 0, "1"),
            ("1.454", -1, "1.5"),
            ("1.454", -2, "1.45"),
            ("1.454", -3, "1.454"),
            ("1.454", -4, "1.4540"),
            ("1.454", -5, "1.45400"),
            ("1.554", 0, "2"),
            ("1.554", -1, "1.6"),
            ("1.554", -2, "1.55"),
            ("0.554", 0, "1"),
            ("0.454", 0, "0"),
            ("0.454", -5, "0.45400"),
            ("0", 0, "0"),
            ("0", -1, "0.0"),
            ("0", -2, "0.00"),
            ("0", 1, "0"),
            ("5", -2, "5.00"),
            ("5", -1, "5.0"),
            ("5", 0, "5"),
            ("500", -2, "500.00"),
            ("545", 1, "550"),
            ("545", 2, "500"),
            ("545", 3, "1000"),
            ("545", 4, "0"),
            ("499", 3, "0"),
            ("499", 4, "0"),
        ];

        for (input, scale, expected) in cases {
            let d = Decimal::from_str(input).expect(input);
            let dd = d.rescale(scale).unwrap_or(d);
            let res = format!("{}", dd);
            if expected != &res {
                panic!("{} with scale {} not format as {}, got {}",
                       input,
                       scale,
                       expected,
                       res);
            }
        }
    }

    #[test]
    fn test_decimal_add() {
        let cases = vec![
            ("2", "3", "5"),
            ("2454495034", "3451204593", "5905699627"),
            ("24544.95034", ".3451204593", "24545.2954604593"),
            (".1", ".1", "0.2"),
            (".1", "-.1", "0.0"),
            ("0", "1.001", "1.001"),
        ];
        for (a, b, exp) in cases {
            let lhs: Decimal = a.parse().unwrap();
            let rhs: Decimal = b.parse().unwrap();
            let res = lhs + rhs;
            let res_str = format!("{}", res);
            assert_eq!(res_str, exp.to_owned());
        }
    }
}
