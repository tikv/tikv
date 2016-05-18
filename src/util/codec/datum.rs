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


use std::cmp::Ordering;
use std::{str, i64};
use std::io::Write;
use std::str::FromStr;
use byteorder::{ReadBytesExt, WriteBytesExt};

use super::{number, Result, bytes, convert};
use super::mysql::{Duration, MAX_FSP, Decimal, DecimalEncoder, DecimalDecoder};

const NIL_FLAG: u8 = 0;
const BYTES_FLAG: u8 = 1;
const COMPACT_BYTES_FLAG: u8 = 2;
const INT_FLAG: u8 = 3;
const UINT_FLAG: u8 = 4;
const FLOAT_FLAG: u8 = 5;
const DECIMAL_FLAG: u8 = 6;
const DURATION_FLAG: u8 = 7;
const MAX_FLAG: u8 = 250;
// TODO: support more flags

#[derive(PartialEq, Debug, Clone)]
pub enum Datum {
    Null,
    I64(i64),
    U64(u64),
    F64(f64),
    Dur(Duration),
    Bytes(Vec<u8>),
    Dec(Decimal),
    Min,
    Max,
}

fn cmp_f64(l: f64, r: f64) -> Result<Ordering> {
    l.partial_cmp(&r)
        .ok_or_else(|| invalid_type!("{} and {} can't be compared", l, r))
}

#[allow(should_implement_trait)]
impl Datum {
    pub fn cmp(&self, datum: &Datum) -> Result<Ordering> {
        match *datum {
            Datum::Null => {
                match *self {
                    Datum::Null => Ok(Ordering::Equal),
                    _ => Ok(Ordering::Greater),
                }
            }
            Datum::Min => {
                match *self {
                    Datum::Null => Ok(Ordering::Less),
                    Datum::Min => Ok(Ordering::Equal),
                    _ => Ok(Ordering::Greater),
                }
            }
            Datum::Max => {
                match *self {
                    Datum::Max => Ok(Ordering::Equal),
                    _ => Ok(Ordering::Less),
                }
            }
            Datum::I64(i) => self.cmp_i64(i),
            Datum::U64(u) => self.cmp_u64(u),
            Datum::F64(f) => self.cmp_f64(f),
            Datum::Bytes(ref bs) => self.cmp_bytes(bs),
            Datum::Dur(ref d) => self.cmp_dur(d),
            Datum::Dec(ref d) => self.cmp_dec(d),
        }
    }

    fn cmp_i64(&self, i: i64) -> Result<Ordering> {
        match *self {
            Datum::I64(ii) => Ok(ii.cmp(&i)),
            Datum::U64(u) => {
                if i < 0 || u > i64::MAX as u64 {
                    Ok(Ordering::Greater)
                } else {
                    Ok(u.cmp(&(i as u64)))
                }
            }
            _ => self.cmp_f64(i as f64),
        }
    }

    fn cmp_u64(&self, u: u64) -> Result<Ordering> {
        match *self {
            Datum::I64(i) => {
                if i < 0 || u > i64::MAX as u64 {
                    Ok(Ordering::Less)
                } else {
                    Ok(i.cmp(&(u as i64)))
                }
            }
            Datum::U64(uu) => Ok(uu.cmp(&u)),
            _ => self.cmp_f64(u as f64),
        }
    }

    fn cmp_f64(&self, f: f64) -> Result<Ordering> {
        match *self {
            Datum::Null | Datum::Min => Ok(Ordering::Less),
            Datum::Max => Ok(Ordering::Greater),
            Datum::I64(i) => cmp_f64(i as f64, f),
            Datum::U64(u) => cmp_f64(u as f64, f),
            Datum::F64(ff) => cmp_f64(ff, f),
            Datum::Bytes(ref bs) => {
                let ff = try!(convert::bytes_to_f64(bs));
                cmp_f64(ff, f)
            }
            Datum::Dur(ref d) => {
                let ff = d.to_secs();
                cmp_f64(ff, f)
            }
            Datum::Dec(ref d) => {
                let ff = try!(d.to_f64());
                cmp_f64(ff, f)
            }
        }
    }

    fn cmp_dur(&self, d: &Duration) -> Result<Ordering> {
        match *self {
            Datum::Dur(ref d2) => Ok(d2.cmp(d)),
            Datum::Bytes(ref bs) => {
                let d2 = try!(Duration::parse(bs, MAX_FSP));
                Ok(d2.cmp(d))
            }
            _ => self.cmp_f64(d.to_secs()),
        }
    }

    fn cmp_bytes(&self, bs: &[u8]) -> Result<Ordering> {
        match *self {
            Datum::Null | Datum::Min => Ok(Ordering::Less),
            Datum::Max => Ok(Ordering::Greater),
            Datum::Bytes(ref bss) => Ok((bss as &[u8]).cmp(bs)),
            Datum::Dur(ref d) => {
                let d2 = try!(Duration::parse(bs, MAX_FSP));
                Ok(d.cmp(&d2))
            }
            Datum::Dec(ref d) => {
                let s = try!(str::from_utf8(bs));
                let d2 = try!(Decimal::from_str(s));
                Ok(d.cmp(&d2))
            }
            _ => {
                let f = try!(convert::bytes_to_f64(bs));
                self.cmp_f64(f)
            }
        }
    }

    fn cmp_dec(&self, dec: &Decimal) -> Result<Ordering> {
        match *self {
            Datum::Dec(ref d) => Ok(d.cmp(dec)),
            Datum::Bytes(ref bs) => {
                let s = try!(str::from_utf8(bs));
                let d = try!(Decimal::from_str(s));
                Ok(d.cmp(dec))
            }
            _ => {
                let f = try!(dec.to_f64());
                self.cmp_f64(f)
            }
        }
    }

    // into_bool converts self to a bool.
    pub fn into_bool(self) -> Result<Option<bool>> {
        let b = match self {
            Datum::I64(i) => Some(i != 0),
            Datum::U64(u) => Some(u != 0),
            Datum::F64(f) => Some(f.round() != 0f64),
            Datum::Bytes(ref bs) => Some(!bs.is_empty() && try!(convert::bytes_to_int(bs)) != 0),
            Datum::Null => None,
            Datum::Dur(d) => Some(!d.is_empty()),
            Datum::Dec(d) => Some(try!(d.to_f64()).round() != 0f64),
            _ => return Err(invalid_type!("can't convert {:?} to bool", self)),
        };
        Ok(b)
    }

    /// into_string convert self into a string.
    pub fn into_string(self) -> Result<String> {
        let s = match self {
            Datum::I64(i) => format!("{}", i),
            Datum::U64(u) => format!("{}", u),
            Datum::F64(f) => format!("{}", f),
            Datum::Bytes(bs) => try!(String::from_utf8(bs)),
            Datum::Dur(d) => format!("{}", d),
            Datum::Dec(d) => format!("{}", d),
            d => return Err(invalid_type!("can't convert {:?} to string", d)),
        };
        Ok(s)
    }
}

impl From<bool> for Datum {
    fn from(b: bool) -> Datum {
        if b {
            Datum::I64(1)
        } else {
            Datum::I64(0)
        }
    }
}

impl<T: Into<Datum>> From<Option<T>> for Datum {
    fn from(opt: Option<T>) -> Datum {
        match opt {
            None => Datum::Null,
            Some(t) => t.into(),
        }
    }
}

impl<'a> From<&'a [u8]> for Datum {
    fn from(data: &'a [u8]) -> Datum {
        Datum::Bytes(data.to_vec())
    }
}

impl From<Duration> for Datum {
    fn from(dur: Duration) -> Datum {
        Datum::Dur(dur)
    }
}

pub trait DatumDecoder: DecimalDecoder {
    /// `decode_datum` decodes on a datum from a byte slice generated by tidb.
    fn decode_datum(&mut self) -> Result<Datum> {
        let flag = try!(self.read_u8());
        match flag {
            INT_FLAG => self.decode_i64().map(Datum::I64),
            UINT_FLAG => self.decode_u64().map(Datum::U64),
            BYTES_FLAG => self.decode_bytes(false).map(Datum::Bytes),
            COMPACT_BYTES_FLAG => self.decode_compact_bytes().map(Datum::Bytes),
            NIL_FLAG => Ok(Datum::Null),
            FLOAT_FLAG => self.decode_f64().map(Datum::F64),
            DURATION_FLAG => {
                let nanos = try!(self.decode_i64());
                let dur = try!(Duration::from_nanos(nanos, MAX_FSP));
                Ok(Datum::Dur(dur))
            }
            DECIMAL_FLAG => self.decode_decimal().map(Datum::Dec),
            f => Err(invalid_type!("unsupported data type `{}`", f)),
        }
    }

    /// `decode` decodes all datum from a byte slice generated by tidb.
    fn decode(&mut self) -> Result<Vec<Datum>> {
        let mut res = vec![];

        while self.remaining() > 0 {
            let v = try!(self.decode_datum());
            res.push(v);
        }

        Ok(res)
    }
}

impl<T: DecimalDecoder> DatumDecoder for T {}

pub trait DatumEncoder: DecimalEncoder {
    /// Encode values to buf slice.
    fn encode(&mut self, values: &[Datum], comparable: bool) -> Result<()> {
        let mut find_min = false;
        for v in values {
            if find_min {
                return Err(invalid_type!("MinValue should be the last datum.".to_owned()));
            }
            match *v {
                Datum::I64(i) => {
                    try!(self.write_u8(INT_FLAG));
                    try!(self.encode_i64(i));
                }
                Datum::U64(u) => {
                    try!(self.write_u8(UINT_FLAG));
                    try!(self.encode_u64(u));
                }
                Datum::Bytes(ref bs) => {
                    if comparable {
                        try!(self.write_u8(BYTES_FLAG));
                        try!(self.encode_bytes(bs, false));
                    } else {
                        try!(self.write_u8(COMPACT_BYTES_FLAG));
                        try!(self.encode_compact_bytes(bs));
                    }
                }
                Datum::F64(f) => {
                    try!(self.write_u8(FLOAT_FLAG));
                    try!(self.encode_f64(f));
                }
                Datum::Null => try!(self.write_u8(NIL_FLAG)),
                Datum::Min => {
                    try!(self.write_u8(BYTES_FLAG)); // for backward compatibility
                    find_min = true;
                }
                Datum::Max => try!(self.write_u8(MAX_FLAG)),
                Datum::Dur(ref d) => {
                    try!(self.write_u8(DURATION_FLAG));
                    try!(self.encode_i64(d.to_nanos()));
                }
                Datum::Dec(ref d) => {
                    try!(self.write_u8(DECIMAL_FLAG));
                    try!(self.encode_decimal(d));
                }
            }
        }
        Ok(())
    }
}

impl<T: Write> DatumEncoder for T {}

/// Get the approximate needed buffer size of values.
///
/// This function ensures that encoded values must fit in the given buffer size.
#[allow(match_same_arms)]
pub fn approximate_size(values: &[Datum], comparable: bool) -> usize {
    values.iter()
        .map(|v| {
            1 +
            match *v {
                Datum::I64(_) => number::I64_SIZE,
                Datum::U64(_) => number::U64_SIZE,
                Datum::F64(_) => number::F64_SIZE,
                Datum::Dur(_) => number::I64_SIZE,
                Datum::Bytes(ref bs) => {
                    if comparable {
                        bytes::max_encoded_bytes_size(bs.len())
                    } else {
                        bs.len() + number::MAX_VAR_I64_LEN
                    }
                }
                Datum::Dec(ref d) => d.max_encode_bytes(),
                Datum::Null | Datum::Min | Datum::Max => 0,
            }
        })
        .sum()
}

pub fn encode(values: &[Datum], comparable: bool) -> Result<Vec<u8>> {
    let mut buf = Vec::with_capacity(approximate_size(values, comparable));
    try!(buf.encode(values, comparable));
    buf.shrink_to_fit();
    Ok(buf)
}

pub fn encode_key(values: &[Datum]) -> Result<Vec<u8>> {
    encode(values, true)
}

pub fn encode_value(values: &[Datum]) -> Result<Vec<u8>> {
    encode(values, false)
}

#[cfg(test)]
mod test {
    use super::*;
    use util::codec::mysql::{MAX_FSP, Duration, Decimal};
    use util::as_slice;

    use std::cmp::Ordering;
    use std::time::Duration as StdDuration;
    use std::{i8, u8, i16, u16, i32, u32, i64, u64};

    fn same_type(l: &Datum, r: &Datum) -> bool {
        match (l, r) {
            (&Datum::I64(_), &Datum::I64(_)) |
            (&Datum::U64(_), &Datum::U64(_)) |
            (&Datum::F64(_), &Datum::F64(_)) |
            (&Datum::Max, &Datum::Max) |
            (&Datum::Min, &Datum::Min) |
            (&Datum::Bytes(_), &Datum::Bytes(_)) |
            (&Datum::Dur(_), &Datum::Dur(_)) |
            (&Datum::Dec(_), &Datum::Dec(_)) |
            (&Datum::Null, &Datum::Null) => true,
            _ => false,
        }
    }

    #[test]
    fn test_datum_codec() {
        let table =
            vec![vec![Datum::I64(1)],
                 vec![Datum::F64(1.0), Datum::F64(3.15), b"123".as_ref().into()],
                 vec![Datum::U64(1), Datum::F64(3.15), b"123".as_ref().into(), Datum::I64(-1)],
                 vec![Datum::Null],
                 vec![Duration::new(StdDuration::from_millis(23), false, MAX_FSP)
                          .unwrap()
                          .into(),
                      Duration::new(StdDuration::from_millis(23), true, MAX_FSP)
                          .unwrap()
                          .into()],
                 vec![Datum::U64(1),
                      Datum::Dec(Decimal::from_f64(2.3).unwrap()),
                      Datum::Dec("-34".parse().unwrap())],
                 vec![Datum::Dec("1234.00".parse().unwrap()),
                      Datum::Dec("1234".parse().unwrap()),
                      Datum::Dec("12.34".parse().unwrap()),
                      Datum::Dec("12.340".parse().unwrap()),
                      Datum::Dec("0.1234".parse().unwrap()),
                      Datum::Dec("0.0".parse().unwrap()),
                      Datum::Dec("0".parse().unwrap()),
                      Datum::Dec("-0.0".parse().unwrap()),
                      Datum::Dec("-0.0000".parse().unwrap()),
                      Datum::Dec("-1234.00".parse().unwrap()),
                      Datum::Dec("-1234".parse().unwrap()),
                      Datum::Dec("-12.34".parse().unwrap()),
                      Datum::Dec("-12.340".parse().unwrap()),
                      Datum::Dec("-0.1234".parse().unwrap())]];

        for vs in table {
            let mut buf = encode_key(&vs).unwrap();
            let decoded = buf.as_slice().decode().unwrap();
            assert_eq!(vs, decoded);

            buf = encode_value(&vs).unwrap();
            let decoded = buf.as_slice().decode().unwrap();
            assert_eq!(vs, decoded);
        }
    }

    #[test]
    fn test_datum_cmp() {
        let tests = vec![
            (Datum::F64(1.0), Datum::F64(1.0), Ordering::Equal),
            (Datum::F64(1.0), b"1".as_ref().into(), Ordering::Equal),
            (Datum::I64(1), Datum::I64(1), Ordering::Equal),
            (Datum::I64(-1), Datum::I64(1), Ordering::Less),
            (Datum::I64(-1), b"-1".as_ref().into(), Ordering::Equal),
            (Datum::U64(1), Datum::U64(1), Ordering::Equal),
            (Datum::U64(1), Datum::I64(-1), Ordering::Greater),
            (Datum::U64(1), b"1".as_ref().into(), Ordering::Equal),
            (Datum::Dec(1i64.into()), Datum::Dec(1i64.into()), Ordering::Equal),
            (Datum::Dec(1i64.into()), b"2".as_ref().into(), Ordering::Less),
            (Datum::Dec(1i64.into()), b"0.2".as_ref().into(), Ordering::Greater),
            (Datum::Dec(1i64.into()), b"1".as_ref().into(), Ordering::Equal),
            (b"1".as_ref().into(), b"1".as_ref().into(), Ordering::Equal),
            (b"1".as_ref().into(), Datum::I64(-1), Ordering::Greater),
            (b"1".as_ref().into(), Datum::U64(1), Ordering::Equal),
            (b"1".as_ref().into(), Datum::Dec(1i64.into()), Ordering::Equal),
            (Datum::Null, Datum::I64(2), Ordering::Less),
            (Datum::Null, Datum::Null, Ordering::Equal),

            (false.into(), Datum::Null, Ordering::Greater),
            (false.into(), true.into(), Ordering::Less),
            (true.into(), true.into(), Ordering::Equal),
            (false.into(), false.into(), Ordering::Equal),
            (true.into(), Datum::I64(2), Ordering::Less),

            (Datum::F64(1.23), Datum::Null, Ordering::Greater),
            (Datum::F64(0.0), Datum::F64(3.45), Ordering::Less),
            (Datum::F64(354.23), Datum::F64(3.45), Ordering::Greater),
            (Datum::F64(3.452), Datum::F64(3.452), Ordering::Equal),

            (Datum::I64(432), Datum::Null, Ordering::Greater),
            (Datum::I64(-4), Datum::I64(32), Ordering::Less),
            (Datum::I64(4), Datum::I64(-32), Ordering::Greater),
            (Datum::I64(432), Datum::I64(12), Ordering::Greater),
            (Datum::I64(23), Datum::I64(128), Ordering::Less),
            (Datum::I64(123), Datum::I64(123), Ordering::Equal),
            (Datum::I64(23), Datum::I64(123), Ordering::Less),
            (Datum::I64(133), Datum::I64(183), Ordering::Less),

            (Datum::U64(123), Datum::U64(183), Ordering::Less),
            (Datum::U64(2), Datum::I64(-2), Ordering::Greater),
            (Datum::U64(2), Datum::I64(1), Ordering::Greater),

            (b"".as_ref().into(), Datum::Null, Ordering::Greater),
            (b"".as_ref().into(), b"24".as_ref().into(), Ordering::Less),
            (b"aasf".as_ref().into(), b"4".as_ref().into(), Ordering::Greater),
            (b"".as_ref().into(), b"".as_ref().into(), Ordering::Equal),

            (Duration::new(StdDuration::from_millis(34), false, 2).unwrap().into(),
             Datum::Null, Ordering::Greater),
            (Duration::new(StdDuration::from_millis(3340), false, 2).unwrap().into(),
             Duration::new(StdDuration::from_millis(29034), false, 2).unwrap().into(),
             Ordering::Less),
            (Duration::new(StdDuration::from_millis(3340), false, 2).unwrap().into(),
             Duration::new(StdDuration::from_millis(34), false, 2).unwrap().into(),
             Ordering::Greater),
            (Duration::new(StdDuration::from_millis(34), false, 2).unwrap().into(),
             Duration::new(StdDuration::from_millis(34), false, 2).unwrap().into(),
             Ordering::Equal),
            (Duration::new(StdDuration::from_millis(34), true, 2).unwrap().into(),
             Datum::Null, Ordering::Greater),
            (Duration::new(StdDuration::from_millis(0), true, 2).unwrap().into(),
             Datum::I64(0), Ordering::Equal),
            (Duration::new(StdDuration::from_millis(3340), false, 2).unwrap().into(),
             Duration::new(StdDuration::from_millis(29034), true, 2).unwrap().into(),
             Ordering::Greater),
            (Duration::new(StdDuration::from_millis(3340), true, 2).unwrap().into(),
             Duration::new(StdDuration::from_millis(34), false, 2).unwrap().into(),
             Ordering::Less),
            (Duration::new(StdDuration::from_millis(34), false, 2).unwrap().into(),
             Duration::new(StdDuration::from_millis(34), true, 2).unwrap().into(),
             Ordering::Greater),

            (Datum::Dec("1234".parse().unwrap()), Datum::Dec("123400".parse().unwrap()),
             Ordering::Less),
            (Datum::Dec("12340".parse().unwrap()), Datum::Dec("123400".parse().unwrap()),
             Ordering::Less),
            (Datum::Dec("1234".parse().unwrap()), Datum::Dec("1234.5".parse().unwrap()),
             Ordering::Less),
            (Datum::Dec("1234".parse().unwrap()), Datum::Dec("1234.0000".parse().unwrap()),
             Ordering::Equal),
            (Datum::Dec("1234".parse().unwrap()), Datum::Dec("12.34".parse().unwrap()),
             Ordering::Greater),
            (Datum::Dec("12.34".parse().unwrap()), Datum::Dec("12.35".parse().unwrap()),
             Ordering::Less),
            (Datum::Dec("0.12".parse().unwrap()), Datum::Dec("0.1234".parse().unwrap()),
             Ordering::Less),
            (Datum::Dec("0.1234".parse().unwrap()), Datum::Dec("12.3400".parse().unwrap()),
             Ordering::Less),
            (Datum::Dec("0.1234".parse().unwrap()), Datum::Dec("0.1235".parse().unwrap()),
             Ordering::Less),
            (Datum::Dec("0.123400".parse().unwrap()), Datum::Dec("12.34".parse().unwrap()),
             Ordering::Less),
            (Datum::Dec("12.34000".parse().unwrap()), Datum::Dec("12.34".parse().unwrap()),
             Ordering::Equal),
            (Datum::Dec("0.01234".parse().unwrap()), Datum::Dec("0.01235".parse().unwrap()),
             Ordering::Less),
            (Datum::Dec("0.1234".parse().unwrap()), Datum::Dec("0".parse().unwrap()),
             Ordering::Greater),
            (Datum::Dec("0.0000".parse().unwrap()), Datum::Dec("0".parse().unwrap()),
             Ordering::Equal),
            (Datum::Dec("0.0001".parse().unwrap()), Datum::Dec("0".parse().unwrap()),
             Ordering::Greater),
            (Datum::Dec("0.0001".parse().unwrap()), Datum::Dec("0.0000".parse().unwrap()),
             Ordering::Greater),
            (Datum::Dec("0".parse().unwrap()), Datum::Dec("-0.0000".parse().unwrap()),
             Ordering::Equal),
            (Datum::Dec("-0.0001".parse().unwrap()), Datum::Dec("0".parse().unwrap()),
             Ordering::Less),
            (Datum::Dec("-0.1234".parse().unwrap()), Datum::Dec("0".parse().unwrap()),
             Ordering::Less),
            (Datum::Dec("-0.1234".parse().unwrap()), Datum::Dec("-0.12".parse().unwrap()),
             Ordering::Less),
            (Datum::Dec("-0.12".parse().unwrap()), Datum::Dec("-0.1234".parse().unwrap()),
             Ordering::Greater),
            (Datum::Dec("-0.12".parse().unwrap()), Datum::Dec("-0.1200".parse().unwrap()),
             Ordering::Equal),
            (Datum::Dec("-0.1234".parse().unwrap()), Datum::Dec("0.1234".parse().unwrap()),
             Ordering::Less),
            (Datum::Dec("-1.234".parse().unwrap()), Datum::Dec("-12.34".parse().unwrap()),
             Ordering::Greater),
            (Datum::Dec("-0.1234".parse().unwrap()), Datum::Dec("-12.34".parse().unwrap()),
             Ordering::Greater),
            (Datum::Dec("-12.34".parse().unwrap()), Datum::Dec("1234".parse().unwrap()),
             Ordering::Less),
            (Datum::Dec("-12.34".parse().unwrap()), Datum::Dec("-12.35".parse().unwrap()),
             Ordering::Greater),
            (Datum::Dec("-0.01234".parse().unwrap()), Datum::Dec("-0.01235".parse().unwrap()),
             Ordering::Greater),
            (Datum::Dec("-1234".parse().unwrap()), Datum::Dec("-123400".parse().unwrap()),
             Ordering::Greater),
            (Datum::Dec("-12340".parse().unwrap()), Datum::Dec("-123400".parse().unwrap()),
             Ordering::Greater),

            // Test for int type decimal.
            (Datum::Dec((-1i64).into()), Datum::Dec(1i64.into()), Ordering::Less),
            (Datum::Dec(i64::MAX.into()), Datum::Dec(i64::MIN.into()), Ordering::Greater),
            (Datum::Dec(i64::MAX.into()), Datum::Dec(i32::MAX.into()), Ordering::Greater),
            (Datum::Dec(i32::MIN.into()), Datum::Dec(i16::MAX.into()), Ordering::Less),
            (Datum::Dec(i64::MIN.into()), Datum::Dec(i8::MAX.into()), Ordering::Less),
            (Datum::Dec(0i64.into()), Datum::Dec(i8::MAX.into()), Ordering::Less),
            (Datum::Dec(i8::MIN.into()), Datum::Dec(0i64.into()), Ordering::Less),
            (Datum::Dec(i16::MIN.into()), Datum::Dec(i16::MAX.into()), Ordering::Less),
            (Datum::Dec(1i64.into()), Datum::Dec((-1i64).into()), Ordering::Greater),
            (Datum::Dec(1i64.into()), Datum::Dec(0i64.into()), Ordering::Greater),
            (Datum::Dec((-1i64).into()), Datum::Dec(0i64.into()), Ordering::Less),
            (Datum::Dec(0i64.into()), Datum::Dec(0i64.into()), Ordering::Equal),
            (Datum::Dec(i16::MAX.into()), Datum::Dec(i16::MAX.into()), Ordering::Equal),

            // Test for uint type decimal.
            (Datum::Dec(0u64.into()), Datum::Dec(0u64.into()), Ordering::Equal),
            (Datum::Dec(1u64.into()), Datum::Dec(0u64.into()), Ordering::Greater),
            (Datum::Dec(0u64.into()), Datum::Dec(1u64.into()), Ordering::Less),
            (Datum::Dec(i8::MAX.into()), Datum::Dec(i16::MAX.into()), Ordering::Less),
            (Datum::Dec(u32::MAX.into()), Datum::Dec(i32::MAX.into()), Ordering::Greater),
            (Datum::Dec(u8::MAX.into()), Datum::Dec(i8::MAX.into()), Ordering::Greater),
            (Datum::Dec(u16::MAX.into()), Datum::Dec(i32::MAX.into()), Ordering::Less),
            (Datum::Dec(u64::MAX.into()), Datum::Dec(i64::MAX.into()), Ordering::Greater),
            (Datum::Dec(i64::MAX.into()), Datum::Dec(u32::MAX.into()), Ordering::Greater),
            (Datum::Dec(u64::MAX.into()), Datum::Dec(0u64.into()), Ordering::Greater),
            (Datum::Dec(0u64.into()), Datum::Dec(u64::MAX.into()), Ordering::Less),

            (b"abc".as_ref().into(), b"ab".as_ref().into(), Ordering::Greater),
            (b"123".as_ref().into(), Datum::I64(1234), Ordering::Less),
            (b"".as_ref().into(), Datum::Null, Ordering::Greater),
        ];

        for (lhs, rhs, ret) in tests {
            if ret != lhs.cmp(&rhs).unwrap() {
                panic!("{:?} should be {:?} to {:?}", lhs, ret, rhs);
            }

            let rev_ret = ret.reverse();

            if rev_ret != rhs.cmp(&lhs).unwrap() {
                panic!("{:?} should be {:?} to {:?}", rhs, rev_ret, lhs);
            }

            if same_type(&lhs, &rhs) {
                let lhs_bs = encode_key(as_slice(&lhs)).unwrap();
                let rhs_bs = encode_key(as_slice(&rhs)).unwrap();

                if ret != lhs_bs.cmp(&rhs_bs) {
                    panic!("{:?} should be {:?} to {:?} when encoded", lhs, ret, rhs);
                }
            }
        }
    }

    #[test]
    fn test_datum_to_bool() {
        let tests = vec![
            (Datum::I64(0), Some(false)),
            (Datum::I64(-1), Some(true)),
            (Datum::U64(0), Some(false)),
            (Datum::U64(1), Some(true)),
            (Datum::F64(0f64), Some(false)),
            (Datum::F64(0.4), Some(false)),
            (Datum::F64(0.5), Some(true)),
            (Datum::F64(-0.5), Some(true)),
            (Datum::F64(-0.4), Some(false)),
            (Datum::Null, None),
            (b"".as_ref().into(), Some(false)),
            (b"0.5".as_ref().into(), Some(false)),
            (b"0".as_ref().into(), Some(false)),
            (b"2".as_ref().into(), Some(true)),
            (b"abc".as_ref().into(), Some(false)),
            (Duration::parse(b"11:11:11.999999", MAX_FSP).unwrap().into(), Some(true)),
            (Datum::Dec(Decimal::from_f64(0.1415926).unwrap()), Some(false)),
            (Datum::Dec(0u64.into()), Some(false)),
        ];
        for (d, b) in tests {
            if d.clone().into_bool().unwrap() != b {
                panic!("expect {:?} to be {:?}", d, b);
            }
        }
    }
}
