// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

const SIGN_MARK: u64 = 1 << 63;

#[inline]
pub fn encode_i64_to_comparable_u64(v: i64) -> u64 {
    (v as u64) ^ SIGN_MARK
}

#[inline]
pub fn decode_comparable_u64_to_i64(u: u64) -> i64 {
    (u ^ SIGN_MARK) as i64
}

#[inline]
pub fn encode_f64_to_comparable_u64(v: f64) -> u64 {
    let u: u64 = v.to_bits();
    if v.is_sign_positive() {
        u | SIGN_MARK
    } else {
        !u
    }
}

#[inline]
pub fn decode_comparable_u64_to_f64(u: u64) -> f64 {
    let u = if u & SIGN_MARK > 0 {
        u & (!SIGN_MARK)
    } else {
        !u
    };
    f64::from_bits(u)
}

#[test]
fn test_encode_i64_to_comparable_u64() {
    assert_eq!(encode_i64_to_comparable_u64(1 << 63), 0);
}

#[test]
fn test_decode_comparable_u64_to_i64() {
    assert_eq!(decode_comparable_u64_to_i64((1 << 63) + 10), 10);
}

#[test]
fn test_encode_f64_to_comparable_u64() {
    assert_eq!(encode_f64_to_comparable_u64(0.0), !(1 << 63) + 1);
    assert_eq!(encode_f64_to_comparable_u64(-0.0), !(1 << 63));
}

#[test]
#[allow(clippy::float_cmp)]
fn test_decode_comparable_u64_to_f64() {
    assert_eq!(decode_comparable_u64_to_f64(1 << 63), 0.0);
    assert_ne!(decode_comparable_u64_to_f64(0), 0.0);
}
