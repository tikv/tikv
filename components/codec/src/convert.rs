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
