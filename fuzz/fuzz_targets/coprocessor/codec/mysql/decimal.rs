#![no_main]
#[macro_use]
extern crate libfuzzer_sys;
extern crate tikv;

use std::mem;
use tikv::coprocessor::codec::mysql::decimal::{Decimal, RoundMode};

fn make_u64<I>(iter: &mut I) -> u64
where
    I: Iterator<Item = u8>,
{
    let mut bytes = [0u8; 8];
    for byte in &mut bytes {
        *byte = iter.next().unwrap();
    }
    unsafe { mem::transmute(bytes) }
}

fn fuzz<I>(lhs: &Decimal, rhs: &Decimal, iter: &mut I)
where
    I: Iterator<Item = u8>,
{
    let _ = lhs.clone().abs();
    let _ = lhs.ceil();
    let _ = lhs.floor();
    let _ = lhs.prec_and_frac();

    let mode = match iter.next().unwrap() % 3 {
        0 => RoundMode::HalfEven,
        1 => RoundMode::Truncate,
        _ => RoundMode::Ceiling,
    };
    let frac = iter.next().unwrap() as i8;
    let _ = lhs.clone().round(frac, mode.clone());

    let shift = make_u64(iter) as isize;
    let _ = lhs.clone().shift(shift);

    let _ = lhs.as_i64();
    let _ = lhs.as_u64();
    let _ = lhs.as_f64();
    let _ = lhs.is_zero();
    let _ = lhs.approximate_encoded_size();

    let frac_inc = iter.next().unwrap();
    lhs.clone().div(rhs.clone(), frac_inc);

    let _ = lhs > rhs;

    let _ = lhs + rhs;
    let _ = lhs - rhs;
    let _ = lhs * rhs;
    let _ = lhs.clone() / rhs.clone();
    let _ = lhs.clone() % rhs.clone();
    let _ = -lhs.clone();
}

fuzz_target!(|data: &[u8]| {
    if data.is_empty() {
        return;
    }
    let mut iter = data.iter().cycle().cloned();
    let f1 = f64::from_bits(make_u64(&mut iter));
    let f2 = f64::from_bits(make_u64(&mut iter));
    let decimal1 = Decimal::from_f64(f1);
    let decimal2 = Decimal::from_f64(f2);

    let (decimal1, decimal2) = match (decimal1, decimal2) {
        (Ok(d1), Ok(d2)) => (d1, d2),
        _ => return,
    };

    fuzz(&decimal1, &decimal2, &mut iter);
    fuzz(&decimal2, &decimal1, &mut iter);
});
