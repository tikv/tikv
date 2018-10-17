#![no_main]
#[macro_use]
extern crate libfuzzer_sys;
extern crate tikv;

use std::mem;
use tikv::util::codec::number::*;

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

fn fuzz_number_u64_encode(data: &[u8]) {
    if data.is_empty() {
        return;
    }
    let mut iter = data.iter().cycle().cloned();
    let n = make_u64(&mut iter);
    let mut buf = vec![];
    let _ = buf.encode_u64(n);
    let _ = buf.encode_u64_le(n);
    let _ = buf.encode_u64_desc(n);
    let _ = buf.encode_var_u64(n);
}

fn fuzz_number_u64_decode(data: &[u8]) {
    let buf = data.to_owned();
    let _ = decode_u64(&mut buf.as_slice());
    let _ = decode_u64_desc(&mut buf.as_slice());
    let _ = decode_u64_le(&mut buf.as_slice());
}

fn make_i64<I>(iter: &mut I) -> i64
where
    I: Iterator<Item = u8>,
{
    let mut bytes = [0u8; 8];
    for byte in &mut bytes {
        *byte = iter.next().unwrap();
    }
    unsafe { mem::transmute(bytes) }
}

fn fuzz_number_i64_encode(data: &[u8]) {
    if data.is_empty() {
        return;
    }
    let mut iter = data.iter().cycle().cloned();
    let n = make_i64(&mut iter);
    let mut buf = vec![];
    let _ = buf.encode_i64(n);
    let _ = buf.encode_i64_le(n);
    let _ = buf.encode_i64_desc(n);
    let _ = buf.encode_var_i64(n);
}

fn fuzz_number_i64_decode(data: &[u8]) {
    let buf = data.to_owned();
    let _ = decode_i64(&mut buf.as_slice());
    let _ = decode_i64_desc(&mut buf.as_slice());
    let _ = decode_i64_le(&mut buf.as_slice());
}

fn make_f64<I>(iter: &mut I) -> f64
where
    I: Iterator<Item = u8>,
{
    let mut bytes = [0u8; 8];
    for byte in &mut bytes {
        *byte = iter.next().unwrap();
    }
    unsafe { mem::transmute(bytes) }
}

fn fuzz_number_f64_encode(data: &[u8]) {
    if data.is_empty() {
        return;
    }
    let mut iter = data.iter().cycle().cloned();
    let n = make_f64(&mut iter);
    let mut buf = vec![];
    let _ = buf.encode_f64(n);
    let _ = buf.encode_f64_le(n);
    let _ = buf.encode_f64_desc(n);
}

fn fuzz_number_f64_decode(data: &[u8]) {
    let buf = data.to_owned();
    let _ = decode_f64(&mut buf.as_slice());
    let _ = decode_f64_desc(&mut buf.as_slice());
    let _ = decode_f64_le(&mut buf.as_slice());
}

fn make_u32<I>(iter: &mut I) -> u32
where
    I: Iterator<Item = u8>,
{
    let mut bytes = [0u8; 4];
    for byte in &mut bytes {
        *byte = iter.next().unwrap();
    }
    unsafe { mem::transmute(bytes) }
}

fn fuzz_number_u32_encode(data: &[u8]) {
    if data.is_empty() {
        return;
    }
    let mut iter = data.iter().cycle().cloned();
    let n = make_u32(&mut iter);
    let mut buf = vec![];
    let _ = buf.encode_u32(n);
    let _ = buf.encode_u32_le(n);
}

fn fuzz_number_u32_decode(data: &[u8]) {
    let buf = data.to_owned();
    let _ = decode_u32(&mut buf.as_slice());
    let _ = decode_u32_le(&mut buf.as_slice());
}

fn make_i32<I>(iter: &mut I) -> i32
where
    I: Iterator<Item = u8>,
{
    let mut bytes = [0u8; 4];
    for byte in &mut bytes {
        *byte = iter.next().unwrap();
    }
    unsafe { mem::transmute(bytes) }
}

fn fuzz_number_i32_encode(data: &[u8]) {
    if data.is_empty() {
        return;
    }
    let mut iter = data.iter().cycle().cloned();
    let n = make_i32(&mut iter);
    let mut buf = vec![];
    let _ = buf.encode_i32_le(n);
}

fn fuzz_number_i32_decode(data: &[u8]) {
    let buf = data.to_owned();
    let _ = decode_i32_le(&mut buf.as_slice());
}

fn make_u16<I>(iter: &mut I) -> u16
where
    I: Iterator<Item = u8>,
{
    let mut bytes = [0u8; 2];
    for byte in &mut bytes {
        *byte = iter.next().unwrap();
    }
    unsafe { mem::transmute(bytes) }
}

fn fuzz_number_u16_encode(data: &[u8]) {
    if data.is_empty() {
        return;
    }
    let mut iter = data.iter().cycle().cloned();
    let n = make_u16(&mut iter);
    let mut buf = vec![];
    let _ = buf.encode_u16(n);
    let _ = buf.encode_u16_le(n);
}

fn fuzz_number_u16_decode(data: &[u8]) {
    let buf = data.to_owned();
    let _ = decode_u16(&mut buf.as_slice());
    let _ = decode_u16_le(&mut buf.as_slice());
}

fuzz_target!(|data: &[u8]| {
    fuzz_number_u64_encode(data);
    fuzz_number_u64_decode(data);

    fuzz_number_i64_encode(data);
    fuzz_number_i64_decode(data);

    fuzz_number_f64_encode(data);
    fuzz_number_f64_decode(data);

    fuzz_number_u32_encode(data);
    fuzz_number_u32_decode(data);

    fuzz_number_i32_encode(data);
    fuzz_number_i32_decode(data);

    fuzz_number_u16_encode(data);
    fuzz_number_u16_decode(data);
});
