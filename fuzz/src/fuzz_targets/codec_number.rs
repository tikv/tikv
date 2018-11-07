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

#![no_main]
#[macro_use]
extern crate libfuzzer_sys;
extern crate tikv;
extern crate tikv_fuzz;

use tikv::util::codec::number::*;
use tikv_fuzz::util::number_maker;

fn fuzz_number_u64_encode(data: &[u8]) {
    if data.is_empty() {
        return;
    }
    let mut iter = data.iter().cycle().cloned();
    let n = number_maker::make_u64(&mut iter);
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

fn fuzz_number_i64_encode(data: &[u8]) {
    if data.is_empty() {
        return;
    }
    let mut iter = data.iter().cycle().cloned();
    let n = number_maker::make_i64(&mut iter);
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

fn fuzz_number_f64_encode(data: &[u8]) {
    if data.is_empty() {
        return;
    }
    let mut iter = data.iter().cycle().cloned();
    let n = number_maker::make_f64(&mut iter);
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

fn fuzz_number_u32_encode(data: &[u8]) {
    if data.is_empty() {
        return;
    }
    let mut iter = data.iter().cycle().cloned();
    let n = number_maker::make_u32(&mut iter);
    let mut buf = vec![];
    let _ = buf.encode_u32(n);
    let _ = buf.encode_u32_le(n);
}

fn fuzz_number_u32_decode(data: &[u8]) {
    let buf = data.to_owned();
    let _ = decode_u32(&mut buf.as_slice());
    let _ = decode_u32_le(&mut buf.as_slice());
}

fn fuzz_number_i32_encode(data: &[u8]) {
    if data.is_empty() {
        return;
    }
    let mut iter = data.iter().cycle().cloned();
    let n = number_maker::make_i32(&mut iter);
    let mut buf = vec![];
    let _ = buf.encode_i32_le(n);
}

fn fuzz_number_i32_decode(data: &[u8]) {
    let buf = data.to_owned();
    let _ = decode_i32_le(&mut buf.as_slice());
}

fn fuzz_number_u16_encode(data: &[u8]) {
    if data.is_empty() {
        return;
    }
    let mut iter = data.iter().cycle().cloned();
    let n = number_maker::make_u16(&mut iter);
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
