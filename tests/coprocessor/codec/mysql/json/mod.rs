// Copyright 2017 PingCAP, Inc.
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
extern crate serde_json;

use std::fs::File;
use std::io::prelude::*;
use std::io::BufReader;
use std::path::{Path, PathBuf};
use test::Bencher;

use tikv::coprocessor::codec::mysql::{Json, JsonDecoder, JsonEncoder};


#[inline]
fn open_file_read_lines<P: AsRef<Path>>(filename: P) -> Vec<String> {
    File::open(filename)
        .map(BufReader::new)
        .and_then(|reader| {
            reader.lines().collect::<::std::io::Result<Vec<_>>>()
        })
        .unwrap()
}

pub fn load_test_jsons() -> Vec<String> {
    let mut file = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    file.push("tests/coprocessor/codec/mysql/json/world_bank.json");
    open_file_read_lines(file)
}

#[test]
fn test_load_test_jsons() {
    let json_texts = load_test_jsons();
    assert_eq!(json_texts.len(), 500);
}

#[bench]
fn bench_encode_binary(b: &mut Bencher) {
    let jsons: Vec<Json> = load_test_jsons()
        .into_iter()
        .map(|t| t.parse().unwrap())
        .collect();
    let mut buf = Vec::with_capacity(65536);
    b.iter(|| for j in &jsons {
        buf.clear();
        buf.encode_json(j).unwrap();
    });
}

#[bench]
fn bench_encode_text(b: &mut Bencher) {
    let jsons: Vec<Json> = load_test_jsons()
        .into_iter()
        .map(|t| t.parse().unwrap())
        .collect();
    let mut buf = Vec::with_capacity(65536);
    b.iter(|| for j in &jsons {
        buf.clear();
        serde_json::to_writer(&mut buf, j).unwrap();
    });
}

#[bench]
fn bench_decode_text(b: &mut Bencher) {
    let texts = load_test_jsons();
    b.iter(|| for text in &texts {
        text.parse::<Json>().unwrap();
    });
}

#[bench]
fn bench_decode_binary(b: &mut Bencher) {
    let binaries = load_test_jsons()
        .into_iter()
        .map(|t| t.parse::<Json>().unwrap())
        .map(|j| {
            let mut buf = Vec::new();
            buf.encode_json(&j).unwrap();
            buf
        })
        .collect::<Vec<Vec<u8>>>();
    b.iter(|| for binary in &binaries {
        binary.as_slice().decode_json().unwrap();
    });
}
