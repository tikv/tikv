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

use std::io;
use std::io::prelude::*;
use std::process::{Command, Stdio};
use std::thread;
use test::Bencher;

use tikv::coprocessor::codec::mysql::{Json, JsonDecoder, JsonEncoder};

fn download_and_extract_file(url: &str) -> io::Result<String> {
    let mut dl_child = Command::new("curl")
        .arg(url)
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()?;
    let mut tar_child = Command::new("tar")
        .args(&["xjf", "-", "--to-stdout"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()?;

    let mut dl_output = dl_child.stdout.take().unwrap();
    let mut tar_input = tar_child.stdin.take().unwrap();
    let th = thread::spawn(move || -> io::Result<()> {
        let mut buf = vec![0; 4096];
        loop {
            let nbytes = try!(dl_output.read(&mut buf));
            if nbytes > 0 {
                try!(tar_input.write(&buf[0..nbytes]));
                continue;
            }
            return Ok(());
        }
    });

    try!(dl_child.wait());
    let output = try!(tar_child.wait_with_output());
    try!(th.join().unwrap());
    assert_eq!(output.status.code(), Some(0));
    return Ok(String::from_utf8(output.stdout).unwrap());
}

pub fn load_test_jsons() -> Vec<String> {
    let url = "https://download.pingcap.org/resources/world_bank.json.bz2";
    download_and_extract_file(url)
        .unwrap()
        .split('\n')
        .filter(|s| !s.is_empty())
        .map(|s| s.to_owned())
        .collect::<Vec<_>>();
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
