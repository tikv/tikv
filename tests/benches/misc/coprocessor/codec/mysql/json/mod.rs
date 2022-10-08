// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    borrow::ToOwned,
    io,
    io::prelude::*,
    process::{Command, Stdio},
    thread,
};

use test::Bencher;
use tidb_query_datatype::codec::mysql::{Json, JsonDecoder, JsonEncoder};

fn download_and_extract_file(url: &str) -> io::Result<String> {
    let mut dl_child = Command::new("curl")
        .arg(url)
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()?;
    let mut tar_child = Command::new("tar")
        .args(&["xzf", "-", "--to-stdout"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()?;

    let mut dl_output = dl_child.stdout.take().unwrap();
    let mut tar_input = tar_child.stdin.take().unwrap();
    let th = thread::spawn(move || -> io::Result<()> {
        let mut buf = vec![0; 4096];
        loop {
            let nbytes = dl_output.read(&mut buf)?;
            if nbytes > 0 {
                tar_input.write_all(&buf[0..nbytes])?;
                continue;
            }
            return Ok(());
        }
    });

    let output = tar_child.wait_with_output()?;
    dl_child.wait()?;
    th.join().unwrap()?;
    assert_eq!(output.status.code(), Some(0));
    Ok(String::from_utf8(output.stdout).unwrap())
}

pub fn load_test_jsons() -> io::Result<Vec<String>> {
    let url = "https://download.pingcap.org/resources/world_bank.json.tar.gz";
    download_and_extract_file(url).map(|raw: String| {
        raw.split('\n')
            .filter(|s| !s.is_empty())
            .map(ToOwned::to_owned)
            .collect::<Vec<_>>()
    })
}

#[ignore]
#[bench]
fn bench_encode_binary(b: &mut Bencher) {
    let jsons: Vec<Json> = load_test_jsons()
        .unwrap()
        .into_iter()
        .map(|t| t.parse().unwrap())
        .collect();
    let mut buf = Vec::with_capacity(65536);
    b.iter(|| {
        for j in &jsons {
            buf.clear();
            buf.write_json(j.as_ref()).unwrap();
        }
    });
}

#[ignore]
#[bench]
fn bench_encode_text(b: &mut Bencher) {
    let jsons: Vec<Json> = load_test_jsons()
        .unwrap()
        .into_iter()
        .map(|t| t.parse().unwrap())
        .collect();
    let mut buf = Vec::with_capacity(65536);
    b.iter(|| {
        for j in &jsons {
            buf.clear();
            ::serde_json::to_writer(&mut buf, &j.as_ref()).unwrap();
        }
    });
}

#[ignore]
#[bench]
fn bench_decode_text(b: &mut Bencher) {
    let texts = load_test_jsons().unwrap();
    b.iter(|| {
        for text in &texts {
            text.parse::<Json>().unwrap();
        }
    });
}

#[ignore]
#[bench]
fn bench_decode_binary(b: &mut Bencher) {
    let binaries = load_test_jsons()
        .unwrap()
        .into_iter()
        .map(|t| t.parse::<Json>().unwrap())
        .map(|j| {
            let mut buf = Vec::new();
            buf.write_json(j.as_ref()).unwrap();
            buf
        })
        .collect::<Vec<Vec<u8>>>();
    b.iter(|| {
        for binary in &binaries {
            binary.as_slice().read_json().unwrap();
        }
    });
}
