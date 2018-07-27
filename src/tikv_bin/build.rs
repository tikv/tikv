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

extern crate time;

use std::env;
use std::process::Command;

fn main() {
    println!("cargo:rustc-env=TIKV_BUILD_TIME={}", utc_time());
    println!("cargo:rustc-env=TIKV_BUILD_COMMIT={}", commit_hash());
    println!("cargo:rustc-env=TIKV_BUILD_BRANCH={}", branch_info());
    println!("cargo:rustc-env=TIKV_BUILD_RUSTC={}", rustc_version());
}

fn utc_time() -> String {
    let utc = time::now_utc();
    format!("{}", utc.strftime("%Y-%m-%d %H:%M:%S").unwrap())
}

fn commit_hash() -> String {
    let mut cmd = Command::new("git");
    cmd.args(&["rev-parse", "HEAD"]);
    cmd.output()
        .ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .map(|v| v.trim_right().to_owned())
        .unwrap_or("None".to_owned())
}

fn branch_info() -> String {
    let mut cmd = Command::new("git");
    cmd.args(&["rev-parse", "--abbrev-ref", "HEAD"]);
    cmd.output()
        .ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .map(|v| v.trim_right().to_owned())
        .unwrap_or("None".to_owned())
}

fn rustc_version() -> String {
    let mut cmd = Command::new(env::var("RUSTC").unwrap());
    cmd.args(&["--version"]);
    cmd.output()
        .ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .map(|v| v.trim_left_matches("rustc").trim().to_owned())
        .unwrap()
}
