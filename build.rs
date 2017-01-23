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
use std::fs::File;
use std::io::Write;
use std::path::PathBuf;
use std::process::Command;

fn main() {
    let out_dir = PathBuf::from(env::var_os("OUT_DIR").unwrap());

    File::create(out_dir.join("build-info.txt"))
        .unwrap()
        .write_all(build_info().as_bytes())
        .unwrap();
}

// build_info returns a string of commit hash and utc time.
fn build_info() -> String {
    // explicit separates outputs by '\n'.
    format!("{}\n{}\n{}",
            commit_hash().trim_right(),
            utc_time(),
            rustc_version())
}

fn utc_time() -> String {
    let utc = time::now_utc();
    format!("{}", utc.strftime("%Y-%m-%d %I:%M:%S").unwrap())
}

fn commit_hash() -> String {
    let mut cmd = Command::new("git");
    cmd.args(&["rev-parse", "HEAD"]);
    cmd.output().ok().and_then(|o| String::from_utf8(o.stdout).ok()).unwrap_or("None".to_owned())
}

fn rustc_version() -> String {
    let mut cmd = Command::new("rustc");
    cmd.args(&["--version"]);
    cmd.output()
        .ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .map(|v| v.trim_left_matches("rustc").trim().to_owned())
        .unwrap()
}
