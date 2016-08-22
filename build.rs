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

use std::env;
use std::fs::File;
use std::io::Write;
use std::error::Error;
use std::path::PathBuf;
use std::process::Command;

struct Ignore;

impl<E> From<E> for Ignore
    where E: Error
{
    fn from(_: E) -> Ignore {
        Ignore
    }
}

fn main() {
    let out_dir = PathBuf::from(env::var_os("OUT_DIR").unwrap());

    File::create(out_dir.join("build-info.txt"))
        .unwrap()
        .write_all(build_info().as_bytes())
        .unwrap();
}

// build_info returns a string of commit hash and utc time or an empty string.
fn build_info() -> String {
    match (commit_hash(), utc_time()) {
        // explicit separates outputs by '\n'.
        (Ok(hash), Ok(date)) => format!("{}\n{}", hash.trim_right(), date),
        _ => String::new(),
    }
}

fn utc_time() -> Result<String, Ignore> {
    Ok(try!(String::from_utf8(try!(Command::new("date")
            .args(&["-u", "+%Y-%m-%d %I:%M:%S"])
            .output())
        .stdout)))
}

fn commit_hash() -> Result<String, Ignore> {
    Ok(try!(String::from_utf8(try!(Command::new("git")
            .args(&["rev-parse", "HEAD"])
            .output())
        .stdout)))
}
