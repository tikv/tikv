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

/// A macro that evaluates to current branch, commit, UTC time and rustc version
/// at compile time. These information is printed out when passing `--version`.
extern crate proc_macro;
extern crate proc_macro2;
extern crate time;
#[macro_use]
extern crate quote;
extern crate rustc_version;

use std::process::Command;

use proc_macro2::Literal;

#[proc_macro]
pub fn build_info(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    // Input parameters are not allowed
    assert!(input.to_string().is_empty());

    let commit_hash = Literal::string(&commit_hash());
    let branch_info = Literal::string(&branch_info());
    let utc_time = Literal::string(&utc_time());
    let rustc_ver = Literal::string(&rustc_ver());

    let output = quote! {
        (#commit_hash, #branch_info, #utc_time, #rustc_ver)
    };
    output.into()
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
        .map(|s| s.trim_right().to_owned())
        .unwrap_or_else(|| "None".to_owned())
}

fn branch_info() -> String {
    let mut cmd = Command::new("git");
    cmd.args(&["rev-parse", "--abbrev-ref", "HEAD"]);
    cmd.output()
        .ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .map(|s| s.trim_right().to_owned())
        .unwrap_or_else(|| "None".to_owned())
}

fn rustc_ver() -> String {
    let version = rustc_version::version_meta().unwrap();
    version.short_version_string
}
