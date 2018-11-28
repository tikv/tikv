// Copyright 2016 rust-fuzz developers
//
// Licensed under the Apache License, Version 2.0, <LICENSE-APACHE or
// http://apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. This file may not be
// copied, modified, or distributed except according to those terms.

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

//! Command line utility to run fuzz tests.
//!
//! Adopted from https://github.com/rust-fuzz/targets

#[macro_use]
extern crate structopt;
#[macro_use]
extern crate clap;
#[macro_use]
extern crate failure;
#[macro_use]
extern crate lazy_static;
extern crate cargo_metadata;

use std::env;
use std::path::PathBuf;
use std::process::Command;

use failure::{Error, ResultExt};

lazy_static! {
    static ref WORKSPACE_ROOT: PathBuf = {
        let meta = cargo_metadata::metadata(None).unwrap();
        PathBuf::from(meta.workspace_root)
    };

    static ref FUZZ_ROOT: PathBuf = WORKSPACE_ROOT.join("fuzz");
}

#[derive(StructOpt, Debug)]
enum Cli {
    /// Run matched fuzz test with specific fuzzer.
    #[structopt(name = "run")]
    Run {
        /// The fuzzer to use.
        fuzzer: Fuzzer,
        /// Filter fuzz test to run. Empty means no filter.
        #[structopt(default_value = "")]
        filter: String,
    }
}

arg_enum! {
    #[derive(Debug, PartialEq, Eq, Clone, Copy)]
    enum Fuzzer {
        Afl,
        Honggfuzz,
        Libfuzzer
    }
}

impl Fuzzer {
    /// Get Cargo package name of corresponding fuzzers.
    fn package_name(&self) -> &'static str {
        match self {
            Fuzzer::Afl => "fuzzer-afl",
            Fuzzer::Honggfuzz => "fuzzer-honggfuzz",
            Fuzzer::Libfuzzer => "fuzzer-libfuzzer",
        }
    }

    /// Get Cargo directory of corresponding fuzzers.
    fn directory(&self) -> PathBuf {
        FUZZ_ROOT.join(self.package_name())
    }
}

fn main() {
    use structopt::StructOpt;

    match Cli::from_args() {
        Cli::Run { fuzzer, filter } => {
            run(fuzzer, &filter).unwrap();
        }
    }
}

/// Run one target fuzz test with specific fuzzer
fn run(fuzzer: Fuzzer, filter: &str) -> Result<(), Error> {
    match fuzzer {
        Fuzzer::Afl => run_afl(filter),
        Fuzzer::Honggfuzz => run_honggfuzz(filter),
        Fuzzer::Libfuzzer => run_libfuzzer(filter),
    }
}

/// Run one target fuzz test using AFL
fn run_afl(_filter: &str) -> Result<(), Error>  {
    // AFL requires initial inputs, so leave it to future.
    // General process:
    // 1. cargo afl build (in fuzzer-afl directory)
    // 2. cargo afl fuzz -i in -o out target/debug/fuzzer-afl
    unimplemented!()
}

/// Run one target fuzz test using Honggfuzz
fn run_honggfuzz(filter: &str) -> Result<(), Error> {
    let fuzzer = Fuzzer::Honggfuzz;

    let fuzzer_bin = Command::new("cargo")
        .args(&[
            "hfuzz",
            "run",
            fuzzer.package_name()
        ])
        .env("TIKV_FUZZ_TARGETS", filter)
        .current_dir(fuzzer.directory())
        .spawn()
        .context(format!("Failed to run {}", fuzzer))?
        .wait()
        .context(format!("Failed to wait {}", fuzzer))?;

    if !fuzzer_bin.success() {
        Err(format_err!("{} exited with code {:?}", fuzzer, fuzzer_bin.code()))?;
    }

    Ok(())
}

/// Run one target fuzz test using Libfuzzer
fn run_libfuzzer(filter: &str) -> Result<(), Error>  {
    let fuzzer = Fuzzer::Libfuzzer;

    #[cfg(target_os = "macos")]
    let target_platform = "x86_64-apple-darwin";
    #[cfg(target_os = "linux")]
    let target_platform = "x86_64-unknown-linux-gnu";
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    panic!("libfuzzer-sys only supports Linux and macOS");

    let rust_flags = format!(
        "{} {}",
        env::var("RUSTFLAGS").unwrap_or_default(),
        "--cfg fuzzing \
         -C passes=sancov \
         -C llvm-args=-sanitizer-coverage-level=4 \
         -C llvm-args=-sanitizer-coverage-trace-pc-guard \
         -C llvm-args=-sanitizer-coverage-trace-compares \
         -C llvm-args=-sanitizer-coverage-trace-divs \
         -C llvm-args=-sanitizer-coverage-trace-geps \
         -C llvm-args=-sanitizer-coverage-prune-blocks=0 \
         -Z sanitizer=address"
    );

    let mut asan_options = env::var("ASAN_OPTIONS").unwrap_or_default();
    asan_options.push_str(" detect_odr_violation=0");

    let fuzzer_bin = Command::new("cargo")
        .args(&[
            "run",
            "--target",
            &target_platform,
            "--package",
            fuzzer.package_name(),
        ])
        .env("RUSTFLAGS", &rust_flags)
        .env("ASAN_OPTIONS", &asan_options)
        .env("TIKV_FUZZ_TARGETS", filter)
        .current_dir(&*WORKSPACE_ROOT)
        .spawn()
        .context(format!("Failed to run {}", fuzzer))?
        .wait()
        .context(format!("Failed to wait {}", fuzzer))?;

    if !fuzzer_bin.success() {
        Err(format_err!("{} exited with code {:?}", fuzzer, fuzzer_bin.code()))?;
    }

    Ok(())
}
