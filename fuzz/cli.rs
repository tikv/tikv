// Copyright 2016 rust-fuzz developers
//
// Licensed under the Apache License, Version 2.0, <LICENSE-APACHE or
// http://apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. This file may not be
// copied, modified, or distributed except according to those terms.

// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

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
extern crate regex;

use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use failure::{Error, ResultExt};

lazy_static! {
    static ref WORKSPACE_ROOT: PathBuf = {
        let meta = cargo_metadata::metadata(None).unwrap();
        PathBuf::from(meta.workspace_root)
    };
    static ref FUZZ_ROOT: PathBuf = WORKSPACE_ROOT.join("fuzz");
    static ref FUZZ_TARGETS: Vec<String> = {
        let source = FUZZ_ROOT.join("targets/mod.rs");
        let targets_rs = fs::read_to_string(&source).unwrap();
        let match_fuzz_fs = regex::Regex::new(r"pub fn fuzz_(\w+)\(").unwrap();
        let target_names = match_fuzz_fs
            .captures_iter(&targets_rs)
            .map(|x| format!("fuzz_{}", &x[1]));
        target_names.collect()
    };
    static ref SEED_ROOT: PathBuf = FUZZ_ROOT.join("common/seeds");
}

#[derive(StructOpt, Debug)]
enum Cli {
    /// List all available targets
    #[structopt(name = "list-targets")]
    ListTargets,
    /// Run matched fuzz test with specific fuzzer.
    #[structopt(name = "run")]
    Run {
        /// The fuzzer to use.
        fuzzer: Fuzzer,
        /// The target fuzz to run.
        target: String,
    },
}

arg_enum! {
    #[derive(Debug, PartialEq, Eq, Clone, Copy)]
    enum Fuzzer {
        Afl,
        Honggfuzz,
        Libfuzzer,
    }
}

impl Fuzzer {
    /// Get Cargo package name of corresponding fuzzers.
    fn package_name(self) -> &'static str {
        match self {
            Fuzzer::Afl => "fuzzer-afl",
            Fuzzer::Honggfuzz => "fuzzer-honggfuzz",
            Fuzzer::Libfuzzer => "fuzzer-libfuzzer",
        }
    }

    /// Get Cargo directory of corresponding fuzzers.
    fn directory(self) -> PathBuf {
        FUZZ_ROOT.join(self.package_name())
    }
}

fn main() -> Result<(), i32> {
    use structopt::StructOpt;

    match Cli::from_args() {
        Cli::ListTargets => {
            for target in &*FUZZ_TARGETS {
                println!("{}", target);
            }
        }
        Cli::Run { fuzzer, target } => {
            if let Err(error) = run(fuzzer, &target) {
                eprintln!("Running fuzzer failed: {}", error);
                return Err(1);
            }
        }
    }
    Ok(())
}

/// Write the fuzz target source file from corresponding template file.
///
/// `target` must be a valid target.
fn write_fuzz_target_source_file(fuzzer: Fuzzer, target: &str) -> Result<(), Error> {
    use std::io::Write;

    let template_file_path = fuzzer.directory().join("template.rs");
    let template = fs::read_to_string(&template_file_path).context(format!(
        "Error reading template file {}",
        template_file_path.display()
    ))?;

    let target_file_path = fuzzer.directory().join(&format!("src/bin/{}.rs", target));
    let mut file = fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&target_file_path)
        .context(format!(
            "Error writing fuzz target source file {}",
            target_file_path.display()
        ))?;

    let source = template.replace("__FUZZ_CLI_TARGET__", &target).replace(
        "__FUZZ_GENERATE_COMMENT__",
        "NOTE: AUTO GENERATED FROM `template.rs`",
    );

    file.write_all(source.as_bytes())?;
    Ok(())
}

/// Run one target fuzz test with specific fuzzer
fn run(fuzzer: Fuzzer, target: &str) -> Result<(), Error> {
    if FUZZ_TARGETS.iter().find(|x| *x == target).is_none() {
        panic!(
            "Unknown fuzz target `{}`. Run `list-targets` command to see available fuzz targets.",
            target
        );
    }
    write_fuzz_target_source_file(fuzzer, target)?;
    match fuzzer {
        Fuzzer::Afl => run_afl(target),
        Fuzzer::Honggfuzz => run_honggfuzz(target),
        Fuzzer::Libfuzzer => run_libfuzzer(target),
    }
}

/// Get input seeds for fuzz target
fn get_seed_dir(target: &str) -> PathBuf {
    let seeds_dir = SEED_ROOT.join(target);
    if seeds_dir.exists() {
        seeds_dir
    } else {
        SEED_ROOT.join("default")
    }
}

/// Create corpus dir for fuzz target
fn create_corpus_dir(base: impl AsRef<Path>, target: &str) -> Result<PathBuf, Error> {
    let base = base.as_ref();
    let corpus_dir = base.join(&format!("corpus-{}", target));
    fs::create_dir_all(&corpus_dir).context(format!(
        "unable to create corpus dir for {}{}",
        base.display(),
        target
    ))?;
    Ok(corpus_dir)
}

fn pre_check(command: &mut Command, hint: &str) -> Result<(), Error> {
    let check = command
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .unwrap();
    if !check.success() {
        Err(format_err!(
            "Pre-checking for fuzzing failed. Consider run `{}` before fuzzing.",
            hint
        ))
    } else {
        Ok(())
    }
}

/// Run one target fuzz test using AFL
fn run_afl(target: &str) -> Result<(), Error> {
    let fuzzer = Fuzzer::Afl;

    let seed_dir = get_seed_dir(target);
    let corpus_dir = create_corpus_dir(fuzzer.directory(), target)?;

    pre_check(
        Command::new("cargo").args(&["afl", "--version"]),
        "cargo install afl",
    )?;

    // 1. cargo afl build (in fuzzer-afl directory)
    let fuzzer_build = Command::new("cargo")
        .args(&["afl", "build", "--bin", target])
        .current_dir(fuzzer.directory())
        .spawn()
        .context(format!("Failed to build {}", fuzzer))?
        .wait()
        .context(format!("Failed to complete building {}", fuzzer))?;

    if !fuzzer_build.success() {
        return Err(format_err!(
            "error building afl instrumented binary, exit code {:?}",
            fuzzer_build.code()
        ));
    }

    // 2. cargo afl fuzz -i {seed_dir} -o {corpus_dir} target/debug/{instrumented_binary}
    let instrumented_bin = WORKSPACE_ROOT.join("target/debug").join(target);
    let fuzzer_bin = Command::new("cargo")
        .args(&["afl", "fuzz"])
        .arg("-i")
        .arg(&seed_dir)
        .arg("-o")
        .arg(&corpus_dir)
        .arg(&instrumented_bin)
        .current_dir(fuzzer.directory())
        .spawn()
        .context(format!("Failed to run {}", fuzzer))?
        .wait()
        .context(format!("Failed to wait {}", fuzzer))?;

    if !fuzzer_bin.success() {
        return Err(format_err!(
            "{} exited with code {:?}",
            fuzzer,
            fuzzer_bin.code()
        ));
    }

    Ok(())
}

/// Run one target fuzz test using Honggfuzz
fn run_honggfuzz(target: &str) -> Result<(), Error> {
    pre_check(
        Command::new("cargo").args(&["hfuzz", "version"]),
        "cargo install hfuzz --version 0.5.34",
    )?;

    let fuzzer = Fuzzer::Honggfuzz;

    let mut rust_flags = env::var("RUSTFLAGS").unwrap_or_default();
    rust_flags.push_str("-Z sanitizer=address");

    let hfuzz_args = format!(
        "-f {} \
         --exit_upon_crash \
         {}",
        get_seed_dir(target).to_string_lossy(),
        env::var("HFUZZ_RUN_ARGS").unwrap_or_default()
    );

    let fuzzer_bin = Command::new("cargo")
        .args(&["hfuzz", "run", target])
        .env("RUSTFLAGS", &rust_flags)
        .env("HFUZZ_RUN_ARGS", &hfuzz_args)
        .current_dir(fuzzer.directory())
        .spawn()
        .context(format!("Failed to run {}", fuzzer))?
        .wait()
        .context(format!("Failed to wait {}", fuzzer))?;

    if !fuzzer_bin.success() {
        return Err(format_err!(
            "{} exited with code {:?}",
            fuzzer,
            fuzzer_bin.code()
        ));
    }

    Ok(())
}

/// Run one target fuzz test using Libfuzzer
fn run_libfuzzer(target: &str) -> Result<(), Error> {
    let fuzzer = Fuzzer::Libfuzzer;
    let seed_dir = get_seed_dir(target);
    let corpus_dir = create_corpus_dir(fuzzer.directory(), target)?;

    #[cfg(target_os = "macos")]
    let target_platform = "x86_64-apple-darwin";
    #[cfg(target_os = "linux")]
    let target_platform = "x86_64-unknown-linux-gnu";
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    panic!("libfuzzer-sys only supports Linux and macOS");

    // FIXME: The -C codegen-units=1 and -C incremental=..
    // below seem to workaround some difficult issues in Rust nightly
    // https://github.com/rust-lang/rust/issues/53945.
    // If this is ever fixed remember to remove the fuzz-incremental
    // entry from .gitignore.
    let mut rust_flags = env::var("RUSTFLAGS").unwrap_or_default();
    rust_flags.push_str(
        "--cfg fuzzing \
         -C codegen-units=1 \
         -C incremental=fuzz-incremental \
         -C passes=sancov \
         -C llvm-args=-sanitizer-coverage-level=4 \
         -C llvm-args=-sanitizer-coverage-trace-pc-guard \
         -C llvm-args=-sanitizer-coverage-trace-compares \
         -C llvm-args=-sanitizer-coverage-trace-divs \
         -C llvm-args=-sanitizer-coverage-trace-geps \
         -C llvm-args=-sanitizer-coverage-prune-blocks=0 \
         -C debug-assertions=on \
         -C debuginfo=0 \
         -C opt-level=3 \
         -Z sanitizer=address",
    );

    let mut asan_options = env::var("ASAN_OPTIONS").unwrap_or_default();
    asan_options.push_str(" detect_odr_violation=0");

    let fuzzer_bin = Command::new("cargo")
        .args(&["run", "--target", &target_platform, "--bin", target, "--"])
        .arg(&corpus_dir)
        .arg(&seed_dir)
        .env("RUSTFLAGS", &rust_flags)
        .env("ASAN_OPTIONS", &asan_options)
        .current_dir(fuzzer.directory())
        .spawn()
        .context(format!("Failed to run {}", fuzzer))?
        .wait()
        .context(format!("Failed to wait {}", fuzzer))?;

    if !fuzzer_bin.success() {
        return Err(format_err!(
            "{} exited with code {:?}",
            fuzzer,
            fuzzer_bin.code()
        ));
    }

    Ok(())
}
