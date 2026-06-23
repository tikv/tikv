// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

#[allow(unused_extern_crates)]
extern crate tikv_alloc;

use std::process::Command;

const ENGINE_LOG_DIR: &str = "ctl-engine-info-log";

fn assert_no_engine_log_dir(args: &[&str]) {
    let cwd = tempfile::tempdir().expect("create temporary directory");

    let output = Command::new(env!("CARGO_BIN_EXE_tikv-ctl"))
        .args(args)
        .current_dir(cwd.path())
        .output()
        .expect("run tikv-ctl");

    assert!(
        output.status.success(),
        "tikv-ctl {args:?} failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );

    assert!(
        !cwd.path().join(ENGINE_LOG_DIR).exists(),
        "tikv-ctl {args:?} created {ENGINE_LOG_DIR}",
    );
}

#[test]
fn local_invocations_do_not_create_engine_log_dir() {
    let cases: &[&[&str]] = &[
        &[],
        &["--help"],
        &["--version"],
        &["--to-escaped", "74"],
        &["--to-hex", "t"],
        &["--encode", "t"],
        &["--decode", r"t\000\000\000\000\000\000\000\370"],
    ];

    for args in cases {
        assert_no_engine_log_dir(args);
    }
}
