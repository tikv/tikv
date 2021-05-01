// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::env;
use std::path::Path;

fn main() {
    let out_dir = env::var_os("OUT_DIR").unwrap();
    let build_dir = Path::new(&out_dir)
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .parent()
        .unwrap();
    let plugin_path = build_dir.join("libexample_plugin.so");
    println!(
        "cargo:rustc-env=CARGO_DYLIB_FILE_EXAMPLE_PLUGIN={}",
        plugin_path.display()
    );
}
