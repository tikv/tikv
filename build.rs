// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::env;
use std::path::Path;

fn main() {
    locate_coprocessor_plugins(&["example-plugin"]);
}

/// Locates compiled coprocessor plugins and makes their paths available via environment variables.
///
/// This is a bit hacky, but can be removed as soon as RFC
/// <https://rust-lang.github.io/rfcs/3028-cargo-binary-dependencies.html> is available.
///
/// Why do we need this?
/// This is because we want to use some coprocessor tests during tests. We can't just add them as a
/// `dev-dependency`, because of how the CI works. Thus, we want to include them in our tests with
/// `include_bytes!()`. However, we don't really know where the artifacts are located, so we need to
/// find them with this function and expose the locations via environment variables.
///
/// Coprocessor plugins need to be added as a `build-dependency` in `Cargo.toml` for TiKV.
fn locate_coprocessor_plugins(plugin_names: &[&str]) {
    let out_dir = env::var_os("OUT_DIR").unwrap();
    let build_dir = Path::new(&out_dir)
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .parent()
        .unwrap();

    for plugin_name in plugin_names {
        let plugin_path = build_dir.join(pkgname_to_libname(plugin_name));
        println!("cargo:rerun-if-changed={}", plugin_path.display());
        println!(
            "cargo:rustc-env=CARGO_DYLIB_FILE_EXAMPLE_PLUGIN={}",
            plugin_path.display()
        );
    }
}

/// Converts a Rust `dylib` crate name to the platform specific library name for the compiled
/// artifact.
pub fn pkgname_to_libname(pkgname: &str) -> String {
    let pkgname = pkgname.to_string().replace("-", "_");
    if cfg!(target_os = "windows") {
        format!("{}.dll", pkgname)
    } else if cfg!(target_os = "macos") {
        format!("lib{}.dylib", pkgname)
    } else {
        format!("lib{}.so", pkgname)
    }
}
