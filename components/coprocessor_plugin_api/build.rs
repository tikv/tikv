// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

fn main() {
    println!("cargo:rerun-if-changed=*");
    println!("cargo:rustc-env=API_VERSION={}", env!("CARGO_PKG_VERSION"));
    println!(
        "cargo:rustc-env=TARGET={}",
        std::env::var("TARGET").unwrap()
    );
    println!(
        "cargo:rustc-env=RUSTC_VERSION={}",
        rustc_version::version_meta().unwrap().short_version_string
    );
}
