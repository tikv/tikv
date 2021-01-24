extern crate bindgen;
extern crate walkdir;

use bindgen::{Builder, EnumVariation};
use std::io::Read;
use std::path::Path;
use std::{env, fs};
use walkdir::WalkDir;

fn add_ffi_src_head(dir: String, mut config: Builder) -> Builder {
    let mut headers = Vec::new();
    for result in WalkDir::new(Path::new(&dir)) {
        let dent = result.expect("Error happened when search headers");
        if !dent.file_type().is_file() {
            continue;
        }
        let mut file = fs::File::open(dent.path()).expect("Couldn't open headers");
        let mut buf = String::new();
        file.read_to_string(&mut buf)
            .expect("Couldn't read header content");
        headers.push(String::from(dent.path().to_str().unwrap()));
    }
    headers.sort();
    for path in headers {
        config = config.header(path);
    }
    config
}

fn gen_ffi_code() {
    let mut builder = bindgen::Builder::default()
        .clang_arg("-std=c++17")
        .clang_arg("-x")
        .clang_arg("c++")
        .layout_tests(false)
        .derive_copy(false)
        .enable_cxx_namespaces()
        .default_enum_style(EnumVariation::Rust {
            non_exhaustive: false,
        });

    let dir = env::var("RAFT_STORE_PROXY_FFI_SRC_DIR").unwrap();

    builder = add_ffi_src_head(dir, builder);

    let bindings = builder.generate().unwrap();

    let tar_file = env::var("RAFT_STORE_PROXY_FFI_TAR_FILE").unwrap_or(String::from(
        "components/raftstore/src/tiflash_ffi/interfaces.rs",
    ));

    bindings.write_to_file(tar_file).unwrap();
}

fn main() {
    gen_ffi_code();
}
