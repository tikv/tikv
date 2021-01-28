extern crate bindgen;
extern crate walkdir;

use bindgen::EnumVariation;
use clap::{App, Arg};
use std::fs;
use std::io::Read;
use std::path::Path;
use walkdir::WalkDir;

fn scan_ffi_src_head(dir: &str) -> Vec<String> {
    let mut headers = Vec::new();
    for result in WalkDir::new(Path::new(dir)) {
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
    headers
}

fn gen_ffi_code() {
    let matches = App::new("RaftStore Proxy FFI Generator")
        .author("tongzhigao@pingcap.com")
        .arg(
            Arg::with_name("ffi_src_dir")
                .long("ffi_src_dir")
                .help("Sets the path of FFI source code directory")
                .required(true)
                .takes_value(true),
        )
        .arg(
            Arg::with_name("ffi_tar_file")
                .long("ffi_tar_file")
                .help("Sets the target file path of generated FFI rust code")
                .default_value("components/raftstore/src/engine_store_ffi/interfaces.rs")
                .takes_value(true),
        )
        .get_matches();

    let src_dir = matches.value_of("ffi_src_dir").unwrap();
    let tar_file = matches.value_of("ffi_tar_file").unwrap();

    println!("\nFFI src dir path is {}", src_dir);

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

    let headers = scan_ffi_src_head(src_dir);

    {
        println!("Scan and get src files");
        for f in &headers {
            println!("    {}", f);
        }
    }

    for path in headers {
        builder = builder.header(path);
    }

    println!("Start generate rust code into {}\n", tar_file);

    let bindings = builder.generate().unwrap();

    bindings.write_to_file(tar_file).unwrap();
}

fn main() {
    gen_ffi_code();
}
