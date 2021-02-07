use bindgen::EnumVariation;
use clap::{App, Arg};
use std::collections::HashMap;
use std::fs;
use std::io::{Read, Write};
use std::path::Path;
use walkdir::WalkDir;

fn read_file_to_string<P: AsRef<Path>>(path: P, expect: &str) -> String {
    let mut file = fs::File::open(path).expect("Couldn't open headers");
    let mut buff = String::new();
    file.read_to_string(&mut buff).expect(expect);
    buff
}

fn scan_ffi_src_head(dir: &str) -> (Vec<String>, String) {
    let mut headers = Vec::new();
    let mut headers_buff = HashMap::new();
    for result in WalkDir::new(Path::new(dir)) {
        let dent = result.expect("Error happened when search headers");
        if !dent.file_type().is_file() {
            continue;
        }
        let buff = read_file_to_string(dent.path(), "Couldn't open headers");
        let head_file_path = String::from(dent.path().to_str().unwrap());
        if !head_file_path.ends_with(".h") {
            continue;
        }
        headers.push(head_file_path.clone());
        headers_buff.insert(head_file_path, buff);
    }
    headers.sort();
    let md5_sum = {
        let mut context = md5::Context::new();
        for name in &headers {
            let buff = headers_buff.get(name).unwrap();
            context.consume(buff);
        }
        let digest = context.compute();
        format!("{:x}", digest)
    };
    (headers, md5_sum)
}

fn read_version_file(version_cpp_file: &str) -> (String, u32) {
    let buff = read_file_to_string(version_cpp_file, "Couldn't open version file");
    let md5_version: Vec<_> = buff.split("//").collect();
    let (md5_sum, version) = (md5_version[1], md5_version[2]);
    let version = version.parse::<u32>().unwrap();
    (md5_sum.to_string(), version)
}

fn make_version_file(md5_sum: String, version: u32, tar_version_head_path: &str) {
    let buff = format!("//{}//{}//\n#pragma once\n#include <cstdint>\nnamespace DB {{ constexpr uint32_t RAFT_STORE_PROXY_VERSION = {}; }}", md5_sum, version, version);
    let tmp_path = format!("{}.tmp", tar_version_head_path);
    let mut file = fs::File::create(&tmp_path).expect("Couldn't create tmp cpp version head file");
    file.write(buff.as_bytes()).unwrap();
    fs::rename(tmp_path.as_str(), tar_version_head_path)
        .expect("Couldn't make cpp version head file");
}

fn gen_ffi_code() {
    const NOT_CHANGE_VERSION_ARG_NAME: &str = "not-change-version";
    let matches = App::new("RaftStore Proxy FFI Generator")
        .author("tongzhigao@pingcap.com")
        .arg(
            Arg::with_name(NOT_CHANGE_VERSION_ARG_NAME)
                .long(NOT_CHANGE_VERSION_ARG_NAME)
                .help("Do not update version anyway"),
        )
        .get_matches();

    let not_change_version = matches.is_present(NOT_CHANGE_VERSION_ARG_NAME);

    let src_dir = "raftstore-proxy/ffi/src/RaftStoreProxyFFI";
    let tar_file = "components/raftstore/src/engine_store_ffi/interfaces.rs";
    let version_cpp_file = format!("{}/@version", src_dir);

    let (ori_md5_sum, ori_version) = read_version_file(version_cpp_file.as_str());
    println!("\nFFI src dir path is {}", src_dir);
    println!("Original version is {}", ori_version);
    let new_version = if not_change_version {
        println!("Do not update version anyway");
        ori_version
    } else {
        ori_version + 1
    };

    let (headers, md5_sum) = scan_ffi_src_head(src_dir);

    {
        println!("Scan and get src files");
        for f in &headers {
            println!("    {}", f);
        }
        if md5_sum == ori_md5_sum {
            println!("Check md5 sum equal");
        } else {
            println!(
                "Check md5 sum not equal, original is {}, current is {}, start to generate rust code with version {}", ori_md5_sum, md5_sum,
                new_version
            );
            make_version_file(md5_sum, new_version, version_cpp_file.as_str());
        }
    }

    let mut builder = bindgen::Builder::default()
        .clang_arg("-std=c++11")
        .clang_arg("-x")
        .clang_arg("c++")
        .clang_arg("-Wno-pragma-once-outside-header")
        .layout_tests(false)
        .derive_copy(false)
        .enable_cxx_namespaces()
        .default_enum_style(EnumVariation::Rust {
            non_exhaustive: false,
        });

    for path in headers {
        builder = builder.header(path);
    }

    let bindings = builder.generate().unwrap();

    let buff = bindings.to_string();
    let ori_buff = read_file_to_string(tar_file, "Couldn't open rust ffi code file");
    if ori_buff == buff {
        println!("There is no need to overwrite rust ffi code file");
    } else {
        println!("Start generate rust code into {}\n", tar_file);
        let mut file = fs::File::create(tar_file).expect("Couldn't create rust ffi code file");
        file.write(buff.as_bytes()).unwrap();
    }
}

fn main() {
    gen_ffi_code();
}
