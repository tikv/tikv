use std::{
    collections::{hash_map::DefaultHasher, HashMap},
    fs,
    hash::{Hash, Hasher},
    io::{Read, Write},
    path::Path,
};

use walkdir::WalkDir;

type VersionType = u64;

const RAFT_STORE_PROXY_VERSION_PREFIX: &str = "RAFT_STORE_PROXY_VERSION";
const DB_PREFIX: &str = "    pub mod DB {\n";
const DB_SUFFIX: &str = "\n    }";
const ROOT_PREFIX: &str = "pub mod root {\n";
const ROOT_SUFFIX: &str = "\n}\n";

fn read_file_to_string<P: AsRef<Path>>(path: P, expect: &str) -> String {
    let mut file = fs::File::open(path).expect(expect);
    let mut buff = String::new();
    file.read_to_string(&mut buff).expect(expect);
    buff
}

fn filter_by_namespace(buff: &str) -> String {
    let mut res = String::new();
    // pub mod root {?
    let a1 = buff.find(ROOT_PREFIX).unwrap() + ROOT_PREFIX.len();
    res.push_str(&buff[..a1]);
    // ?    pub mod DB {
    let b1 = buff.find(DB_PREFIX).unwrap();
    let b2 = (&buff[b1..]).find(DB_SUFFIX).unwrap() + DB_SUFFIX.len() + b1;
    // println!("{}", &buff[b1..b2]);
    res.push_str(&buff[b1..b2]);
    res.push_str(ROOT_SUFFIX);
    res
}

fn scan_ffi_src_head(dir: &str) -> (Vec<String>, VersionType) {
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
    let hash_version = {
        let mut hasher = DefaultHasher::new();
        for name in &headers {
            let buff = headers_buff.get(name).unwrap();
            buff.hash(&mut hasher);
        }
        hasher.finish()
    };
    (headers, hash_version)
}

fn read_version_file(version_cpp_file: &str) -> VersionType {
    let buff = read_file_to_string(version_cpp_file, "Couldn't open version file");
    let begin = buff.find(RAFT_STORE_PROXY_VERSION_PREFIX).unwrap();
    let buff = &buff[(begin + RAFT_STORE_PROXY_VERSION_PREFIX.len() + 3)..buff.len()];
    let end = buff.find("ull").unwrap();
    let buff = &buff[..end];
    let version = buff.parse::<VersionType>().unwrap();
    version
}

fn make_version_file(version: VersionType, tar_version_head_path: &str) {
    let buff = format!(
        "#pragma once\n#include <cstdint>\nnamespace DB {{ constexpr uint64_t {} = {}ull; }}",
        RAFT_STORE_PROXY_VERSION_PREFIX, version
    );
    let tmp_path = format!("{}.tmp", tar_version_head_path);
    let mut file = fs::File::create(&tmp_path).expect("Couldn't create tmp cpp version head file");
    file.write(buff.as_bytes()).unwrap();
    fs::rename(&tmp_path, tar_version_head_path).expect("Couldn't make cpp version head file");
}

pub fn gen_ffi_code() {
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    let src_dir = format!(
        "{}/../raftstore-proxy/ffi/src/RaftStoreProxyFFI",
        manifest_dir
    );
    let tar_file = format!(
        "{}/../components/engine_store_ffi/src/interfaces.rs",
        manifest_dir
    );
    let version_cpp_file = format!("{}/@version", src_dir);

    let ori_version = read_version_file(&version_cpp_file);
    println!("\nFFI src dir path is {}", src_dir);
    println!("Original version is {}", ori_version);

    let (headers, hash_version) = scan_ffi_src_head(&src_dir);

    {
        println!("Scan and get src files");
        for f in &headers {
            println!("    {}", f);
        }
        if ori_version == hash_version {
            println!("Check hash version equal & NOT overwrite version");
        } else {
            println!(
                "Current hash version is {}, start to generate rust code with version {}",
                ori_version, hash_version
            );
            make_version_file(hash_version, &version_cpp_file);
        }
    }

    let mut builder = bindgen::Builder::default()
        .clang_arg("-xc++")
        .clang_arg("-std=c++11")
        .clang_arg("-Wno-pragma-once-outside-header")
        .layout_tests(false)
        .derive_copy(false)
        .enable_cxx_namespaces()
        .disable_header_comment()
        .default_enum_style(bindgen::EnumVariation::Rust {
            non_exhaustive: false,
        });

    for path in headers {
        builder = builder.header(path);
    }

    let bindings = builder.generate().unwrap();

    let buff = bindings.to_string();
    let buff = filter_by_namespace(&buff);
    let ori_buff = read_file_to_string(&tar_file, "Couldn't open rust ffi code file");
    if ori_buff == buff {
        println!("There is no need to overwrite rust ffi code file");
    } else {
        println!("Start generate rust code into {}\n", tar_file);
        let mut file = fs::File::create(tar_file).expect("Couldn't create rust ffi code file");
        file.write(buff.as_bytes()).unwrap();
    }
}
