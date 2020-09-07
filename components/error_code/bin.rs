use std::{fs, io::Write, path::Path};

use error_code::*;

fn main() {
    let err_codes = vec![
        codec::ALL_ERROR_CODES.iter(),
        coprocessor::ALL_ERROR_CODES.iter(),
        encryption::ALL_ERROR_CODES.iter(),
        engine::ALL_ERROR_CODES.iter(),
        pd::ALL_ERROR_CODES.iter(),
        raft::ALL_ERROR_CODES.iter(),
        raftstore::ALL_ERROR_CODES.iter(),
        sst_importer::ALL_ERROR_CODES.iter(),
        storage::ALL_ERROR_CODES.iter(),
    ];
    let mut content = String::new();
    err_codes
        .into_iter()
        .flatten()
        .map(|c| {
            let s = toml::to_string_pretty(c).unwrap();
            format!("[error.{}]\n", c.code) + s.as_str() + "\n"
        })
        .for_each(|s| content.push_str(&s));
    let path = Path::new("./etc/error_code.toml");
    let mut f = fs::File::create(&path).unwrap();
    f.write_all(content.as_bytes()).unwrap();
    f.sync_all().unwrap();
}
