use std::{fs, io::Write, path::Path};

use error_code::*;

fn main() {
    let err_codes = vec![
        cloud::ALL_ERROR_CODES.iter(),
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
    let path = Path::new("./etc/error_code.toml");
    let mut f = fs::File::create(&path).unwrap();
    err_codes
        .into_iter()
        .flatten()
        .map(|c| format!("[\"{}\"]\nerror = '''\n{}\n'''\n\n", c.code, c.code))
        .for_each(|s| {
            f.write_all(s.as_bytes()).unwrap();
        });

    f.sync_all().unwrap();
}
