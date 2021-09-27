// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use crate::IOBytes;
use crate::IOType;
use std::cell::Cell;
use std::fs::File;
use std::io::BufRead;
use std::io::BufReader;
use std::path::PathBuf;
use std::str::FromStr;

thread_local! {
    static IO_TYPE: Cell<IOType> = Cell::new(IOType::Other)
}

pub fn set_io_type(new_io_type: IOType) {
    IO_TYPE.with(|io_type| {
        io_type.set(new_io_type);
    });
}

pub fn get_io_type() -> IOType {
    IO_TYPE.with(|io_type| io_type.get())
}

pub(crate) fn fetch_io_bytes(_io_type: IOType) -> IOBytes {
    let tid = nix::unistd::gettid();

    let io_file_path = PathBuf::from("/proc/self/task")
        .join(format!("{}", tid))
        .join("io");

    if let Ok(io_file) = File::open(io_file_path) {
        return IOBytes::from_io_file(io_file);
    }

    IOBytes::default()
}

impl IOBytes {
    fn from_io_file<R: std::io::Read>(r: R) -> IOBytes {
        let reader = BufReader::new(r);
        let mut io_bytes = IOBytes::default();

        for line in reader.lines().flatten() {
            if line.is_empty() || !line.contains(' ') {
                continue;
            }
            let mut s = line.split_whitespace();

            if let (Some(field), Some(value)) = (s.next(), s.next()) {
                if let Ok(value) = u64::from_str(value) {
                    match &field[..field.len() - 1] {
                        "read_bytes" => io_bytes.read = value,
                        "write_bytes" => io_bytes.write = value,
                        _ => continue,
                    }
                }
            }
        }

        io_bytes
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;
    use tempfile::TempDir;

    use super::*;

    #[test]
    fn test_write_bytes() {
        let dir = TempDir::new().unwrap();
        let mut file = File::create(dir.path().join("test_write.txt")).unwrap();

        let origin_io_bytes = fetch_io_bytes(IOType::Other);
        for i in 1..100 {
            file.write_all(" ".to_string().as_bytes()).unwrap();
            file.sync_all().unwrap();

            let io_bytes = fetch_io_bytes(IOType::Other);

            assert_eq!(i * 4096 + origin_io_bytes.write, io_bytes.write);
        }
    }

    #[bench]
    fn bench_fetch_io_bytes(b: &mut test::Bencher) {
        b.iter(|| {
            fetch_io_bytes(IOType::Other)
        });
    }
}
