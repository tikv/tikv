// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

use raftstore::store::{self, RaftStorage, SnapState};
use kvproto::raftpb::Snapshot;
use kvproto::msgpb::SnapshotFile;
use protobuf::{ProtobufError, Message};

use std::sync::Arc;
use std::fmt::{self, Formatter, Display};
use std::io::Write;
use std::path::{Path};
use std::{error, io, fs};
use crc::{crc32, Hasher32};
use byteorder::{ByteOrder, LittleEndian};

use util::worker::Runnable;

/// Snapshot generating task.
pub struct Task {
    store_id: u64,
    storage: Arc<RaftStorage>,
}

impl Task {
    pub fn new(store_id: u64, storage: Arc<RaftStorage>) -> Task {
        Task { storage: storage, store_id: store_id}
    }
}

impl Display for Task {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "Snapshot Task for {}", self.storage.rl().get_region_id())
    }
}

quick_error! {
    #[derive(Debug)]
    enum Error {
        Other(err: Box<error::Error + Sync + Send>) {
            from()
            cause(err.as_ref())
            description(err.description())
            display("snap failed {:?}", err)
        }
        Io(err: io::Error) {
            from()
            cause(err)
            description(err.description())
            display("io error {}", err)
        }
        Protobuf(err: ProtobufError) {
            from()
            cause(err)
            description(err.description())
            display("Protobuf {}", err)
        }
    }
}

pub struct Runner;

impl Runner {
    fn generate_snap(&self, task: &Task) -> Result<(Snapshot, u64), Error> {
        // do we need to check leader here?
        let db = task.storage.rl().get_engine();
        let raw_snap;
        let region_id;
        let ranges;
        let applied_idx;
        let term;

        {
            let storage = task.storage.rl();
            raw_snap = db.snapshot();
            region_id = storage.get_region_id();
            ranges = storage.region_key_ranges();
            applied_idx = box_try!(storage.load_applied_index(&raw_snap));
            term = box_try!(storage.term(applied_idx));
        }

        let snap = box_try!(store::do_snapshot(&raw_snap, region_id, ranges, applied_idx, term));
        Ok((snap, region_id))
    }

    fn save_snapshot(&self, snapshot: &Snapshot,
                     store_id: u64,
                     region_id: u64,
                     dir: &str) -> Result<SnapshotFile, Error> {
        let mut snapshot_file = SnapshotFile::new();
        snapshot_file.set_region(region_id);
        snapshot_file.set_term(snapshot.get_metadata().get_term());
        snapshot_file.set_index(snapshot.get_metadata().get_index());

        let file_name = snapshot_file_path(dir, store_id, &snapshot_file);
        print!("save_snapshot file name: {}\n", file_name);
        let metadata = fs::metadata(&file_name);
        if let Ok(attr) = metadata {
            if attr.is_file() {
                return Err(box_err!("snapshot {} already exist!", file_name));
            }
        }
        let tmp_file_name = format!("{}.tmp", &file_name);
        let f = box try!(fs::File::create(&tmp_file_name));
        let mut crc_writer = CRCWriter::new(f);
        let mut buf = [0; 4];
        let header_len = snapshot.compute_size();
        LittleEndian::write_u32(&mut buf, header_len);
        try!(crc_writer.write(&buf));

        // TODO file format will change
        try!(snapshot.write_to_writer(&mut crc_writer));

        let checksum = crc_writer.sum();
        LittleEndian::write_u32(&mut buf, checksum);
        try!(crc_writer.write(&buf));

        try!(fs::rename(tmp_file_name, file_name));
        Ok(snapshot_file)
    }
}

impl Runnable<Task> for Runner {
    fn run(&mut self, task: Task) {
        let res = self.generate_snap(&task);
        if let Err(e) = res {
            error!("failed to generate snap: {:?}!!!", e);
            task.storage.wl().snap_state = SnapState::Failed;
            return;
        }

        let (snap, region_id) = res.unwrap();
        match self.save_snapshot(&snap, task.store_id, region_id, "/tmp/") {
            Err(e) => {
                error!("save snapshot file failed: {:?}!!!", e);
                task.storage.wl().snap_state = SnapState::Failed;
                return;
            },
            Ok(_) => {
                task.storage.wl().snap_state = SnapState::Snap(snap);
            },
        }

        print!("write snapshot file success!\n");
    }
}

struct CRCWriter<T: Write> {
    digest: crc32::Digest,
    writer: T,
}

impl<T: Write> CRCWriter<T> {
    fn new(writer: T) -> CRCWriter<T> {
        CRCWriter {
            digest: crc32::Digest::new(crc32::IEEE),
            writer: writer,
        }
    }
    fn sum(&self) -> u32 {
        self.digest.sum32()
    }
}

impl<T: Write> Write for CRCWriter<T> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.digest.write(buf);
        self.writer.write(buf)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.writer.flush()
    }
}

pub fn snapshot_file_path(dir: &str, store_id: u64, file: &SnapshotFile) -> String {
    // let file_name: String = format!("{}{}/{}_{}_{}",
    let file_name: String = format!("{}{}/{}_{}_{}",
                            dir,
                            store_id,
                            file.get_region(),
                            file.get_term(),
                            file.get_index());
    {
        let path = Path::new(&file_name);
        let _ = fs::create_dir_all(path.parent().unwrap());
    }
    file_name
}

#[cfg(test)]
mod tests {
    use std::io::{self, Read};
    use std::fs;
    use super::*;
    use kvproto::raftpb::Snapshot;
    use byteorder::{ByteOrder, LittleEndian};
    use protobuf::Message;

    #[test]
    fn test_save_snapshot() {
        let runner = Runner;
        let mut snapshot = Snapshot::new();
        snapshot.mut_metadata().set_term(32);
        snapshot.mut_metadata().set_index(2);
        runner.save_snapshot(&snapshot, 4, "./").unwrap();

        let file_name = "./4_32_2";
        let metadata = fs::metadata(&file_name);
        let attr = metadata.unwrap();
        assert!(attr.is_file());

        let mut f = fs::File::open(&file_name).unwrap();
        let mut buf: [u8; 4] = [0; 4];
        f.read(&mut buf);
        let head_len = LittleEndian::read_u32(&mut buf);
        let should_be = snapshot.compute_size();
        assert_eq!(should_be, head_len);
        fs::remove_file(&file_name);
    }
}
