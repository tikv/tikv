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
use protobuf::{ProtobufError, Message};

use std::sync::Arc;
use std::fmt::{self, Formatter, Display};
use std::io::{self, Write};
use std::error;
use std::fs;
use crc::{crc32, Hasher32};
use byteorder::{ByteOrder, LittleEndian};

use util::worker::Runnable;

/// Snapshot generating task.
pub struct Task {
    storage: Arc<RaftStorage>,
}

impl Task {
    pub fn new(storage: Arc<RaftStorage>) -> Task {
        Task { storage: storage }
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

    fn save_snapshot(&self, snapshot: &Snapshot, region_id: u64, dir: &str) -> Result<(), Error> {
        let file_name = format!("{}{}_{}_{}",
                                dir,
                                region_id,
                                snapshot.get_metadata().get_term(),
                                snapshot.get_metadata().get_index());
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

        try!(fs::rename(file_name, tmp_file_name));
        Ok(())
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
        if let Err(e) = self.save_snapshot(&snap, region_id, "/tmp/") {
            error!("save snapshot file failed: {:?}!!!", e);
            task.storage.wl().snap_state = SnapState::Failed;
            return;
        }
        task.storage.wl().snap_state = SnapState::Snap(snap);
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
