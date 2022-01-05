// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    fs::{self, File},
    io::{BufReader, Read},
    path::PathBuf,
};

use crate::*;
use byteorder::{ByteOrder, LittleEndian};
use bytes::{Buf, Bytes, BytesMut};

pub(crate) struct WALIterator {
    dir: PathBuf,
    epoch_id: u32,
    buf: BytesMut,
    pub(crate) offset: u64,
}

const READER_BUF_SIZE: usize = 256 * 1024;
const MAX_BATCH_SIZE: usize = 256 * 1024 * 1024;

impl WALIterator {
    pub(crate) fn new(dir: PathBuf, epoch_id: u32) -> Self {
        Self {
            dir,
            epoch_id,
            buf: BytesMut::new(),
            offset: 0,
        }
    }

    pub(crate) fn iterate<F>(&mut self, mut f: F) -> Result<()>
    where
        F: FnMut(u32, &[u8]),
    {
        let filename = wal_file_name(&self.dir, self.epoch_id);
        let fd = fs::File::open(filename)?;
        let mut buf_reader = BufReader::new(fd);
        loop {
            match self.read_batch(&mut buf_reader) {
                Err(err) => {
                    if let Error::EOF = err {
                        return Ok(());
                    }
                    return Err(err);
                }
                Ok(data) => {
                    let mut batch = data.chunk();
                    if batch.len() == 0 {
                        return Ok(());
                    }
                    while batch.len() > 0 {
                        let tp = LittleEndian::read_u32(batch);
                        batch = &batch[4..];
                        let length = LittleEndian::read_u32(batch) as usize;
                        batch = &batch[4..];
                        let entry = &batch[..length];
                        batch = &batch[length..];
                        f(tp, entry);
                    }
                }
            }
        }
    }

    pub(crate) fn read_batch(&mut self, reader: &mut BufReader<File>) -> Result<&[u8]> {
        let header_buf = &mut [0u8; BATCH_HEADER_SIZE][..];
        reader.read_exact(header_buf)?;
        let epoch_id = LittleEndian::read_u32(header_buf);
        if epoch_id != self.epoch_id {
            return Err(Error::EOF);
        }
        let checksum = LittleEndian::read_u32(&header_buf[4..]);
        let length = LittleEndian::read_u32(&header_buf[8..]) as usize;
        if length > MAX_BATCH_SIZE {
            return Err(Error::EOF);
        }
        let remained_length = ((BATCH_HEADER_SIZE + length + ALIGN_SIZE - 1) & ALIGN_MASK as usize)
            - BATCH_HEADER_SIZE;
        self.buf.resize(remained_length, 0);
        reader.read_exact(&mut self.buf[..])?;
        let batch = &self.buf[..length];
        if checksum != crc32c::crc32c(batch) {
            return Err(Error::EOF);
        }
        Ok(batch)
    }
}

pub(crate) fn parse_state(entry: &[u8]) -> (u64, &[u8], &[u8]) {
    let mut entry = entry;
    let region_id = LittleEndian::read_u64(entry);
    entry = &entry[8..];
    let key_len = LittleEndian::read_u16(entry) as usize;
    entry = &entry[2..];
    let key = &entry[..key_len];
    let val = &entry[key_len..];
    (region_id, key, val)
}

pub(crate) fn parse_log(entry: &[u8]) -> RaftLogOp {
    let mut entry = entry;
    let region_id = LittleEndian::read_u64(entry);
    entry = &entry[8..];
    let index = LittleEndian::read_u64(entry);
    entry = &entry[8..];
    let term = LittleEndian::read_u32(entry);
    entry = &entry[4..];
    let e_type = LittleEndian::read_i32(entry);
    entry = &entry[4..];
    let context = entry[0];
    entry = &entry[1..];
    let data = Bytes::copy_from_slice(entry);
    RaftLogOp {
        region_id,
        index,
        term,
        e_type,
        context,
        data,
    }
}

pub(crate) fn parse_truncate(entry: &[u8]) -> (u64, u64) {
    let region_id = LittleEndian::read_u64(entry);
    let index = LittleEndian::read_u64(&entry[8..]);
    (region_id, index)
}
