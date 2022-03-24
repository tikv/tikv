// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    fs::{self, File},
    io::{BufReader, Read},
    path::PathBuf,
};

use crate::*;
use byteorder::{ByteOrder, LittleEndian};
use bytes::{Buf, BytesMut};

pub(crate) struct WALIterator {
    dir: PathBuf,
    epoch_id: u32,
    buf: BytesMut,
    pub(crate) offset: u64,
}

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
        F: FnMut(RegionData),
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
                        let region_data = RegionData::decode(batch);
                        batch = &batch[region_data.encoded_len()..];
                        f(region_data);
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
        let aligned_length = (BATCH_HEADER_SIZE + length + ALIGN_SIZE - 1) as u64 & ALIGN_MASK;
        let remained_length = aligned_length as usize - BATCH_HEADER_SIZE;
        self.buf.resize(remained_length, 0);
        reader.read_exact(&mut self.buf[..])?;
        let batch = &self.buf[..length];
        if checksum != crc32fast::hash(batch) {
            return Err(Error::EOF);
        }
        self.offset += aligned_length;
        Ok(batch)
    }
}
