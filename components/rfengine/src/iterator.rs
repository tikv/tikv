// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    fs::{self, File},
    io::{BufReader, Read},
    path::PathBuf,
};

use bytes::{Buf, BytesMut};
use slog_global::warn;

use crate::{
    worker::wal_file_name,
    write_batch::RegionBatch,
    writer::{DmaBuffer, WalHeader, BATCH_HEADER_SIZE},
    Error, Result,
};

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
        F: FnMut(RegionBatch),
    {
        let filename = wal_file_name(self.dir.as_path(), self.epoch_id);
        let fd = fs::File::open(filename)?;
        let mut buf_reader = BufReader::new(fd);
        let header = match self.check_wal_header(&mut buf_reader) {
            Ok(header) => header,
            Err(e) => {
                // FIXME(youjiali1995): Due to recycling WAL file, we can't distinguish data
                // corruption at the tail of WAL.
                warn!("failed to check WAL header: {}", e);
                return Ok(());
            }
        };
        loop {
            match self.read_batch(&mut buf_reader, &header) {
                Err(err) => {
                    if let Error::EOF = err {
                        return Ok(());
                    }
                    return Err(err);
                }
                Ok(data) => {
                    let mut batch = data.chunk();
                    if batch.is_empty() {
                        return Ok(());
                    }
                    while !batch.is_empty() {
                        let region_data = RegionBatch::decode(batch);
                        batch = &batch[region_data.encoded_len()..];
                        f(region_data);
                    }
                }
            }
        }
    }

    fn check_wal_header(&mut self, reader: &mut BufReader<File>) -> Result<WalHeader> {
        let mut buf = [0u8; WalHeader::len()];
        reader.read_exact(&mut buf)?;
        self.offset += WalHeader::len() as u64;
        WalHeader::decode(&buf)
    }

    pub(crate) fn read_batch(
        &mut self,
        reader: &mut BufReader<File>,
        _header: &WalHeader,
    ) -> Result<&[u8]> {
        let mut header_buf = [0u8; BATCH_HEADER_SIZE];
        reader.read_exact(header_buf.as_mut_slice())?;
        let mut header_buf = header_buf.as_slice();
        let epoch_id = header_buf.get_u32_le();
        if epoch_id == 0 {
            return Err(Error::EOF);
        }
        if epoch_id != self.epoch_id {
            return Err(Error::Epoch);
        }
        let checksum = header_buf.get_u32_le();
        let length = header_buf.get_u32_le() as usize;
        if length > MAX_BATCH_SIZE {
            return Err(Error::Length);
        }
        let aligned_length = DmaBuffer::aligned_len(BATCH_HEADER_SIZE + length);
        let remained_length = aligned_length - BATCH_HEADER_SIZE;
        self.buf.resize(remained_length, 0);
        reader.read_exact(&mut self.buf[..])?;
        let batch = &self.buf[..length];
        if checksum != crc32fast::hash(batch) {
            return Err(Error::Checksum);
        }
        self.offset += aligned_length as u64;
        Ok(batch)
    }
}
