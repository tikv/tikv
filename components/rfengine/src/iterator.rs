// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    fs::{self, File},
    io::{BufReader, Read},
    path::PathBuf,
};

use bytes::{Buf, BytesMut};

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
            Err(Error::EOF) => {
                return Ok(());
            }
            Err(e) => return Err(e),
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

    pub(crate) fn check_wal_header(&mut self, reader: &mut BufReader<File>) -> Result<WalHeader> {
        let mut buf = [0u8; WalHeader::len()];
        reader.read_exact(&mut buf)?;
        self.offset += WalHeader::len() as u64;
        match WalHeader::decode(&buf) {
            Ok(header) => return Ok(header),
            Err(err) => {
                // Haven't written the header.
                if buf.iter().all(|v| *v == 0) {
                    return Err(Error::EOF);
                }
                // Header is corrupt, but the first batch header is empty which means there
                // is no data in this WAL. Treat it like EOF and WAL writer will rewrite the
                // header.
                reader.read_exact(&mut buf[..BATCH_HEADER_SIZE])?;
                if buf.iter().take(BATCH_HEADER_SIZE).all(|v| *v == 0) {
                    return Err(Error::EOF);
                }
                // Header corruption.
                return Err(err);
            }
        }
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
        let checksum = header_buf.get_u32_le();
        let length = header_buf.get_u32_le() as usize;
        if epoch_id == 0 && checksum == 0 && length == 0 {
            return Err(Error::EOF);
        }
        if epoch_id != self.epoch_id {
            return Err(Error::Corruption("epoch mismatch".to_owned()));
        }
        if length > MAX_BATCH_SIZE {
            return Err(Error::Corruption("length mismatch".to_owned()));
        }
        let aligned_length = DmaBuffer::aligned_len(BATCH_HEADER_SIZE + length);
        let remained_length = aligned_length - BATCH_HEADER_SIZE;
        self.buf.resize(remained_length, 0);
        reader.read_exact(&mut self.buf[..])?;
        let batch = &self.buf[..length];
        if checksum != crc32fast::hash(batch) {
            return Err(Error::Corruption("checksum mismatch".to_owned()));
        }
        self.offset += aligned_length as u64;
        Ok(batch)
    }
}
