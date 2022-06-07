// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    os::unix::fs::{FileExt, MetadataExt},
    path::Path,
};

use bytes::Bytes;

pub trait File: Sync + Send {
    // id returns the id of the file.
    fn id(&self) -> u64;

    // size returns the size of the file.
    fn size(&self) -> u64;

    // read reads the data at given offset.
    fn read(&self, off: u64, length: usize) -> std::io::Result<Bytes>;

    // read_at reads the data to the buffer.
    fn read_at(&self, buf: &mut [u8], offset: u64) -> std::io::Result<()>;
}

pub struct LocalFile {
    id: u64,
    size: u64,
    fd: std::fs::File,
}

impl LocalFile {
    pub fn open(id: u64, path: &Path) -> std::io::Result<LocalFile> {
        let fd = std::fs::File::open(path)?;
        let meta = fd.metadata()?;
        let local_file = LocalFile {
            id,
            fd,
            size: meta.size(),
        };
        Ok(local_file)
    }
}

impl File for LocalFile {
    fn id(&self) -> u64 {
        self.id
    }

    fn size(&self) -> u64 {
        self.size
    }

    fn read(&self, off: u64, length: usize) -> std::io::Result<Bytes> {
        let mut buf = vec![0; length];
        self.fd.read_at(&mut buf, off)?;
        Ok(Bytes::from(buf))
    }

    fn read_at(&self, buf: &mut [u8], offset: u64) -> std::io::Result<()> {
        self.fd.read_at(buf, offset)?;
        Ok(())
    }
}

#[derive(Clone)]
pub struct InMemFile {
    pub id: u64,
    data: Bytes,
    pub size: u64,
}

impl InMemFile {
    pub fn new(id: u64, data: Bytes) -> Self {
        let size = data.len() as u64;
        Self { id, data, size }
    }
}

impl File for InMemFile {
    fn id(&self) -> u64 {
        self.id
    }

    fn size(&self) -> u64 {
        self.size
    }

    fn read(&self, off: u64, length: usize) -> std::io::Result<Bytes> {
        let off_usize = off as usize;
        Ok(self.data.slice(off_usize..off_usize + length))
    }

    fn read_at(&self, buf: &mut [u8], offset: u64) -> std::io::Result<()> {
        let off_usize = offset as usize;
        let length = buf.len();
        buf.copy_from_slice(&self.data[off_usize..off_usize + length]);
        Ok(())
    }
}
