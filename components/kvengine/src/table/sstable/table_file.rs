use bytes::{Bytes, BytesMut};
use std::{fs, fs::File, io, os::unix::prelude::FileExt, path::{PathBuf}};
use crate::table::table::{Result};


pub struct TableFile {
    pub id: u64,
    data: Bytes,
    pub size: u64,
    file_path: Option<PathBuf>,
    file: Option<fs::File>,
}

impl TableFile {
    pub fn new_local(id: u64, file_path: PathBuf) -> Result<Self> {
        let file = File::create(&file_path)?;
        let meta = file.metadata()?;
        Ok(Self{
            id: id,
            data: Bytes::new(),
            file: Some(file),
            file_path: Some(file_path),
            size: meta.len(),
        })
    }

    pub fn new_in_mem(id: u64, data: Bytes) -> Self {
        let size = data.len() as u64;
        Self {
            id: id,
            data: data,
            size: size,
            file: None,
            file_path: None,
        }
    }

    pub fn read(&self, off: u64, length: usize) -> Result<Bytes> {
        if self.data.len() > 0 {
            let off_usize = off as usize;
            return Ok(self.data.slice(off_usize..off_usize+length));
        }
        // let file = self.file.unwrap();
        let mut buf = BytesMut::with_capacity(length);
        unsafe {buf.set_len(length)};
        self.file.as_ref().unwrap().read_at(buf.as_mut(), off)?;
        return Ok(buf.freeze());
    }

    pub fn delete(&self) {
        if let Some(path) = &self.file_path {
            fs::remove_file(path);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_table_file() {
        println!("ok")
    }
}