// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use super::{get_io_rate_limiter, get_io_type, IOOp, IORateLimiter};

use std::fmt::{self, Debug, Formatter};
use std::fs;
use std::io::{self, Read, Seek, Write};
use std::path::Path;
use std::sync::Arc;

use fs2::FileExt;

/// A wrapper around `std::fs::File` with capability to track and regulate IO flow.
pub struct File {
    inner: fs::File,
    limiter: Option<Arc<IORateLimiter>>,
}

impl Debug for File {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self.inner)
    }
}

impl File {
    pub fn open<P: AsRef<Path>>(path: P) -> io::Result<File> {
        let inner = fs::File::open(path)?;
        Ok(File {
            inner,
            limiter: get_io_rate_limiter(),
        })
    }

    #[cfg(test)]
    pub fn open_with_limiter<P: AsRef<Path>>(
        path: P,
        limiter: Option<Arc<IORateLimiter>>,
    ) -> io::Result<File> {
        let inner = fs::File::open(path)?;
        Ok(File { inner, limiter })
    }

    pub fn create<P: AsRef<Path>>(path: P) -> io::Result<File> {
        let inner = fs::File::create(path)?;
        Ok(File {
            inner,
            limiter: get_io_rate_limiter(),
        })
    }

    #[cfg(test)]
    pub fn create_with_limiter<P: AsRef<Path>>(
        path: P,
        limiter: Option<Arc<IORateLimiter>>,
    ) -> io::Result<File> {
        let inner = fs::File::create(path)?;
        Ok(File { inner, limiter })
    }

    pub fn from_raw_file(file: fs::File) -> io::Result<File> {
        Ok(File {
            inner: file,
            limiter: get_io_rate_limiter(),
        })
    }

    pub fn sync_all(&self) -> io::Result<()> {
        self.inner.sync_all()
    }

    pub fn sync_data(&self) -> io::Result<()> {
        self.inner.sync_data()
    }

    pub fn set_len(&self, size: u64) -> io::Result<()> {
        self.inner.set_len(size)
    }

    pub fn metadata(&self) -> io::Result<fs::Metadata> {
        self.inner.metadata()
    }

    pub fn try_clone(&self) -> io::Result<File> {
        let inner = self.inner.try_clone()?;
        Ok(File {
            inner,
            limiter: get_io_rate_limiter(),
        })
    }

    pub fn set_permissions(&self, perm: fs::Permissions) -> io::Result<()> {
        self.inner.set_permissions(perm)
    }
}

impl Read for File {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if let Some(limiter) = &mut self.limiter {
            let mut remains = buf.len();
            let mut pos = 0;
            while remains > 0 {
                let allowed = limiter.request(get_io_type(), IOOp::Read, remains);
                let read = self.inner.read(&mut buf[pos..pos + allowed])?;
                pos += read;
                remains -= read;
                if read == 0 {
                    break;
                }
            }
            Ok(pos)
        } else {
            self.inner.read(buf)
        }
    }
}

impl Seek for File {
    fn seek(&mut self, pos: io::SeekFrom) -> io::Result<u64> {
        self.inner.seek(pos)
    }
}

impl Write for File {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if let Some(limiter) = &mut self.limiter {
            let mut remains = buf.len();
            let mut pos = 0;
            while remains > 0 {
                let allowed = limiter.request(get_io_type(), IOOp::Write, remains);
                let written = self.inner.write(&buf[pos..pos + allowed])?;
                pos += written;
                remains -= written;
                if written == 0 {
                    break;
                }
            }
            Ok(pos)
        } else {
            self.inner.write(buf)
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

/// fs2::FileExt
impl File {
    pub fn duplicate(&self) -> io::Result<File> {
        Ok(File {
            inner: self.inner.duplicate()?,
            limiter: get_io_rate_limiter(),
        })
    }

    pub fn allocated_size(&self) -> io::Result<u64> {
        self.inner.allocated_size()
    }

    pub fn allocate(&self, len: u64) -> io::Result<()> {
        self.inner.allocate(len)
    }

    pub fn lock_shared(&self) -> io::Result<()> {
        self.inner.lock_shared()
    }

    pub fn lock_exclusive(&self) -> io::Result<()> {
        self.inner.lock_exclusive()
    }

    pub fn try_lock_shared(&self) -> io::Result<()> {
        self.inner.try_lock_shared()
    }

    pub fn try_lock_exclusive(&self) -> io::Result<()> {
        self.inner.try_lock_exclusive()
    }

    pub fn unlock(&self) -> io::Result<()> {
        self.inner.unlock()
    }
}

pub struct OpenOptions(fs::OpenOptions);

impl OpenOptions {
    pub fn new() -> Self {
        OpenOptions(fs::OpenOptions::new())
    }

    pub fn read(&mut self, read: bool) -> &mut Self {
        self.0.read(read);
        self
    }

    pub fn write(&mut self, write: bool) -> &mut Self {
        self.0.write(write);
        self
    }

    pub fn append(&mut self, append: bool) -> &mut Self {
        self.0.append(append);
        self
    }

    pub fn truncate(&mut self, truncate: bool) -> &mut Self {
        self.0.truncate(truncate);
        self
    }

    pub fn create(&mut self, create: bool) -> &mut Self {
        self.0.create(create);
        self
    }

    pub fn create_new(&mut self, create_new: bool) -> &mut Self {
        self.0.create_new(create_new);
        self
    }

    pub fn open<P: AsRef<Path>>(&self, path: P) -> io::Result<File> {
        File::from_raw_file(self.0.open(path)?)
    }
}

impl Default for OpenOptions {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use tempfile::Builder;

    use super::super::*;
    use super::*;

    #[test]
    fn test_instrumented_file() {
        let tmp_dir = Builder::new()
            .prefix("test_instrumented_file")
            .tempdir()
            .unwrap();
        let limiter = Arc::new(IORateLimiter::new_for_test());
        // make sure read at most one bytes at a time
        limiter.set_io_rate_limit(20 /* 1s / refill_period */);
        let stats = limiter.statistics().unwrap();

        let tmp_file = tmp_dir.path().join("instrumented.txt");
        let content = String::from("drink full and descend");
        {
            let _guard = WithIOType::new(IOType::ForegroundWrite);
            let mut f = File::create_with_limiter(&tmp_file, Some(limiter.clone())).unwrap();
            f.write_all(content.as_bytes()).unwrap();
            f.sync_all().unwrap();
            assert_eq!(
                stats.fetch(IOType::ForegroundWrite, IOOp::Write),
                content.len()
            );
        }
        {
            let _guard = WithIOType::new(IOType::Export);
            let mut buffer = String::new();
            let mut f = File::open_with_limiter(&tmp_file, Some(limiter)).unwrap();
            assert_eq!(f.read_to_string(&mut buffer).unwrap(), content.len());
            assert_eq!(buffer, content);
            // read_to_string only exit when file.read() returns zero, which means
            // it requires two EOF reads to finish the call.
            assert_eq!(stats.fetch(IOType::Export, IOOp::Read), content.len() + 2);
        }
    }

    #[cfg(unix)]
    #[test]
    fn test_unix_file_allocate_failure() {
        let tmp_dir = Builder::new()
            .prefix("test_unix_file_allocate_failure")
            .tempdir()
            .unwrap();
        let data_path = tmp_dir.path();
        let file_path = data_path.join(SPACE_PLACEHOLDER_FILE);
        let f = File::create(file_path).unwrap();
        // EINVAL when len == 0.
        assert_eq!(
            f.allocate(0).unwrap_err().raw_os_error().unwrap(),
            libc::EINVAL
        );
    }
}
