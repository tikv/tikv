// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::io::{Read, Result, Write};
use std::sync::Arc;

/// An I/O rate limiter
///
/// Throttles the maximum bytes per second written or read.
pub trait IOLimiterExt {
    type IOLimiter: IOLimiter;
}

pub trait IOLimiter {
    /// # Arguments
    ///
    /// - `bytes_per_sec`: controls the total write rate of compaction and flush in bytes per second.
    fn new(bytes_per_sec: i64) -> Self;

    /// Sets the rate limit in bytes per second
    fn set_bytes_per_second(&self, bytes_per_sec: i64);

    /// Requests an access token to read or write bytes. If this request can not be satisfied, the call is blocked.
    fn request(&self, bytes: i64);

    /// Gets the max bytes that can be granted in a single burst.
    fn get_max_bytes_per_time(&self) -> i64;

    /// Gets the total bytes that have gone through the rate limiter.
    fn get_total_bytes_through(&self) -> i64;

    /// Gets the rate limit in bytes per second.
    fn get_bytes_per_second(&self) -> i64;

    /// Gets the total number of requests that have gone through rate limiter
    fn get_total_requests(&self) -> i64;
}

pub struct LimitWriter<'a, T: Write, L: IOLimiter> {
    limiter: Option<Arc<L>>,
    writer: &'a mut T,
}

impl<'a, T: Write + 'a, L: IOLimiter> LimitWriter<'a, T, L> {
    pub fn new(limiter: Option<Arc<L>>, writer: &'a mut T) -> LimitWriter<'a, T, L> {
        LimitWriter { limiter, writer }
    }
}

impl<'a, T: Write + 'a, L: IOLimiter> Write for LimitWriter<'a, T, L> {
    fn write(&mut self, buf: &[u8]) -> Result<usize> {
        let total = buf.len();
        if let Some(ref limiter) = self.limiter {
            let single = limiter.get_max_bytes_per_time() as usize;
            let mut curr = 0;
            let mut end;
            while curr < total {
                if curr + single >= total {
                    end = total;
                } else {
                    end = curr + single;
                }
                limiter.request((end - curr) as i64);
                self.writer.write_all(&buf[curr..end])?;
                curr = end;
            }
        } else {
            self.writer.write_all(buf)?;
        }
        Ok(total)
    }

    fn flush(&mut self) -> Result<()> {
        self.writer.flush()?;
        Ok(())
    }
}

/// A limited reader.
///
/// The read limits the bytes per second read from an underlying reader.
pub struct LimitReader<'a, T: Read, L: IOLimiter> {
    limiter: Option<Arc<L>>,
    reader: &'a mut T,
}

impl<'a, T: Read + 'a, L: IOLimiter> LimitReader<'a, T, L> {
    /// Create a new `LimitReader`.
    pub fn new(limiter: Option<Arc<L>>, reader: &'a mut T) -> LimitReader<'a, T, L> {
        LimitReader { limiter, reader }
    }
}

impl<'a, T: Read + 'a, L: IOLimiter> Read for LimitReader<'a, T, L> {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        if let Some(ref limiter) = self.limiter {
            let total = buf.len();
            let single = limiter.get_max_bytes_per_time() as usize;
            let mut count = 0;
            let mut curr = 0;
            let mut end;
            while curr < total {
                if curr + single >= total {
                    end = total;
                } else {
                    end = curr + single;
                }
                limiter.request((end - curr) as i64);
                count += self.reader.read(&mut buf[curr..end])?;
                curr = end;
            }
            Ok(count)
        } else {
            self.reader.read(buf)
        }
    }
}
