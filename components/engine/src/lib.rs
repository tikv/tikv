// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

#![recursion_limit = "200"]

#[macro_use(
    kv,
    slog_kv,
    slog_error,
    slog_warn,
    slog_record,
    slog_b,
    slog_log,
    slog_record_static
)]
extern crate slog;
#[macro_use]
extern crate slog_global;
#[macro_use]
extern crate prometheus;
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate quick_error;
#[allow(unused_extern_crates)]
extern crate tikv_alloc;

use std::sync::Arc;
use std::{error, result};
use std::{ptr, slice};

pub mod rocks;
pub mod util;

pub use self::rocks::{
    CFHandle, DBIterator, DBVector, Range, ReadOptions, Snapshot, SyncSnapshot, WriteBatch,
    WriteOptions, DB,
};

pub type CfName = &'static str;
pub const CF_DEFAULT: CfName = "default";
pub const CF_LOCK: CfName = "lock";
pub const CF_WRITE: CfName = "write";
pub const CF_RAFT: CfName = "raft";
// Cfs that should be very large generally.
pub const LARGE_CFS: &[CfName] = &[CF_DEFAULT, CF_WRITE];
pub const ALL_CFS: &[CfName] = &[CF_DEFAULT, CF_LOCK, CF_WRITE, CF_RAFT];
pub const DATA_CFS: &[CfName] = &[CF_DEFAULT, CF_LOCK, CF_WRITE];
pub const DATA_KEY_PREFIX_LEN: usize = 1;

// A copy of `tikv_util::escape`.
// TODO: remove it once util becomes a component.
fn escape(data: &[u8]) -> String {
    let mut escaped = Vec::with_capacity(data.len() * 4);
    for &c in data {
        match c {
            b'\n' => escaped.extend_from_slice(br"\n"),
            b'\r' => escaped.extend_from_slice(br"\r"),
            b'\t' => escaped.extend_from_slice(br"\t"),
            b'"' => escaped.extend_from_slice(b"\\\""),
            b'\\' => escaped.extend_from_slice(br"\\"),
            _ => {
                if c >= 0x20 && c < 0x7f {
                    // c is printable
                    escaped.push(c);
                } else {
                    escaped.push(b'\\');
                    escaped.push(b'0' + (c >> 6));
                    escaped.push(b'0' + ((c >> 3) & 7));
                    escaped.push(b'0' + (c & 7));
                }
            }
        }
    }
    escaped.shrink_to_fit();
    unsafe { String::from_utf8_unchecked(escaped) }
}

quick_error! {
    #[derive(Debug)]
    pub enum Error {
        // RocksDb uses plain string as the error.
        RocksDb(msg: String) {
            from()
            description("RocksDb error")
            display("RocksDb {}", msg)
        }
        // FIXME: It should not know Region.
        NotInRange( key: Vec<u8>, regoin_id: u64, start: Vec<u8>, end: Vec<u8>) {
            description("Key is out of range")
            display(
                "Key {:?} is out of [region {}] [{:?}, {:?})",
                escape(&key), regoin_id, escape(&start), escape(&end)
            )
        }
        Protobuf(err: protobuf::ProtobufError) {
            from()
            cause(err)
            description(err.description())
            display("Protobuf {}", err)
        }
        Io(err: std::io::Error) {
            from()
            cause(err)
            description(err.description())
            display("Io {}", err)
        }

        Other(err: Box<dyn error::Error + Sync + Send>) {
            from()
            cause(err.as_ref())
            description(err.description())
            display("{:?}", err)
        }
    }
}

pub type Result<T> = result::Result<T, Error>;

impl From<Error> for raft::Error {
    fn from(err: Error) -> raft::Error {
        raft::Error::Store(raft::StorageError::Other(err.into()))
    }
}

impl From<Error> for kvproto::errorpb::Error {
    fn from(err: Error) -> kvproto::errorpb::Error {
        let mut errorpb = kvproto::errorpb::Error::new();
        errorpb.set_message(error::Error::description(&err).to_owned());

        if let Error::NotInRange(key, region_id, start_key, end_key) = err {
            errorpb.mut_key_not_in_region().set_key(key);
            errorpb.mut_key_not_in_region().set_region_id(region_id);
            errorpb.mut_key_not_in_region().set_start_key(start_key);
            errorpb.mut_key_not_in_region().set_end_key(end_key);
        }

        errorpb
    }
}

#[derive(Clone, Debug)]
pub struct Engines {
    pub kv: Arc<DB>,
    pub raft: Arc<DB>,
}

impl Engines {
    pub fn new(kv_engine: Arc<DB>, raft_engine: Arc<DB>) -> Engines {
        Engines {
            kv: kv_engine,
            raft: raft_engine,
        }
    }

    pub fn write_kv(&self, wb: &WriteBatch) -> Result<()> {
        self.kv.write(wb).map_err(Error::RocksDb)
    }

    pub fn write_kv_opt(&self, wb: &WriteBatch, opts: &WriteOptions) -> Result<()> {
        self.kv.write_opt(wb, opts).map_err(Error::RocksDb)
    }

    pub fn sync_kv(&self) -> Result<()> {
        self.kv.sync_wal().map_err(Error::RocksDb)
    }

    pub fn write_raft(&self, wb: &WriteBatch) -> Result<()> {
        self.raft.write(wb).map_err(Error::RocksDb)
    }

    pub fn write_raft_opt(&self, wb: &WriteBatch, opts: &WriteOptions) -> Result<()> {
        self.raft.write_opt(wb, opts).map_err(Error::RocksDb)
    }

    pub fn sync_raft(&self) -> Result<()> {
        self.raft.sync_wal().map_err(Error::RocksDb)
    }
}

// TODO: refactor this trait into rocksdb trait.
pub trait Peekable {
    fn get_value(&self, key: &[u8]) -> Result<Option<DBVector>>;
    fn get_value_cf(&self, cf: &str, key: &[u8]) -> Result<Option<DBVector>>;

    fn get_msg<M: protobuf::Message>(&self, key: &[u8]) -> Result<Option<M>> {
        let value = self.get_value(key)?;

        if value.is_none() {
            return Ok(None);
        }

        let mut m = M::new();
        m.merge_from_bytes(&value.unwrap())?;
        Ok(Some(m))
    }

    fn get_msg_cf<M: protobuf::Message>(&self, cf: &str, key: &[u8]) -> Result<Option<M>> {
        let value = self.get_value_cf(cf, key)?;

        if value.is_none() {
            return Ok(None);
        }

        let mut m = M::new();
        m.merge_from_bytes(&value.unwrap())?;
        Ok(Some(m))
    }
}

#[derive(Clone, PartialEq)]
enum SeekMode {
    TotalOrder,
    Prefix,
}

pub struct BoundKeyBuilder {
    buf: Vec<u8>,
    start: usize,
}

impl BoundKeyBuilder {
    pub fn from_vec(vec: Vec<u8>) -> Self {
        Self { buf: vec, start: 0 }
    }

    pub fn from_slice(s: &[u8], start: usize) -> Self {
        let mut buf = Vec::with_capacity(s.len() + start);
        unsafe {
            buf.set_len(start);
            buf.extend_from_slice(s);
        }
        Self { buf, start }
    }

    pub fn add_key_prefix(&mut self, prefix: &[u8]) {
        assert!(self.start >= prefix.len());
        unsafe {
            ptr::copy(
                prefix.as_ptr(),
                self.buf.as_mut_ptr().add(self.start - prefix.len()),
                prefix.len(),
            );
            self.start -= prefix.len();
        }
    }

    pub fn as_ptr(&self) -> *const u8 {
        unsafe { self.buf.as_ptr().add(self.start) }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn len(&self) -> usize {
        self.buf.len() - self.start
    }

    pub fn as_slice(&self) -> &[u8] {
        unsafe { slice::from_raw_parts(self.as_ptr(), self.len()) }
    }

    pub fn build(self) -> Vec<u8> {
        if self.start == 0 {
            self.buf
        } else {
            let mut res = Vec::with_capacity(self.len());
            unsafe {
                res.extend_from_slice(slice::from_raw_parts(self.as_ptr(), self.len()));
            }
            res
        }
    }
}

pub struct IterOption {
    lower_bound: Option<BoundKeyBuilder>,
    upper_bound: Option<BoundKeyBuilder>,
    prefix_same_as_start: bool,
    fill_cache: bool,
    seek_mode: SeekMode,
}

impl IterOption {
    pub fn new(
        lower_bound: Option<BoundKeyBuilder>,
        upper_bound: Option<BoundKeyBuilder>,
        fill_cache: bool,
    ) -> IterOption {
        IterOption {
            lower_bound,
            upper_bound,
            prefix_same_as_start: false,
            fill_cache,
            seek_mode: SeekMode::TotalOrder,
        }
    }

    pub fn use_prefix_seek(mut self) -> IterOption {
        self.seek_mode = SeekMode::Prefix;
        self
    }

    pub fn total_order_seek_used(&self) -> bool {
        self.seek_mode == SeekMode::TotalOrder
    }

    pub fn fill_cache(&mut self, v: bool) {
        self.fill_cache = v;
    }

    pub fn lower_bound(&self) -> Option<&[u8]> {
        self.lower_bound.as_ref().map(|v| v.as_slice())
    }

    pub fn set_lower_bound(&mut self, bound: &[u8], reserved_prefix_len: usize) {
        let builder = BoundKeyBuilder::from_slice(bound, reserved_prefix_len);
        self.lower_bound = Some(builder);
    }

    pub fn set_vec_lower_bound(&mut self, bound: Vec<u8>) {
        self.lower_bound = Some(BoundKeyBuilder::from_vec(bound));
    }

    pub fn add_lower_bound_prefix(&mut self, prefix: &[u8]) {
        if let Some(ref mut builder) = self.lower_bound {
            builder.add_key_prefix(prefix);
        }
    }

    pub fn upper_bound(&self) -> Option<&[u8]> {
        self.upper_bound.as_ref().map(|v| v.as_slice())
    }

    pub fn set_upper_bound(&mut self, bound: &[u8], reserved_prefix_len: usize) {
        let builder = BoundKeyBuilder::from_slice(bound, reserved_prefix_len);
        self.upper_bound = Some(builder);
    }

    pub fn set_vec_upper_bound(&mut self, bound: Vec<u8>) {
        self.upper_bound = Some(BoundKeyBuilder::from_vec(bound));
    }

    pub fn add_upper_bound_prefix(&mut self, prefix: &[u8]) {
        if let Some(ref mut builder) = self.upper_bound {
            builder.add_key_prefix(prefix);
        }
    }

    pub fn set_prefix_same_as_start(mut self, enable: bool) -> IterOption {
        self.prefix_same_as_start = enable;
        self
    }

    pub fn build_read_opts(self) -> ReadOptions {
        let mut opts = ReadOptions::new();
        opts.fill_cache(self.fill_cache);
        if self.total_order_seek_used() {
            opts.set_total_order_seek(true);
        } else if self.prefix_same_as_start {
            opts.set_prefix_same_as_start(true);
        }
        if let Some(builder) = self.lower_bound {
            opts.set_iterate_lower_bound(builder.build());
        }
        if let Some(builder) = self.upper_bound {
            opts.set_iterate_upper_bound(builder.build());
        }
        opts
    }
}

impl Default for IterOption {
    fn default() -> IterOption {
        IterOption {
            lower_bound: None,
            upper_bound: None,
            prefix_same_as_start: false,
            fill_cache: true,
            seek_mode: SeekMode::TotalOrder,
        }
    }
}

// TODO: refactor this trait into rocksdb trait.
pub trait Iterable {
    fn new_iterator(&self, iter_opt: IterOption) -> DBIterator<&DB>;
    fn new_iterator_cf(&self, _: &str, iter_opt: IterOption) -> Result<DBIterator<&DB>>;
    // scan scans database using an iterator in range [start_key, end_key), calls function f for
    // each iteration, if f returns false, terminates this scan.
    fn scan<F>(&self, start_key: &[u8], end_key: &[u8], fill_cache: bool, f: F) -> Result<()>
    where
        F: FnMut(&[u8], &[u8]) -> Result<bool>,
    {
        let start = BoundKeyBuilder::from_slice(start_key, DATA_KEY_PREFIX_LEN);
        let end = BoundKeyBuilder::from_slice(end_key, DATA_KEY_PREFIX_LEN);
        let iter_opt = IterOption::new(Some(start), Some(end), fill_cache);
        scan_impl(self.new_iterator(iter_opt), start_key, f)
    }

    // like `scan`, only on a specific column family.
    fn scan_cf<F>(
        &self,
        cf: &str,
        start_key: &[u8],
        end_key: &[u8],
        fill_cache: bool,
        f: F,
    ) -> Result<()>
    where
        F: FnMut(&[u8], &[u8]) -> Result<bool>,
    {
        let start = BoundKeyBuilder::from_slice(start_key, DATA_KEY_PREFIX_LEN);
        let end = BoundKeyBuilder::from_slice(end_key, DATA_KEY_PREFIX_LEN);
        let iter_opt = IterOption::new(Some(start), Some(end), fill_cache);
        scan_impl(self.new_iterator_cf(cf, iter_opt)?, start_key, f)
    }

    // Seek the first key >= given key, if no found, return None.
    fn seek(&self, key: &[u8]) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        let mut iter = self.new_iterator(IterOption::default());
        iter.seek(key.into());
        Ok(iter.kv())
    }

    // Seek the first key >= given key, if no found, return None.
    fn seek_cf(&self, cf: &str, key: &[u8]) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        let mut iter = self.new_iterator_cf(cf, IterOption::default())?;
        iter.seek(key.into());
        Ok(iter.kv())
    }
}

fn scan_impl<F>(mut it: DBIterator<&DB>, start_key: &[u8], mut f: F) -> Result<()>
where
    F: FnMut(&[u8], &[u8]) -> Result<bool>,
{
    it.seek(start_key.into());
    while it.valid() {
        let r = f(it.key(), it.value())?;

        if !r || !it.next() {
            break;
        }
    }

    Ok(())
}

use self::rocks::Writable;

pub trait Mutable: Writable {
    fn put_msg<M: protobuf::Message>(&self, key: &[u8], m: &M) -> Result<()> {
        let value = m.write_to_bytes()?;
        self.put(key, &value)?;
        Ok(())
    }

    // TOOD: change CFHandle to str.
    fn put_msg_cf<M: protobuf::Message>(&self, cf: &CFHandle, key: &[u8], m: &M) -> Result<()> {
        let value = m.write_to_bytes()?;
        self.put_cf(cf, key, &value)?;
        Ok(())
    }

    fn del(&self, key: &[u8]) -> Result<()> {
        self.delete(key)?;
        Ok(())
    }
}
