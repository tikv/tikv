// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

#![cfg_attr(test, feature(test))]
#![feature(thread_id_value)]
#![feature(box_patterns)]
#![feature(str_split_once)]

#[macro_use(fail_point)]
extern crate fail;
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate quick_error;
#[macro_use(slog_o)]
extern crate slog;
#[macro_use]
extern crate derive_more;
#[cfg(test)]
extern crate test;

use std::collections::hash_map::Entry;
use std::collections::vec_deque::{Iter, VecDeque};
use std::fs::File;
use std::ops::{Deref, DerefMut};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::time::Duration;
use std::{env, thread, u64};

use rand::rngs::ThreadRng;

#[macro_use]
pub mod log;
pub mod buffer_vec;
pub mod codec;
pub mod config;
pub mod future;
#[macro_use]
pub mod macros;
pub mod callback;
pub mod deadline;
pub mod keybuilder;
pub mod logger;
pub mod lru;
pub mod metrics;
pub mod mpsc;
pub mod stream;
pub mod sys;
pub mod time;
pub mod timer;
pub mod trace;
pub mod worker;
pub mod yatp_pool;

static PANIC_WHEN_UNEXPECTED_KEY_OR_DATA: AtomicBool = AtomicBool::new(false);

pub fn panic_when_unexpected_key_or_data() -> bool {
    PANIC_WHEN_UNEXPECTED_KEY_OR_DATA.load(Ordering::SeqCst)
}

pub fn set_panic_when_unexpected_key_or_data(flag: bool) {
    PANIC_WHEN_UNEXPECTED_KEY_OR_DATA.store(flag, Ordering::SeqCst);
}

static PANIC_MARK: AtomicBool = AtomicBool::new(false);

pub fn set_panic_mark() {
    PANIC_MARK.store(true, Ordering::SeqCst);
}

pub fn panic_mark_is_on() -> bool {
    PANIC_MARK.load(Ordering::SeqCst)
}

pub const PANIC_MARK_FILE: &str = "panic_mark_file";

pub fn panic_mark_file_path<P: AsRef<Path>>(data_dir: P) -> PathBuf {
    data_dir.as_ref().join(PANIC_MARK_FILE)
}

pub fn create_panic_mark_file<P: AsRef<Path>>(data_dir: P) {
    let file = panic_mark_file_path(data_dir);
    File::create(&file).unwrap();
}

// Copied from file_system to avoid cyclic dependency
fn file_exists<P: AsRef<Path>>(file: P) -> bool {
    let path = file.as_ref();
    path.exists() && path.is_file()
}

pub fn panic_mark_file_exists<P: AsRef<Path>>(data_dir: P) -> bool {
    let path = panic_mark_file_path(data_dir);
    file_exists(path)
}

pub const NO_LIMIT: u64 = u64::MAX;

pub trait AssertClone: Clone {}

pub trait AssertCopy: Copy {}

pub trait AssertSend: Send {}

pub trait AssertSync: Sync {}

/// Take slices in the range.
///
/// ### Panic
///
/// if [low, high) is out of bound.
pub fn slices_in_range<T>(entry: &VecDeque<T>, low: usize, high: usize) -> (&[T], &[T]) {
    let (first, second) = entry.as_slices();
    if low >= first.len() {
        (&second[low - first.len()..high - first.len()], &[])
    } else if high <= first.len() {
        (&first[low..high], &[])
    } else {
        (&first[low..], &second[..high - first.len()])
    }
}

pub struct DefaultRng {
    rng: ThreadRng,
}

impl DefaultRng {
    fn new() -> DefaultRng {
        DefaultRng {
            rng: rand::thread_rng(),
        }
    }
}

impl Default for DefaultRng {
    fn default() -> DefaultRng {
        DefaultRng::new()
    }
}

impl Deref for DefaultRng {
    type Target = ThreadRng;

    fn deref(&self) -> &ThreadRng {
        &self.rng
    }
}

impl DerefMut for DefaultRng {
    fn deref_mut(&mut self) -> &mut ThreadRng {
        &mut self.rng
    }
}

/// A handy shortcut to replace `RwLock` write/read().unwrap() pattern to
/// shortcut wl and rl.
pub trait HandyRwLock<T> {
    fn wl(&self) -> RwLockWriteGuard<'_, T>;
    fn rl(&self) -> RwLockReadGuard<'_, T>;
}

impl<T> HandyRwLock<T> for RwLock<T> {
    fn wl(&self) -> RwLockWriteGuard<'_, T> {
        self.write().unwrap()
    }

    fn rl(&self) -> RwLockReadGuard<'_, T> {
        self.read().unwrap()
    }
}

/// A function to escape a byte array to a readable ascii string.
/// escape rules follow golang/protobuf.
/// <https://github.com/golang/protobuf/blob/master/proto/text.go#L578>
///
/// # Examples
///
/// ```
/// use tikv_util::escape;
///
/// assert_eq!(r"ab", escape(b"ab"));
/// assert_eq!(r"a\\023", escape(b"a\\023"));
/// assert_eq!(r"a\000", escape(b"a\0"));
/// assert_eq!("a\\r\\n\\t '\\\"\\\\", escape(b"a\r\n\t '\"\\"));
/// assert_eq!(r"\342\235\244\360\237\220\267", escape("❤🐷".as_bytes()));
/// ```
pub fn escape(data: &[u8]) -> String {
    let mut escaped = Vec::with_capacity(data.len() * 4);
    for &c in data {
        match c {
            b'\n' => escaped.extend_from_slice(br"\n"),
            b'\r' => escaped.extend_from_slice(br"\r"),
            b'\t' => escaped.extend_from_slice(br"\t"),
            b'"' => escaped.extend_from_slice(b"\\\""),
            b'\\' => escaped.extend_from_slice(br"\\"),
            _ => {
                if (0x20..0x7f).contains(&c) {
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

/// A function to unescape an escaped string to a byte array.
///
/// # Panic
///
/// If s is not a properly encoded string.
pub fn unescape(s: &str) -> Vec<u8> {
    let mut buf = Vec::with_capacity(s.len());
    let mut bytes = s.bytes();
    while let Some(b) = bytes.next() {
        if b != b'\\' {
            buf.push(b);
            continue;
        }
        match bytes.next().unwrap() {
            b'"' => buf.push(b'"'),
            b'\'' => buf.push(b'\''),
            b'\\' => buf.push(b'\\'),
            b'n' => buf.push(b'\n'),
            b't' => buf.push(b'\t'),
            b'r' => buf.push(b'\r'),
            b'x' => {
                macro_rules! next_hex {
                    () => {
                        bytes.next().map(char::from).unwrap().to_digit(16).unwrap()
                    };
                }
                // Can coerce as u8 since the range of possible values is constrained to
                // between 00 and FF.
                buf.push(((next_hex!() << 4) + next_hex!()) as u8);
            }
            b => {
                let b1 = b - b'0';
                let b2 = bytes.next().unwrap() - b'0';
                let b3 = bytes.next().unwrap() - b'0';
                buf.push((b1 << 6) + (b2 << 3) + b3);
            }
        }
    }
    buf.shrink_to_fit();
    buf
}

/// A helper trait for `Entry` to accept a failable closure.
pub trait TryInsertWith<'a, V, E> {
    fn or_try_insert_with<F: FnOnce() -> Result<V, E>>(self, default: F) -> Result<&'a mut V, E>;
}

impl<'a, T: 'a, V: 'a, E> TryInsertWith<'a, V, E> for Entry<'a, T, V> {
    fn or_try_insert_with<F: FnOnce() -> Result<V, E>>(self, default: F) -> Result<&'a mut V, E> {
        match self {
            Entry::Occupied(e) => Ok(e.into_mut()),
            Entry::Vacant(e) => {
                let v = default()?;
                Ok(e.insert(v))
            }
        }
    }
}

pub fn get_tag_from_thread_name() -> Option<String> {
    thread::current()
        .name()
        .and_then(|name| name.split("::").skip(1).last())
        .map(From::from)
}

/// Invokes the wrapped closure when dropped.
pub struct DeferContext<T: FnOnce()> {
    t: Option<T>,
}

impl<T: FnOnce()> DeferContext<T> {
    pub fn new(t: T) -> DeferContext<T> {
        DeferContext { t: Some(t) }
    }
}

impl<T: FnOnce()> Drop for DeferContext<T> {
    fn drop(&mut self) {
        self.t.take().unwrap()()
    }
}

/// Represents a value of one of two possible types (a more generic Result.)
#[derive(Debug, Clone)]
pub enum Either<L, R> {
    Left(L),
    Right(R),
}

impl<L, R> Either<L, R> {
    #[inline]
    pub fn as_ref(&self) -> Either<&L, &R> {
        match *self {
            Either::Left(ref l) => Either::Left(l),
            Either::Right(ref r) => Either::Right(r),
        }
    }

    #[inline]
    pub fn as_mut(&mut self) -> Either<&mut L, &mut R> {
        match *self {
            Either::Left(ref mut l) => Either::Left(l),
            Either::Right(ref mut r) => Either::Right(r),
        }
    }

    #[inline]
    pub fn left(self) -> Option<L> {
        match self {
            Either::Left(l) => Some(l),
            _ => None,
        }
    }

    #[inline]
    pub fn right(self) -> Option<R> {
        match self {
            Either::Right(r) => Some(r),
            _ => None,
        }
    }
}

/// A simple ring queue with fixed capacity.
pub struct RingQueue<T> {
    buf: VecDeque<T>,
    cap: usize,
}

impl<T> RingQueue<T> {
    #[inline]
    fn len(&self) -> usize {
        self.buf.len()
    }

    pub fn with_capacity(cap: usize) -> RingQueue<T> {
        RingQueue {
            buf: VecDeque::with_capacity(cap),
            cap,
        }
    }

    pub fn push(&mut self, t: T) {
        if self.len() == self.cap {
            self.buf.pop_front();
        }
        self.buf.push_back(t);
    }

    pub fn iter(&self) -> Iter<'_, T> {
        self.buf.iter()
    }

    pub fn swap_remove_front<F>(&mut self, f: F) -> Option<T>
    where
        F: FnMut(&T) -> bool,
    {
        if let Some(pos) = self.buf.iter().position(f) {
            self.buf.swap_remove_front(pos)
        } else {
            None
        }
    }
}

#[inline]
pub fn is_even(n: usize) -> bool {
    n & 1 == 0
}

pub struct MustConsumeVec<T> {
    tag: &'static str,
    v: Vec<T>,
}

impl<T> MustConsumeVec<T> {
    #[inline]
    pub fn new(tag: &'static str) -> MustConsumeVec<T> {
        MustConsumeVec::with_capacity(tag, 0)
    }

    #[inline]
    pub fn with_capacity(tag: &'static str, cap: usize) -> MustConsumeVec<T> {
        MustConsumeVec {
            tag,
            v: Vec::with_capacity(cap),
        }
    }
}

impl<T> Deref for MustConsumeVec<T> {
    type Target = Vec<T>;

    fn deref(&self) -> &Vec<T> {
        &self.v
    }
}

impl<T> DerefMut for MustConsumeVec<T> {
    fn deref_mut(&mut self) -> &mut Vec<T> {
        &mut self.v
    }
}

impl<T> Drop for MustConsumeVec<T> {
    fn drop(&mut self) {
        if !self.is_empty() {
            safe_panic!("resource leak detected: {}.", self.tag);
        }
    }
}

/// Exit the whole process when panic.
pub fn set_panic_hook(panic_abort: bool, data_dir: &str) {
    use std::panic;
    use std::process;

    // HACK! New a backtrace ahead for caching necessary elf sections of this
    // tikv-server, in case it can not open more files during panicking
    // which leads to no stack info (0x5648bdfe4ff2 - <no info>).
    //
    // Crate backtrace caches debug info in a static variable `STATE`,
    // and the `STATE` lives forever once it has been created.
    // See more: https://github.com/alexcrichton/backtrace-rs/blob/\
    //           597ad44b131132f17ed76bf94ac489274dd16c7f/\
    //           src/symbolize/libbacktrace.rs#L126-L159
    // Caching is slow, spawn it in another thread to speed up.
    thread::Builder::new()
        .name(thd_name!("backtrace-loader"))
        .spawn(::backtrace::Backtrace::new)
        .unwrap();

    let data_dir = data_dir.to_string();
    let orig_hook = panic::take_hook();
    panic::set_hook(Box::new(move |info: &panic::PanicInfo<'_>| {
        use slog::Drain;
        if slog_global::borrow_global().is_enabled(::slog::Level::Error) {
            let msg = match info.payload().downcast_ref::<&'static str>() {
                Some(s) => *s,
                None => match info.payload().downcast_ref::<String>() {
                    Some(s) => &s[..],
                    None => "Box<Any>",
                },
            };
            let thread = thread::current();
            let name = thread.name().unwrap_or("<unnamed>");
            let loc = info
                .location()
                .map(|l| format!("{}:{}", l.file(), l.line()));
            let bt = backtrace::Backtrace::new();
            crit!("{}", msg;
                "thread_name" => name,
                "location" => loc.unwrap_or_else(|| "<unknown>".to_owned()),
                "backtrace" => format_args!("{:?}", bt),
            );
        } else {
            orig_hook(info);
        }

        // There might be remaining logs in the async logger.
        // To collect remaining logs and also collect future logs, replace the old one with a
        // terminal logger.
        if let Some(level) = ::log::max_level().to_level() {
            let drainer = logger::text_format(logger::term_writer());
            let _ = logger::init_log(
                drainer,
                logger::convert_log_level_to_slog_level(level),
                false, // Use sync logger to avoid an unnecessary log thread.
                false, // It is initialized already.
                vec![],
                0,
            );
        }

        // If PANIC_MARK is true, create panic mark file.
        if panic_mark_is_on() {
            create_panic_mark_file(data_dir.clone());
        }

        if panic_abort {
            process::abort();
        } else {
            unsafe {
                // Calling process::exit would trigger global static to destroy, like C++
                // static variables of RocksDB, which may cause other threads encounter
                // pure virtual method call. So calling libc::_exit() instead to skip the
                // cleanup process.
                libc::_exit(1);
            }
        }
    }))
}

/// Checks environment variables that affect TiKV.
pub fn check_environment_variables() {
    if cfg!(unix) && env::var("TZ").is_err() {
        env::set_var("TZ", ":/etc/localtime");
        warn!("environment variable `TZ` is missing, using `/etc/localtime`");
    }

    if let Ok(var) = env::var("GRPC_POLL_STRATEGY") {
        info!(
            "environment variable is present";
            "GRPC_POLL_STRATEGY" => var
        );
    }

    for proxy in &["http_proxy", "https_proxy"] {
        if let Ok(var) = env::var(proxy) {
            info!("environment variable is present";
                *proxy => var
            );
        }
    }
}

#[inline]
pub fn is_zero_duration(d: &Duration) -> bool {
    d.as_secs() == 0 && d.subsec_nanos() == 0
}

pub fn empty_shared_slice<T>() -> Arc<[T]> {
    Vec::new().into()
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::rc::Rc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::*;

    use tempfile::Builder;

    #[test]
    fn test_panic_mark_file_path() {
        let dir = Builder::new()
            .prefix("test_panic_mark_file_path")
            .tempdir()
            .unwrap();
        let panic_mark_file = panic_mark_file_path(dir.path());
        assert_eq!(panic_mark_file, dir.path().join(PANIC_MARK_FILE))
    }

    #[test]
    fn test_panic_mark_file_exists() {
        let dir = Builder::new()
            .prefix("test_panic_mark_file_exists")
            .tempdir()
            .unwrap();
        create_panic_mark_file(dir.path());
        assert!(panic_mark_file_exists(dir.path()));
    }

    #[test]
    fn test_fixed_ring_queue() {
        let mut queue = RingQueue::with_capacity(10);
        for num in 0..20 {
            queue.push(num);
            assert_eq!(queue.len(), cmp::min(num + 1, 10));
        }
        assert_eq!(None, queue.swap_remove_front(|i| *i == 20));
        for i in 0..6 {
            assert_eq!(Some(12 + i), queue.swap_remove_front(|e| *e == 12 + i));
            assert_eq!(queue.len(), 9 - i);
        }

        let left: Vec<_> = queue.iter().cloned().collect();
        assert_eq!(vec![10, 11, 18, 19], left);
        for _ in 0..4 {
            queue.swap_remove_front(|_| true).unwrap();
        }
        assert_eq!(None, queue.swap_remove_front(|_| true));
    }

    #[test]
    fn test_defer() {
        let should_panic = Rc::new(AtomicBool::new(true));
        let sp = Rc::clone(&should_panic);
        defer!(assert!(!sp.load(Ordering::SeqCst)));
        should_panic.store(false, Ordering::SeqCst);
    }

    #[test]
    fn test_rwlock_deadlock() {
        // If the test runs over 60s, then there is a deadlock.
        let mu = RwLock::new(Some(1));
        {
            let _clone = foo(&mu.rl());
            let mut data = mu.wl();
            assert!(data.is_some());
            *data = None;
        }

        {
            match foo(&mu.rl()) {
                Some(_) | None => {
                    let res = mu.try_write();
                    assert!(res.is_err());
                }
            }
        }

        fn foo(a: &Option<usize>) -> Option<usize> {
            *a
        }
    }

    #[test]
    fn test_slices_vec_deque() {
        for first in 0..10 {
            let mut v = VecDeque::with_capacity(10);
            for i in 0..first {
                v.push_back(i);
            }
            for i in first..10 {
                v.push_back(i - first);
            }
            v.drain(..first);
            for i in 0..first {
                v.push_back(10 + i - first);
            }
            for len in 0..10 {
                for low in 0..=len {
                    for high in low..=len {
                        let (p1, p2) = super::slices_in_range(&v, low, high);
                        let mut res = vec![];
                        res.extend_from_slice(p1);
                        res.extend_from_slice(p2);
                        let exp: Vec<_> = (low..high).collect();
                        assert_eq!(
                            res, exp,
                            "[{}, {}) in {:?} with first: {}",
                            low, high, v, first
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn test_must_consume_vec() {
        let mut v = MustConsumeVec::new("test");
        v.push(2);
        v.push(3);
        assert_eq!(v.len(), 2);
        v.drain(..);
    }

    #[test]
    fn test_resource_leak() {
        let res = panic_hook::recover_safe(|| {
            let mut v = MustConsumeVec::new("test");
            v.push(2);
        });
        res.unwrap_err();
    }

    #[test]
    fn test_unescape() {
        // No escapes
        assert_eq!(unescape(r"ab"), b"ab");
        // Escaped backslash
        assert_eq!(unescape(r"a\\023"), b"a\\023");
        // Escaped three digit octal
        assert_eq!(unescape(r"a\000"), b"a\0");
        assert_eq!(unescape(r"\342\235\244\360\237\220\267"), "❤🐷".as_bytes());
        // Whitespace
        assert_eq!(unescape("a\\r\\n\\t '\\\"\\\\"), b"a\r\n\t '\"\\");
        // Hex Octals
        assert_eq!(unescape(r"abc\x64\x65\x66ghi"), b"abcdefghi");
        assert_eq!(unescape(r"JKL\x4d\x4E\x4fPQR"), b"JKLMNOPQR");
    }

    #[test]
    fn test_is_zero() {
        assert!(is_zero_duration(&Duration::new(0, 0)));
        assert!(!is_zero_duration(&Duration::new(1, 0)));
        assert!(!is_zero_duration(&Duration::new(0, 1)));
    }

    #[test]
    fn test_must_consume_vec_dtor_not_abort() {
        let res = panic_hook::recover_safe(|| {
            let mut v = MustConsumeVec::new("test");
            v.push(2);
            panic!("Panic with MustConsumeVec non-empty");
            // It would abort if there was a double-panic in dtor, thus
            // the test would fail.
        });
        res.unwrap_err();
    }
}
