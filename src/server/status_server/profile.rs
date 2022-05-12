// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.
use std::{
    fs::{File, Metadata},
    io::Read,
    path::PathBuf,
    pin::Pin,
    process::Command,
    sync::Mutex as StdMutex,
    time::{Duration, UNIX_EPOCH},
};

use chrono::{offset::Local, DateTime};
use futures::{
    channel::oneshot::{self, Sender},
    future::BoxFuture,
    select,
    task::{Context, Poll},
    Future, FutureExt, Stream, StreamExt,
};
use lazy_static::lazy_static;
#[cfg(target_arch = "x86_64")]
use pprof::protos::Message;
use regex::Regex;
use tempfile::{NamedTempFile, TempDir};
#[cfg(not(test))]
use tikv_alloc::{activate_prof, deactivate_prof, dump_prof};
use tokio::sync::{Mutex, MutexGuard};

#[cfg(test)]
pub use self::test_utils::TEST_PROFILE_MUTEX;
#[cfg(test)]
use self::test_utils::{activate_prof, deactivate_prof, dump_prof};

// File name suffix for periodically dumped heap profiles.
const HEAP_PROFILE_SUFFIX: &str = ".heap";

lazy_static! {
    // If it's locked it means there are already a heap or CPU profiling.
    static ref PROFILE_MUTEX: Mutex<()> = Mutex::new(());
    // The channel is used to deactivate a profiling.
    static ref PROFILE_ACTIVE: StdMutex<Option<(Sender<()>, TempDir)>> = StdMutex::new(None);

    // To normalize thread names.
    static ref THREAD_NAME_RE: Regex =
        Regex::new(r"^(?P<thread_name>[a-z-_ :]+?)(-?\d)*$").unwrap();
    static ref THREAD_NAME_REPLACE_SEPERATOR_RE: Regex = Regex::new(r"[_ ]").unwrap();
}

type OnEndFn<I, T> = Box<dyn FnOnce(I) -> Result<T, String> + Send + 'static>;

struct ProfileGuard<'a, I, T> {
    _guard: MutexGuard<'a, ()>,
    item: Option<I>,
    on_end: Option<OnEndFn<I, T>>,
    end: BoxFuture<'static, Result<(), String>>,
}

impl<'a, I, T> Unpin for ProfileGuard<'a, I, T> {}

impl<'a, I, T> ProfileGuard<'a, I, T> {
    fn new<F1, F2>(
        on_start: F1,
        on_end: F2,
        end: BoxFuture<'static, Result<(), String>>,
    ) -> Result<ProfileGuard<'a, I, T>, String>
    where
        F1: FnOnce() -> Result<I, String>,
        F2: FnOnce(I) -> Result<T, String> + Send + 'static,
    {
        let _guard = match PROFILE_MUTEX.try_lock() {
            Ok(guard) => guard,
            _ => return Err("Already in Profiling".to_owned()),
        };
        let item = on_start()?;
        Ok(ProfileGuard {
            _guard,
            item: Some(item),
            on_end: Some(Box::new(on_end) as OnEndFn<I, T>),
            end,
        })
    }
}

impl<'a, I, T> Future for ProfileGuard<'a, I, T> {
    type Output = Result<T, String>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.end.as_mut().poll(cx) {
            Poll::Ready(res) => {
                let item = self.item.take().unwrap();
                let on_end = self.on_end.take().unwrap();
                let r = match (res, on_end(item)) {
                    (Ok(_), r) => r,
                    (Err(errmsg), _) => Err(errmsg),
                };
                Poll::Ready(r)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

/// Trigger a heap profie and return the content.
#[allow(dead_code)]
pub async fn start_one_heap_profile<F>(end: F, use_jeprof: bool) -> Result<Vec<u8>, String>
where
    F: Future<Output = Result<(), String>> + Send + 'static,
{
    let on_start = || activate_prof().map_err(|e| format!("activate_prof: {}", e));

    let on_end = move |_| {
        deactivate_prof().map_err(|e| format!("deactivate_prof: {}", e))?;
        let f = NamedTempFile::new().map_err(|e| format!("create tmp file fail: {}", e))?;
        let path = f.path().to_str().unwrap();
        dump_prof(path).map_err(|e| format!("dump_prof: {}", e))?;
        if use_jeprof {
            jeprof_heap_profile(path)
        } else {
            read_file(path)
        }
    };

    ProfileGuard::new(on_start, on_end, end.boxed())?.await
}

/// Activate heap profile and call `callback` if successfully.
/// `deactivate_heap_profile` can only be called after it's notified from `callback`.
pub async fn activate_heap_profile<S, F>(
    dump_period: S,
    store_path: PathBuf,
    callback: F,
) -> Result<(), String>
where
    S: Stream<Item = Result<(), String>> + Send + Unpin + 'static,
    F: FnOnce() + Send + 'static,
{
    let (tx, rx) = oneshot::channel();
    let dir = tempfile::Builder::new()
        .prefix("heap-")
        .tempdir_in(store_path)
        .map_err(|e| format!("create temp directory: {}", e))?;
    let dir_path = dir.path().to_str().unwrap().to_owned();

    let on_start = move || {
        let mut activate = PROFILE_ACTIVE.lock().unwrap();
        assert!(activate.is_none());
        activate_prof().map_err(|e| format!("activate_prof: {}", e))?;
        *activate = Some((tx, dir));
        callback();
        info!("periodical heap profiling is started");
        Ok(())
    };

    let on_end = |_| {
        deactivate_heap_profile();
        deactivate_prof().map_err(|e| format!("deactivate_prof: {}", e))
    };

    let end = async move {
        select! {
            _ = rx.fuse() => {
                info!("periodical heap profiling is canceled");
                Ok(())
            },
            res = dump_heap_profile_periodically(dump_period, dir_path).fuse() => {
                warn!("the heap profiling dump loop shouldn't break"; "res" => ?res);
                res
            }
        }
    };

    ProfileGuard::new(on_start, on_end, end.boxed())?.await
}

/// Deactivate heap profile. Return `false` if it hasn't been activated.
pub fn deactivate_heap_profile() -> bool {
    let mut activate = PROFILE_ACTIVE.lock().unwrap();
    activate.take().is_some()
}

/// Currently, on aarch64 architectures, the underlying libgcc/llvm-libunwind/... which pprof-rs
/// depends on has a segmentation fault (when backtracking happens in the signal handler).
/// So, for now, we only allow the x86_64 architecture to perform real profiling, other
/// architectures will directly return an error until we fix the seg-fault in backtrace.
#[cfg(not(target_arch = "x86_64"))]
pub async fn start_one_cpu_profile<F>(
    _end: F,
    _frequency: i32,
    _protobuf: bool,
) -> Result<Vec<u8>, String>
where
    F: Future<Output = Result<(), String>> + Send + 'static,
{
    Err("unsupported arch".to_string())
}

/// Trigger one cpu profile.
#[cfg(target_arch = "x86_64")]
pub async fn start_one_cpu_profile<F>(
    end: F,
    frequency: i32,
    protobuf: bool,
) -> Result<Vec<u8>, String>
where
    F: Future<Output = Result<(), String>> + Send + 'static,
{
    let on_start = || {
        let guard = pprof::ProfilerGuardBuilder::default()
            .frequency(frequency)
            .blocklist(&["libc", "libgcc", "pthread", "vdso"])
            .build()
            .map_err(|e| format!("pprof::ProfilerGuardBuilder::build fail: {}", e))?;
        Ok(guard)
    };

    let on_end = move |guard: pprof::ProfilerGuard<'static>| {
        let report = guard
            .report()
            .frames_post_processor(move |frames| {
                let name = extract_thread_name(&frames.thread_name);
                frames.thread_name = name;
            })
            .build()
            .map_err(|e| format!("create cpu profiling report fail: {}", e))?;
        let mut body = Vec::new();
        if protobuf {
            let profile = report
                .pprof()
                .map_err(|e| format!("generate pprof from report fail: {}", e))?;
            profile
                .write_to_vec(&mut body)
                .map_err(|e| format!("encode pprof into bytes fail: {}", e))?;
        } else {
            report
                .flamegraph(&mut body)
                .map_err(|e| format!("generate flamegraph from report fail: {}", e))?;
        }
        Ok(body)
    };

    ProfileGuard::new(on_start, on_end, end.boxed())?.await
}

pub fn read_file(path: &str) -> Result<Vec<u8>, String> {
    let mut f = File::open(path).map_err(|e| format!("open {} fail: {}", path, e))?;
    let mut buf = Vec::new();
    f.read_to_end(&mut buf)
        .map_err(|e| format!("read {} fail: {}", path, e))?;
    Ok(buf)
}

pub fn jeprof_heap_profile(path: &str) -> Result<Vec<u8>, String> {
    info!("using jeprof to process {}", path);
    let output = Command::new("./jeprof")
        .args(&["--show_bytes", "./bin/tikv-server", path, "--svg"])
        .output()
        .map_err(|e| format!("jeprof: {}", e))?;
    if !output.status.success() {
        let stderr = std::str::from_utf8(&output.stderr).unwrap_or("invalid utf8");
        return Err(format!("jeprof stderr: {:?}", stderr));
    }
    Ok(output.stdout)
}

pub fn list_heap_profiles() -> Result<Vec<(String, String)>, String> {
    let path = match &*PROFILE_ACTIVE.lock().unwrap() {
        Some((_, ref dir)) => dir.path().to_str().unwrap().to_owned(),
        None => return Ok(vec![]),
    };

    let dir = std::fs::read_dir(&path).map_err(|e| format!("read dir fail: {}", e))?;
    let mut profiles = Vec::new();
    for item in dir {
        let item = match item {
            Ok(x) => x,
            _ => continue,
        };
        let f = item.path().to_str().unwrap().to_owned();
        if !f.ends_with(HEAP_PROFILE_SUFFIX) {
            continue;
        }
        let ct = item.metadata().map(|x| last_change_epoch(&x)).unwrap();
        let dt = DateTime::<Local>::from(UNIX_EPOCH + Duration::from_secs(ct));
        profiles.push((f, dt.format("%Y-%m-%d %H:%M:%S").to_string()));
    }

    // Reverse sort them.
    profiles.sort_by(|x, y| y.1.cmp(&x.1));
    info!("list_heap_profiles gets {} items", profiles.len());
    Ok(profiles)
}

async fn dump_heap_profile_periodically<S>(mut period: S, dir: String) -> Result<(), String>
where
    S: Stream<Item = Result<(), String>> + Send + Unpin + 'static,
{
    let mut id = 0;
    while let Some(res) = period.next().await {
        let _ = res?;
        id += 1;
        let path = format!("{}/{:0>6}{}", dir, id, HEAP_PROFILE_SUFFIX);
        dump_prof(&path).map_err(|e| format!("dump_prof: {}", e))?;
        info!("a heap profile is dumped to {}", path);
    }
    Ok(())
}

#[cfg_attr(not(target_arch = "x86_64"), allow(dead_code))]
fn extract_thread_name(thread_name: &str) -> String {
    THREAD_NAME_RE
        .captures(thread_name)
        .and_then(|cap| {
            cap.name("thread_name").map(|thread_name| {
                THREAD_NAME_REPLACE_SEPERATOR_RE
                    .replace_all(thread_name.as_str(), "-")
                    .into_owned()
            })
        })
        .unwrap_or_else(|| thread_name.to_owned())
}

// Re-define some heap profiling functions because heap-profiling is not enabled for tests.
#[cfg(test)]
mod test_utils {
    use std::sync::Mutex;

    use tikv_alloc::error::ProfResult;

    lazy_static! {
        pub static ref TEST_PROFILE_MUTEX: Mutex<()> = Mutex::new(());
    }

    pub fn activate_prof() -> ProfResult<()> {
        Ok(())
    }
    pub fn deactivate_prof() -> ProfResult<()> {
        Ok(())
    }
    pub fn dump_prof(_: &str) -> ProfResult<()> {
        Ok(())
    }
}

#[cfg(unix)]
fn last_change_epoch(metadata: &Metadata) -> u64 {
    use std::os::unix::fs::MetadataExt;
    metadata.ctime() as u64
}

#[cfg(not(unix))]
fn last_change_epoch(metadata: &Metadata) -> u64 {
    0
}

#[cfg(test)]
mod tests {
    use std::sync::mpsc::sync_channel;

    use futures::{channel::mpsc, executor::block_on, SinkExt};
    use tokio::runtime;

    use super::*;

    #[test]
    fn test_last_change_epoch() {
        let f = tempfile::tempfile().unwrap();
        assert!(last_change_epoch(&f.metadata().unwrap()) > 0);
    }

    #[test]
    fn test_extract_thread_name() {
        assert_eq!(&extract_thread_name("test-name-1"), "test-name");
        assert_eq!(&extract_thread_name("grpc-server-5"), "grpc-server");
        assert_eq!(&extract_thread_name("rocksdb:bg1000"), "rocksdb:bg");
        assert_eq!(&extract_thread_name("raftstore-1-100"), "raftstore");
        assert_eq!(&extract_thread_name("snap sender1000"), "snap-sender");
        assert_eq!(&extract_thread_name("snap_sender1000"), "snap-sender");
    }

    // Test there is at most 1 concurrent profiling.
    #[test]
    #[cfg(target_arch = "x86_64")]
    fn test_profile_guard_concurrency() {
        use std::{thread, time::Duration};

        use futures::{channel::oneshot, TryFutureExt};

        let _test_guard = TEST_PROFILE_MUTEX.lock().unwrap();
        let rt = runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .build()
            .unwrap();

        let expected = "Already in Profiling";

        let (tx1, rx1) = oneshot::channel();
        let rx1 = rx1.map_err(|_| "channel canceled".to_owned());
        let res1 = rt.spawn(start_one_cpu_profile(rx1, 99, false));
        thread::sleep(Duration::from_millis(100));

        let (_tx2, rx2) = oneshot::channel();
        let rx2 = rx2.map_err(|_| "channel canceled".to_owned());
        let res2 = rt.spawn(start_one_cpu_profile(rx2, 99, false));
        assert_eq!(block_on(res2).unwrap().unwrap_err(), expected);

        let (_tx2, rx2) = oneshot::channel();
        let rx2 = rx2.map_err(|_| "channel canceled".to_owned());
        let res2 = rt.spawn(start_one_heap_profile(rx2, false));
        assert_eq!(block_on(res2).unwrap().unwrap_err(), expected);

        let (_tx2, rx2) = mpsc::channel(1);
        let res2 = rt.spawn(activate_heap_profile(rx2, std::env::temp_dir(), || {}));
        assert_eq!(block_on(res2).unwrap().unwrap_err(), expected);

        drop(tx1);
        assert!(block_on(res1).unwrap().is_err());
    }

    #[test]
    fn test_profile_guard_toggle() {
        let _test_guard = TEST_PROFILE_MUTEX.lock().unwrap();
        let rt = runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .build()
            .unwrap();

        // Test activated profiling can be stopped by canceling the period stream.
        let (tx, rx) = mpsc::channel(1);
        let res = rt.spawn(activate_heap_profile(rx, std::env::temp_dir(), || {}));
        drop(tx);
        assert!(block_on(res).unwrap().is_ok());

        // Test activated profiling can be stopped by the handle.
        let (tx, rx) = sync_channel::<i32>(1);
        let on_activated = move || drop(tx);
        let check_activated = move || rx.recv().is_err();

        let (_tx, _rx) = mpsc::channel(1);
        let res = rt.spawn(activate_heap_profile(
            _rx,
            std::env::temp_dir(),
            on_activated,
        ));
        assert!(check_activated());
        assert!(deactivate_heap_profile());
        assert!(block_on(res).unwrap().is_ok());
    }

    #[test]
    fn test_heap_profile_exit() {
        let _test_guard = TEST_PROFILE_MUTEX.lock().unwrap();
        let rt = runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .build()
            .unwrap();

        // Test heap profiling can be stopped by sending an error.
        let (mut tx, rx) = mpsc::channel(1);
        let res = rt.spawn(activate_heap_profile(rx, std::env::temp_dir(), || {}));
        block_on(tx.send(Err("test".to_string()))).unwrap();
        assert!(block_on(res).unwrap().is_err());

        // Test heap profiling can be activated again.
        let (tx, rx) = sync_channel::<i32>(1);
        let on_activated = move || drop(tx);
        let check_activated = move || rx.recv().is_err();

        let (_tx, _rx) = mpsc::channel(1);
        let res = rt.spawn(activate_heap_profile(
            _rx,
            std::env::temp_dir(),
            on_activated,
        ));
        assert!(check_activated());
        assert!(deactivate_heap_profile());
        assert!(block_on(res).unwrap().is_ok());
    }
}
