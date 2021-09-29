// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::fs::File;
use std::io::Read;
use std::pin::Pin;
use std::process::Command;
use std::sync::Mutex as StdMutex;

use futures::channel::oneshot::{self, Sender};
use futures::future::BoxFuture;
use futures::task::{Context, Poll};
use futures::{select, Future, FutureExt, Stream, StreamExt};
use lazy_static::lazy_static;
use pprof::protos::Message;
use regex::Regex;
use tempfile::NamedTempFile;
use tikv_alloc::{activate_prof, deactivate_prof, dump_prof};
use tokio::sync::{Mutex, MutexGuard};

// File name suffix for periodically dumped heap profiles.
const HEAP_PROFILE_SUFFIX: &str = ".heap";

lazy_static! {
    // If it's locked it means there are already a heap or CPU profiling.
    static ref PROFILE_MUTEX: Mutex<()> = Mutex::new(());
    // The channel is used to deactivate a profiling.
    static ref PROFILE_ACTIVE: StdMutex<Option<Sender<()>>> = StdMutex::new(None);

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
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        match self.end.as_mut().poll(cx) {
            Poll::Ready(Ok(_)) => {
                let item = self.item.take().unwrap();
                let on_end = self.on_end.take().unwrap();
                Poll::Ready(on_end(item))
            }
            Poll::Ready(Err(errmsg)) => Poll::Ready(Err(errmsg)),
            Poll::Pending => Poll::Pending,
        }
    }
}

/// Trigger a heap profie and return the content.
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

/// Activate heap profile.
pub async fn activate_heap_profile<S, F>(dump_period: S, callback: F) -> Result<(), String>
where
    S: Stream<Item = Result<(), String>> + Send + Unpin + 'static,
    F: FnOnce() + Send + 'static,
{
    let (tx, rx) = oneshot::channel();

    let on_start = move || {
        let mut activate = PROFILE_ACTIVE.lock().unwrap();
        assert!(activate.is_none());
        *activate = Some(tx);
        activate_prof().map_err(|e| format!("activate_prof: {}", e))?;
        callback();
        Ok(())
    };

    let on_end = |_| deactivate_prof().map_err(|e| format!("deactivate_prof: {}", e));

    let end = async move {
        select! {
            _ = rx.fuse() => {},
            _ = dump_heap_profile_periodically(dump_period).fuse() => {
                warn!("the heap profiling dump loop shouldn't break");
            }
        }
        Ok(())
    };

    ProfileGuard::new(on_start, on_end, end.boxed())?.await
}

/// Deactivate heap profile. Return `false` if it hasn't been activated.
pub fn deactivate_heap_profile() -> bool {
    let mut activate = PROFILE_ACTIVE.lock().unwrap();
    activate.take().is_some()
}

/// Trigger one cpu profile.
pub async fn start_one_cpu_profile<F>(
    end: F,
    frequency: i32,
    protobuf: bool,
) -> Result<Vec<u8>, String>
where
    F: Future<Output = Result<(), String>> + Send + 'static,
{
    let on_start = || {
        let guard = pprof::ProfilerGuard::new(frequency)
            .map_err(|e| format!("pprof::ProfileGuard::new fail: {}", e))?;
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
                .encode(&mut body)
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
    let output = Command::new("./jeprof")
        .args(&["--show_bytes", "./bin/tikv-server", path, "--svg"])
        .output()
        .map_err(|e| format!("jeprof: {}", e))?;
    if !output.status.success() {
        let msg = format!("jeprof stderr: {:?}", std::str::from_utf8(&output.stderr));
        return Err(msg);
    }
    Ok(output.stdout)
}

pub fn list_heap_profiles() -> Result<Vec<u8>, String> {
    let dir = std::fs::read_dir(".").map_err(|e| format!("read dir fail: {}", e))?;
    let mut profiles = Vec::new();
    for item in dir {
        let item = match item {
            Ok(x) => x,
            _ => continue,
        };
        let path = item.path();
        if !path.ends_with(HEAP_PROFILE_SUFFIX) {
            continue;
        }
        let f = path.to_str().unwrap().to_owned();
        let ct = item.metadata().and_then(|x| x.created()).unwrap();
        profiles.push((f, format!("{:?}", ct)));
    }

    profiles.sort_by(|x, y| x.1.cmp(&y.1));
    let text = profiles
        .into_iter()
        .map(|(f, ct)| format!("{}\t\t{}", f, ct))
        .collect::<Vec<_>>()
        .join("\n")
        .into_bytes();
    Ok(text)
}

async fn dump_heap_profile_periodically<S>(mut period: S) -> Result<(), String>
where
    S: Stream<Item = Result<(), String>> + Send + Unpin + 'static,
{
    let mut id = 0;
    while let Some(Ok(_)) = period.next().await {
        id += 1;
        let path = format!("{:0>6}{}", id, HEAP_PROFILE_SUFFIX);
        dump_prof(&path).map_err(|e| format!("dump_prof: {}", e))?;
    }
    Ok(())
}

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

#[cfg(test)]
mod tests {
    use super::*;
    use futures::channel::{mpsc, oneshot};
    use futures::executor::block_on;
    use futures::TryFutureExt;
    use std::thread;
    use std::time::Duration;
    use tokio::runtime;

    #[test]
    fn test_extract_thread_name() {
        assert_eq!(&extract_thread_name("test-name-1"), "test-name");
        assert_eq!(&extract_thread_name("grpc-server-5"), "grpc-server");
        assert_eq!(&extract_thread_name("rocksdb:bg1000"), "rocksdb:bg");
        assert_eq!(&extract_thread_name("raftstore-1-100"), "raftstore");
        assert_eq!(&extract_thread_name("snap sender1000"), "snap-sender");
        assert_eq!(&extract_thread_name("snap_sender1000"), "snap-sender");
    }

    // Test behaviors of `super::ProfileGuard`. It's impossible to split it into multiple
    // cases because `PROFILE_MUTEX` is a global lock, which can't be shared fairly in multiple
    // functions.
    #[test]
    fn test_profile_guard() {
        let rt = runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .build()
            .unwrap();

        // Test there is at most 1 concurrent profiling.
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
        let res2 = rt.spawn(activate_heap_profile(rx2, || {}));
        assert_eq!(block_on(res2).unwrap().unwrap_err(), expected);

        drop(tx1);
        assert!(block_on(res1).unwrap().is_err());

        // TODO: Add test cases for toggling heap profile.
    }
}
