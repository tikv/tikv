// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.
use std::{
    fs::File,
    io::{Read, Write},
    pin::Pin,
    process::{Command, Stdio},
    sync::Mutex,
};

use futures::{
    future::BoxFuture,
    task::{Context, Poll},
    Future, FutureExt,
};
use lazy_static::lazy_static;
use pprof::protos::Message;
use regex::Regex;
use tempfile::NamedTempFile;
#[cfg(not(test))]
use tikv_alloc::dump_prof;

#[cfg(test)]
use self::test_utils::dump_prof;
#[cfg(test)]
pub use self::test_utils::TEST_PROFILE_MUTEX;

lazy_static! {
    // If it's some it means there are already a CPU profiling.
    static ref CPU_PROFILE_ACTIVE: Mutex<Option<()>> = Mutex::new(None);

    // To normalize thread names.
    static ref THREAD_NAME_RE: Regex =
        Regex::new(r"^(?P<thread_name>[a-z-_ :]+?)(-?\d)*$").unwrap();
    static ref THREAD_NAME_REPLACE_SEPERATOR_RE: Regex = Regex::new(r"[_ ]").unwrap();
}

type OnEndFn<I, T> = Box<dyn FnOnce(I) -> Result<T, String> + Send + 'static>;

struct ProfileRunner<I, T> {
    item: Option<I>,
    on_end: Option<OnEndFn<I, T>>,
    end: BoxFuture<'static, Result<(), String>>,
}

impl<I, T> Unpin for ProfileRunner<I, T> {}

impl<I, T> ProfileRunner<I, T> {
    fn new<F1, F2>(
        on_start: F1,
        on_end: F2,
        end: BoxFuture<'static, Result<(), String>>,
    ) -> Result<Self, String>
    where
        F1: FnOnce() -> Result<I, String>,
        F2: FnOnce(I) -> Result<T, String> + Send + 'static,
    {
        let item = on_start()?;
        Ok(ProfileRunner {
            item: Some(item),
            on_end: Some(Box::new(on_end) as OnEndFn<I, T>),
            end,
        })
    }
}

impl<I, T> Future for ProfileRunner<I, T> {
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

/// Trigger a heap profile and return the content.
pub fn dump_one_heap_profile() -> Result<NamedTempFile, String> {
    let f = NamedTempFile::new().map_err(|e| format!("create tmp file fail: {}", e))?;
    let path = f.path();
    dump_prof(path.to_str().unwrap()).map_err(|e| format!("dump_prof: {}", e))?;
    Ok(f)
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
    if CPU_PROFILE_ACTIVE.lock().unwrap().is_some() {
        return Err("Already in CPU Profiling".to_owned());
    }

    let on_start = || {
        let mut activate = CPU_PROFILE_ACTIVE.lock().unwrap();
        assert!(activate.is_none());
        *activate = Some(());
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
        drop(guard);
        *CPU_PROFILE_ACTIVE.lock().unwrap() = None;

        Ok(body)
    };

    ProfileRunner::new(on_start, on_end, end.boxed())?.await
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
    let bin = std::env::current_exe().map_err(|e| format!("get current exe path fail: {}", e))?;
    let mut jeprof = Command::new("perl")
        .args([
            "/dev/stdin",
            "--show_bytes",
            &bin.as_os_str().to_string_lossy(),
            path,
            "--svg",
        ])
        .stdin(Stdio::piped())
        .spawn()
        .map_err(|e| format!("spawn jeprof fail: {}", e))?;
    jeprof
        .stdin
        .take()
        .unwrap()
        .write_all(include_bytes!("jeprof.in"))
        .unwrap();
    let output = jeprof
        .wait_with_output()
        .map_err(|e| format!("jeprof: {}", e))?;
    if !output.status.success() {
        let stderr = std::str::from_utf8(&output.stderr).unwrap_or("invalid utf8");
        return Err(format!("jeprof stderr: {:?}", stderr));
    }
    Ok(output.stdout)
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

// Re-define some heap profiling functions because heap-profiling is not enabled
// for tests.
#[cfg(test)]
mod test_utils {
    use std::sync::Mutex;

    use tikv_alloc::error::ProfResult;

    lazy_static! {
        pub static ref TEST_PROFILE_MUTEX: Mutex<()> = Mutex::new(());
    }

    pub fn dump_prof(_: &str) -> ProfResult<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use futures::executor::block_on;
    use tokio::runtime;

    use super::*;

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
    fn test_profile_guard_concurrency() {
        use std::{thread, time::Duration};

        use futures::{channel::oneshot, TryFutureExt};

        let _test_guard = TEST_PROFILE_MUTEX.lock().unwrap();
        let rt = runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .build()
            .unwrap();

        let expected = "Already in CPU Profiling";

        let (tx1, rx1) = oneshot::channel();
        let rx1 = rx1.map_err(|_| "channel canceled".to_owned());
        let res1 = rt.spawn(start_one_cpu_profile(rx1, 99, false));
        thread::sleep(Duration::from_millis(100));

        let (_tx2, rx2) = oneshot::channel();
        let rx2 = rx2.map_err(|_| "channel canceled".to_owned());
        let res2 = rt.spawn(start_one_cpu_profile(rx2, 99, false));
        assert_eq!(block_on(res2).unwrap().unwrap_err(), expected);

        drop(tx1);
        block_on(res1).unwrap().unwrap_err();
    }
}
